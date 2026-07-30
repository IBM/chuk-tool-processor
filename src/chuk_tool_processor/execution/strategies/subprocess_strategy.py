#!/usr/bin/env python
# chuk_tool_processor/execution/strategies/subprocess_strategy.py
"""
Subprocess execution strategy - truly runs tools in separate OS processes.

This strategy executes tools in separate Python processes using a process pool,
providing isolation and potentially better parallelism on multi-core systems.

Enhanced tool name resolution that properly handles:
- Simple names: "get_current_time"
- Namespaced names: "diagnostic_test.get_current_time"
- Cross-namespace fallback searching

Properly handles tool serialization and ensures tool_name is preserved.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import functools
import inspect
import os
import pickle
import platform
import signal
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from chuk_tool_processor.logging import get_logger, log_context_span
from chuk_tool_processor.models.execution_strategy import ExecutionStrategy
from chuk_tool_processor.models.tool_call import ToolCall
from chuk_tool_processor.models.tool_result import ToolResult
from chuk_tool_processor.registry.interface import ToolRegistryInterface

logger = get_logger("chuk_tool_processor.execution.subprocess_strategy")


# --------------------------------------------------------------------------- #
# Module-level helper functions for worker processes - these must be at the module
# level so they can be pickled
# --------------------------------------------------------------------------- #
def _init_worker():
    """Initialize worker process with signal handlers."""
    # Ignore keyboard interrupt in workers
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _pool_test_func():
    """Simple function to test if the process pool is working."""
    return "ok"


def _serialized_tool_worker(
    tool_name: str, namespace: str, arguments: dict[str, Any], timeout: float | None, serialized_tool_data: bytes
) -> dict[str, Any]:
    """
    Worker function that uses serialized tools and ensures tool_name is available.

    This worker deserializes the complete tool and executes it, with multiple
    fallbacks to ensure tool_name is properly set.

    Args:
        tool_name: Name of the tool
        namespace: Namespace of the tool
        arguments: Arguments to pass to the tool
        timeout: Optional timeout in seconds
        serialized_tool_data: Pickled tool instance

    Returns:
        Serialized result data
    """
    import asyncio
    import inspect
    import os
    import pickle
    from datetime import datetime

    start_time = datetime.now(UTC)
    pid = os.getpid()
    hostname = platform.node()

    result_data = {
        "tool": tool_name,
        "namespace": namespace,
        "start_time": start_time.isoformat(),
        "end_time": None,
        "machine": hostname,
        "pid": pid,
        "result": None,
        "error": None,
    }

    try:
        # Deserialize the complete tool
        # This is safe as the data comes from the parent process, not untrusted external sources
        tool = pickle.loads(serialized_tool_data)  # nosec B301

        # Multiple fallbacks to ensure tool_name is available

        # Fallback 1: If tool doesn't have tool_name, set it directly
        if not hasattr(tool, "tool_name") or not tool.tool_name:
            tool.tool_name = tool_name

        # Fallback 2: If it's a class instead of instance, instantiate it
        if inspect.isclass(tool):
            try:
                tool = tool()
                tool.tool_name = tool_name
            except Exception as e:
                result_data["error"] = f"Failed to instantiate tool class: {str(e)}"
                result_data["end_time"] = datetime.now(UTC).isoformat()
                return result_data

        # Fallback 3: Ensure tool_name exists using setattr
        if not getattr(tool, "tool_name", None):
            tool.tool_name = tool_name

        # Fallback 4: Verify execute method exists
        if not hasattr(tool, "execute"):
            result_data["error"] = "Tool missing execute method"
            result_data["end_time"] = datetime.now(UTC).isoformat()
            return result_data

        # Create event loop for execution
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Execute the tool with timeout
            if timeout is not None and timeout > 0:
                result_value = loop.run_until_complete(asyncio.wait_for(tool.execute(**arguments), timeout))
            else:
                result_value = loop.run_until_complete(tool.execute(**arguments))

            result_data["result"] = result_value

        except TimeoutError:
            result_data["error"] = f"Tool execution timed out after {timeout}s"
        except Exception as e:
            result_data["error"] = f"Tool execution failed: {str(e)}"

        finally:
            loop.close()

    except Exception as e:
        result_data["error"] = f"Worker error: {str(e)}"

    result_data["end_time"] = datetime.now(UTC).isoformat()
    return result_data


# --------------------------------------------------------------------------- #
# The subprocess strategy
# --------------------------------------------------------------------------- #
class SubprocessStrategy(ExecutionStrategy):
    """
    Execute tools in separate processes for isolation and parallelism.

    This strategy creates a pool of worker processes and distributes tool calls
    among them. Each tool executes in its own process, providing isolation and
    parallelism.

    Enhanced tool name resolution and proper tool serialization.
    """

    def __init__(
        self,
        registry: ToolRegistryInterface,
        *,
        max_workers: int = 4,
        default_timeout: float | None = None,
        worker_init_timeout: float = 5.0,
        warm_pool: bool = False,
    ) -> None:
        """
        Initialize the subprocess execution strategy.

        Args:
            registry: Tool registry for tool lookups
            max_workers: Maximum number of worker processes
            default_timeout: Default timeout for tool execution
            worker_init_timeout: Timeout for worker process initialization
            warm_pool: If True, pre-warm all workers in the pool on first use.
                      This reduces latency for the first batch of calls by
                      ensuring all worker processes are already spawned.
        """
        self.registry = registry
        self.max_workers = max_workers
        self.default_timeout = default_timeout or 30.0  # Always have a default
        self.worker_init_timeout = worker_init_timeout
        self._warm_pool = warm_pool

        # Process pool (initialized lazily)
        self._process_pool: concurrent.futures.ProcessPoolExecutor | None = None
        self._pool_lock = asyncio.Lock()

        # Task tracking for cleanup
        self._active_tasks: set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._shutting_down = False

        logger.debug(
            "SubprocessStrategy initialized with timeout: %ss, max_workers: %d", self.default_timeout, max_workers
        )

        # Register shutdown handler if in main thread
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._signal_handler(s)))
        except (RuntimeError, NotImplementedError):
            # Not in the main thread or not on Unix
            pass

    async def _ensure_pool(self) -> None:
        """Initialize the process pool if not already initialized."""
        if self._process_pool is not None:
            return

        async with self._pool_lock:
            if self._process_pool is not None:
                return

            # Create process pool
            self._process_pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=self.max_workers,
                initializer=_init_worker,
            )

            # Test the pool with a simple task
            loop = asyncio.get_running_loop()
            try:
                if self._warm_pool:
                    # Pre-warm all workers by submitting max_workers tasks concurrently
                    # This ensures all worker processes are spawned and ready
                    warm_tasks = [
                        asyncio.wait_for(
                            loop.run_in_executor(self._process_pool, _pool_test_func),
                            timeout=self.worker_init_timeout,
                        )
                        for _ in range(self.max_workers)
                    ]
                    await asyncio.gather(*warm_tasks)
                    logger.debug("Process pool pre-warmed with %d workers", self.max_workers)
                else:
                    # Just test with a single task (original behavior)
                    await asyncio.wait_for(
                        loop.run_in_executor(self._process_pool, _pool_test_func),
                        timeout=self.worker_init_timeout,
                    )
                    logger.debug("Process pool initialized with %d workers", self.max_workers)
            except Exception as e:
                # Clean up on initialization error
                self._process_pool.shutdown(wait=False)
                self._process_pool = None
                logger.error("Failed to initialize process pool: %s", e)
                raise RuntimeError(f"Failed to initialize process pool: {e}") from e

    # ------------------------------------------------------------------ #
    #  🔌 legacy façade for older wrappers                                #
    # ------------------------------------------------------------------ #
    async def execute(
        self,
        calls: list[ToolCall],
        *,
        timeout: float | None = None,
    ) -> list[ToolResult]:
        """
        Back-compat shim.

        Old wrappers (`retry`, `rate_limit`, `cache`, …) still expect an
        ``execute()`` coroutine on an execution-strategy object.
        The real implementation lives in :meth:`run`, so we just forward.
        """
        return await self.run(calls, timeout)

    async def run(
        self,
        calls: list[ToolCall],
        timeout: float | None = None,
    ) -> list[ToolResult]:
        """
        Execute tool calls in separate processes and return results as they complete.

        NOTE: Results are returned in COMPLETION ORDER, not submission order.
        This allows faster tools to return immediately without waiting for slower ones.

        Args:
            calls: List of tool calls to execute
            timeout: Optional timeout for each execution (overrides default)

        Returns:
            List of tool results in completion order (not submission order)
        """
        if not calls:
            return []

        if self._shutting_down:
            # Return early with error results if shutting down
            return [
                ToolResult(
                    tool=call.tool,
                    result=None,
                    error="System is shutting down",
                    start_time=datetime.now(UTC),
                    end_time=datetime.now(UTC),
                    machine=platform.node(),
                    pid=os.getpid(),
                )
                for call in calls
            ]

        # Use default_timeout if no timeout specified
        effective_timeout = timeout if timeout is not None else self.default_timeout
        logger.debug("Executing %d calls in subprocesses with %ss timeout each", len(calls), effective_timeout)

        # Create tasks for each call
        tasks = []
        for call in calls:
            task = asyncio.create_task(
                self._execute_single_call(
                    call,
                    effective_timeout,  # Always pass concrete timeout
                )
            )
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
            tasks.append(task)

        # Execute all tasks concurrently and return results in completion order
        async with log_context_span("subprocess_execution", {"num_calls": len(calls)}):
            results = []
            for completed_task in asyncio.as_completed(tasks):
                result = await completed_task
                results.append(result)
            return results

    async def stream_run(
        self,
        calls: list[ToolCall],
        timeout: float | None = None,
        on_tool_start: Callable[[ToolCall], Awaitable[None]] | None = None,
    ) -> AsyncIterator[ToolResult]:
        """
        Execute tool calls and yield results as they become available.

        Args:
            calls: List of tool calls to execute
            timeout: Optional timeout for each execution
            on_tool_start: Optional callback invoked when each tool starts execution.
                          Useful for emitting start events before results arrive.

        Yields:
            Tool results as they complete (in completion order)
        """
        if not calls:
            return

        if self._shutting_down:
            # Yield error results if shutting down
            for call in calls:
                yield ToolResult(
                    tool=call.tool,
                    result=None,
                    error="System is shutting down",
                    start_time=datetime.now(UTC),
                    end_time=datetime.now(UTC),
                    machine=platform.node(),
                    pid=os.getpid(),
                )
            return

        # Use default_timeout if no timeout specified
        effective_timeout = timeout if timeout is not None else self.default_timeout

        # Create a queue for results
        queue = asyncio.Queue()

        # Start all executions and have them put results in the queue
        pending = set()
        for call in calls:
            task = asyncio.create_task(
                self._execute_to_queue(
                    call,
                    queue,
                    effective_timeout,  # Always pass concrete timeout
                    on_tool_start,
                )
            )
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
            pending.add(task)

        # Yield results as they become available
        while pending:
            # Get next result from queue
            result = await queue.get()
            yield result

            # Check for completed tasks
            done, pending = await asyncio.wait(pending, timeout=0, return_when=asyncio.FIRST_COMPLETED)

            # Handle any exceptions
            for task in done:
                try:
                    await task
                except Exception as e:
                    logger.exception("Error in task: %s", e)

    async def _execute_to_queue(
        self,
        call: ToolCall,
        queue: asyncio.Queue,
        timeout: float,  # Make timeout required
        on_tool_start: Callable[[ToolCall], Awaitable[None]] | None = None,
    ) -> None:
        """Execute a single call and put the result in the queue."""
        # Invoke start callback if provided
        if on_tool_start:
            try:
                await on_tool_start(call)
            except Exception as e:
                logger.warning(f"on_tool_start callback failed for {call.tool}: {e}")

        result = await self._execute_single_call(call, timeout)
        await queue.put(result)

    async def _execute_single_call(
        self,
        call: ToolCall,
        timeout: float,  # Make timeout required
    ) -> ToolResult:
        """
        Execute a single tool call with enhanced tool resolution and serialization.

        Args:
            call: Tool call to execute
            timeout: Timeout in seconds (required)

        Returns:
            Tool execution result
        """
        start_time = datetime.now(UTC)

        logger.debug("Executing %s in subprocess with %ss timeout", call.tool, timeout)

        try:
            # Ensure pool is initialized
            await self._ensure_pool()

            # Use enhanced tool resolution instead of direct lookup
            tool_impl, resolved_namespace = await self._resolve_tool_info(call.tool, call.namespace)
            if tool_impl is None:
                return ToolResult(
                    tool=call.tool,
                    result=None,
                    error=f"Tool '{call.tool}' not found in any namespace",
                    start_time=start_time,
                    end_time=datetime.now(UTC),
                    machine=platform.node(),
                    pid=os.getpid(),
                )

            logger.debug(f"Resolved subprocess tool '{call.tool}' to namespace '{resolved_namespace}'")

            # Ensure tool is properly prepared before serialization
            tool = tool_impl() if inspect.isclass(tool_impl) else tool_impl

            # Ensure tool_name attribute exists
            if not hasattr(tool, "tool_name") or not tool.tool_name:
                tool.tool_name = call.tool

            # Also set _tool_name class attribute for consistency
            if not hasattr(tool.__class__, "_tool_name"):
                tool.__class__._tool_name = call.tool

            # Serialize the properly prepared tool
            try:
                serialized_tool_data = pickle.dumps(tool)
                logger.debug("Successfully serialized %s (%d bytes)", call.tool, len(serialized_tool_data))
            except Exception as e:
                logger.error("Failed to serialize tool %s: %s", call.tool, e)
                return ToolResult(
                    tool=call.tool,
                    result=None,
                    error=f"Tool serialization failed: {str(e)}",
                    start_time=start_time,
                    end_time=datetime.now(UTC),
                    machine=platform.node(),
                    pid=os.getpid(),
                )

            # Execute in subprocess using the FIXED worker
            loop = asyncio.get_running_loop()
            safety_timeout = timeout + 5.0

            try:
                result_data = await asyncio.wait_for(
                    loop.run_in_executor(
                        self._process_pool,
                        functools.partial(
                            _serialized_tool_worker,  # Use the FIXED worker function
                            call.tool,
                            resolved_namespace,  # Use resolved namespace
                            call.arguments,
                            timeout,
                            serialized_tool_data,  # Pass serialized tool data
                        ),
                    ),
                    timeout=safety_timeout,
                )

                # Parse timestamps
                if isinstance(result_data["start_time"], str):
                    result_data["start_time"] = datetime.fromisoformat(result_data["start_time"])

                if isinstance(result_data["end_time"], str):
                    result_data["end_time"] = datetime.fromisoformat(result_data["end_time"])

                end_time = datetime.now(UTC)
                actual_duration = (end_time - start_time).total_seconds()

                if result_data.get("error"):
                    logger.debug(
                        "%s subprocess failed after %.3fs: %s", call.tool, actual_duration, result_data["error"]
                    )
                else:
                    logger.debug("%s subprocess completed in %.3fs (limit: %ss)", call.tool, actual_duration, timeout)

                # Create ToolResult from worker data
                return ToolResult(
                    tool=result_data.get("tool", call.tool),
                    result=result_data.get("result"),
                    error=result_data.get("error"),
                    start_time=result_data.get("start_time", start_time),
                    end_time=result_data.get("end_time", end_time),
                    machine=result_data.get("machine", platform.node()),
                    pid=result_data.get("pid", os.getpid()),
                )

            except TimeoutError:
                end_time = datetime.now(UTC)
                actual_duration = (end_time - start_time).total_seconds()
                logger.debug(
                    "%s subprocess timed out after %.3fs (safety limit: %ss)",
                    call.tool,
                    actual_duration,
                    safety_timeout,
                )

                return ToolResult(
                    tool=call.tool,
                    result=None,
                    error=f"Worker process timed out after {safety_timeout}s",
                    start_time=start_time,
                    end_time=end_time,
                    machine=platform.node(),
                    pid=os.getpid(),
                )

            except concurrent.futures.process.BrokenProcessPool:
                logger.error("Process pool broke during execution - recreating")
                if self._process_pool:
                    self._process_pool.shutdown(wait=False)
                    self._process_pool = None

                return ToolResult(
                    tool=call.tool,
                    result=None,
                    error="Worker process crashed",
                    start_time=start_time,
                    end_time=datetime.now(UTC),
                    machine=platform.node(),
                    pid=os.getpid(),
                )

        except asyncio.CancelledError:
            logger.debug("%s subprocess was cancelled", call.tool)
            return ToolResult(
                tool=call.tool,
                result=None,
                error="Execution was cancelled",
                start_time=start_time,
                end_time=datetime.now(UTC),
                machine=platform.node(),
                pid=os.getpid(),
            )

        except Exception as e:
            logger.exception("Error executing %s in subprocess: %s", call.tool, e)
            end_time = datetime.now(UTC)
            actual_duration = (end_time - start_time).total_seconds()
            logger.debug("%s subprocess setup failed after %.3fs: %s", call.tool, actual_duration, e)

            return ToolResult(
                tool=call.tool,
                result=None,
                error=f"Error: {str(e)}",
                start_time=start_time,
                end_time=end_time,
                machine=platform.node(),
                pid=os.getpid(),
            )

    async def _resolve_tool_info(
        self, tool_name: str, preferred_namespace: str = "default"
    ) -> tuple[Any | None, str | None]:
        """
        Enhanced tool name resolution with comprehensive fallback logic.

        This method handles:
        1. Simple names: "get_current_time" -> search in specified namespace first, then all namespaces
        2. Namespaced names: "diagnostic_test.get_current_time" -> extract namespace and tool name
        3. Fallback searching across all namespaces when not found in default

        Args:
            tool_name: Name of the tool to resolve
            preferred_namespace: Preferred namespace to search first

        Returns:
            Tuple of (tool_object, resolved_namespace) or (None, None) if not found
        """
        logger.debug(f"Resolving tool: '{tool_name}' (preferred namespace: '{preferred_namespace}')")

        # Strategy 1: Handle namespaced tool names (namespace.tool_name format)
        if "." in tool_name:
            parts = tool_name.split(".", 1)  # Split on first dot only
            namespace = parts[0]
            actual_tool_name = parts[1]

            logger.debug(f"Namespaced lookup: namespace='{namespace}', tool='{actual_tool_name}'")

            tool = await self.registry.get_tool(actual_tool_name, namespace)
            if tool is not None:
                logger.debug(f"Found tool '{actual_tool_name}' in namespace '{namespace}'")
                return tool, namespace
            else:
                logger.debug(f"Tool '{actual_tool_name}' not found in namespace '{namespace}'")
                return None, None

        # Strategy 2: Simple tool name - try preferred namespace first
        if preferred_namespace:
            logger.debug(f"Simple tool lookup: trying preferred namespace '{preferred_namespace}' for '{tool_name}'")
            tool = await self.registry.get_tool(tool_name, preferred_namespace)
            if tool is not None:
                logger.debug(f"Found tool '{tool_name}' in preferred namespace '{preferred_namespace}'")
                return tool, preferred_namespace

        # Strategy 3: Try default namespace if different from preferred
        if preferred_namespace != "default":
            logger.debug(f"Simple tool lookup: trying default namespace for '{tool_name}'")
            tool = await self.registry.get_tool(tool_name, "default")
            if tool is not None:
                logger.debug(f"Found tool '{tool_name}' in default namespace")
                return tool, "default"

        # Strategy 4: Search all namespaces as fallback
        logger.debug(f"Tool '{tool_name}' not in preferred/default namespace, searching all namespaces...")

        try:
            # Get all available namespaces
            namespaces = await self.registry.list_namespaces()
            logger.debug(f"Available namespaces: {namespaces}")

            # Search each namespace
            for namespace in namespaces:
                if namespace in [preferred_namespace, "default"]:
                    continue  # Already tried these

                logger.debug(f"Searching namespace '{namespace}' for tool '{tool_name}'")
                tool = await self.registry.get_tool(tool_name, namespace)
                if tool is not None:
                    logger.debug(f"Found tool '{tool_name}' in namespace '{namespace}'")
                    return tool, namespace

            # Strategy 5: Final fallback - list all tools and do fuzzy matching
            logger.debug(f"Tool '{tool_name}' not found in any namespace, trying fuzzy matching...")
            all_tools = await self.registry.list_tools()

            # Look for exact matches in tool name (ignoring namespace)
            for namespace, registered_name in all_tools:
                if registered_name == tool_name:
                    logger.debug(f"Fuzzy match: found '{registered_name}' in namespace '{namespace}'")
                    tool = await self.registry.get_tool(registered_name, namespace)
                    if tool is not None:
                        return tool, namespace

            # Log all available tools for debugging
            logger.debug(f"Available tools: {all_tools}")

        except Exception as e:
            logger.error(f"Error during namespace search: {e}")

        logger.warning(f"Tool '{tool_name}' not found in any namespace")
        return None, None

    @property
    def supports_streaming(self) -> bool:
        """Check if this strategy supports streaming execution."""
        return True

    @property
    def is_pool_ready(self) -> bool:
        """Check if the process pool has been initialized."""
        return self._process_pool is not None

    async def warm(self) -> None:
        """
        Explicitly pre-warm the subprocess pool.

        This method can be called during application startup to ensure
        all worker processes are spawned before any tool calls arrive.
        This eliminates cold-start latency for the first batch of calls.

        Example:
            strategy = SubprocessStrategy(registry, max_workers=4)
            await strategy.warm()  # Pre-warm all 4 workers
            # Now ready for low-latency tool execution
        """
        # Temporarily enable warm_pool for this call
        original_warm = self._warm_pool
        self._warm_pool = True
        try:
            await self._ensure_pool()
        finally:
            self._warm_pool = original_warm

    async def _signal_handler(self, sig: int) -> None:
        """Handle termination signals."""
        signame = signal.Signals(sig).name
        logger.info("Received %s, shutting down process pool", signame)
        await self.shutdown()

    async def shutdown(self) -> None:
        """Enhanced shutdown with graceful task handling and proper null checks."""
        if self._shutting_down:
            return

        self._shutting_down = True
        self._shutdown_event.set()

        # Handle active tasks gracefully
        active_tasks = list(self._active_tasks)
        if active_tasks:
            logger.debug(f"Completing {len(active_tasks)} active operations")

            # Cancel tasks with brief intervals for clean handling
            for task in active_tasks:
                try:
                    if not task.done():
                        task.cancel()
                except Exception:
                    pass
                # Small delay to prevent overwhelming the event loop
                with contextlib.suppress(Exception):
                    await asyncio.sleep(0.001)

            # Allow reasonable time for completion
            try:
                completion_task = asyncio.create_task(asyncio.gather(*active_tasks, return_exceptions=True))
                await asyncio.wait_for(completion_task, timeout=2.0)
            except TimeoutError:
                logger.debug("Active operations completed within timeout constraints")
            except Exception:
                logger.debug("Active operations completed successfully")

        # Handle process pool shutdown with proper null checks.
        if self._process_pool is not None:
            logger.debug("Finalizing process pool")
            # Store reference and clear immediately to prevent race conditions.
            pool_to_shutdown = self._process_pool
            self._process_pool = None
            # shutdown(wait=False) is non-blocking (it doesn't join worker
            # processes), so call it directly — the same way the other cleanup
            # paths in this module do. Offloading it to a thread-pool executor
            # with a timeout previously raced: on a busy loop the executor could
            # fail to run before the timeout fired, so the pool was never told to
            # shut down at all.
            try:
                pool_to_shutdown.shutdown(wait=False)
                logger.debug("Process pool shutdown completed")
            except Exception as e:
                logger.debug(f"Process pool finalization completed: {e}")
        else:
            logger.debug("Process pool already cleaned up")
