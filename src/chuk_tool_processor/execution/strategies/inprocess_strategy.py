#!/usr/bin/env python
# chuk_tool_processor/execution/strategies/inprocess_strategy.py
"""
In-process execution strategy for tools with proper timeout handling.

This strategy executes tools concurrently in the same process using asyncio.
It has special support for streaming tools, accessing their stream_execute method
directly to enable true item-by-item streaming.

PARALLEL EXECUTION:
- All tool calls execute concurrently using asyncio tasks
- Results are returned/yielded as each tool completes (completion order, not submission order)
- Faster tools return immediately without waiting for slower ones
- Use run() for batch results in completion order
- Use stream_run() to yield results as they arrive

STREAMING SUPPORT:
- stream_run() yields ToolResult objects as each tool completes
- Optional on_tool_start callback for emitting start events
- True streaming for tools that implement stream_execute

Enhanced tool name resolution that properly handles:
- Simple names: "get_current_time"
- Namespaced names: "diagnostic_test.get_current_time"
- Cross-namespace fallback searching

Ensures consistent timeout handling across all execution paths.
ENHANCED: Clean shutdown handling to prevent anyio cancel scope errors.
"""

from __future__ import annotations

import asyncio
import builtins
import inspect
import os
import platform
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Any

from chuk_tool_processor.logging import get_logger, log_context_span
from chuk_tool_processor.models.execution_strategy import ExecutionStrategy
from chuk_tool_processor.models.return_order import ReturnOrder
from chuk_tool_processor.models.tool_call import ToolCall
from chuk_tool_processor.models.tool_result import ToolResult
from chuk_tool_processor.registry.interface import ToolRegistryInterface

logger = get_logger("chuk_tool_processor.execution.inprocess_strategy")


# --------------------------------------------------------------------------- #
# Async no-op context-manager (used when no semaphore configured)
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def _noop_cm():
    yield


# --------------------------------------------------------------------------- #
class InProcessStrategy(ExecutionStrategy):
    """Execute tools in the local event-loop with optional concurrency cap and consistent timeout handling."""

    def __init__(
        self,
        registry: ToolRegistryInterface,
        default_timeout: float | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        """
        Initialize the in-process execution strategy.

        Args:
            registry: Tool registry to use for tool lookups
            default_timeout: Default timeout for tool execution
            max_concurrency: Maximum number of concurrent executions
        """
        self.registry = registry
        self.default_timeout = default_timeout or 30.0  # Always have a default
        self._sem = asyncio.Semaphore(max_concurrency) if max_concurrency else None

        # Task tracking for cleanup
        self._active_tasks = set()
        self._shutting_down = False
        self._shutdown_event = asyncio.Event()

        # Tracking for which calls are being handled directly by the executor
        # to prevent duplicate streaming results
        self._direct_streaming_calls = set()

        logger.debug(
            "InProcessStrategy initialized with timeout: %ss, max_concurrency: %s",
            self.default_timeout,
            max_concurrency,
        )

    # ------------------------------------------------------------------ #
    def mark_direct_streaming(self, call_ids: set[str]) -> None:
        """
        Mark tool calls that are being handled directly by the executor.

        Args:
            call_ids: Set of call IDs that should be skipped during streaming
                      because they're handled directly
        """
        self._direct_streaming_calls.update(call_ids)

    def clear_direct_streaming(self) -> None:
        """Clear the list of direct streaming calls."""
        self._direct_streaming_calls.clear()

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

    # ------------------------------------------------------------------ #
    async def run(
        self,
        calls: list[ToolCall],
        timeout: float | None = None,
        return_order: ReturnOrder | str = ReturnOrder.COMPLETION,
    ) -> list[ToolResult]:
        """
        Execute tool calls concurrently and return results.

        Args:
            calls: List of tool calls to execute
            timeout: Optional timeout for execution
            return_order: Order to return results in:
                - "completion" (default): Results return as each tool completes
                - "submission": Results return in the same order as the input calls

        Returns:
            List of tool results in the specified order
        """
        if not calls:
            return []

        # Normalize return_order to enum
        if isinstance(return_order, str):
            return_order = ReturnOrder(return_order)

        # Use default_timeout if no timeout specified
        effective_timeout = timeout if timeout is not None else self.default_timeout
        logger.debug(
            "Executing %d calls with %ss timeout each, return_order=%s",
            len(calls),
            effective_timeout,
            return_order.value,
        )

        # Create all tasks immediately so they start running in parallel
        # Track task -> call_id mapping for submission order
        task_to_call_id: dict[asyncio.Task, str] = {}
        tasks = []
        for call in calls:
            task = asyncio.create_task(self._execute_single_call(call, effective_timeout))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
            task_to_call_id[task] = call.id
            tasks.append(task)

        async with log_context_span("inprocess_execution", {"num_calls": len(calls)}):
            if return_order == ReturnOrder.COMPLETION:
                # Use as_completed to return results as they finish
                results = []
                for completed_task in asyncio.as_completed(tasks):
                    result = await completed_task
                    results.append(result)
                return results
            else:
                # Wait for all tasks and return in submission order
                completed_results = await asyncio.gather(*tasks, return_exceptions=False)
                # Results are already in submission order since gather preserves order
                return list(completed_results)

    # ------------------------------------------------------------------ #
    async def stream_run(
        self,
        calls: list[ToolCall],
        timeout: float | None = None,
        on_tool_start: Callable[[ToolCall], Awaitable[None]] | None = None,
    ) -> AsyncIterator[ToolResult]:
        """
        Execute tool calls concurrently and *yield* results as soon as they are
        produced in completion order (not submission order).

        This method allows results to stream back as each tool completes, without
        waiting for all tools to finish. Faster tools return immediately.

        Args:
            calls: List of tool calls to execute concurrently
            timeout: Optional timeout for each tool execution
            on_tool_start: Optional callback invoked when each tool starts execution.
                          Useful for emitting start events before results arrive.

        Yields:
            ToolResult objects as each tool completes (in completion order)
        """
        if not calls:
            return

        # Use default_timeout if no timeout specified
        effective_timeout = timeout if timeout is not None else self.default_timeout

        queue: asyncio.Queue[ToolResult] = asyncio.Queue()
        tasks = {
            asyncio.create_task(self._stream_tool_call(call, queue, effective_timeout, on_tool_start))
            for call in calls
            if call.id not in self._direct_streaming_calls
        }

        # 🔑 keep consuming until every worker‐task finished *and*
        #    the queue is empty
        while tasks or not queue.empty():
            try:
                result = await queue.get()
                yield result
            except asyncio.CancelledError:
                break

            # clear finished tasks (frees exceptions as well)
            done, tasks = await asyncio.wait(tasks, timeout=0)
            for t in done:
                t.result()  # re-raise if a task crashed

    async def _stream_tool_call(
        self,
        call: ToolCall,
        queue: asyncio.Queue,
        timeout: float,  # Make timeout required
        on_tool_start: Callable[[ToolCall], Awaitable[None]] | None = None,
    ) -> None:
        """
        Execute a tool call with streaming support.

        This looks up the tool and if it's a streaming tool, it accesses
        stream_execute directly to get item-by-item streaming.

        Args:
            call: The tool call to execute
            queue: Queue to put results into
            timeout: Timeout in seconds (required)
            on_tool_start: Optional callback to invoke when tool execution starts
        """
        # Skip if call is being handled directly by the executor
        if call.id in self._direct_streaming_calls:
            return

        if self._shutting_down:
            # Early exit if shutting down
            now = datetime.now(UTC)
            result = ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error="System is shutting down",
                start_time=now,
                end_time=now,
                machine=platform.node(),
                pid=os.getpid(),
            )
            await queue.put(result)
            return

        # Invoke start callback if provided
        if on_tool_start:
            try:
                await on_tool_start(call)
            except Exception as e:
                logger.warning(f"on_tool_start callback failed for {call.tool}: {e}")

        try:
            # Use enhanced tool resolution instead of direct lookup
            tool_impl, resolved_namespace = await self._resolve_tool_info(call.tool, call.namespace)
            if tool_impl is None:
                # Tool not found
                now = datetime.now(UTC)
                result = ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=None,
                    error=f"Tool '{call.tool}' not found in any namespace",
                    start_time=now,
                    end_time=now,
                    machine=platform.node(),
                    pid=os.getpid(),
                )
                await queue.put(result)
                return

            logger.debug(f"Resolved streaming tool '{call.tool}' to namespace '{resolved_namespace}'")

            # Instantiate if class
            tool = tool_impl() if inspect.isclass(tool_impl) else tool_impl

            # Use semaphore if available
            guard = self._sem if self._sem is not None else _noop_cm()

            async with guard:
                # Check if this is a streaming tool
                if hasattr(tool, "supports_streaming") and tool.supports_streaming and hasattr(tool, "stream_execute"):
                    # Use direct streaming for streaming tools
                    await self._stream_with_timeout(tool, call, queue, timeout)
                else:
                    # Use regular execution for non-streaming tools
                    result = await self._execute_single_call(call, timeout)
                    await queue.put(result)

        except asyncio.CancelledError:
            # Handle cancellation gracefully
            now = datetime.now(UTC)
            result = ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error="Execution was cancelled",
                start_time=now,
                end_time=now,
                machine=platform.node(),
                pid=os.getpid(),
            )
            await queue.put(result)

        except Exception as e:
            # Handle other errors
            now = datetime.now(UTC)
            result = ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=f"Error setting up execution: {e}",
                start_time=now,
                end_time=now,
                machine=platform.node(),
                pid=os.getpid(),
            )
            await queue.put(result)

    async def _stream_with_timeout(
        self,
        tool: Any,
        call: ToolCall,
        queue: asyncio.Queue,
        timeout: float,  # Make timeout required
    ) -> None:
        """
        Stream results from a streaming tool with timeout support.

        This method accesses the tool's stream_execute method directly
        and puts each yielded result into the queue.

        Args:
            tool: The tool instance
            call: Tool call data
            queue: Queue to put results into
            timeout: Timeout in seconds (required)
        """
        start_time = datetime.now(UTC)
        machine = platform.node()
        pid = os.getpid()

        logger.debug("Streaming %s with %ss timeout", call.tool, timeout)

        # Define the streaming task
        async def streamer():
            try:
                async for result in tool.stream_execute(**call.arguments):
                    # Create a ToolResult for each streamed item
                    now = datetime.now(UTC)
                    tool_result = ToolResult(
                        call_id=call.id,
                        tool=call.tool,
                        result=result,
                        error=None,
                        start_time=start_time,
                        end_time=now,
                        machine=machine,
                        pid=pid,
                    )
                    await queue.put(tool_result)
            except Exception as e:
                # Handle errors during streaming
                now = datetime.now(UTC)
                error_result = ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=None,
                    error=f"Streaming error: {str(e)}",
                    start_time=start_time,
                    end_time=now,
                    machine=machine,
                    pid=pid,
                )
                await queue.put(error_result)

        try:
            # Always execute with timeout
            await asyncio.wait_for(streamer(), timeout)
            logger.debug("%s streaming completed within %ss", call.tool, timeout)

        except TimeoutError:
            # Handle timeout
            now = datetime.now(UTC)
            actual_duration = (now - start_time).total_seconds()
            logger.debug("%s streaming timed out after %.3fs (limit: %ss)", call.tool, actual_duration, timeout)

            timeout_result = ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=f"Streaming timeout after {timeout}s",
                start_time=start_time,
                end_time=now,
                machine=machine,
                pid=pid,
            )
            await queue.put(timeout_result)

        except Exception as e:
            # Handle other errors
            now = datetime.now(UTC)
            logger.debug("%s streaming failed: %s", call.tool, e)

            error_result = ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=f"Error during streaming: {str(e)}",
                start_time=start_time,
                end_time=now,
                machine=machine,
                pid=pid,
            )
            await queue.put(error_result)

    async def _execute_to_queue(
        self,
        call: ToolCall,
        queue: asyncio.Queue,
        timeout: float,  # Make timeout required
    ) -> None:
        """Execute a single call and put the result in the queue."""
        # Skip if call is being handled directly by the executor
        if call.id in self._direct_streaming_calls:
            return

        result = await self._execute_single_call(call, timeout)
        await queue.put(result)

    # ------------------------------------------------------------------ #
    async def _execute_single_call(
        self,
        call: ToolCall,
        timeout: float,  # Make timeout required, not optional
    ) -> ToolResult:
        """
        Execute a single tool call with guaranteed timeout.

        The entire invocation - including argument validation - is wrapped
        by the semaphore to honour *max_concurrency*.

        Args:
            call: Tool call to execute
            timeout: Timeout in seconds (required)

        Returns:
            Tool execution result
        """
        pid = os.getpid()
        machine = platform.node()
        start = datetime.now(UTC)

        logger.debug("Executing %s with %ss timeout", call.tool, timeout)

        # Early exit if shutting down
        if self._shutting_down:
            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error="System is shutting down",
                start_time=start,
                end_time=datetime.now(UTC),
                machine=machine,
                pid=pid,
            )

        try:
            # Use enhanced tool resolution instead of direct lookup
            impl, resolved_namespace = await self._resolve_tool_info(call.tool, call.namespace)
            if impl is None:
                return ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=None,
                    error=f"Tool '{call.tool}' not found in any namespace",
                    start_time=start,
                    end_time=datetime.now(UTC),
                    machine=machine,
                    pid=pid,
                )

            logger.debug(f"Resolved tool '{call.tool}' to namespace '{resolved_namespace}'")

            # Instantiate if class
            tool = impl() if inspect.isclass(impl) else impl

            # Use semaphore if available
            guard = self._sem if self._sem is not None else _noop_cm()

            try:
                async with guard:
                    return await self._run_with_timeout(tool, call, timeout, start, machine, pid)
            except Exception as exc:
                logger.exception("Unexpected error while executing %s", call.tool)
                return ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=None,
                    error=f"Unexpected error: {exc}",
                    start_time=start,
                    end_time=datetime.now(UTC),
                    machine=machine,
                    pid=pid,
                )
        except asyncio.CancelledError:
            # Handle cancellation gracefully
            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error="Execution was cancelled",
                start_time=start,
                end_time=datetime.now(UTC),
                machine=machine,
                pid=pid,
            )
        except Exception as exc:
            logger.exception("Error setting up execution for %s", call.tool)
            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=f"Setup error: {exc}",
                start_time=start,
                end_time=datetime.now(UTC),
                machine=machine,
                pid=pid,
            )

    async def _run_with_timeout(
        self,
        tool: Any,
        call: ToolCall,
        timeout: float,  # Make timeout required, not optional
        start: datetime,
        machine: str,
        pid: int,
    ) -> ToolResult:
        """
        Resolve the correct async entry-point and invoke it with a guaranteed timeout.

        Args:
            tool: Tool instance
            call: Tool call data
            timeout: Timeout in seconds (required)
            start: Start time for the execution
            machine: Machine name
            pid: Process ID

        Returns:
            Tool execution result
        """
        if hasattr(tool, "_aexecute") and inspect.iscoroutinefunction(getattr(type(tool), "_aexecute", None)):
            fn = tool._aexecute
        elif hasattr(tool, "execute") and inspect.iscoroutinefunction(getattr(tool, "execute", None)):
            fn = tool.execute
        else:
            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=(
                    "Tool must implement *async* '_aexecute' or 'execute'. Synchronous entry-points are not supported."
                ),
                start_time=start,
                end_time=datetime.now(UTC),
                machine=machine,
                pid=pid,
            )

        try:
            # Always apply timeout
            logger.debug("Applying %ss timeout to %s", timeout, call.tool)

            try:
                result_val = await asyncio.wait_for(fn(**call.arguments), timeout=timeout)

                end_time = datetime.now(UTC)
                actual_duration = (end_time - start).total_seconds()
                logger.debug("%s completed in %.3fs (limit: %ss)", call.tool, actual_duration, timeout)

                return ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=result_val,
                    error=None,
                    start_time=start,
                    end_time=end_time,
                    machine=machine,
                    pid=pid,
                )
            except TimeoutError:
                # Handle timeout
                end_time = datetime.now(UTC)
                actual_duration = (end_time - start).total_seconds()
                logger.debug("%s timed out after %.3fs (limit: %ss)", call.tool, actual_duration, timeout)

                return ToolResult(
                    call_id=call.id,
                    tool=call.tool,
                    result=None,
                    error=f"Timeout after {timeout}s",
                    start_time=start,
                    end_time=end_time,
                    machine=machine,
                    pid=pid,
                )

        except asyncio.CancelledError:
            # Handle cancellation explicitly
            logger.debug("%s was cancelled", call.tool)
            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error="Execution was cancelled",
                start_time=start,
                end_time=datetime.now(UTC),
                machine=machine,
                pid=pid,
            )
        except Exception as exc:
            logger.exception("Error executing %s: %s", call.tool, exc)
            end_time = datetime.now(UTC)
            actual_duration = (end_time - start).total_seconds()
            logger.debug("%s failed after %.3fs: %s", call.tool, actual_duration, exc)

            return ToolResult(
                call_id=call.id,
                tool=call.tool,
                result=None,
                error=str(exc),
                start_time=start,
                end_time=end_time,
                machine=machine,
                pid=pid,
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
            for tool_info in all_tools:
                if tool_info.name == tool_name:
                    logger.debug(f"Fuzzy match: found '{tool_info.name}' in namespace '{tool_info.namespace}'")
                    tool = await self.registry.get_tool(tool_info.name, tool_info.namespace)
                    if tool is not None:
                        return tool, tool_info.namespace

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

    async def shutdown(self) -> None:
        """
        Enhanced shutdown with clean task management.

        This version prevents anyio cancel scope errors by handling
        task cancellation more gracefully with individual error handling
        and reasonable timeouts.
        """
        if self._shutting_down:
            return

        self._shutting_down = True
        self._shutdown_event.set()

        # Manage active tasks cleanly
        active_tasks = list(self._active_tasks)
        if active_tasks:
            logger.debug(f"Completing {len(active_tasks)} in-process operations")

            # Handle each task individually with brief delays
            for task in active_tasks:
                try:
                    if not task.done():
                        task.cancel()
                except Exception:
                    pass
                # Small delay between cancellations to avoid overwhelming the event loop
                with suppress(builtins.BaseException):
                    await asyncio.sleep(0.001)

            # Allow reasonable time for completion with timeout
            try:
                await asyncio.wait_for(asyncio.gather(*active_tasks, return_exceptions=True), timeout=2.0)
            except Exception:
                # Suppress all errors during shutdown to prevent cancel scope issues
                logger.debug("In-process operations completed within expected parameters")
