# chuk_tool_processor/core/processor.py
"""
Async-native core processor for tool execution.

This module provides the central ToolProcessor class which handles:
- Tool call parsing from various input formats
- Tool execution using configurable strategies
- Application of execution wrappers (caching, retries, etc.)
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json as stdlib_json  # Use stdlib json for consistent hashing
import time
from typing import Any

from chuk_tool_processor.core.context import (
    ExecutionContext,
    execution_scope,
)
from chuk_tool_processor.execution.bulkhead import Bulkhead, BulkheadConfig
from chuk_tool_processor.execution.strategies.inprocess_strategy import (
    InProcessStrategy,
)
from chuk_tool_processor.execution.wrappers.caching import (
    CachingToolExecutor,
    InMemoryCache,
)
from chuk_tool_processor.execution.wrappers.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitBreakerExecutor,
)
from chuk_tool_processor.execution.wrappers.rate_limiting import (
    RateLimitedToolExecutor,
    RateLimiter,
)
from chuk_tool_processor.execution.wrappers.retry import (
    RetryableToolExecutor,
    RetryConfig,
)
from chuk_tool_processor.logging import (
    get_logger,
    log_context_span,
    log_tool_call,
    metrics,
    request_logging,
)
from chuk_tool_processor.models.tool_call import ToolCall
from chuk_tool_processor.models.tool_result import ToolResult
from chuk_tool_processor.plugins.discovery import (
    discover_default_plugins,
    plugin_registry,
)
from chuk_tool_processor.registry import ToolRegistryInterface, ToolRegistryProvider
from chuk_tool_processor.utils import fast_json as json


class ToolProcessor:
    """
    Main class for processing tool calls from LLM responses.

    ToolProcessor combines parsing, execution, and result handling with full async support.
    It provides production-ready features including timeouts, retries, caching, rate limiting,
    and circuit breaking.

    Examples:
        Basic usage with context manager:

        >>> import asyncio
        >>> from chuk_tool_processor import ToolProcessor, register_tool
        >>>
        >>> @register_tool(name="calculator")
        ... class Calculator:
        ...     async def execute(self, a: int, b: int) -> dict:
        ...         return {"result": a + b}
        >>>
        >>> async def main():
        ...     async with ToolProcessor() as processor:
        ...         llm_output = '<tool name="calculator" args=\'{"a": 5, "b": 3}\'/>'
        ...         results = await processor.process(llm_output)
        ...         print(results[0].result)  # {'result': 8}
        >>>
        >>> asyncio.run(main())

        Production configuration:

        >>> processor = ToolProcessor(
        ...     default_timeout=30.0,
        ...     enable_caching=True,
        ...     cache_ttl=600,
        ...     enable_rate_limiting=True,
        ...     global_rate_limit=100,  # 100 requests/minute
        ...     enable_retries=True,
        ...     max_retries=3,
        ...     enable_circuit_breaker=True,
        ... )

        Manual cleanup:

        >>> processor = ToolProcessor()
        >>> try:
        ...     results = await processor.process(llm_output)
        ... finally:
        ...     await processor.close()

    Attributes:
        registry: Tool registry containing registered tools
        strategy: Execution strategy (InProcess or Subprocess)
        executor: Wrapped executor with caching, retries, etc.
        parsers: List of parser plugins for extracting tool calls
    """

    def __init__(
        self,
        registry: ToolRegistryInterface | None = None,
        strategy: (Any | None) = None,  # Strategy can be InProcessStrategy or SubprocessStrategy
        default_timeout: float = 10.0,
        max_concurrency: int | None = None,
        enable_caching: bool = True,
        cache_ttl: int = 300,
        enable_rate_limiting: bool = False,
        global_rate_limit: int | None = None,
        tool_rate_limits: dict[str, tuple] | None = None,
        enable_retries: bool = True,
        max_retries: int = 3,
        retry_config: RetryConfig | None = None,
        enable_circuit_breaker: bool = False,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
        parser_plugins: list[str] | None = None,
        # New: Bulkhead configuration
        bulkhead_config: BulkheadConfig | None = None,
        enable_bulkhead: bool = False,
    ):
        """
        Initialize the tool processor.

        Args:
            registry: Tool registry to use. If None, uses the global registry.
            strategy: Optional execution strategy (default: InProcessStrategy).
                Use SubprocessStrategy for isolated execution of untrusted code.
            default_timeout: Default timeout for tool execution in seconds.
                Individual tools can override this. Default: 10.0
            max_concurrency: Maximum number of concurrent tool executions.
                If None, uses unlimited concurrency. Default: None
            enable_caching: Whether to enable result caching. Caches results
                based on tool name and arguments. Default: True
            cache_ttl: Default cache TTL in seconds. Results older than this
                are evicted. Default: 300 (5 minutes)
            enable_rate_limiting: Whether to enable rate limiting. Prevents
                API abuse and quota exhaustion. Default: False
            global_rate_limit: Optional global rate limit (requests per minute).
                Applies to all tools unless overridden. Default: None
            tool_rate_limits: Dict mapping tool names to (limit, period) tuples.
                Example: {"api_call": (10, 60)} = 10 requests per 60 seconds
            enable_retries: Whether to enable automatic retries on transient
                failures. Uses exponential backoff. Default: True
            max_retries: Maximum number of retry attempts. Total attempts will
                be max_retries + 1 (initial + retries). Default: 3
            retry_config: Optional custom retry configuration. If provided,
                overrides max_retries. See RetryConfig for details.
            enable_circuit_breaker: Whether to enable circuit breaker pattern.
                Opens circuit after repeated failures to prevent cascading
                failures. Default: False
            circuit_breaker_threshold: Number of consecutive failures before
                opening circuit. Default: 5
            circuit_breaker_timeout: Seconds to wait before attempting recovery
                (transition from OPEN to HALF_OPEN). Default: 60.0
            parser_plugins: List of parser plugin names to use. If None, uses
                all available parsers (XML, OpenAI, JSON). Default: None
            bulkhead_config: Configuration for per-tool/namespace concurrency limits.
                Enables bulkhead pattern for resource isolation. Default: None
            enable_bulkhead: Whether to enable bulkhead pattern. Default: False

        Raises:
            ImportError: If required dependencies are not installed.

        Example:
            >>> # Production configuration with all features
            >>> processor = ToolProcessor(
            ...     default_timeout=30.0,
            ...     max_concurrency=20,
            ...     enable_caching=True,
            ...     cache_ttl=600,
            ...     enable_rate_limiting=True,
            ...     global_rate_limit=100,
            ...     tool_rate_limits={
            ...         "expensive_api": (10, 60),
            ...         "free_api": (100, 60),
            ...     },
            ...     enable_retries=True,
            ...     max_retries=3,
            ...     enable_circuit_breaker=True,
            ...     circuit_breaker_threshold=5,
            ...     circuit_breaker_timeout=60.0,
            ... )
        """
        self.logger = get_logger("chuk_tool_processor.processor")

        # Store initialization parameters for lazy initialization
        self._registry = registry
        self._strategy = strategy
        self.default_timeout = default_timeout
        self.max_concurrency = max_concurrency
        self.enable_caching = enable_caching
        self.cache_ttl = cache_ttl
        self.enable_rate_limiting = enable_rate_limiting
        self.global_rate_limit = global_rate_limit
        self.tool_rate_limits = tool_rate_limits
        self.enable_retries = enable_retries
        self.max_retries = max_retries
        self.retry_config = retry_config
        self.enable_circuit_breaker = enable_circuit_breaker
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_timeout = circuit_breaker_timeout
        self.parser_plugin_names = parser_plugins
        self.bulkhead_config = bulkhead_config
        self.enable_bulkhead = enable_bulkhead

        # Placeholder for initialized components (typed as Optional for type safety)
        self.registry: ToolRegistryInterface | None = None
        self.strategy: Any | None = None  # Strategy type is complex, use Any for now
        self.executor: Any | None = None  # Executor type is complex, use Any for now
        self.parsers: list[Any] = []  # Parser types vary, use Any for now
        self.bulkhead: Bulkhead | None = None  # Bulkhead for concurrency isolation

        # Flag for tracking initialization state
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """
        Initialize the processor asynchronously.

        This method ensures all components are properly initialized before use.
        It is called automatically by other methods if needed.
        """
        # Ensure only one initialization happens at a time
        async with self._init_lock:
            if self._initialized:
                return

            self.logger.debug("Initializing tool processor")

            # Get the registry
            if self._registry is not None:
                self.registry = self._registry
            else:
                self.registry = await ToolRegistryProvider.get_registry()

            # Create execution strategy if needed
            if self._strategy is not None:
                self.strategy = self._strategy
            else:
                self.strategy = InProcessStrategy(
                    registry=self.registry,
                    default_timeout=self.default_timeout,
                    max_concurrency=self.max_concurrency,
                )

            # Set up the executor chain with optional wrappers
            executor = self.strategy

            # Apply wrappers in reverse order (innermost first)
            # Circuit breaker goes innermost (closest to actual execution)
            if self.enable_circuit_breaker:
                self.logger.debug("Enabling circuit breaker")
                circuit_config = CircuitBreakerConfig(
                    failure_threshold=self.circuit_breaker_threshold,
                    reset_timeout=self.circuit_breaker_timeout,
                )
                executor = CircuitBreakerExecutor(
                    executor=executor,
                    default_config=circuit_config,
                )

            if self.enable_retries:
                self.logger.debug("Enabling retry logic")
                # Use custom retry config if provided, otherwise create default
                retry_cfg = self.retry_config or RetryConfig(max_retries=self.max_retries)
                executor = RetryableToolExecutor(
                    executor=executor,
                    default_config=retry_cfg,
                )

            if self.enable_rate_limiting:
                self.logger.debug("Enabling rate limiting")
                rate_limiter = RateLimiter(
                    global_limit=self.global_rate_limit,
                    tool_limits=self.tool_rate_limits,
                )
                executor = RateLimitedToolExecutor(
                    executor=executor,
                    limiter=rate_limiter,
                )

            if self.enable_caching:
                self.logger.debug("Enabling result caching")
                cache = InMemoryCache(default_ttl=self.cache_ttl)
                executor = CachingToolExecutor(
                    executor=executor,
                    cache=cache,
                    default_ttl=self.cache_ttl,
                )

            self.executor = executor

            # Initialize bulkhead if enabled
            if self.enable_bulkhead:
                self.logger.debug("Enabling bulkhead concurrency isolation")
                self.bulkhead = Bulkhead(self.bulkhead_config)

            # Initialize parser plugins
            # Discover plugins if not already done
            plugins = plugin_registry.list_plugins().get("parser", [])
            if not plugins:
                discover_default_plugins()
                plugins = plugin_registry.list_plugins().get("parser", [])

            # Get parser plugins
            if self.parser_plugin_names:
                self.parsers = [
                    plugin_registry.get_plugin("parser", name)
                    for name in self.parser_plugin_names
                    if plugin_registry.get_plugin("parser", name)
                ]
            else:
                self.parsers = [plugin_registry.get_plugin("parser", name) for name in plugins]

            self.logger.debug(f"Initialized with {len(self.parsers)} parser plugins")
            self._initialized = True

    async def process(
        self,
        data: str | dict[str, Any] | list[dict[str, Any]],
        timeout: float | None = None,
        use_cache: bool = True,  # noqa: ARG002
        request_id: str | None = None,
        context: ExecutionContext | None = None,
        return_order: str | None = None,
    ) -> list[ToolResult]:
        """
        Process tool calls from various LLM output formats.

        This method handles different input types from various LLM providers:

        **String Input (Anthropic Claude style)**:
            Parses tool calls from XML-like text using registered parsers.

            Example:
                >>> llm_output = '<tool name="calculator" args=\'{"a": 5, "b": 3}\'/>'
                >>> results = await processor.process(llm_output)

        **Dict Input (OpenAI style)**:
            Processes an OpenAI-style tool_calls object.

            Example:
                >>> openai_output = {
                ...     "tool_calls": [
                ...         {
                ...             "type": "function",
                ...             "function": {
                ...                 "name": "calculator",
                ...                 "arguments": '{"a": 5, "b": 3}'
                ...             }
                ...         }
                ...     ]
                ... }
                >>> results = await processor.process(openai_output)

        **List[Dict] Input (Direct tool calls)**:
            Processes a list of individual tool call dictionaries.

            Example:
                >>> direct_calls = [
                ...     {"tool": "calculator", "arguments": {"a": 5, "b": 3}},
                ...     {"tool": "weather", "arguments": {"city": "London"}}
                ... ]
                >>> results = await processor.process(direct_calls)

        Args:
            data: Input data containing tool calls. Can be:
                - String: XML/text format (e.g., Anthropic Claude)
                - Dict: OpenAI tool_calls format
                - List[Dict]: Direct tool call list
            timeout: Optional timeout in seconds for tool execution.
                Overrides default_timeout if provided. Default: None
            use_cache: Whether to use cached results. If False, forces
                fresh execution even if cached results exist. Default: True
            request_id: Optional request ID for tracing and logging.
                If not provided, a UUID will be generated. Default: None
            context: Optional ExecutionContext for request-scoped data.
                Carries user_id, tenant_id, traceparent, deadline, etc.
                If provided, context.request_id takes precedence over request_id.
            return_order: Order to return results in. Can be:
                - "completion" (default): Results return as each tool completes
                - "submission": Results return in the same order as submitted

        Returns:
            List of ToolResult objects. Each result contains:
                - tool: Name of the tool that was executed
                - result: The tool's output (None if error)
                - error: Error message if execution failed (None if success)
                - duration: Execution time in seconds
                - cached: Whether result was retrieved from cache

            **Always returns a list** (never None), even if empty.

        Raises:
            ToolNotFoundError: If a tool is not registered in the registry
            ToolTimeoutError: If tool execution exceeds timeout
            ToolCircuitOpenError: If circuit breaker is open for a tool
            ToolRateLimitedError: If rate limit is exceeded

        Example:
            >>> async with ToolProcessor() as processor:
            ...     # Process Claude-style XML
            ...     results = await processor.process(
            ...         '<tool name="echo" args=\'{"message": "hello"}\'/>'
            ...     )
            ...
            ...     # Check results
            ...     for result in results:
            ...         if result.error:
            ...             print(f"Error: {result.error}")
            ...         else:
            ...             print(f"Success: {result.result}")
            ...             print(f"Duration: {result.duration}s")
            ...             print(f"From cache: {result.cached}")
        """
        # Ensure initialization
        await self.initialize()

        # Determine effective request_id (context takes precedence)
        effective_request_id = context.request_id if context else request_id

        # Determine effective timeout (context deadline takes precedence)
        effective_timeout = timeout
        if context and context.deadline:
            remaining = context.remaining_time
            if remaining is not None:
                # Use the smaller of explicit timeout and remaining deadline
                effective_timeout = min(effective_timeout, remaining) if effective_timeout is not None else remaining

        # Set up execution context scope if provided
        context_manager = execution_scope(context) if context else None

        # Create request context
        async with request_logging(effective_request_id):
            # Handle different input types
            if isinstance(data, str):
                # Text processing
                self.logger.debug(f"Processing text ({len(data)} chars)")
                calls = await self._extract_tool_calls(data)
            elif isinstance(data, dict):
                # Handle OpenAI format with tool_calls array
                if "tool_calls" in data and isinstance(data["tool_calls"], list):
                    calls = []
                    for tc in data["tool_calls"]:
                        if "function" in tc and isinstance(tc["function"], dict):
                            function = tc["function"]
                            name = function.get("name")
                            args_str = function.get("arguments", "{}")

                            # Parse arguments
                            try:
                                args = json.loads(args_str) if isinstance(args_str, str) else args_str
                            except json.JSONDecodeError:
                                args = {"raw": args_str}

                            if name:
                                # Build ToolCall kwargs, only include id if present
                                call_kwargs: dict[str, Any] = {
                                    "tool": name,
                                    "arguments": args,
                                }
                                if "id" in tc and tc["id"]:
                                    call_kwargs["id"] = tc["id"]
                                calls.append(ToolCall(**call_kwargs))
                else:
                    # Assume it's a single tool call
                    calls = [ToolCall(**data)]
            elif isinstance(data, list):
                # List of tool calls
                calls = [ToolCall(**tc) for tc in data]
            else:
                # Defensive: handle unexpected types at runtime
                # This shouldn't happen per type signature, but helps with debugging
                self.logger.warning(f"Unsupported input type: {type(data)}")  # type: ignore[unreachable]
                return []

            if not calls:
                self.logger.debug("No tool calls found")
                return []

            self.logger.debug(f"Found {len(calls)} tool calls")

            # Execute tool calls
            async with log_context_span("tool_execution", {"num_calls": len(calls)}):
                if self.registry is None:
                    raise RuntimeError("Registry not initialized. Call initialize() first.")
                if self.executor is None:
                    raise RuntimeError("Executor not initialized. Call initialize() first.")

                # Check if any tools are unknown - search across all namespaces
                unknown_tools = []
                all_tools = await self.registry.list_tools()  # Returns list of ToolInfo objects
                tool_names_in_registry = {tool.name for tool in all_tools}

                for call in calls:
                    if call.tool not in tool_names_in_registry:
                        unknown_tools.append(call.tool)

                if unknown_tools:
                    self.logger.debug(f"Unknown tools: {unknown_tools}")

                # Execute tools (with context scope if provided)
                async def _execute_with_context() -> list[ToolResult]:
                    if self.executor is None:
                        raise RuntimeError("Executor not initialized. Call initialize() first.")
                    # If return_order is specified and strategy supports it, call run() directly
                    # This bypasses the wrapper chain but preserves return_order semantics
                    if return_order is not None and self.strategy is not None and hasattr(self.strategy, "run"):
                        result: list[ToolResult] = await self.strategy.run(
                            calls, timeout=effective_timeout, return_order=return_order
                        )
                    else:
                        result = await self.executor.execute(calls, timeout=effective_timeout)
                    return result

                if context_manager:
                    async with context_manager:
                        results = await _execute_with_context()
                else:
                    results = await _execute_with_context()

                # Log metrics for each tool call
                for call, result in zip(calls, results, strict=False):
                    await log_tool_call(call, result)

                    # Record metrics
                    duration = (result.end_time - result.start_time).total_seconds()
                    await metrics.log_tool_execution(
                        tool=call.tool,
                        success=result.error is None,
                        duration=duration,
                        error=result.error,
                        cached=getattr(result, "cached", False),
                        attempts=getattr(result, "attempts", 1),
                    )

                return results

    async def process_text(
        self,
        text: str,
        timeout: float | None = None,
        use_cache: bool = True,
        request_id: str | None = None,
    ) -> list[ToolResult]:
        """
        Process text to extract and execute tool calls.

        Legacy alias for process() with string input.

        Args:
            text: Text to process.
            timeout: Optional timeout for execution.
            use_cache: Whether to use cached results.
            request_id: Optional request ID for logging.

        Returns:
            List of tool results.
        """
        return await self.process(
            data=text,
            timeout=timeout,
            use_cache=use_cache,
            request_id=request_id,
        )

    async def execute(
        self,
        calls: list[ToolCall],
        timeout: float | None = None,
        use_cache: bool = True,
    ) -> list[ToolResult]:
        """
        Execute a list of ToolCall objects directly.

        This is a lower-level method for executing tool calls when you already
        have parsed ToolCall objects. For most use cases, prefer process()
        which handles parsing automatically.

        Args:
            calls: List of ToolCall objects to execute. Each call must have:
                - tool: Name of the tool to execute
                - arguments: Dictionary of arguments for the tool
            timeout: Optional timeout in seconds for tool execution.
                Overrides default_timeout if provided. Default: None
            use_cache: Whether to use cached results. If False, forces
                fresh execution even if cached results exist. Default: True

        Returns:
            List of ToolResult objects, one per input ToolCall.
            **Always returns a list** (never None), even if empty.

            Each result contains:
                - tool: Name of the tool that was executed
                - result: The tool's output (None if error)
                - error: Error message if execution failed (None if success)
                - duration: Execution time in seconds
                - cached: Whether result was retrieved from cache

        Raises:
            RuntimeError: If processor is not initialized
            ToolNotFoundError: If a tool is not registered
            ToolTimeoutError: If tool execution exceeds timeout
            ToolCircuitOpenError: If circuit breaker is open
            ToolRateLimitedError: If rate limit is exceeded

        Example:
            >>> from chuk_tool_processor import ToolCall
            >>>
            >>> # Create tool calls directly
            >>> calls = [
            ...     ToolCall(tool="calculator", arguments={"a": 5, "b": 3}),
            ...     ToolCall(tool="weather", arguments={"city": "London"}),
            ... ]
            >>>
            >>> async with ToolProcessor() as processor:
            ...     results = await processor.execute(calls)
            ...     for result in results:
            ...         print(f"{result.tool}: {result.result}")
        """
        # Ensure initialization
        await self.initialize()

        # Safety check: ensure we have an executor
        if self.executor is None:
            raise RuntimeError("Executor not initialized. Call initialize() first.")

        # Execute with the configured executor
        results = await self.executor.execute(
            calls=calls,
            timeout=timeout,
            use_cache=use_cache,
        )

        # Ensure we always return a list (never None)
        return results if results is not None else []

    async def _extract_tool_calls(self, text: str) -> list[ToolCall]:
        """
        Extract tool calls from text using all available parsers.

        PERFORMANCE: Uses content sniffing to try most likely parser first,
        with early exit on success. Falls back to concurrent parsing if needed.

        Args:
            text: Text to parse.

        Returns:
            List of tool calls.
        """
        all_calls: list[ToolCall] = []

        # Try each parser
        async with log_context_span("parsing", {"text_length": len(text)}):
            # PERFORMANCE: Smart parser selection based on content hints
            # Most inputs match exactly ONE format, so try the obvious one first
            likely_parser = None

            # Quick content sniffing (cheap string checks)
            if '"tool_calls"' in text or '"function"' in text:
                # Likely OpenAI format
                likely_parser = next((p for p in self.parsers if "OpenAI" in p.__class__.__name__), None)
            elif text.strip().startswith("{") and ('"name"' in text and '"arguments"' in text):
                # Likely direct JSON tool format
                likely_parser = next((p for p in self.parsers if "Json" in p.__class__.__name__), None)
            elif "<tool" in text or "</tool>" in text:
                # Likely XML format
                likely_parser = next((p for p in self.parsers if "Xml" in p.__class__.__name__), None)

            # PERFORMANCE: Early exit path - try likely parser first
            if likely_parser:
                try:
                    result = await self._try_parser(likely_parser, text)
                    if result and isinstance(result, list) and len(result) > 0:
                        # Success! Return immediately without trying other parsers
                        all_calls.extend(result)
                        # Skip to deduplication
                        if len(all_calls) <= 1:
                            # Fast path: single call, no dedup needed
                            return all_calls
                        # Jump to dedup section
                        return self._deduplicate_calls(all_calls)
                except Exception:
                    # Failed, fall through to try all parsers
                    pass

            # PERFORMANCE: Fallback - try all parsers concurrently
            parse_tasks = []

            # Create parsing tasks
            for parser in self.parsers:
                parse_tasks.append(self._try_parser(parser, text))

            # Execute all parsers concurrently
            parser_results = await asyncio.gather(*parse_tasks, return_exceptions=True)

            # Collect successful results
            for result in parser_results:  # type: ignore[assignment]
                # Skip exceptions (return_exceptions=True gives us Exception | result)
                if isinstance(result, list):
                    # Type narrowing: result is list[ToolCall] here, not BaseException
                    all_calls.extend(result)

        # PERFORMANCE: Skip deduplication for single calls (common case)
        return self._deduplicate_calls(all_calls)

    def _deduplicate_calls(self, all_calls: list[ToolCall]) -> list[ToolCall]:
        """
        Remove duplicate tool calls from the list.

        PERFORMANCE: Fast path for single calls (no dedup needed).

        Args:
            all_calls: List of tool calls (may contain duplicates).

        Returns:
            List of unique tool calls.
        """
        # PERFORMANCE: Fast path - no deduplication needed for 0 or 1 calls
        if len(all_calls) <= 1:
            return all_calls

        # ------------------------------------------------------------------ #
        # Remove duplicates - use a stable digest instead of hashing a
        # frozenset of argument items (which breaks on unhashable types).
        # ------------------------------------------------------------------ #
        def _args_digest(args: dict[str, Any]) -> str:
            """Return a stable hash for any JSON-serialisable payload."""
            # Use stdlib json for consistent hashing across orjson/stdlib
            blob = stdlib_json.dumps(args, sort_keys=True, default=str)
            return hashlib.md5(blob.encode(), usedforsecurity=False).hexdigest()  # nosec B324

        unique_calls: dict[str, ToolCall] = {}
        for call in all_calls:
            key = f"{call.tool}:{_args_digest(call.arguments)}"
            unique_calls[key] = call

        return list(unique_calls.values())

    async def _try_parser(self, parser: Any, text: str) -> list[ToolCall]:
        """Try a single parser with metrics and logging."""
        parser_name = parser.__class__.__name__

        async with log_context_span(f"parser.{parser_name}", log_duration=True):
            start_time = time.time()

            try:
                # Try to parse
                calls: list[ToolCall] = await parser.try_parse(text)

                # Log success
                duration = time.time() - start_time
                await metrics.log_parser_metric(
                    parser=parser_name,
                    success=True,
                    duration=duration,
                    num_calls=len(calls),
                )

                return calls

            except Exception as e:
                # Log failure
                duration = time.time() - start_time
                await metrics.log_parser_metric(
                    parser=parser_name,
                    success=False,
                    duration=duration,
                    num_calls=0,
                )
                self.logger.debug(f"Parser {parser_name} failed: {str(e)}")
                return []

    # ------------------------------------------------------------------ #
    #  Tool discovery and introspection                                  #
    # ------------------------------------------------------------------ #
    async def list_tools(self) -> list[str]:
        """
        List all registered tool names.

        This method provides programmatic access to all tools in the registry.

        Returns:
            List of tool names (strings).

        Example:
            >>> async with ToolProcessor() as processor:
            ...     tools = await processor.list_tools()
            ...     for name in tools:
            ...         print(f"Available tool: {name}")

        Raises:
            RuntimeError: If processor is not initialized. Call initialize()
                or use the processor in a context manager.
        """
        await self.initialize()

        if self.registry is None:
            raise RuntimeError("Registry not initialized")

        # Get ToolInfo objects and extract names
        tools = await self.registry.list_tools()
        return [tool.name for tool in tools]

    async def get_tool_count(self) -> int:
        """
        Get the number of registered tools.

        Returns:
            Number of registered tools.

        Example:
            >>> async with ToolProcessor() as processor:
            ...     count = await processor.get_tool_count()
            ...     print(f"Total tools: {count}")
        """
        await self.initialize()

        if self.registry is None:
            raise RuntimeError("Registry not initialized")

        tool_tuples = await self.registry.list_tools()
        return len(tool_tuples)

    # ------------------------------------------------------------------ #
    #  Context manager support for automatic cleanup                     #
    # ------------------------------------------------------------------ #
    async def __aenter__(self):
        """Context manager entry - ensures initialization."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        await self.close()
        return False

    async def close(self) -> None:
        """
        Close the processor and clean up resources.

        This method ensures proper cleanup of executor resources, caches,
        and any other stateful components.
        """
        self.logger.debug("Closing tool processor")

        try:
            # Close the executor if it has a close method
            if self.executor and hasattr(self.executor, "close"):
                close_method = self.executor.close
                if inspect.iscoroutinefunction(close_method):
                    await close_method()
                elif callable(close_method):
                    close_method()

            # Close the strategy if it has a close method
            if self.strategy and hasattr(self.strategy, "close"):
                close_method = self.strategy.close
                if inspect.iscoroutinefunction(close_method):
                    await close_method()
                elif callable(close_method):
                    result = close_method()
                    # Check if the result is a coroutine and await it
                    if asyncio.iscoroutine(result):
                        await result

            # Clear cached results if using caching
            if self.enable_caching and self.executor:
                # Walk the executor chain to find the CachingToolExecutor
                current = self.executor
                while current:
                    if isinstance(current, CachingToolExecutor):
                        if hasattr(current.cache, "clear"):
                            clear_method = current.cache.clear
                            if inspect.iscoroutinefunction(clear_method):
                                await clear_method()
                            else:
                                clear_result = clear_method()
                                if asyncio.iscoroutine(clear_result):
                                    await clear_result
                        break
                    current = getattr(current, "executor", None)

            self.logger.debug("Tool processor closed successfully")

        except Exception as e:
            self.logger.error(f"Error during processor cleanup: {e}")


# Create a global processor instance
_global_processor: ToolProcessor | None = None
_processor_lock = asyncio.Lock()


async def get_default_processor() -> ToolProcessor:
    """Get or initialize the default global processor."""
    global _global_processor

    if _global_processor is None:
        async with _processor_lock:
            if _global_processor is None:
                _global_processor = ToolProcessor()
                await _global_processor.initialize()

    return _global_processor


async def process(
    data: str | dict[str, Any] | list[dict[str, Any]],
    timeout: float | None = None,
    use_cache: bool = True,
    request_id: str | None = None,
) -> list[ToolResult]:
    """
    Process tool calls with the default processor.

    Args:
        data: Input data (text, dict, or list of dicts)
        timeout: Optional timeout for execution
        use_cache: Whether to use cached results
        request_id: Optional request ID for logging

    Returns:
        List of tool results
    """
    processor = await get_default_processor()
    return await processor.process(
        data=data,
        timeout=timeout,
        use_cache=use_cache,
        request_id=request_id,
    )


async def process_text(
    text: str,
    timeout: float | None = None,
    use_cache: bool = True,
    request_id: str | None = None,
) -> list[ToolResult]:
    """
    Process text with the default processor.

    Legacy alias for backward compatibility.

    Args:
        text: Text to process.
        timeout: Optional timeout for execution.
        use_cache: Whether to use cached results.
        request_id: Optional request ID for logging.

    Returns:
        List of tool results.
    """
    processor = await get_default_processor()
    return await processor.process_text(
        text=text,
        timeout=timeout,
        use_cache=use_cache,
        request_id=request_id,
    )
