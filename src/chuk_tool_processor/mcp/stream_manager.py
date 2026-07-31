# chuk_tool_processor/mcp/stream_manager.py
"""
StreamManager for CHUK Tool Processor - Enhanced with robust shutdown handling and headers support

Supports optional middleware for:
- Retry with exponential backoff
- Circuit breaker pattern
- Rate limiting

The bulk of the behaviour lives in focused mixins:
- :mod:`._stream_manager_init` - per-transport initialisation
- :mod:`._stream_manager_resources` - tool/resource/prompt queries
- :mod:`._stream_manager_lifecycle` - shutdown, streams, health/reconnect
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.transport import (
    HTTPStreamableTransport,
    MCPBaseTransport,
    SSETransport,
    StdioTransport,
    TimeoutConfig,
)
from chuk_tool_processor.mcp.transport.models import MCPToolDefinition, ServerInfo

from ._stream_manager_init import StreamManagerInitMixin
from ._stream_manager_lifecycle import StreamManagerLifecycleMixin
from ._stream_manager_resources import StreamManagerResourcesMixin

if TYPE_CHECKING:
    from chuk_tool_processor.mcp.middleware import MiddlewareConfig, MiddlewareStack

logger = get_logger("chuk_tool_processor.mcp.stream_manager")

# Keep the transport symbols importable from this module for backwards
# compatibility with anything patching ``stream_manager.<Transport>``.
__all__ = ["StreamManager"]

_ = (HTTPStreamableTransport, SSETransport, StdioTransport)  # re-exported for patching


class StreamManager(StreamManagerInitMixin, StreamManagerResourcesMixin, StreamManagerLifecycleMixin):
    """
    Manager for MCP server streams with support for multiple transport types.

    Enhanced with robust shutdown handling and proper headers support.

    Updated to support the latest transports:
    - STDIO (process-based)
    - SSE (Server-Sent Events) with headers support
    - HTTP Streamable (modern replacement for SSE, spec 2025-11-25) with graceful headers handling

    Supports optional middleware for production-grade tool execution:
    - Retry with exponential backoff and deadline-aware timeouts
    - Circuit breaker to prevent cascading failures
    - Rate limiting (global and per-tool)

    Example with middleware:
        from chuk_tool_processor.mcp.middleware import MiddlewareConfig

        config = MiddlewareConfig(
            retry_enabled=True,
            retry_max_retries=3,
            circuit_breaker_enabled=True,
        )
        sm = StreamManager(middleware_config=config)
    """

    def __init__(
        self,
        timeout_config: TimeoutConfig | None = None,
        middleware_config: MiddlewareConfig | None = None,
    ) -> None:
        self.transports: dict[str, MCPBaseTransport] = {}
        self.server_info: list[ServerInfo] = []
        # Default routing map (tool name -> server). First server to advertise a
        # name owns it; see _register_tools for the first-wins collision policy.
        self.tool_to_server_map: dict[str, str] = {}
        # Every server that advertised each tool name, in registration order.
        # Lets callers detect/resolve name collisions instead of silently
        # inheriting whichever server registered last.
        self.tool_to_servers: dict[str, list[str]] = {}
        self.server_names: dict[int, str] = {}
        self.all_tools: list[MCPToolDefinition] = []
        self._lock = asyncio.Lock()
        self._closed = False  # Track if we've been closed
        self.timeout_config = timeout_config or TimeoutConfig()

        # Middleware support
        self._middleware_config = middleware_config
        self._middleware_stack: MiddlewareStack | None = None

    # ------------------------------------------------------------------ #
    #  factory helpers with enhanced error handling                      #
    # ------------------------------------------------------------------ #
    @classmethod
    async def create(
        cls,
        config_file: str,
        servers: list[str],
        server_names: dict[int, str] | None = None,
        transport_type: str = "stdio",
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,  # NEW: Timeout for entire initialization
    ) -> StreamManager:
        """Create StreamManager with timeout protection."""
        inst = cls()
        await inst.initialize(
            config_file,
            servers,
            server_names,
            transport_type,
            default_timeout=default_timeout,
            initialization_timeout=initialization_timeout,
        )
        return inst

    @classmethod
    async def create_with_sse(
        cls,
        servers: list[dict[str, str]],
        server_names: dict[int, str] | None = None,
        connection_timeout: float = 10.0,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,  # NEW
        oauth_refresh_callback: any | None = None,  # NEW: OAuth token refresh callback
    ) -> StreamManager:
        """Create StreamManager with SSE transport and timeout protection."""
        inst = cls()
        await inst.initialize_with_sse(
            servers,
            server_names,
            connection_timeout=connection_timeout,
            default_timeout=default_timeout,
            initialization_timeout=initialization_timeout,
            oauth_refresh_callback=oauth_refresh_callback,  # NEW: Pass OAuth callback
        )
        return inst

    @classmethod
    async def create_with_stdio(
        cls,
        servers: list[dict[str, Any]],
        server_names: dict[int, str] | None = None,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,
    ) -> StreamManager:
        """Create StreamManager with STDIO transport and timeout protection (no config file needed)."""
        inst = cls()
        await inst.initialize_with_stdio(
            servers,
            server_names,
            default_timeout=default_timeout,
            initialization_timeout=initialization_timeout,
        )
        return inst

    @classmethod
    async def create_with_http_streamable(
        cls,
        servers: list[dict[str, str]],
        server_names: dict[int, str] | None = None,
        connection_timeout: float = 30.0,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,  # NEW
        oauth_refresh_callback: any | None = None,  # NEW: OAuth token refresh callback
    ) -> StreamManager:
        """Create StreamManager with HTTP Streamable transport and timeout protection."""
        inst = cls()
        await inst.initialize_with_http_streamable(
            servers,
            server_names,
            connection_timeout=connection_timeout,
            default_timeout=default_timeout,
            initialization_timeout=initialization_timeout,
            oauth_refresh_callback=oauth_refresh_callback,  # NEW: Pass OAuth callback
        )
        return inst

    # ------------------------------------------------------------------ #
    #  Context manager support for automatic cleanup                     #
    # ------------------------------------------------------------------ #
    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        await self.close()

    @classmethod
    @asynccontextmanager
    async def create_managed(
        cls,
        config_file: str,
        servers: list[str],
        server_names: dict[int, str] | None = None,
        transport_type: str = "stdio",
        default_timeout: float = 30.0,
    ):
        """Context manager factory for automatic cleanup."""
        stream_manager = None
        try:
            stream_manager = await cls.create(
                config_file=config_file,
                servers=servers,
                server_names=server_names,
                transport_type=transport_type,
                default_timeout=default_timeout,
            )
            yield stream_manager
        finally:
            if stream_manager:
                await stream_manager.close()

    # ------------------------------------------------------------------ #
    #  tool registry / queries                                           #
    # ------------------------------------------------------------------ #
    def get_all_tools(self) -> list[dict[str, Any]]:
        return [t.model_dump() for t in self.all_tools]

    def _register_tools(self, tools: list[MCPToolDefinition], server_name: str) -> None:
        """Map tool names to their owning server using a first-wins policy.

        The first server to advertise a given tool name owns it for default
        (unpinned) routing. If a later server advertises the same name, we keep
        the original owner and log a prominent warning rather than silently
        rerouting every future call to the newcomer — a bare tool name is not a
        trust boundary, so a second server (malicious, compromised, or merely
        reusing a common name like ``read_file``) must not be able to hijack a
        name an earlier, trusted server already provides. Callers can still reach
        the shadowed tool deliberately via ``call_tool(name, server_name=...)``.
        """
        for t in tools:
            if not t.name:
                continue
            providers = self.tool_to_servers.setdefault(t.name, [])
            if server_name not in providers:
                providers.append(server_name)
            owner = self.tool_to_server_map.get(t.name)
            if owner is None:
                self.tool_to_server_map[t.name] = server_name
            elif owner != server_name:
                logger.warning(
                    "MCP tool name collision: '%s' is already provided by server '%s'; "
                    "ignoring the tool of the same name from server '%s' for default routing. "
                    "Call it explicitly with server_name='%s' if you meant that server.",
                    t.name,
                    owner,
                    server_name,
                    server_name,
                )

    def get_server_for_tool(self, tool_name: str) -> str | None:
        return self.tool_to_server_map.get(tool_name)

    def get_servers_for_tool(self, tool_name: str) -> list[str]:
        """All servers that advertised ``tool_name``, in registration order.

        More than one entry means the name is shadowed; only the first owns
        default routing (see :meth:`_register_tools`).
        """
        return list(self.tool_to_servers.get(tool_name, []))

    def get_tool_collisions(self) -> dict[str, list[str]]:
        """Tool names advertised by more than one server, mapped to those servers.

        Empty when there are no collisions. The first server in each list is the
        one that receives unpinned calls; the rest are reachable only by passing
        ``server_name`` explicitly to :meth:`call_tool`.
        """
        return {name: list(servers) for name, servers in self.tool_to_servers.items() if len(servers) > 1}

    def get_server_info(self) -> list[dict[str, Any]]:
        return [s.model_dump() for s in self.server_info]

    def set_session_id(self, session_id: str | None) -> None:
        """
        Set the session ID on all HTTP/SSE transports.

        This allows dynamically updating the session ID at runtime,
        which is useful when the session ID is only known after agent initialization.

        Args:
            session_id: Session ID to set, or None to clear it
        """
        for name, transport in self.transports.items():
            if hasattr(transport, "set_session_id"):
                transport.set_session_id(session_id)
                logger.debug("Set session ID for transport %s", name)

    # ------------------------------------------------------------------ #
    #  tool execution                                                    #
    # ------------------------------------------------------------------ #
    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        server_name: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Call a tool on the appropriate server with timeout and optional middleware support.

        When middleware is configured (retry, circuit breaker, rate limiting), tool calls
        are executed through the middleware stack. Otherwise, direct transport execution
        is used.

        Args:
            tool_name: Name of the tool to call
            arguments: Tool arguments as a dictionary
            server_name: Optional server name (auto-detected if not provided)
            timeout: Optional timeout in seconds (used as deadline budget with middleware)

        Returns:
            dict with either result data or {"isError": True, "error": "..."} on failure
        """
        if self._closed:
            return {
                "isError": True,
                "error": "StreamManager is closed",
            }

        # Use middleware stack if configured
        if self._middleware_stack is not None:
            return await self._middleware_stack.call_tool(tool_name, arguments, timeout)

        # Direct execution (no middleware)
        return await self._direct_call_tool(tool_name, arguments, server_name, timeout)

    async def _direct_call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        server_name: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Direct tool execution without middleware (internal use)."""
        server_name = server_name or self.get_server_for_tool(tool_name)
        if not server_name or server_name not in self.transports:
            return {
                "isError": True,
                "error": f"No server found for tool: {tool_name}",
            }

        transport = self.transports[server_name]

        if timeout is not None:
            logger.debug("Calling tool '%s' with %ss timeout", tool_name, timeout)
            try:
                if hasattr(transport, "call_tool"):
                    import inspect

                    sig = inspect.signature(transport.call_tool)
                    if "timeout" in sig.parameters:
                        return await transport.call_tool(tool_name, arguments, timeout=timeout)
                    else:
                        return await asyncio.wait_for(transport.call_tool(tool_name, arguments), timeout=timeout)
                else:
                    return await asyncio.wait_for(transport.call_tool(tool_name, arguments), timeout=timeout)
            except TimeoutError:
                logger.warning("Tool '%s' timed out after %ss", tool_name, timeout)
                return {
                    "isError": True,
                    "error": f"Tool call timed out after {timeout}s",
                }
        else:
            return await transport.call_tool(tool_name, arguments)

    # ------------------------------------------------------------------ #
    #  middleware                                                        #
    # ------------------------------------------------------------------ #
    def enable_middleware(self, config: MiddlewareConfig | None = None) -> None:
        """Enable middleware with the given configuration.

        Can be called after initialization to enable or reconfigure middleware.

        Args:
            config: Middleware configuration (uses defaults if None)
        """
        from chuk_tool_processor.mcp.middleware import MiddlewareConfig, MiddlewareStack

        self._middleware_config = config or MiddlewareConfig()
        self._middleware_stack = MiddlewareStack(self, self._middleware_config)
        status = self._middleware_stack.get_status()
        logger.info(
            "Middleware enabled: retry=%s, circuit_breaker=%s, rate_limiting=%s",
            status.retry is not None,
            status.circuit_breaker is not None,
            status.rate_limiting is not None,
        )

    def disable_middleware(self) -> None:
        """Disable middleware, returning to direct transport execution."""
        self._middleware_stack = None
        self._middleware_config = None
        logger.info("Middleware disabled")

    def get_middleware_status(self) -> Any:
        """Get middleware status for diagnostics.

        Returns:
            MiddlewareStatus model or None if middleware is not enabled
        """
        if self._middleware_stack is None:
            return None
        return self._middleware_stack.get_status()

    @property
    def middleware_enabled(self) -> bool:
        """Check if middleware is enabled."""
        return self._middleware_stack is not None
