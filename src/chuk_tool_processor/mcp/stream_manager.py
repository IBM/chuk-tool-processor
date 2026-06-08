# chuk_tool_processor/mcp/stream_manager.py
"""
StreamManager for CHUK Tool Processor - Enhanced with robust shutdown handling and headers support

Supports optional middleware for:
- Retry with exponential backoff
- Circuit breaker pattern
- Rate limiting
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

# --------------------------------------------------------------------------- #
#  CHUK imports                                                               #
# --------------------------------------------------------------------------- #
from chuk_mcp.config import load_config  # type: ignore[import-untyped]

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.models import MCPTransport
from chuk_tool_processor.mcp.transport import (
    HTTPStreamableTransport,
    MCPBaseTransport,
    SSETransport,
    StdioTransport,
    TimeoutConfig,
)
from chuk_tool_processor.mcp.transport.models import MCPToolDefinition, ServerInfo

if TYPE_CHECKING:
    from chuk_tool_processor.mcp.middleware import MiddlewareConfig, MiddlewareStack

logger = get_logger("chuk_tool_processor.mcp.stream_manager")


class StreamManager:
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
        self.tool_to_server_map: dict[str, str] = {}
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
    #  NEW: Context manager support for automatic cleanup               #
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
    #  initialisation - stdio / sse / http_streamable                    #
    # ------------------------------------------------------------------ #
    async def initialize(
        self,
        config_file: str,
        servers: list[str],
        server_names: dict[int, str] | None = None,
        transport_type: str = "stdio",
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,
    ) -> None:
        """Initialize with graceful headers handling for all transport types."""
        if self._closed:
            raise RuntimeError("Cannot initialize a closed StreamManager")

        # Convert transport_type string to enum
        try:
            transport_enum = MCPTransport(transport_type)
        except ValueError:
            logger.error("Unsupported transport type: %s", transport_type)
            return

        async with self._lock:
            self.server_names = server_names or {}

            for idx, server_name in enumerate(servers):
                try:
                    if transport_enum == MCPTransport.STDIO:
                        params, server_timeout = await load_config(config_file, server_name)
                        # Use per-server timeout if specified, otherwise use global default
                        effective_timeout = server_timeout if server_timeout is not None else default_timeout
                        logger.debug(
                            f"Server '{server_name}' using timeout: {effective_timeout}s (per-server: {server_timeout}, default: {default_timeout})"
                        )
                        # Use initialization_timeout for connection_timeout since subprocess
                        # launch can take time (e.g., uvx downloading packages)
                        transport: MCPBaseTransport = StdioTransport(
                            params, connection_timeout=initialization_timeout, default_timeout=effective_timeout
                        )
                    elif transport_enum == MCPTransport.SSE:
                        logger.debug(
                            "Using SSE transport in initialize() - consider using initialize_with_sse() instead"
                        )
                        params, server_timeout = await load_config(config_file, server_name)
                        # Use per-server timeout if specified, otherwise use global default
                        effective_timeout = server_timeout if server_timeout is not None else default_timeout

                        if isinstance(params, dict) and "url" in params:
                            sse_url = params["url"]
                            api_key = params.get("api_key")
                            headers = params.get("headers", {})
                        else:
                            sse_url = "http://localhost:8000"
                            api_key = None
                            headers = {}
                            logger.debug("No URL configured for SSE transport, using default: %s", sse_url)

                        # Build SSE transport with optional headers
                        transport_params = {"url": sse_url, "api_key": api_key, "default_timeout": effective_timeout}
                        if headers:
                            transport_params["headers"] = headers

                        transport = SSETransport(**transport_params)

                    elif transport_enum in (MCPTransport.HTTP, MCPTransport.HTTP_STREAMABLE):
                        logger.debug(
                            "Using HTTP Streamable transport in initialize() - consider using initialize_with_http_streamable() instead"
                        )
                        params, server_timeout = await load_config(config_file, server_name)
                        # Use per-server timeout if specified, otherwise use global default
                        effective_timeout = server_timeout if server_timeout is not None else default_timeout

                        if isinstance(params, dict) and "url" in params:
                            http_url = params["url"]
                            api_key = params.get("api_key")
                            headers = params.get("headers", {})
                            session_id = params.get("session_id")
                        else:
                            http_url = "http://localhost:8000"
                            api_key = None
                            headers = {}
                            session_id = None
                            logger.debug("No URL configured for HTTP Streamable transport, using default: %s", http_url)

                        # IMPORTANT: If transport already exists for this server, preserve its session ID
                        if server_name in self.transports:
                            existing_transport = self.transports[server_name]
                            if hasattr(existing_transport, "session_id") and existing_transport.session_id:
                                session_id = existing_transport.session_id
                                logger.debug(f"Preserving session ID for {server_name}: {session_id}")

                        # Build HTTP transport (headers not supported yet)
                        transport_params = {
                            "url": http_url,
                            "api_key": api_key,
                            "default_timeout": effective_timeout,
                            "session_id": session_id,
                        }
                        # Note: headers not added until HTTPStreamableTransport supports them
                        if headers:
                            logger.debug("Headers provided but not supported in HTTPStreamableTransport yet")

                        transport = HTTPStreamableTransport(**transport_params)

                    else:
                        logger.error("Unsupported transport type: %s", transport_type)
                        continue

                    # Initialize with timeout protection
                    try:
                        if not await asyncio.wait_for(transport.initialize(), timeout=initialization_timeout):
                            logger.warning("Failed to init %s", server_name)
                            continue
                    except TimeoutError:
                        logger.error("Timeout initialising %s (timeout=%ss)", server_name, initialization_timeout)
                        continue

                    self.transports[server_name] = transport

                    # Ping and get tools with timeout protection (use longer timeouts for slow servers)
                    status = (
                        "Up"
                        if await asyncio.wait_for(transport.send_ping(), timeout=self.timeout_config.operation)
                        else "Down"
                    )
                    raw_tools = await asyncio.wait_for(transport.get_tools(), timeout=self.timeout_config.operation)
                    tools = [MCPToolDefinition.model_validate(t) for t in raw_tools]

                    for t in tools:
                        if t.name:
                            self.tool_to_server_map[t.name] = server_name
                    self.all_tools.extend(tools)

                    self.server_info.append(ServerInfo(id=idx, name=server_name, tools=len(tools), status=status))
                    logger.debug("Initialised %s - %d tool(s)", server_name, len(tools))
                except TimeoutError:
                    logger.error("Timeout initialising %s", server_name)
                except Exception as exc:
                    logger.error("Error initialising %s: %s", server_name, exc)

            logger.debug(
                "StreamManager ready - %d server(s), %d tool(s)",
                len(self.transports),
                len(self.all_tools),
            )

    async def initialize_with_sse(
        self,
        servers: list[dict[str, str]],
        server_names: dict[int, str] | None = None,
        connection_timeout: float = 10.0,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,
        oauth_refresh_callback: any | None = None,  # NEW: OAuth token refresh callback
    ) -> None:
        """Initialize with SSE transport with optional headers support."""
        if self._closed:
            raise RuntimeError("Cannot initialize a closed StreamManager")

        async with self._lock:
            self.server_names = server_names or {}

            for idx, cfg in enumerate(servers):
                name, url = cfg.get("name"), cfg.get("url")
                if not (name and url):
                    logger.error("Bad server config: %s", cfg)
                    continue
                try:
                    # Build SSE transport parameters with optional headers
                    transport_params = {
                        "url": url,
                        "api_key": cfg.get("api_key"),
                        "connection_timeout": connection_timeout,
                        "default_timeout": default_timeout,
                    }

                    # Add headers if provided
                    headers = cfg.get("headers", {})
                    if headers:
                        logger.debug("SSE %s: Using configured headers: %s", name, list(headers.keys()))
                        transport_params["headers"] = headers

                    # Add OAuth refresh callback if provided (NEW)
                    if oauth_refresh_callback:
                        transport_params["oauth_refresh_callback"] = oauth_refresh_callback
                        logger.debug("SSE %s: OAuth refresh callback configured", name)

                    transport = SSETransport(**transport_params)

                    try:
                        if not await asyncio.wait_for(transport.initialize(), timeout=initialization_timeout):
                            logger.warning("Failed to init SSE %s", name)
                            continue
                    except TimeoutError:
                        logger.error("Timeout initialising SSE %s (timeout=%ss)", name, initialization_timeout)
                        continue

                    self.transports[name] = transport
                    # Use longer timeouts for slow servers (ping can take time after initialization)
                    status = (
                        "Up"
                        if await asyncio.wait_for(transport.send_ping(), timeout=self.timeout_config.operation)
                        else "Down"
                    )
                    raw_tools = await asyncio.wait_for(transport.get_tools(), timeout=self.timeout_config.operation)
                    tools = [MCPToolDefinition.model_validate(t) for t in raw_tools]

                    for t in tools:
                        if t.name:
                            self.tool_to_server_map[t.name] = name
                    self.all_tools.extend(tools)

                    self.server_info.append(ServerInfo(id=idx, name=name, tools=len(tools), status=status))
                    logger.debug("Initialised SSE %s - %d tool(s)", name, len(tools))
                except TimeoutError:
                    logger.error("Timeout initialising SSE %s", name)
                except Exception as exc:
                    logger.error("Error initialising SSE %s: %s", name, exc)

            logger.debug(
                "StreamManager ready - %d SSE server(s), %d tool(s)",
                len(self.transports),
                len(self.all_tools),
            )

    async def initialize_with_stdio(
        self,
        servers: list[dict[str, Any]],
        server_names: dict[int, str] | None = None,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,
    ) -> None:
        """Initialize with STDIO transport directly from server configs (no config file needed)."""
        if self._closed:
            raise RuntimeError("Cannot initialize a closed StreamManager")

        async with self._lock:
            self.server_names = server_names or {}

            for idx, cfg in enumerate(servers):
                name = cfg.get("name")
                command = cfg.get("command")
                args = cfg.get("args", [])
                env = cfg.get("env")

                if not (name and command):
                    logger.error("Bad STDIO server config (missing name or command): %s", cfg)
                    continue

                try:
                    # Build STDIO transport parameters
                    transport_params = {
                        "command": command,
                        "args": args,
                    }
                    if env:
                        transport_params["env"] = env

                    logger.debug("STDIO %s: command=%s, args=%s", name, command, args)

                    transport = StdioTransport(
                        transport_params, connection_timeout=initialization_timeout, default_timeout=default_timeout
                    )

                    try:
                        if not await asyncio.wait_for(transport.initialize(), timeout=initialization_timeout):
                            logger.warning("Failed to init STDIO %s", name)
                            continue
                    except TimeoutError:
                        logger.error("Timeout initialising STDIO %s (timeout=%ss)", name, initialization_timeout)
                        continue

                    self.transports[name] = transport

                    # Ping and get tools with timeout protection
                    status = (
                        "Up"
                        if await asyncio.wait_for(transport.send_ping(), timeout=self.timeout_config.operation)
                        else "Down"
                    )
                    raw_tools = await asyncio.wait_for(transport.get_tools(), timeout=self.timeout_config.operation)
                    tools = [MCPToolDefinition.model_validate(t) for t in raw_tools]

                    for t in tools:
                        if t.name:
                            self.tool_to_server_map[t.name] = name
                    self.all_tools.extend(tools)

                    self.server_info.append(ServerInfo(id=idx, name=name, tools=len(tools), status=status))
                    logger.debug("Initialised STDIO %s - %d tool(s)", name, len(tools))
                except TimeoutError:
                    logger.error("Timeout initialising STDIO %s", name)
                except Exception as exc:
                    logger.error("Error initialising STDIO %s: %s", name, exc)

            logger.debug(
                "StreamManager ready - %d STDIO server(s), %d tool(s)",
                len(self.transports),
                len(self.all_tools),
            )

    async def initialize_with_http_streamable(
        self,
        servers: list[dict[str, str]],
        server_names: dict[int, str] | None = None,
        connection_timeout: float = 30.0,
        default_timeout: float = 30.0,
        initialization_timeout: float = 60.0,
        oauth_refresh_callback: any | None = None,  # NEW: OAuth token refresh callback
    ) -> None:
        """Initialize with HTTP Streamable transport with graceful headers handling."""
        if self._closed:
            raise RuntimeError("Cannot initialize a closed StreamManager")

        logger.debug(f"initialize_with_http_streamable: initialization_timeout={initialization_timeout}")

        async with self._lock:
            self.server_names = server_names or {}

            for idx, cfg in enumerate(servers):
                name, url = cfg.get("name"), cfg.get("url")
                if not (name and url):
                    logger.error("Bad server config: %s", cfg)
                    continue
                try:
                    # Build HTTP Streamable transport parameters
                    transport_params = {
                        "url": url,
                        "api_key": cfg.get("api_key"),
                        "connection_timeout": connection_timeout,
                        "default_timeout": default_timeout,
                        "session_id": cfg.get("session_id"),
                    }

                    # Handle headers if provided
                    headers = cfg.get("headers", {})
                    if headers:
                        transport_params["headers"] = headers
                        logger.debug("HTTP Streamable %s: Custom headers configured: %s", name, list(headers.keys()))

                    # Add OAuth refresh callback if provided (NEW)
                    if oauth_refresh_callback:
                        transport_params["oauth_refresh_callback"] = oauth_refresh_callback
                        logger.debug("HTTP Streamable %s: OAuth refresh callback configured", name)

                    transport = HTTPStreamableTransport(**transport_params)

                    logger.debug(f"Calling transport.initialize() for {name} with timeout={initialization_timeout}s")
                    try:
                        if not await asyncio.wait_for(transport.initialize(), timeout=initialization_timeout):
                            logger.warning("Failed to init HTTP Streamable %s", name)
                            continue
                    except TimeoutError:
                        logger.error(
                            "Timeout initialising HTTP Streamable %s (timeout=%ss)", name, initialization_timeout
                        )
                        continue
                    logger.debug(f"Successfully initialized {name}")

                    self.transports[name] = transport
                    # Use longer timeouts for slow servers (ping can take time after initialization)
                    status = (
                        "Up"
                        if await asyncio.wait_for(transport.send_ping(), timeout=self.timeout_config.operation)
                        else "Down"
                    )
                    raw_tools = await asyncio.wait_for(transport.get_tools(), timeout=self.timeout_config.operation)
                    tools = [MCPToolDefinition.model_validate(t) for t in raw_tools]

                    for t in tools:
                        if t.name:
                            self.tool_to_server_map[t.name] = name
                    self.all_tools.extend(tools)

                    self.server_info.append(ServerInfo(id=idx, name=name, tools=len(tools), status=status))
                    logger.debug("Initialised HTTP Streamable %s - %d tool(s)", name, len(tools))
                except TimeoutError:
                    logger.error("Timeout initialising HTTP Streamable %s", name)
                except Exception as exc:
                    logger.error("Error initialising HTTP Streamable %s: %s", name, exc)

            logger.debug(
                "StreamManager ready - %d HTTP Streamable server(s), %d tool(s)",
                len(self.transports),
                len(self.all_tools),
            )

    # ------------------------------------------------------------------ #
    #  queries                                                           #
    # ------------------------------------------------------------------ #
    def get_all_tools(self) -> list[dict[str, Any]]:
        return [t.model_dump() for t in self.all_tools]

    def get_server_for_tool(self, tool_name: str) -> str | None:
        return self.tool_to_server_map.get(tool_name)

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

    async def list_tools(self, server_name: str) -> list[dict[str, Any]]:
        """List all tools available from a specific server."""
        if self._closed:
            logger.warning("Cannot list tools: StreamManager is closed")
            return []

        if server_name not in self.transports:
            logger.error("Server '%s' not found in transports", server_name)
            return []

        transport = self.transports[server_name]

        try:
            tools = await asyncio.wait_for(transport.get_tools(), timeout=self.timeout_config.operation)
            logger.debug("Found %d tools for server %s", len(tools), server_name)
            return tools
        except TimeoutError:
            logger.error("Timeout listing tools for server %s", server_name)
            return []
        except Exception as e:
            logger.error("Error listing tools for server %s: %s", server_name, e)
            return []

    # ------------------------------------------------------------------ #
    #  EXTRA HELPERS - ping / resources / prompts                        #
    # ------------------------------------------------------------------ #
    async def ping_servers(self) -> list[dict[str, Any]]:
        if self._closed:
            return []

        async def _ping_one(name: str, tr: MCPBaseTransport):
            try:
                ok = await asyncio.wait_for(tr.send_ping(), timeout=self.timeout_config.quick)
            except Exception:
                ok = False
            return {"server": name, "ok": ok}

        return await asyncio.gather(*(_ping_one(n, t) for n, t in self.transports.items()), return_exceptions=True)

    async def list_resources(self) -> list[dict[str, Any]]:
        if self._closed:
            return []

        out: list[dict[str, Any]] = []

        async def _one(name: str, tr: MCPBaseTransport):
            try:
                res = await asyncio.wait_for(tr.list_resources(), timeout=self.timeout_config.operation)
                resources = res.get("resources", []) if isinstance(res, dict) else res
                for item in resources:
                    item = dict(item)
                    item["server"] = name
                    out.append(item)
            except Exception as exc:
                logger.debug("resources/list failed for %s: %s", name, exc)

        await asyncio.gather(*(_one(n, t) for n, t in self.transports.items()), return_exceptions=True)
        return out

    async def read_resource(self, uri: str, server_name: str | None = None) -> dict[str, Any]:
        """Read a specific resource by URI.

        Args:
            uri: Resource URI to read (e.g. ui://tool/app.html)
            server_name: Optional server name to target. If None, tries all.

        Returns:
            Resource content dict from the server, or empty dict on error.
        """
        if self._closed:
            return {}

        # If a specific server is requested, try just that one
        if server_name and server_name in self.transports:
            transport = self.transports[server_name]
            if hasattr(transport, "read_resource"):
                try:
                    return await asyncio.wait_for(
                        transport.read_resource(uri),
                        timeout=self.timeout_config.operation,
                    )
                except Exception as exc:
                    logger.debug("resources/read failed for %s: %s", server_name, exc)
                    return {}

        # Try all transports until one succeeds
        for name, transport in self.transports.items():
            if not hasattr(transport, "read_resource"):
                continue
            try:
                result = await asyncio.wait_for(
                    transport.read_resource(uri),
                    timeout=self.timeout_config.operation,
                )
                if result:
                    return result
            except Exception as exc:
                logger.debug("resources/read failed for %s: %s", name, exc)
        return {}

    async def get_prompt(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        server_name: str | None = None,
    ) -> dict[str, Any]:
        """Get a specific prompt by name.

        Args:
            name: Prompt name to fetch.
            arguments: Optional arguments used to render the prompt template.
            server_name: Optional server name to target. If None, tries all.

        Returns:
            Prompt result dict from the server, or empty dict on error.
        """
        if self._closed:
            return {}

        # If a specific server is requested, try just that one
        if server_name and server_name in self.transports:
            transport = self.transports[server_name]
            if hasattr(transport, "get_prompt"):
                try:
                    return await asyncio.wait_for(
                        transport.get_prompt(name, arguments),
                        timeout=self.timeout_config.operation,
                    )
                except Exception as exc:
                    logger.debug("prompts/get failed for %s: %s", server_name, exc)
                    return {}

        # Try all transports until one succeeds
        for tname, transport in self.transports.items():
            if not hasattr(transport, "get_prompt"):
                continue
            try:
                result = await asyncio.wait_for(
                    transport.get_prompt(name, arguments),
                    timeout=self.timeout_config.operation,
                )
                if result:
                    return result
            except Exception as exc:
                logger.debug("prompts/get failed for %s: %s", tname, exc)
        return {}

    async def list_prompts(self) -> list[dict[str, Any]]:
        if self._closed:
            return []

        out: list[dict[str, Any]] = []

        async def _one(name: str, tr: MCPBaseTransport):
            try:
                res = await asyncio.wait_for(tr.list_prompts(), timeout=self.timeout_config.operation)
                prompts = res.get("prompts", []) if isinstance(res, dict) else res
                for item in prompts:
                    item = dict(item)
                    item["server"] = name
                    out.append(item)
            except Exception as exc:
                logger.debug("prompts/list failed for %s: %s", name, exc)

        await asyncio.gather(*(_one(n, t) for n, t in self.transports.items()), return_exceptions=True)
        return out

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

    # ------------------------------------------------------------------ #
    #  ENHANCED shutdown with robust error handling                      #
    # ------------------------------------------------------------------ #
    async def close(self) -> None:
        """
        Close all transports safely with enhanced error handling.

        ENHANCED: Uses asyncio.shield() to protect critical cleanup and
        provides multiple fallback strategies for different failure modes.
        """
        if self._closed:
            logger.debug("StreamManager already closed")
            return

        if not self.transports:
            logger.debug("No transports to close")
            self._closed = True
            return

        logger.debug("Closing %d transports...", len(self.transports))

        try:
            # Use shield to protect the cleanup operation from cancellation
            await asyncio.shield(self._do_close_all_transports())
        except asyncio.CancelledError:
            # If shield fails (rare), fall back to synchronous cleanup
            logger.debug("Close operation cancelled, performing synchronous cleanup")
            self._sync_cleanup()
        except Exception as e:
            logger.debug("Error during close: %s", e)
            self._sync_cleanup()
        finally:
            self._closed = True

    async def _do_close_all_transports(self) -> None:
        """Protected cleanup implementation with multiple strategies."""
        close_results = []
        transport_items = list(self.transports.items())

        # Strategy 1: Try concurrent close with timeout
        try:
            await self._concurrent_close(transport_items, close_results)
        except Exception as e:
            logger.debug("Concurrent close failed: %s, falling back to sequential close", e)
            # Strategy 2: Fall back to sequential close
            await self._sequential_close(transport_items, close_results)

        # Always clean up state
        self._cleanup_state()

        # Log summary
        if close_results:
            successful_closes = sum(1 for _, success, _ in close_results if success)
            logger.debug("Transport cleanup: %d/%d closed successfully", successful_closes, len(close_results))

    async def _concurrent_close(self, transport_items: list[tuple[str, MCPBaseTransport]], close_results: list) -> None:
        """Try to close all transports concurrently."""
        close_tasks = []
        for name, transport in transport_items:
            task = asyncio.create_task(self._close_single_transport(name, transport), name=f"close_{name}")
            close_tasks.append((name, task))

        # Wait for all tasks with a reasonable timeout
        if close_tasks:
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*[task for _, task in close_tasks], return_exceptions=True),
                    timeout=self.timeout_config.shutdown,
                )

                # Process results
                for i, (name, _) in enumerate(close_tasks):
                    result = results[i] if i < len(results) else None
                    if isinstance(result, Exception):
                        logger.debug("Transport %s close failed: %s", name, result)
                        close_results.append((name, False, str(result)))
                    else:
                        logger.debug("Transport %s closed successfully", name)
                        close_results.append((name, True, None))

            except TimeoutError:
                # Cancel any remaining tasks
                for name, task in close_tasks:
                    if not task.done():
                        task.cancel()
                        close_results.append((name, False, "timeout"))

                # Brief wait for cancellations to complete
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        asyncio.gather(*[task for _, task in close_tasks], return_exceptions=True),
                        timeout=self.timeout_config.shutdown,
                    )

    async def _sequential_close(self, transport_items: list[tuple[str, MCPBaseTransport]], close_results: list) -> None:
        """Close transports one by one as fallback."""
        for name, transport in transport_items:
            try:
                await asyncio.wait_for(
                    self._close_single_transport(name, transport),
                    timeout=self.timeout_config.shutdown,
                )
                logger.debug("Closed transport: %s", name)
                close_results.append((name, True, None))
            except TimeoutError:
                logger.debug("Transport %s close timed out (normal during shutdown)", name)
                close_results.append((name, False, "timeout"))
            except asyncio.CancelledError:
                logger.debug("Transport %s close cancelled during event loop shutdown", name)
                close_results.append((name, False, "cancelled"))
            except Exception as e:
                logger.debug("Error closing transport %s: %s", name, e)
                close_results.append((name, False, str(e)))

    async def _close_single_transport(self, name: str, transport: MCPBaseTransport) -> None:
        """Close a single transport with error handling."""
        try:
            if hasattr(transport, "close") and callable(transport.close):
                await transport.close()
            else:
                logger.debug("Transport %s has no close method", name)
        except Exception as e:
            logger.debug("Error closing transport %s: %s", name, e)
            raise

    def _sync_cleanup(self) -> None:
        """Synchronous cleanup for use when async cleanup fails."""
        try:
            transport_count = len(self.transports)
            self._cleanup_state()
            logger.debug("Synchronous cleanup completed for %d transports", transport_count)
        except Exception as e:
            logger.debug("Error during synchronous cleanup: %s", e)

    def _cleanup_state(self) -> None:
        """Clean up internal state synchronously."""
        try:
            self.transports.clear()
            self.server_info.clear()
            self.tool_to_server_map.clear()
            self.all_tools.clear()
            self.server_names.clear()
        except Exception as e:
            logger.debug("Error during state cleanup: %s", e)

    # ------------------------------------------------------------------ #
    #  backwards-compat: streams helper                                  #
    # ------------------------------------------------------------------ #
    def get_streams(self) -> list[tuple[Any, Any]]:
        """Return a list of (read_stream, write_stream) tuples for all transports."""
        if self._closed:
            return []

        pairs: list[tuple[Any, Any]] = []

        for tr in self.transports.values():
            if hasattr(tr, "get_streams") and callable(tr.get_streams):
                pairs.extend(tr.get_streams())
                continue

            rd = getattr(tr, "read_stream", None)
            wr = getattr(tr, "write_stream", None)
            if rd and wr:
                pairs.append((rd, wr))

        return pairs

    @property
    def streams(self) -> list[tuple[Any, Any]]:
        """Convenience alias for get_streams()."""
        return self.get_streams()

    # ------------------------------------------------------------------ #
    #  NEW: Health check and diagnostic methods                          #
    # ------------------------------------------------------------------ #
    def is_closed(self) -> bool:
        """Check if the StreamManager has been closed."""
        return self._closed

    def get_transport_count(self) -> int:
        """Get the number of active transports."""
        return len(self.transports)

    async def health_check(self) -> dict[str, Any]:
        """Perform a health check on all transports."""
        if self._closed:
            return {"status": "closed", "transports": {}}

        health_info = {"status": "active", "transport_count": len(self.transports), "transports": {}}

        for name, transport in self.transports.items():
            try:
                ping_ok = await asyncio.wait_for(transport.send_ping(), timeout=self.timeout_config.quick)
                health_info["transports"][name] = {
                    "status": "healthy" if ping_ok else "unhealthy",
                    "ping_success": ping_ok,
                }
            except TimeoutError:
                health_info["transports"][name] = {"status": "timeout", "ping_success": False}
            except Exception as e:
                health_info["transports"][name] = {"status": "error", "ping_success": False, "error": str(e)}

        return health_info

    async def reconnect(self, server_name: str) -> bool:
        """Reconnect a specific transport by name.

        Calls the transport's _attempt_recovery() which does cleanup + reinitialize.
        Returns True if reconnection succeeded, False otherwise.
        """
        if self._closed:
            logger.warning("Cannot reconnect: StreamManager is closed")
            return False

        transport = self.transports.get(server_name)
        if not transport:
            logger.warning("Cannot reconnect: unknown server %s", server_name)
            return False

        logger.info("Reconnecting server: %s", server_name)
        try:
            success = await transport._attempt_recovery()
            if success:
                logger.info("Server %s reconnected successfully", server_name)
            else:
                logger.warning("Server %s reconnection failed", server_name)
            return success
        except Exception as exc:
            logger.error("Error reconnecting server %s: %s", server_name, exc)
            return False
