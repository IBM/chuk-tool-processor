# chuk_tool_processor/mcp/_stream_manager_init.py
"""Transport-initialisation methods for :class:`StreamManager`.

Split out of ``stream_manager.py`` to keep that module focused. Mixed into
``StreamManager`` — the methods below run with a full StreamManager as ``self``
(see the ``TYPE_CHECKING`` block for the state they rely on).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.models import MCPTransport
from chuk_tool_processor.mcp.transport import HTTPStreamableTransport, MCPBaseTransport, SSETransport, StdioTransport
from chuk_tool_processor.mcp.transport.models import MCPToolDefinition, ServerInfo

from ._config import load_config

if TYPE_CHECKING:
    from chuk_tool_processor.mcp.transport import TimeoutConfig

logger = get_logger("chuk_tool_processor.mcp.stream_manager")


class StreamManagerInitMixin:
    """Per-transport initialisation for :class:`StreamManager`."""

    if TYPE_CHECKING:  # state provided by StreamManager.__init__
        transports: dict[str, MCPBaseTransport]
        server_info: list[ServerInfo]
        all_tools: list[MCPToolDefinition]
        server_names: dict[int, str]
        timeout_config: TimeoutConfig
        _lock: asyncio.Lock
        _closed: bool

        def _register_tools(self, tools: list[MCPToolDefinition], server_name: str) -> None: ...

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

                    self._register_tools(tools, server_name)
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

                    self._register_tools(tools, name)
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

                    self._register_tools(tools, name)
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

                    self._register_tools(tools, name)
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
