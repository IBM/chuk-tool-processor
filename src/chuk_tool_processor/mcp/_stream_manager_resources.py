# chuk_tool_processor/mcp/_stream_manager_resources.py
"""Per-server query helpers for :class:`StreamManager`: tool listing, ping, and
the resources/prompts fan-out methods.

Split out of ``stream_manager.py``; mixed into ``StreamManager`` (the methods
run with a full StreamManager as ``self`` — see the ``TYPE_CHECKING`` block).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.transport import MCPBaseTransport

if TYPE_CHECKING:
    from chuk_tool_processor.mcp.transport import TimeoutConfig

logger = get_logger("chuk_tool_processor.mcp.stream_manager")


class StreamManagerResourcesMixin:
    """Tool/resource/prompt queries for :class:`StreamManager`."""

    if TYPE_CHECKING:  # state provided by StreamManager.__init__
        transports: dict[str, MCPBaseTransport]
        timeout_config: TimeoutConfig
        _closed: bool

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
