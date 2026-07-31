# chuk_tool_processor/mcp/_stream_manager_lifecycle.py
"""Shutdown, stream access, and health/diagnostic methods for
:class:`StreamManager`.

Split out of ``stream_manager.py``; mixed into ``StreamManager`` (the methods
run with a full StreamManager as ``self`` — see the ``TYPE_CHECKING`` block).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.transport import MCPBaseTransport

if TYPE_CHECKING:
    from chuk_tool_processor.mcp.transport import TimeoutConfig
    from chuk_tool_processor.mcp.transport.models import MCPToolDefinition, ServerInfo

logger = get_logger("chuk_tool_processor.mcp.stream_manager")


class StreamManagerLifecycleMixin:
    """Close/health/reconnect and stream access for :class:`StreamManager`."""

    if TYPE_CHECKING:  # state provided by StreamManager.__init__
        transports: dict[str, MCPBaseTransport]
        server_info: list[ServerInfo]
        tool_to_server_map: dict[str, str]
        tool_to_servers: dict[str, list[str]]
        all_tools: list[MCPToolDefinition]
        server_names: dict[int, str]
        timeout_config: TimeoutConfig
        _closed: bool

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
            self.tool_to_servers.clear()
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
    #  Health check and diagnostic methods                               #
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
