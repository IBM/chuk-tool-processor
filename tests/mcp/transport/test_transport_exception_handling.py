"""
Test exception handling in the STDIO transport's dual-era connect path.

The transport connects via ``chuk_mcp_rs.connect_dual_stdio``, which either
returns a settled dual-era client or raises. This suite verifies that:

1. A successful connect initialises without any ``None`` checks.
2. Connect failures (timeout, transient, version, generic) are handled
   gracefully — ``initialize()`` returns ``False`` rather than raising.
3. Process-crash metrics are updated on failure.
4. A fresh transport can recover after a prior connect failure.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

_CONNECT = "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio"


def _mock_dual_client(ping=True):
    """Build a mock dual-era client matching the surface the transport uses."""
    client = AsyncMock()
    client.raw_streams = AsyncMock(return_value=(Mock(), Mock()))
    client.ping = AsyncMock(return_value=ping)
    client.close = AsyncMock()
    client.era = "2025-06-18"
    client.protocol_version = "2025-06-18"
    return client


class TestStdioTransportExceptionHandling:
    """Test exception handling in STDIO transport."""

    @pytest.mark.asyncio
    async def test_successful_initialization_no_none_check(self):
        """A successful connect initialises without checking the client for None."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        client = _mock_dual_client(ping=True)
        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = client

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is True
            assert transport._initialized is True
            mock_connect.assert_called_once()
            # Critical: the client is used directly, NOT checked for None.

            await transport.close()

    @pytest.mark.asyncio
    async def test_timeout_error_raises_and_handled(self):
        """A TimeoutError from the dual-era connect is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = TimeoutError("Server didn't respond")

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_retryable_error_raises_and_handled(self):
        """A transient error (e.g. HTTP 401) from connect is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception('HTTP 401: {"error":"invalid_token"}')

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_version_mismatch_error_handled(self):
        """A protocol version-mismatch error from connect is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Unsupported protocol version 2025-06-18")

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is False
            assert transport._initialized is False

            await transport.close()

    @pytest.mark.asyncio
    async def test_general_exception_handled(self):
        """A generic exception from connect is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Unexpected error")

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_no_none_return_from_initialize(self):
        """
        Critical test: a settled connect yields a usable client, never None.

        Ensures ``initialize()`` uses the client directly instead of guarding
        against a ``None`` return that can no longer occur.
        """
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        client = _mock_dual_client(ping=True)
        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = client

            transport = StdioTransport({"command": "test", "args": []})
            success = await transport.initialize()

            assert success is True
            mock_connect.assert_called_once()

            await transport.close()

    @pytest.mark.asyncio
    async def test_metrics_updated_on_error(self):
        """Process-crash metrics are updated correctly on connect errors."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        error_types = [
            TimeoutError("timeout"),
            Exception("general error"),
        ]

        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            for error in error_types:
                mock_connect.side_effect = error

                transport = StdioTransport({"command": "test", "args": []}, enable_metrics=True)
                success = await transport.initialize()

                assert success is False
                metrics = transport.get_metrics()
                # Each error should increment process_crashes on its own transport.
                assert metrics["process_crashes"] == 1

                await transport.close()


# Note: HTTP transport exception handling tests are covered in test_http_streamable.py
# These tests require more complex mocking of the HTTP client structure.


class TestTransportRecovery:
    """Test transport recovery mechanisms."""

    @pytest.mark.asyncio
    async def test_recovery_after_crash(self):
        """A fresh transport can connect after a prior connect failure."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        client = _mock_dual_client(ping=True)
        with patch(_CONNECT, new_callable=AsyncMock) as mock_connect:
            # First connect fails, second succeeds.
            mock_connect.side_effect = [
                Exception("Transient error"),
                client,
            ]

            transport = StdioTransport({"command": "test", "args": []}, enable_metrics=True)
            success1 = await transport.initialize()
            assert success1 is False
            assert transport.get_metrics()["process_crashes"] == 1
            await transport.close()

            transport2 = StdioTransport({"command": "test", "args": []}, enable_metrics=True)
            success2 = await transport2.initialize()

            assert success2 is True
            assert transport2._initialized is True

            await transport2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
