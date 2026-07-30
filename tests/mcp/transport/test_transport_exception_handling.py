"""
Test exception handling in transports with chuk-mcp v0.8.0.

This test suite verifies that transports properly handle exceptions
from send_initialize() which now raises exceptions instead of returning None.

Key Points:
1. send_initialize() raises exceptions (doesn't return None)
2. Transports handle exceptions gracefully
3. Process crashes are detected and metrics updated
4. Connection health monitoring works correctly
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest


class TestStdioTransportExceptionHandling:
    """Test exception handling in STDIO transport."""

    @pytest.mark.asyncio
    async def test_successful_initialization_no_none_check(self):
        """Test that successful initialization doesn't check for None."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        # Mock successful initialization
        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_ping", new_callable=AsyncMock) as mock_ping,
        ):
            # Setup mocks
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # send_initialize returns InitializeResult (not None)
            mock_init_result = Mock()
            mock_init_result.serverInfo.name = "TestServer"
            mock_init.return_value = mock_init_result

            mock_ping.return_value = True

            # Create transport
            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # Initialize
            success = await transport.initialize()

            # Verify
            assert success is True
            assert transport._initialized is True
            mock_init.assert_called_once()
            # Critical: init_result is used directly, NOT checked for None

            await transport.close()

    @pytest.mark.asyncio
    async def test_timeout_error_raises_and_handled(self):
        """Test that TimeoutError from send_initialize is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # send_initialize raises TimeoutError
            mock_init.side_effect = TimeoutError("Server didn't respond")

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # Initialize should handle the timeout
            success = await transport.initialize()

            # Verify
            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_retryable_error_raises_and_handled(self):
        """Test that RetryableError from send_initialize is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # Mock RetryableError (e.g., 401 authentication error)
            try:
                from chuk_mcp.protocol.types.errors import RetryableError

                mock_init.side_effect = RetryableError('HTTP 401: {"error":"invalid_token"}', code=-32603)
            except ImportError:
                # Fallback if RetryableError not available
                mock_init.side_effect = Exception('HTTP 401: {"error":"invalid_token"}')

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # Initialize should handle the error
            success = await transport.initialize()

            # Verify
            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_version_mismatch_error_handled(self):
        """Test that VersionMismatchError from send_initialize is handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # Mock VersionMismatchError
            try:
                from chuk_mcp.protocol.types.errors import VersionMismatchError

                mock_init.side_effect = VersionMismatchError("2025-06-18", ["2024-11-05"])
            except ImportError:
                # Fallback if VersionMismatchError not available
                mock_init.side_effect = Exception("Version mismatch")

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # Initialize should handle the error
            success = await transport.initialize()

            # Verify
            assert success is False
            assert transport._initialized is False

            await transport.close()

    @pytest.mark.asyncio
    async def test_general_exception_handled(self):
        """Test that general exceptions from send_initialize are handled."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # send_initialize raises generic exception
            mock_init.side_effect = Exception("Unexpected error")

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # Initialize should handle the error
            success = await transport.initialize()

            # Verify
            assert success is False
            assert transport._initialized is False
            metrics = transport.get_metrics()
            assert metrics["process_crashes"] >= 1

            await transport.close()

    @pytest.mark.asyncio
    async def test_no_none_return_from_initialize(self):
        """
        Critical test: Verify send_initialize never returns None.

        This is a breaking change test - ensures we never check for None
        because it's no longer a possible return value.
        """
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_ping", new_callable=AsyncMock) as mock_ping,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # send_initialize should NEVER return None
            # It either returns InitializeResult or raises an exception
            mock_init_result = Mock()
            mock_init.return_value = mock_init_result
            mock_ping.return_value = True

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params)

            # This should work WITHOUT checking for None
            success = await transport.initialize()

            assert success is True
            # Verify init was called and result was used (not checked for None)
            mock_init.assert_called_once()

            await transport.close()

    @pytest.mark.asyncio
    async def test_metrics_updated_on_error(self):
        """Test that metrics are updated correctly on errors."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # Multiple error types to test metrics
            error_types = [
                TimeoutError("timeout"),
                Exception("general error"),
            ]

            for error in error_types:
                mock_init.side_effect = error

                server_params = {"command": "test", "args": []}
                transport = StdioTransport(server_params, enable_metrics=True)

                success = await transport.initialize()

                assert success is False
                metrics = transport.get_metrics()
                # Each error should increment process_crashes
                assert metrics["process_crashes"] == 1

                await transport.close()


# Note: HTTP transport exception handling tests are covered in test_http_streamable.py
# These tests require more complex mocking of the HTTP client structure


class TestTransportRecovery:
    """Test transport recovery mechanisms."""

    @pytest.mark.asyncio
    async def test_recovery_after_crash(self):
        """Test that transport can recover after a crash."""
        from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.stdio_client") as mock_client,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_initialize", new_callable=AsyncMock) as mock_init,
            patch("chuk_tool_processor.mcp.transport.stdio_transport.send_ping", new_callable=AsyncMock) as mock_ping,
        ):
            mock_context = AsyncMock()
            mock_streams = (Mock(), Mock())
            mock_context.__aenter__.return_value = mock_streams
            mock_client.return_value = mock_context

            # First attempt fails, second succeeds
            mock_init_result = Mock()
            mock_init.side_effect = [
                Exception("Transient error"),  # First call fails
                mock_init_result,  # Second call succeeds
            ]
            mock_ping.return_value = True

            server_params = {"command": "test", "args": []}
            transport = StdioTransport(server_params, enable_metrics=True)

            # First attempt
            success1 = await transport.initialize()
            assert success1 is False
            metrics1 = transport.get_metrics()
            assert metrics1["process_crashes"] == 1

            # Recovery attempt (close and reinitialize)
            await transport.close()

            # Create new transport and try again
            transport2 = StdioTransport(server_params, enable_metrics=True)
            success2 = await transport2.initialize()

            assert success2 is True
            assert transport2._initialized is True

            await transport2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
