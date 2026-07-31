# tests/mcp/transport/test_http_streamable_transport.py
"""
Tests for HTTPStreamableTransport class with consistent interface.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from chuk_tool_processor.mcp.transport.http_streamable_transport import HTTPStreamableTransport


class TestHTTPStreamableTransport:
    """Test HTTPStreamableTransport class with consistent interface."""

    @pytest.fixture
    def transport(self):
        """Create HTTPStreamableTransport instance with consistent parameters."""
        return HTTPStreamableTransport(
            "http://test.com",
            api_key="api_key",
            connection_timeout=30.0,
            default_timeout=30.0,
            session_id="test-session",
            enable_metrics=True,
        )

    @pytest.fixture
    def transport_no_metrics(self):
        """Create HTTPStreamableTransport instance with metrics disabled."""
        return HTTPStreamableTransport("http://test.com", enable_metrics=False)

    def test_init_url_normalization(self):
        """Test URL normalization to /mcp endpoint."""
        # URL without /mcp gets it added
        transport = HTTPStreamableTransport("http://test.com")
        assert transport.url == "http://test.com/mcp"

        # URL with /mcp is preserved
        transport = HTTPStreamableTransport("http://test.com/mcp")
        assert transport.url == "http://test.com/mcp"

        # URL with trailing slash handled correctly
        transport = HTTPStreamableTransport("http://test.com/")
        assert transport.url == "http://test.com/mcp"

    def test_init_parameters(self, transport):
        """Test initialization with consistent parameters."""
        assert transport.url == "http://test.com/mcp"
        assert transport.api_key == "api_key"
        assert transport.session_id == "test-session"
        assert transport.enable_metrics is True
        assert transport.default_timeout == 30.0
        assert transport.connection_timeout == 30.0

    @pytest.mark.asyncio
    async def test_initialize_success(self, transport):
        """Test successful HTTP Streamable transport initialization with metrics tracking."""
        mock_http_transport = AsyncMock()
        mock_read_stream = AsyncMock()
        mock_write_stream = AsyncMock()
        # Mock the send method for write stream
        mock_write_stream.send = AsyncMock()

        with (
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
                return_value=mock_http_transport,
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_initialize",
                AsyncMock(return_value=Mock(serverInfo=Mock(name="TestServer"))),
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping", AsyncMock(return_value=True)
            ),
        ):
            mock_http_transport.get_streams.return_value = (mock_read_stream, mock_write_stream)

            result = await transport.initialize()

            assert result is True
            assert transport._initialized is True
            assert transport._read_stream == mock_read_stream
            assert transport._write_stream == mock_write_stream

            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["initialization_time"] >= 0  # May be 0 in mocked tests
            assert metrics["last_ping_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_initialize_ping_fails(self, transport):
        """Test HTTP Streamable initialization when ping fails but connection succeeds."""
        mock_http_transport = AsyncMock()
        mock_read_stream = AsyncMock()
        mock_write_stream = AsyncMock()
        # Mock the send method for write stream
        mock_write_stream.send = AsyncMock()

        with (
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
                return_value=mock_http_transport,
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_initialize",
                AsyncMock(return_value=Mock(serverInfo=Mock(name="TestServer"))),
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping", AsyncMock(return_value=False)
            ),
        ):
            mock_http_transport.get_streams.return_value = (mock_read_stream, mock_write_stream)

            result = await transport.initialize()

            # Still considered initialized even if ping fails
            assert result is True
            assert transport._initialized is True

    @pytest.mark.asyncio
    async def test_initialize_timeout(self, transport):
        """Test HTTP Streamable transport initialization timeout."""
        mock_http_transport = AsyncMock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
            return_value=mock_http_transport,
        ):
            # Simulate timeout during context entry
            mock_http_transport.__aenter__.side_effect = TimeoutError()

            # Now expecting TimeoutError to be raised instead of returning False
            with pytest.raises(TimeoutError):
                await transport.initialize()

            assert transport._initialized is False
            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["connection_errors"] == 1

    @pytest.mark.asyncio
    async def test_send_ping_success(self, transport):
        """Test HTTP Streamable ping when initialized with metrics tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping", AsyncMock(return_value=True)
        ):
            result = await transport.send_ping()
            assert result is True

            # Check ping metrics were updated
            metrics = transport.get_metrics()
            assert metrics["last_ping_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_send_ping_not_initialized(self, transport):
        """Test HTTP Streamable ping when not initialized."""
        assert transport._initialized is False
        result = await transport.send_ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_send_ping_exception(self, transport):
        """Test HTTP Streamable ping with exception and metrics tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping",
            AsyncMock(side_effect=Exception("Stream error")),
        ):
            result = await transport.send_ping()
            assert result is False

            # Check stream error metrics
            metrics = transport.get_metrics()
            assert metrics["stream_errors"] == 1

    def test_is_connected(self, transport):
        """Test connection status check (consistent method)."""
        # not initialized
        assert transport.is_connected() is False
        # initialized but no streams
        transport._initialized = True
        assert transport.is_connected() is False
        # fully connected
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        assert transport.is_connected() is True

    @pytest.mark.asyncio
    async def test_get_tools_success(self, transport):
        """Test HTTP Streamable get tools when initialized with performance tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        expected_tools = [{"name": "search"}, {"name": "research"}]

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(return_value={"tools": expected_tools}),
        ):
            tools = await transport.get_tools()
            assert tools == expected_tools

    @pytest.mark.asyncio
    async def test_get_tools_list_response(self, transport):
        """Test HTTP Streamable get tools when response is a list."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        expected_tools = [{"name": "search"}]

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(return_value=expected_tools),
        ):
            tools = await transport.get_tools()
            assert tools == expected_tools

    @pytest.mark.asyncio
    async def test_get_tools_not_initialized(self, transport):
        """Test HTTP Streamable get tools when not initialized."""
        assert transport._initialized is False
        tools = await transport.get_tools()
        assert tools == []

    @pytest.mark.asyncio
    async def test_call_tool_success(self, transport):
        """Test HTTP Streamable call tool when initialized with metrics tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        response = {"result": {"content": [{"type": "text", "text": '{"answer": "success"}'}]}}

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value=response),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is False
            assert result["content"]["answer"] == "success"

            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["total_calls"] == 1
            assert metrics["successful_calls"] == 1
            assert metrics["avg_response_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_call_tool_with_timeout(self, transport):
        """Test HTTP Streamable call tool with custom timeout parameter."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        response = {"result": {"content": "success"}}

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value=response),
        ) as mock_send:
            result = await transport.call_tool("search", {"query": "test"}, timeout=15.0)
            assert result["isError"] is False

            # Verify the call was made with correct parameters
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_tool_not_initialized(self, transport):
        """Test HTTP Streamable call tool when not initialized."""
        assert transport._initialized is False
        result = await transport.call_tool("test", {})
        assert result["isError"] is True
        assert result["error"] == "Transport not initialized"

    @pytest.mark.asyncio
    async def test_call_tool_timeout(self, transport):
        """Test HTTP Streamable call tool with timeout and metrics tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.call_tool("search", {}, timeout=1.0)
            assert result["isError"] is True
            assert "timed out after 1.0s" in result["error"]

            # Check timeout failure metrics
            metrics = transport.get_metrics()
            assert metrics["total_calls"] == 1
            assert metrics["failed_calls"] == 1

    @pytest.mark.asyncio
    async def test_call_tool_exception(self, transport):
        """Test HTTP Streamable call tool with exception and metrics tracking."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(side_effect=Exception("Stream broken")),
        ):
            result = await transport.call_tool("search", {})
            assert result["isError"] is True
            assert "Stream broken" in result["error"]

            # Check failure metrics including stream error
            metrics = transport.get_metrics()
            assert metrics["total_calls"] == 1
            assert metrics["failed_calls"] == 1
            assert metrics["stream_errors"] == 1

    @pytest.mark.asyncio
    async def test_metrics_functionality(self, transport):
        """Test metrics get and reset functionality."""
        # Initial metrics
        metrics = transport.get_metrics()
        assert metrics["total_calls"] == 0
        assert metrics["successful_calls"] == 0
        assert metrics["failed_calls"] == 0
        assert metrics["avg_response_time"] == 0.0
        assert metrics["stream_errors"] == 0

        # Manually increment total_calls first to prevent division by zero
        transport._metrics.total_calls = 1
        transport._update_metrics(0.5, True)  # Success

        transport._metrics.total_calls = 2  # Increment again
        transport._update_metrics(1.0, False)  # Failure

        metrics = transport.get_metrics()
        assert metrics["total_calls"] == 2
        assert metrics["successful_calls"] == 1
        assert metrics["failed_calls"] == 1
        assert metrics["total_time"] == 1.5
        assert metrics["avg_response_time"] == 0.75

        # Test reset
        transport.reset_metrics()
        metrics = transport.get_metrics()
        assert metrics["total_calls"] == 0
        assert metrics["successful_calls"] == 0
        assert metrics["failed_calls"] == 0
        assert metrics["avg_response_time"] == 0.0

    @pytest.mark.asyncio
    async def test_metrics_disabled(self, transport_no_metrics):
        """Test transport behavior with metrics disabled."""
        transport_no_metrics._initialized = True
        transport_no_metrics._read_stream = Mock()
        transport_no_metrics._write_stream = Mock()

        # Metrics should still be available but not actively updated during operations
        assert transport_no_metrics.enable_metrics is False
        metrics = transport_no_metrics.get_metrics()
        assert isinstance(metrics, dict)

    @pytest.mark.asyncio
    async def test_list_resources_success(self, transport):
        """Test listing resources when initialized."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        expected_resources = {"resources": [{"name": "resource1"}]}

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_list",
            AsyncMock(return_value=expected_resources),
        ):
            result = await transport.list_resources()
            assert result == expected_resources

    @pytest.mark.asyncio
    async def test_list_resources_not_initialized(self, transport):
        """Test listing resources when not initialized."""
        transport._initialized = False
        result = await transport.list_resources()
        assert result == {}

    @pytest.mark.asyncio
    async def test_list_prompts_success(self, transport):
        """Test listing prompts when initialized."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        expected_prompts = {"prompts": [{"name": "prompt1"}]}

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_list",
            AsyncMock(return_value=expected_prompts),
        ):
            result = await transport.list_prompts()
            assert result == expected_prompts

    @pytest.mark.asyncio
    async def test_close_success(self, transport):
        """Test HTTP Streamable close when initialized with metrics logging."""
        transport._initialized = True
        # Add some metrics to test logging
        transport._metrics.total_calls = 5
        transport._metrics.successful_calls = 4
        transport._metrics.failed_calls = 1

        mock_http_transport = AsyncMock()
        transport._http_transport = mock_http_transport

        await transport.close()

        assert transport._initialized is False
        assert transport._http_transport is None
        assert transport._read_stream is None
        assert transport._write_stream is None
        mock_http_transport.__aexit__.assert_called_once_with(None, None, None)

    @pytest.mark.asyncio
    async def test_close_no_context(self, transport):
        """Test HTTP Streamable close when no context exists."""
        transport._initialized = False
        await transport.close()
        assert transport._initialized is False

    def test_get_streams(self, transport):
        """Test getting streams for backward compatibility."""
        # No streams when not initialized
        assert transport.get_streams() == []

        # Streams available when initialized
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        streams = transport.get_streams()
        assert len(streams) == 1
        assert streams[0] == (transport._read_stream, transport._write_stream)

    @pytest.mark.asyncio
    async def test_context_manager_success(self, transport):
        """Test using transport as context manager."""
        with (
            patch.object(transport, "initialize", AsyncMock(return_value=True)),
            patch.object(transport, "close", AsyncMock()),
        ):
            async with transport as t:
                assert t is transport
            transport.initialize.assert_called_once()
            transport.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_init_failure(self, transport):
        """Test context manager when initialization fails."""
        with (
            patch.object(transport, "initialize", AsyncMock(return_value=False)),
            pytest.raises(RuntimeError, match="Failed to initialize HTTPStreamableTransport"),
        ):
            async with transport:
                pass

    def test_repr_consistent_format(self, transport):
        """Test string representation follows consistent format."""
        # Not initialized
        repr_str = repr(transport)
        assert "HTTPStreamableTransport" in repr_str
        assert "status=not initialized" in repr_str
        assert "url=http://test.com/mcp" in repr_str

        # Initialized with metrics
        transport._initialized = True
        transport._metrics.total_calls = 10
        transport._metrics.successful_calls = 8

        repr_str = repr(transport)
        assert "status=initialized" in repr_str
        assert "calls: 10" in repr_str
        assert "success: 80.0%" in repr_str

    @pytest.mark.asyncio
    async def test_response_normalization(self, transport):
        """Test that response normalization uses base class methods."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Test various response formats using base class normalization
        test_cases = [
            # Standard MCP content format
            {
                "result": {"content": [{"type": "text", "text": '{"result": "success"}'}]},
                "expected": {"isError": False, "content": {"result": "success"}},
            },
            # Plain text content
            {
                "result": {"content": [{"type": "text", "text": "plain text"}]},
                "expected": {"isError": False, "content": "plain text"},
            },
            # Direct result
            {"result": {"value": 42}, "expected": {"isError": False, "content": {"value": 42}}},
            # Error response
            {"error": {"message": "Tool failed"}, "expected": {"isError": True, "error": "Tool failed"}},
        ]

        for case in test_cases:
            # FIXED: Use correct path without slash
            with patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
                AsyncMock(return_value=case),
            ):
                result = await transport.call_tool("test", {})
                assert result == case["expected"]

    def test_http_parameters_creation(self, transport):
        """Test that HTTP parameters are created correctly."""
        # This test verifies the StreamableHTTPParameters are constructed properly
        # We can't easily test the actual construction without mocking the entire
        # http_client call, but we can verify the transport stores the right values
        assert transport.url == "http://test.com/mcp"
        assert transport.api_key == "api_key"
        assert transport.session_id == "test-session"
        assert transport.connection_timeout == 30.0
        assert transport.default_timeout == 30.0

    def test_init_with_custom_headers(self):
        """Test initialization with custom headers."""
        custom_headers = {"X-Custom": "Value", "Authorization": "Bearer token"}
        transport = HTTPStreamableTransport("http://test.com", headers=custom_headers, enable_metrics=True)
        assert transport.configured_headers == custom_headers

    def test_init_with_session_id(self):
        """Test initialization with session ID."""
        transport = HTTPStreamableTransport("http://test.com", session_id="test-session-123", enable_metrics=True)
        assert transport.session_id == "test-session-123"

    @pytest.mark.asyncio
    async def test_initialize_already_initialized(self, transport):
        """Test initialization when already initialized."""
        transport._initialized = True
        transport._http_transport = Mock()  # Mock the transport object
        result = await transport.initialize()
        assert result is True

    @pytest.mark.asyncio
    async def test_initialize_with_exception(self, transport):
        """Test initialization with general exception."""
        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
            side_effect=Exception("Connection error"),
        ):
            # Now expecting Exception to be raised instead of returning False
            with pytest.raises(Exception, match="Connection error"):
                await transport.initialize()

            assert transport._initialized is False
            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["connection_errors"] == 1

    @pytest.mark.asyncio
    async def test_send_ping_increments_failures(self, transport):
        """Test send_ping increments failure counter."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping",
            AsyncMock(return_value=False),
        ):
            result = await transport.send_ping()
            assert result is False
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_send_ping_resets_failures_on_success(self, transport):
        """Test send_ping resets failure counter on success."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport._consecutive_failures = 2

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping",
            AsyncMock(return_value=True),
        ):
            result = await transport.send_ping()
            assert result is True
            assert transport._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_is_connected_with_too_many_failures(self, transport):
        """Test is_connected returns False with too many failures."""
        transport._initialized = True
        transport._consecutive_failures = 5
        assert transport.is_connected() is False

    @pytest.mark.asyncio
    async def test_get_tools_with_error_response(self, transport):
        """Test get_tools with error response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(side_effect=Exception("List error")),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_call_tool_with_error_in_response(self, transport):
        """Test call_tool with error in response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "Tool error"}}),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is True
            assert result["error"] == "Tool error"

    @pytest.mark.asyncio
    async def test_list_resources_not_initialized_returns_empty(self, transport):
        """Test list_resources when not initialized."""
        result = await transport.list_resources()
        assert result == {}

    @pytest.mark.asyncio
    async def test_list_resources_with_exception(self, transport):
        """Test list_resources with exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_list",
            AsyncMock(side_effect=Exception("List error")),
        ):
            result = await transport.list_resources()
            assert result == {}

    @pytest.mark.asyncio
    async def test_list_prompts_not_initialized(self, transport):
        """Test list_prompts when not initialized."""
        result = await transport.list_prompts()
        assert result == {}

    @pytest.mark.asyncio
    async def test_list_prompts_with_exception(self, transport):
        """Test list_prompts with exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_list",
            AsyncMock(side_effect=Exception("List error")),
        ):
            result = await transport.list_prompts()
            assert result == {}

    @pytest.mark.asyncio
    async def test_read_resource_not_initialized(self, transport):
        """Test read_resource when not initialized."""
        result = await transport.read_resource("test://uri")
        assert result == {}

    @pytest.mark.asyncio
    async def test_read_resource_with_exception(self, transport):
        """Test read_resource with exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_read",
            AsyncMock(side_effect=Exception("Read error")),
        ):
            result = await transport.read_resource("test://uri")
            assert result == {}

    @pytest.mark.asyncio
    async def test_get_prompt_not_initialized(self, transport):
        """Test get_prompt when not initialized."""
        result = await transport.get_prompt("test_prompt")
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_prompt_with_exception(self, transport):
        """Test get_prompt with exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(side_effect=Exception("Get error")),
        ):
            result = await transport.get_prompt("test_prompt")
            assert result == {}

    @pytest.mark.asyncio
    async def test_close_with_metrics_logging(self, transport):
        """Test close with metrics logging."""
        transport._initialized = True
        transport._metrics.total_calls = 10
        transport._metrics.successful_calls = 8
        transport._http_transport = AsyncMock()

        await transport.close()
        assert transport._initialized is False

    @pytest.mark.asyncio
    async def test_close_with_exception(self, transport):
        """Test close when context exit raises exception."""
        transport._initialized = True
        mock_http_transport = AsyncMock()
        transport._http_transport = mock_http_transport
        mock_http_transport.__aexit__.side_effect = Exception("Exit error")

        await transport.close()
        assert transport._initialized is False

    def test_get_streams_returns_read_and_write_streams(self, transport):
        """Test get_streams returns tuple of streams when initialized."""
        mock_read = Mock()
        mock_write = Mock()
        transport._initialized = True
        transport._read_stream = mock_read
        transport._write_stream = mock_write

        streams = transport.get_streams()
        assert streams == [(mock_read, mock_write)]

    def test_get_streams_returns_empty_when_not_initialized(self, transport):
        """Test get_streams returns empty when not initialized."""
        mock_read = Mock()
        mock_write = Mock()
        transport._initialized = False
        transport._read_stream = mock_read
        transport._write_stream = mock_write

        streams = transport.get_streams()
        assert streams == []

    def test_get_streams_returns_empty_when_streams_missing(self, transport):
        """Test get_streams returns empty when streams are None."""
        transport._initialized = True
        transport._read_stream = None
        transport._write_stream = None

        streams = transport.get_streams()
        assert streams == []

    @pytest.mark.asyncio
    async def test_attempt_recovery_success(self, transport):
        """Test _attempt_recovery successfully recovers connection."""
        transport._initialized = False

        with (
            patch.object(transport, "_cleanup", AsyncMock()),
            patch.object(transport, "initialize", AsyncMock(return_value=True)),
        ):
            result = await transport._attempt_recovery()
            assert result is True
            assert transport._metrics.recovery_attempts == 1

    @pytest.mark.asyncio
    async def test_attempt_recovery_failure(self, transport):
        """Test _attempt_recovery handles failure."""
        transport._initialized = False

        with (
            patch.object(transport, "_cleanup", AsyncMock()),
            patch.object(transport, "initialize", AsyncMock(side_effect=Exception("Recovery failed"))),
        ):
            result = await transport._attempt_recovery()
            assert result is False
            assert transport._metrics.recovery_attempts == 1

    @pytest.mark.asyncio
    async def test_initialize_connection_error_increments_metric(self, transport):
        """Test initialize increments connection error metric on failure."""
        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
            side_effect=Exception("Connection error"),
        ):
            # Now expecting Exception to be raised instead of returning False
            with pytest.raises(Exception, match="Connection error"):
                await transport.initialize()

            assert transport._initialized is False
            assert transport._metrics.connection_errors == 1

    @pytest.mark.asyncio
    async def test_list_prompts_not_initialized_check(self, transport):
        """Test list_prompts checks initialization."""
        transport._initialized = False
        result = await transport.list_prompts()
        assert result == {}

    @pytest.mark.asyncio
    async def test_read_resource_success(self, transport):
        """Test read_resource with successful response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_read",
            AsyncMock(return_value={"content": "resource data"}),
        ):
            result = await transport.read_resource("test://uri")
            assert result == {"content": "resource data"}

    @pytest.mark.asyncio
    async def test_get_prompt_success(self, transport):
        """Test get_prompt with successful response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(return_value={"prompt": "test prompt"}),
        ):
            result = await transport.get_prompt("test_prompt")
            assert result == {"prompt": "test prompt"}

    def test_repr_with_session_info(self, transport):
        """Test __repr__ includes session information."""
        transport._initialized = True
        transport.session_id = "test-session-456"
        transport._metrics.total_calls = 5

        repr_str = repr(transport)
        assert "HTTPStreamableTransport" in repr_str
        assert "status=initialized" in repr_str

    @pytest.mark.asyncio
    async def test_send_ping_timeout_exception(self, transport):
        """Test send_ping handles timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.send_ping()
            assert result is False
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_is_connected_warns_on_too_many_failures(self, transport):
        """Test is_connected logs warning when max failures reached."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport._consecutive_failures = 3  # Equals max
        transport._max_consecutive_failures = 3

        # This should trigger the warning log
        result = transport.is_connected()
        assert result is False

    @pytest.mark.asyncio
    async def test_call_tool_updates_metrics(self, transport):
        """Test call_tool updates metrics on success."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"result": {"content": "success"}}),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is False
            assert transport._metrics.total_calls == 1
            assert transport._metrics.successful_calls == 1

    @pytest.mark.asyncio
    async def test_oauth_error_detection(self, transport):
        """Test OAuth error detection helper method."""
        assert transport._is_oauth_error("invalid_token") is True
        assert transport._is_oauth_error("expired token detected") is True
        assert transport._is_oauth_error("OAuth validation failed") is True
        assert transport._is_oauth_error("unauthorized access") is True
        assert transport._is_oauth_error("token expired") is True
        assert transport._is_oauth_error("authentication failed") is True
        assert transport._is_oauth_error("invalid access token") is True
        assert transport._is_oauth_error("some other error") is False
        assert transport._is_oauth_error("") is False
        assert transport._is_oauth_error(None) is False

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_with_refresh_success(self, transport):
        """Test call_tool handles OAuth error with successful token refresh and reconnection."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Mock OAuth refresh callback
        async def mock_oauth_refresh():
            return {"Authorization": "Bearer new-token"}

        transport.oauth_refresh_callback = mock_oauth_refresh

        # First call fails with OAuth error, second succeeds after refresh
        call_count = 0

        async def mock_send_tools_call(read_stream, write_stream, tool_name, arguments):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"error": {"message": "expired token"}}
            return {"result": {"content": "success"}}

        with (
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
                side_effect=mock_send_tools_call,
            ),
            patch.object(transport, "_attempt_recovery", AsyncMock(return_value=True)),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is False
            assert call_count == 2
            assert transport.configured_headers["Authorization"] == "Bearer new-token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_without_callback(self, transport):
        """Test call_tool handles OAuth error without refresh callback."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport.oauth_refresh_callback = None

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "expired token"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "expired token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_refresh_fails(self, transport):
        """Test call_tool when OAuth refresh callback raises exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        async def mock_oauth_refresh():
            raise Exception("Refresh service unavailable")

        transport.oauth_refresh_callback = mock_oauth_refresh

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "expired token"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "expired token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_refresh_no_auth_header(self, transport):
        """Test call_tool when OAuth refresh doesn't return Authorization header."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        async def mock_oauth_refresh():
            return {"X-Custom": "header"}

        transport.oauth_refresh_callback = mock_oauth_refresh

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "expired token"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "expired token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_refresh_returns_none(self, transport):
        """Test call_tool when OAuth refresh returns None."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        async def mock_oauth_refresh():
            return None

        transport.oauth_refresh_callback = mock_oauth_refresh

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "expired token"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "expired token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_recovery_fails(self, transport):
        """Test call_tool when recovery after OAuth refresh fails."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        async def mock_oauth_refresh():
            return {"Authorization": "Bearer new-token"}

        transport.oauth_refresh_callback = mock_oauth_refresh

        with (
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
                AsyncMock(return_value={"error": {"message": "expired token"}}),
            ),
            patch.object(transport, "_attempt_recovery", AsyncMock(return_value=False)),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "expired token"

    @pytest.mark.asyncio
    async def test_call_tool_oauth_non_error_response_resets_failures(self, transport):
        """Test call_tool resets failure counter on successful response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport._consecutive_failures = 2

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"result": {"content": "success"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is False
            assert transport._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_call_tool_oauth_error_does_not_reset_failures(self, transport):
        """Test call_tool doesn't reset failure counter on error response."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport._consecutive_failures = 1

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
            AsyncMock(return_value={"error": {"message": "tool not found"}}),
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            # Failure counter should not be reset
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_test_connection_health_with_httpx_exception(self, transport):
        """Test _test_connection_health handles httpx exceptions."""
        # Mock httpx at import time within the function
        import sys
        from unittest.mock import MagicMock

        mock_httpx = MagicMock()
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.side_effect = Exception("Connection failed")
        mock_httpx.AsyncClient.return_value = mock_client

        # Temporarily add httpx to modules
        old_httpx = sys.modules.get("httpx")
        sys.modules["httpx"] = mock_httpx

        try:
            # Should return True even on exception (don't fail on health check errors)
            result = await transport._test_connection_health()
            assert result is True
        finally:
            # Restore original httpx
            if old_httpx is not None:
                sys.modules["httpx"] = old_httpx
            else:
                sys.modules.pop("httpx", None)

    @pytest.mark.asyncio
    async def test_initialize_connection_health_fails(self, transport):
        """Test initialization when connection health test fails (returns False)."""
        mock_http_transport = AsyncMock()
        mock_read_stream = AsyncMock()
        mock_write_stream = AsyncMock()

        with (
            patch.object(transport, "_test_connection_health", AsyncMock(return_value=False)),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.ChukHTTPTransport",
                return_value=mock_http_transport,
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_initialize",
                AsyncMock(return_value=Mock(serverInfo=Mock(name="TestServer"))),
            ),
            patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_ping", AsyncMock(return_value=True)
            ),
        ):
            mock_http_transport.get_streams.return_value = (mock_read_stream, mock_write_stream)

            result = await transport.initialize()

            # Should still initialize successfully despite health check failure
            assert result is True
            assert transport._initialized is True

    @pytest.mark.asyncio
    async def test_get_tools_with_pydantic_model_dump(self, transport):
        """Test get_tools with Pydantic model that has model_dump method."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Mock a Pydantic tool model with model_dump method
        mock_tool = Mock()
        mock_tool.model_dump.return_value = {"name": "search", "description": "Search tool"}

        mock_response = Mock()
        mock_response.tools = [mock_tool]

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(return_value=mock_response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0] == {"name": "search", "description": "Search tool"}
            mock_tool.model_dump.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_tools_with_pydantic_dict_method(self, transport):
        """Test get_tools with Pydantic model that has dict method."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Create a custom mock object that has dict but not model_dump
        class OldPydanticTool:
            def dict(self):
                return {"name": "research", "description": "Research tool"}

        mock_tool = OldPydanticTool()

        mock_response = Mock()
        mock_response.tools = [mock_tool]

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(return_value=mock_response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0] == {"name": "research", "description": "Research tool"}

    @pytest.mark.asyncio
    async def test_get_tools_with_unexpected_response_type(self, transport):
        """Test get_tools with unexpected response type."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Return a string instead of expected types
        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(return_value="unexpected string"),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_get_tools_timeout_exception(self, transport):
        """Test get_tools with timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            tools = await transport.get_tools()
            assert tools == []
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_call_tool_connection_unhealthy_recovery_fails(self, transport):
        """Test call_tool when connection is unhealthy and recovery fails."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()
        transport._consecutive_failures = 3  # Make connection unhealthy

        with patch.object(transport, "_attempt_recovery", AsyncMock(return_value=False)):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is True
            assert result["error"] == "Failed to recover connection"

    @pytest.mark.asyncio
    async def test_call_tool_connection_error_detection(self, transport):
        """Test call_tool detects connection errors and updates state."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        # Test various connection error messages
        connection_errors = [
            "connection reset by peer",
            "client disconnected",
            "broken pipe error",
            "unexpected EOF",
        ]

        for error_msg in connection_errors:
            transport._initialized = True  # Reset state
            initial_conn_errors = transport._metrics.connection_errors

            with patch(
                "chuk_tool_processor.mcp.transport.http_streamable_transport.send_tools_call",
                AsyncMock(side_effect=Exception(error_msg)),
            ):
                result = await transport.call_tool("search", {"query": "test"})
                assert result["isError"] is True
                assert transport._initialized is False
                assert transport._metrics.connection_errors == initial_conn_errors + 1

    @pytest.mark.asyncio
    async def test_update_metrics_with_disabled_metrics(self, transport_no_metrics):
        """Test _update_metrics when metrics are disabled."""
        # This should not raise an error even with None metrics
        transport_no_metrics._update_metrics(0.5, True)

    @pytest.mark.asyncio
    async def test_list_resources_timeout_exception(self, transport):
        """Test list_resources with timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.list_resources()
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_list_prompts_timeout_exception(self, transport):
        """Test list_prompts with timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.list_prompts()
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_read_resource_timeout_exception(self, transport):
        """Test read_resource with timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_read",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.read_resource("test://uri")
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_get_prompt_timeout_exception(self, transport):
        """Test get_prompt with timeout exception."""
        transport._initialized = True
        transport._read_stream = Mock()
        transport._write_stream = Mock()

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.get_prompt("test_prompt")
            assert result == {}
            assert transport._consecutive_failures == 1

    def test_set_session_id_with_none(self, transport):
        """Test setting session ID to None."""
        transport.session_id = "existing-session"
        transport.set_session_id(None)
        assert transport.session_id is None

    def test_reset_metrics_with_disabled_metrics(self, transport_no_metrics):
        """Test reset_metrics when metrics are disabled."""
        # Should not raise error even with None metrics
        transport_no_metrics.reset_metrics()
        # Metrics should remain None
        assert transport_no_metrics._metrics is None

    def test_get_headers_without_configured_headers(self):
        """Test _get_headers when no custom headers are configured."""
        transport = HTTPStreamableTransport("http://test.com", api_key="test-key")
        headers = transport._get_headers()

        # Should have default headers plus API key
        assert "Content-Type" in headers
        assert "Accept" in headers
        assert "User-Agent" in headers
        assert headers["Authorization"] == "Bearer test-key"

    def test_get_headers_with_configured_headers(self):
        """Test _get_headers properly merges configured headers."""
        custom_headers = {"X-Custom-Header": "custom-value", "X-Another": "another-value"}
        transport = HTTPStreamableTransport("http://test.com", headers=custom_headers, api_key="test-key")
        headers = transport._get_headers()

        # Should have default headers, custom headers, and API key
        assert "Content-Type" in headers
        assert "Accept" in headers
        assert "User-Agent" in headers
        assert headers["X-Custom-Header"] == "custom-value"
        assert headers["X-Another"] == "another-value"
        assert headers["Authorization"] == "Bearer test-key"
