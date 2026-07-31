# tests/mcp/transport/test_stdio_transport.py - FIXED METRICS TEST
"""
Tests for StdioTransport class with consistent interface.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from chuk_mcp_rs import StdioParameters

from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport


def _mock_dual_client(streams=None, ping=True, era="2025-06-18", protocol="2025-06-18"):
    """Build a mock chuk_mcp_rs dual-era client for patching ``connect_dual_stdio``.

    Mirrors the surface ``StdioTransport`` uses: ``raw_streams()``, ``ping()``,
    ``close()``, and the ``era``/``protocol_version`` attributes.
    """
    client = AsyncMock()
    client.raw_streams = AsyncMock(return_value=streams if streams is not None else (Mock(), Mock()))
    client.ping = AsyncMock(return_value=ping)
    client.close = AsyncMock()
    client.era = era
    client.protocol_version = protocol
    return client


class TestStdioTransport:
    """Test StdioTransport class with consistent interface."""

    @pytest.fixture
    def transport(self):
        """Create StdioTransport instance with consistent parameters."""
        server_params = {"command": "python", "args": ["-m", "test_server"], "env": {"TEST": "true"}}
        return StdioTransport(server_params, connection_timeout=30.0, default_timeout=30.0, enable_metrics=True)

    @pytest.fixture
    def transport_no_metrics(self):
        """Create StdioTransport instance with metrics disabled."""
        server_params = {"command": "python", "args": ["-m", "test_server"]}
        return StdioTransport(server_params, enable_metrics=False)

    def test_init_with_dict_params(self, transport):
        """Test initialization with dictionary parameters."""
        assert isinstance(transport.server_params, StdioParameters)
        assert transport.server_params.command == "python"
        assert transport.server_params.args == ["-m", "test_server"]
        assert transport.enable_metrics is True
        assert transport.default_timeout == 30.0

    def test_init_with_stdio_parameters(self):
        """Test initialization with StdioParameters object."""
        params = StdioParameters(command="test", args=["arg1"])
        transport = StdioTransport(params)
        # Note: server_params is a new object with merged env, not the same object
        assert transport.server_params.command == params.command
        assert transport.server_params.args == params.args
        assert transport.enable_metrics is True  # default

    @pytest.mark.asyncio
    async def test_initialize_success(self, transport):
        """Test successful STDIO transport initialization with metrics tracking."""
        mock_streams = (Mock(), Mock())  # (read_stream, write_stream)
        client = _mock_dual_client(streams=mock_streams, ping=True)

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
            AsyncMock(return_value=client),
        ):
            result = await transport.initialize()

            assert result is True
            assert transport._initialized is True
            assert transport._streams == mock_streams

            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["initialization_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_initialize_timeout(self, transport):
        """Test STDIO transport initialization timeout."""
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
            AsyncMock(side_effect=TimeoutError()),
        ):
            result = await transport.initialize()

            assert result is False
            assert transport._initialized is False

    @pytest.mark.asyncio
    async def test_initialize_connect_fails(self, transport):
        """Test STDIO transport initialization when the dual-era connect fails."""
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
            AsyncMock(side_effect=Exception("connect failed")),
        ):
            result = await transport.initialize()

            assert result is False
            assert transport._initialized is False

    @pytest.mark.asyncio
    async def test_send_ping_success(self, transport):
        """Test STDIO ping when initialized with metrics tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._client = _mock_dual_client(ping=True)

        result = await transport.send_ping()
        assert result is True

        # Check ping metrics were updated
        metrics = transport.get_metrics()
        assert metrics["last_ping_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_send_ping_not_initialized(self, transport):
        """Test STDIO ping when not initialized."""
        assert transport._initialized is False
        result = await transport.send_ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_send_ping_exception(self, transport):
        """Test STDIO ping with exception and metrics tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._client = _mock_dual_client()
        transport._client.ping = AsyncMock(side_effect=Exception("Pipe error"))

        result = await transport.send_ping()
        assert result is False

        # Check pipe error metrics
        metrics = transport.get_metrics()
        assert metrics["pipe_errors"] == 1

    def test_is_connected(self, transport):
        """Test connection status check (consistent method)."""
        # not initialized
        assert transport.is_connected() is False
        # initialized
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        assert transport.is_connected() is True
        # initialized but no streams
        transport._streams = None
        assert transport.is_connected() is False

    @pytest.mark.asyncio
    async def test_get_tools_success(self, transport):
        """Test STDIO get tools when initialized with performance tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        expected_tools = [{"name": "search"}, {"name": "research"}]

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value={"tools": expected_tools}),
        ):
            tools = await transport.get_tools()
            assert tools == expected_tools

    @pytest.mark.asyncio
    async def test_get_tools_list_response(self, transport):
        """Test STDIO get tools when response is a list."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        expected_tools = [{"name": "search"}]

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list", AsyncMock(return_value=expected_tools)
        ):
            tools = await transport.get_tools()
            assert tools == expected_tools

    @pytest.mark.asyncio
    async def test_get_tools_not_initialized(self, transport):
        """Test STDIO get tools when not initialized."""
        assert transport._initialized is False
        tools = await transport.get_tools()
        assert tools == []

    @pytest.mark.asyncio
    async def test_call_tool_success(self, transport):
        """Test STDIO call tool when initialized with metrics tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        # Test the STDIO-specific string preservation behavior
        response = {"result": {"content": [{"type": "text", "text": "42"}]}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call", AsyncMock(return_value=response)
        ):
            result = await transport.call_tool("search", {"query": "test"})
            assert result["isError"] is False
            assert result["content"] == "42"  # String preserved for STDIO

            # Check metrics were updated
            metrics = transport.get_metrics()
            assert metrics["total_calls"] == 1
            assert metrics["successful_calls"] == 1
            assert metrics["avg_response_time"] >= 0  # May be 0 in mocked tests

    @pytest.mark.asyncio
    async def test_call_tool_with_timeout(self, transport):
        """Test STDIO call tool with custom timeout parameter."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"result": {"content": "success"}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call", AsyncMock(return_value=response)
        ) as mock_send:
            result = await transport.call_tool("search", {"query": "test"}, timeout=15.0)
            assert result["isError"] is False

            # Verify the call was made with correct parameters
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_tool_not_initialized(self, transport):
        """Test STDIO call tool when not initialized."""
        assert transport._initialized is False
        result = await transport.call_tool("test", {})
        assert result["isError"] is True
        assert result["error"] == "Transport not initialized"

    @pytest.mark.asyncio
    async def test_call_tool_timeout(self, transport):
        """Test STDIO call tool with timeout and metrics tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
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
        """Test STDIO call tool with exception and metrics tracking."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(side_effect=Exception("Pipe broken")),
        ):
            result = await transport.call_tool("search", {})
            assert result["isError"] is True
            assert "Pipe broken" in result["error"]

            # Check failure metrics including pipe error
            metrics = transport.get_metrics()
            assert metrics["total_calls"] == 1
            assert metrics["failed_calls"] == 1
            assert metrics["pipe_errors"] == 1

    @pytest.mark.asyncio
    async def test_metrics_functionality(self, transport):
        """Test metrics get and reset functionality - FIXED."""
        # Initial metrics
        metrics = transport.get_metrics()
        assert metrics["total_calls"] == 0
        assert metrics["successful_calls"] == 0
        assert metrics["failed_calls"] == 0
        assert metrics["avg_response_time"] == 0.0
        assert metrics["pipe_errors"] == 0

        # FIXED: Simulate actual tool calls to test metrics
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        # Simulate successful call
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(return_value={"result": {"content": "success"}}),
        ):
            await transport.call_tool("test", {})

        # Simulate failed call
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(side_effect=Exception("error")),
        ):
            await transport.call_tool("test", {})

        metrics = transport.get_metrics()
        assert metrics["total_calls"] == 2
        assert metrics["successful_calls"] == 1
        assert metrics["failed_calls"] == 1
        assert metrics["avg_response_time"] >= 0  # May be 0 in mocked tests

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
        transport_no_metrics._streams = (Mock(), Mock())

        # Metrics should still be available but not actively updated during operations
        assert transport_no_metrics.enable_metrics is False
        metrics = transport_no_metrics.get_metrics()
        assert isinstance(metrics, dict)

    def test_stdio_specific_content_extraction(self, transport):
        """Test STDIO-specific content extraction with string preservation."""
        # Test numeric string preservation (STDIO-specific behavior)
        content = [{"type": "text", "text": "42"}]
        result = transport._extract_stdio_content(content)
        assert result == "42"  # Preserved as string

        # Test decimal string preservation
        content = [{"type": "text", "text": "3.14"}]
        result = transport._extract_stdio_content(content)
        assert result == "3.14"  # Preserved as string

        # Test JSON object parsing
        content = [{"type": "text", "text": '{"key": "value"}'}]
        result = transport._extract_stdio_content(content)
        assert result == {"key": "value"}  # Parsed as JSON

        # Test non-numeric string
        content = [{"type": "text", "text": "hello"}]
        result = transport._extract_stdio_content(content)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_list_resources_success(self, transport):
        """Test listing resources when initialized."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

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
        transport._streams = (Mock(), Mock())

        expected_prompts = {"prompts": [{"name": "prompt1"}]}

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_list",
            AsyncMock(return_value=expected_prompts),
        ):
            result = await transport.list_prompts()
            assert result == expected_prompts

    @pytest.mark.asyncio
    async def test_read_resource(self, transport):
        """Test reading a specific resource."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        expected_resource = {"content": "resource data"}

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_read",
            AsyncMock(return_value=expected_resource),
        ):
            result = await transport.read_resource("test://resource")
            assert result == expected_resource

    @pytest.mark.asyncio
    async def test_get_prompt(self, transport):
        """Test getting a specific prompt."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        expected_prompt = {"messages": [{"role": "user", "content": "test"}]}

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(return_value=expected_prompt),
        ):
            result = await transport.get_prompt("test_prompt", {"arg": "value"})
            assert result == expected_prompt

    @pytest.mark.asyncio
    async def test_close_success(self, transport):
        """Test STDIO close when initialized with metrics logging."""
        transport._initialized = True
        # Add some metrics to test logging
        transport._metrics["total_calls"] = 5
        transport._metrics["successful_calls"] = 4
        transport._metrics["failed_calls"] = 1

        client = _mock_dual_client()
        transport._client = client

        await transport.close()

        assert transport._initialized is False
        assert transport._client is None
        assert transport._streams is None
        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_no_context(self, transport):
        """Test STDIO close when no context exists."""
        transport._initialized = False
        await transport.close()
        assert transport._initialized is False

    def test_get_streams(self, transport):
        """Test getting streams for backward compatibility."""
        # No streams when not initialized
        assert transport.get_streams() == []

        # Streams available when initialized
        mock_streams = (Mock(), Mock())
        transport._streams = mock_streams
        assert transport.get_streams() == [mock_streams]

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
            pytest.raises(RuntimeError, match="Failed to initialize StdioTransport"),
        ):
            async with transport:
                pass

    def test_repr_consistent_format(self, transport):
        """Test string representation follows consistent format - FIXED."""
        # Not initialized
        repr_str = repr(transport)
        assert "StdioTransport" in repr_str
        assert "status=not initialized" in repr_str
        assert "command=python" in repr_str  # FIXED: Now matches expected format

        # Initialized with metrics
        transport._initialized = True
        transport._metrics["total_calls"] = 10
        transport._metrics["successful_calls"] = 8

        repr_str = repr(transport)
        assert "status=initialized" in repr_str
        assert "calls: 10" in repr_str
        assert "success: 80.0%" in repr_str

    @pytest.mark.asyncio
    async def test_initialize_already_initialized(self, transport):
        """Test initializing when already initialized."""
        transport._initialized = True
        result = await transport.initialize()
        assert result is True

    @pytest.mark.asyncio
    async def test_initialize_with_exception(self, transport):
        """Test initialization with general exception."""
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
            AsyncMock(side_effect=Exception("General error")),
        ):
            result = await transport.initialize()
            assert result is False
            assert transport._initialized is False
            # Check that process crashes metric was incremented
            assert transport._metrics["process_crashes"] == 1

    @pytest.mark.asyncio
    async def test_initialize_ping_fails_but_still_succeeds(self, transport):
        """Test initialization when ping fails but init still succeeds."""
        mock_streams = (Mock(), Mock())
        client = _mock_dual_client(streams=mock_streams, ping=False)

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
            AsyncMock(return_value=client),
        ):
            result = await transport.initialize()
            assert result is True  # Still succeeds even if ping fails
            assert transport._initialized is True
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_get_process_info_no_monitor(self, transport_no_metrics):
        """Test get_process_info when monitoring is disabled."""
        transport_no_metrics.process_monitor = False
        result = await transport_no_metrics._get_process_info()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_process_info_no_process_id(self, transport):
        """Test get_process_info when no process ID is set."""
        transport._process_id = None
        result = await transport._get_process_info()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_process_info_invalid_pid(self, transport):
        """Test get_process_info with invalid PID."""
        transport._process_id = -1
        result = await transport._get_process_info()
        assert result is None

        transport._process_id = 0
        result = await transport._get_process_info()
        assert result is None

    @pytest.mark.asyncio
    async def test_monitor_process_health_disabled(self, transport):
        """Test process health monitoring when disabled."""
        transport.process_monitor = False
        result = await transport._monitor_process_health()
        assert result is True

    @pytest.mark.asyncio
    async def test_monitor_process_health_no_valid_pid(self, transport):
        """Test process health monitoring with no valid PID."""
        transport._process_id = None
        result = await transport._monitor_process_health()
        assert result is True

        transport._process_id = -1
        result = await transport._monitor_process_health()
        assert result is True

    @pytest.mark.asyncio
    async def test_close_when_not_initialized(self, transport):
        """Test closing when not initialized."""
        transport._initialized = False
        await transport.close()
        # Should do nothing without error
        assert transport._initialized is False

    @pytest.mark.asyncio
    async def test_close_with_metrics_logging(self, transport):
        """Test closing with metrics logging."""
        transport._initialized = True
        transport._metrics["total_calls"] = 5
        transport._metrics["successful_calls"] = 4
        transport._client = _mock_dual_client()

        await transport.close()
        assert transport._initialized is False

    @pytest.mark.asyncio
    async def test_close_with_exception(self, transport):
        """Test closing when the client close raises an exception."""
        transport._initialized = True
        client = _mock_dual_client()
        client.close = AsyncMock(side_effect=Exception("Exit error"))
        transport._client = client

        await transport.close()
        # Should still cleanup
        assert transport._initialized is False
        assert transport._client is None

    @pytest.mark.asyncio
    async def test_send_ping_timeout(self, transport):
        """Test send_ping with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._client = _mock_dual_client()
        transport._client.ping = AsyncMock(side_effect=asyncio.TimeoutError)

        result = await transport.send_ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_is_connected_too_many_failures(self, transport):
        """Test is_connected with too many consecutive failures."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._consecutive_failures = 5  # Exceeds max

        assert transport.is_connected() is False

    @pytest.mark.asyncio
    async def test_get_tools_not_initialized_returns_empty(self, transport):
        """Test get_tools when not initialized."""
        transport._initialized = False
        tools = await transport.get_tools()
        assert tools == []

    @pytest.mark.asyncio
    async def test_get_tools_timeout(self, transport):
        """Test get_tools with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            tools = await transport.get_tools()
            assert tools == []
            assert transport._consecutive_failures > 0

    @pytest.mark.asyncio
    async def test_get_tools_exception(self, transport):
        """Test get_tools with exception."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(side_effect=Exception("List error")),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_get_tools_unexpected_response_type(self, transport):
        """Test get_tools with unexpected response type."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value="unexpected string"),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_call_tool_not_initialized_returns_error(self, transport):
        """Test call_tool when not initialized."""
        result = await transport.call_tool("test", {})
        assert result["isError"] is True
        assert result["error"] == "Transport not initialized"

    @pytest.mark.asyncio
    async def test_call_tool_with_error_response(self, transport):
        """Test call_tool with error in response."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        error_response = {"error": {"message": "Tool error"}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(return_value=error_response),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is True
            assert result["error"] == "Tool error"

    @pytest.mark.asyncio
    async def test_call_tool_with_dict_content(self, transport):
        """Test call_tool with dict content."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"result": {"data": "value"}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(return_value=response),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is False
            assert result["content"] == {"data": "value"}

    @pytest.mark.asyncio
    async def test_list_resources_not_initialized_returns_empty(self, transport):
        """Test list_resources when not initialized."""
        result = await transport.list_resources()
        assert result == {}

    @pytest.mark.asyncio
    async def test_list_resources_exception(self, transport):
        """Test list_resources with exception."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

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
    async def test_list_prompts_exception(self, transport):
        """Test list_prompts with exception."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

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
    async def test_read_resource_exception(self, transport):
        """Test read_resource with exception."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

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
    async def test_get_prompt_exception(self, transport):
        """Test get_prompt with exception."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(side_effect=Exception("Get error")),
        ):
            result = await transport.get_prompt("test_prompt")
            assert result == {}

    @pytest.mark.asyncio
    async def test_attempt_recovery(self, transport):
        """Test attempt_recovery method."""
        mock_streams = (Mock(), Mock())
        client = _mock_dual_client(streams=mock_streams, ping=True)

        with (
            patch(
                "chuk_tool_processor.mcp.transport.stdio_transport.connect_dual_stdio",
                AsyncMock(return_value=client),
            ),
            patch("chuk_tool_processor.mcp.transport.stdio_transport.asyncio.sleep", AsyncMock()),
        ):
            await transport._attempt_recovery()
            assert transport._metrics["recovery_attempts"] == 1
            assert transport._metrics["process_restarts"] == 1

    @pytest.mark.asyncio
    async def test_attempt_recovery_fails(self, transport):
        """Test attempt_recovery when recovery fails."""
        with patch.object(transport, "initialize", AsyncMock(side_effect=Exception("Recovery failed"))):
            result = await transport._attempt_recovery()
            assert result is False

    @pytest.mark.asyncio
    async def test_get_process_info_with_valid_process(self, transport):
        """Test get_process_info with valid process ID."""
        transport._process_id = 12345
        transport._process_start_time = 1000.0
        transport.process_monitor = True

        mock_process = Mock()
        mock_process.is_running.return_value = True
        mock_process.status.return_value = "running"
        mock_process.cpu_percent.return_value = 5.0
        mock_process.create_time.return_value = 1000.0
        mock_memory = Mock()
        mock_memory.rss = 100 * 1024 * 1024  # 100 MB
        mock_process.memory_info.return_value = mock_memory

        with (
            patch("chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process", return_value=mock_process),
            patch("chuk_tool_processor.mcp.transport.stdio_transport.time.time", return_value=1100.0),
        ):
            result = await transport._get_process_info()
            assert result is not None
            assert result["pid"] == 12345
            assert result["status"] == "running"
            assert result["cpu_percent"] == 5.0
            assert result["memory_mb"] == 100.0

    @pytest.mark.asyncio
    async def test_get_process_info_process_not_running(self, transport):
        """Test get_process_info when process is not running."""
        transport._process_id = 12345
        transport.process_monitor = True

        mock_process = Mock()
        mock_process.is_running.return_value = False

        with patch("chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process", return_value=mock_process):
            result = await transport._get_process_info()
            assert result is None

    @pytest.mark.asyncio
    async def test_monitor_process_health_with_process_info(self, transport):
        """Test process health monitoring with valid process info."""
        transport._process_id = 12345
        transport.process_monitor = True

        # Mock get_process_info to return valid info
        mock_info = {
            "pid": 12345,
            "status": "running",
            "memory_mb": 500.0,
            "cpu_percent": 10.0,
        }

        with patch.object(transport, "_get_process_info", AsyncMock(return_value=mock_info)):
            result = await transport._monitor_process_health()
            assert result is True
            # Check that metrics were updated
            assert transport._metrics["memory_usage_mb"] == 500.0
            assert transport._metrics["cpu_percent"] == 10.0

    @pytest.mark.asyncio
    async def test_monitor_process_health_zombie_process(self, transport):
        """Test process health monitoring with zombie process."""
        transport._process_id = 12345
        transport.process_monitor = True

        mock_info = {
            "pid": 12345,
            "status": "zombie",
            "memory_mb": 0.0,
            "cpu_percent": 0.0,
        }

        with patch.object(transport, "_get_process_info", AsyncMock(return_value=mock_info)):
            result = await transport._monitor_process_health()
            assert result is False

    @pytest.mark.asyncio
    async def test_monitor_process_health_excessive_memory(self, transport):
        """Test process health monitoring with excessive memory usage."""
        transport._process_id = 12345
        transport.process_monitor = True

        mock_info = {
            "pid": 12345,
            "status": "running",
            "memory_mb": 1500.0,  # > 1024 MB
            "cpu_percent": 10.0,
        }

        with patch.object(transport, "_get_process_info", AsyncMock(return_value=mock_info)):
            result = await transport._monitor_process_health()
            assert result is True  # Still returns True, just logs warning

    @pytest.mark.asyncio
    async def test_send_ping_with_process_health_check(self, transport):
        """Test send_ping with process health monitoring."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._process_id = 12345
        transport.process_monitor = True
        transport._client = _mock_dual_client(ping=True)

        # Mock health check to return False
        with patch.object(transport, "_monitor_process_health", AsyncMock(return_value=False)):
            result = await transport.send_ping()
            assert result is False
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_cleanup_with_valid_process(self, transport):
        """Test cleanup with valid process that needs termination."""
        transport._process_id = 12345
        transport.process_monitor = True

        mock_process = Mock()
        mock_process.is_running.return_value = True

        with patch("chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process", return_value=mock_process):
            await transport._cleanup()
            mock_process.terminate.assert_called_once()
            assert transport._process_id is None

    @pytest.mark.asyncio
    async def test_cleanup_process_timeout_then_kill(self, transport):
        """Test cleanup when process doesn't terminate gracefully."""
        import psutil

        transport._process_id = 12345
        transport.process_monitor = True

        mock_process = Mock()
        mock_process.is_running.return_value = True
        mock_process.wait.side_effect = psutil.TimeoutExpired(2.0)

        with patch("chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process", return_value=mock_process):
            await transport._cleanup()
            mock_process.terminate.assert_called_once()
            mock_process.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_process_no_such_process(self, transport):
        """Test cleanup when process doesn't exist."""
        import psutil

        transport._process_id = 12345
        transport.process_monitor = True

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process",
            side_effect=psutil.NoSuchProcess(12345),
        ):
            await transport._cleanup()
            # Should handle gracefully
            assert transport._process_id is None

    @pytest.mark.asyncio
    async def test_list_prompts_not_initialized_check(self, transport):
        """Test list_prompts verifies initialization."""
        transport._initialized = False
        result = await transport.list_prompts()
        assert result == {}

    @pytest.mark.asyncio
    async def test_read_resource_not_initialized_check(self, transport):
        """Test read_resource verifies initialization."""
        transport._initialized = False
        result = await transport.read_resource("test://uri")
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_prompt_not_initialized_check(self, transport):
        """Test get_prompt verifies initialization."""
        transport._initialized = False
        result = await transport.get_prompt("test")
        assert result == {}

    def test_init_with_stdio_parameters_with_env(self):
        """Test initialization with StdioParameters object that has env."""
        params = StdioParameters(command="test", args=["arg1"], env={"CUSTOM": "value"})
        transport = StdioTransport(params)
        # Verify env was merged with system env
        assert "CUSTOM" in transport.server_params.env
        assert transport.server_params.command == "test"

    @pytest.mark.asyncio
    async def test_get_process_info_with_exceptions(self, transport):
        """Test get_process_info handles various exceptions."""
        transport._process_id = 12345
        transport.process_monitor = True

        # Test with psutil.NoSuchProcess
        import psutil

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process",
            side_effect=psutil.NoSuchProcess(12345),
        ):
            result = await transport._get_process_info()
            assert result is None

        # Test with psutil.AccessDenied
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process",
            side_effect=psutil.AccessDenied(12345),
        ):
            result = await transport._get_process_info()
            assert result is None

        # Test with AttributeError
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.psutil.Process",
            side_effect=AttributeError("No attribute"),
        ):
            result = await transport._get_process_info()
            assert result is None

    @pytest.mark.asyncio
    async def test_monitor_process_health_no_process_info(self, transport):
        """Test process health monitoring when get_process_info returns None."""
        transport._process_id = 12345
        transport.process_monitor = True

        with patch.object(transport, "_get_process_info", AsyncMock(return_value=None)):
            result = await transport._monitor_process_health()
            assert result is True  # Should return True in test environments

    @pytest.mark.asyncio
    async def test_send_ping_false_result(self, transport):
        """Test send_ping when ping returns False."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._client = _mock_dual_client(ping=False)

        result = await transport.send_ping()
        assert result is False
        assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_get_tools_pydantic_model_with_tools_attribute(self, transport):
        """Test get_tools with Pydantic model containing tools attribute."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        # Mock a Pydantic model with tools attribute
        mock_tool = Mock()
        mock_tool.model_dump.return_value = {"name": "test_tool"}
        mock_response = Mock()
        mock_response.tools = [mock_tool]

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=mock_response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "test_tool"

    @pytest.mark.asyncio
    async def test_get_tools_pydantic_model_with_dict_method(self, transport):
        """Test get_tools with older Pydantic model using dict() method."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        # Mock an older Pydantic model with dict() method
        mock_tool = Mock()
        mock_tool.dict.return_value = {"name": "test_tool"}
        delattr(mock_tool, "model_dump")  # Remove model_dump to simulate older version
        mock_response = Mock()
        mock_response.tools = [mock_tool]

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=mock_response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "test_tool"

    @pytest.mark.asyncio
    async def test_get_tools_pydantic_model_dump(self, transport):
        """Test get_tools with Pydantic model that has model_dump."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        mock_response = Mock()
        mock_response.model_dump.return_value = {"tools": [{"name": "test_tool"}]}
        delattr(mock_response, "tools")  # Ensure it doesn't have tools attribute

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=mock_response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "test_tool"

    @pytest.mark.asyncio
    async def test_get_tools_dict_with_nested_result(self, transport):
        """Test get_tools with dict response containing nested result.tools."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"result": {"tools": [{"name": "nested_tool"}]}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "nested_tool"

    @pytest.mark.asyncio
    async def test_get_tools_jsonrpc_format_with_dict_result(self, transport):
        """Test get_tools with JSON-RPC format response."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"jsonrpc": "2.0", "result": {"tools": [{"name": "rpc_tool"}]}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "rpc_tool"

    @pytest.mark.asyncio
    async def test_get_tools_jsonrpc_format_with_list_result(self, transport):
        """Test get_tools with JSON-RPC format response where result is a list."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"jsonrpc": "2.0", "result": [{"name": "rpc_tool_list"}]}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert len(tools) == 1
            assert tools[0]["name"] == "rpc_tool_list"

    @pytest.mark.asyncio
    async def test_get_tools_jsonrpc_format_with_other_result(self, transport):
        """Test get_tools with JSON-RPC format response where result is neither dict nor list."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"jsonrpc": "2.0", "result": "unexpected"}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_get_tools_dict_without_tools_key(self, transport):
        """Test get_tools with dict response without tools key."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"other_key": "value"}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_get_tools_jsonrpc_format_dict_without_tools(self, transport):
        """Test get_tools with JSON-RPC format where result dict has no tools key."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        response = {"jsonrpc": "2.0", "result": {"other": "data"}}

        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_list",
            AsyncMock(return_value=response),
        ):
            tools = await transport.get_tools()
            assert tools == []

    @pytest.mark.asyncio
    async def test_call_tool_with_connection_recovery(self, transport):
        """Test call_tool attempts recovery when connection is unhealthy."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._consecutive_failures = 5  # Make connection unhealthy

        # Mock recovery to fail
        with patch.object(transport, "_attempt_recovery", AsyncMock(return_value=False)):
            result = await transport.call_tool("test", {})
            assert result["isError"] is True
            assert "Failed to recover connection" in result["error"]

    @pytest.mark.asyncio
    async def test_call_tool_with_successful_recovery(self, transport):
        """Test call_tool successfully recovers connection."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())
        transport._consecutive_failures = 5  # Make connection unhealthy

        response = {"result": {"content": "success"}}

        # Mock recovery to succeed
        with (
            patch.object(transport, "_attempt_recovery", AsyncMock(return_value=True)),
            patch(
                "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
                AsyncMock(return_value=response),
            ),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is False

    @pytest.mark.asyncio
    async def test_call_tool_with_process_error(self, transport):
        """Test call_tool handles process-related errors."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        # Test with broken pipe error
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(side_effect=Exception("Broken pipe error")),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is True
            assert transport._initialized is False
            assert transport._metrics["process_crashes"] == 1

        # Reset for next test
        transport._initialized = True
        transport._metrics["process_crashes"] = 0

        # Test with process died error
        with patch(
            "chuk_tool_processor.mcp.transport.stdio_transport.send_tools_call",
            AsyncMock(side_effect=Exception("process died unexpectedly")),
        ):
            result = await transport.call_tool("test", {})
            assert result["isError"] is True
            assert transport._initialized is False
            assert transport._metrics["process_crashes"] == 1

    @pytest.mark.asyncio
    async def test_normalize_response_with_direct_content(self, transport):
        """Test normalize response with direct content field."""
        response = {"content": [{"type": "text", "text": "direct content"}]}
        result = transport._normalize_mcp_response(response)
        assert result["isError"] is False
        assert result["content"] == "direct content"

    @pytest.mark.asyncio
    async def test_normalize_response_with_plain_response(self, transport):
        """Test normalize response with plain response object."""
        response = {"data": "value"}
        result = transport._normalize_mcp_response(response)
        assert result["isError"] is False
        assert result["content"] == {"data": "value"}

    def test_extract_stdio_content_non_list(self, transport):
        """Test extract content with non-list input."""
        result = transport._extract_stdio_content("plain string")
        assert result == "plain string"

    def test_extract_stdio_content_empty_list(self, transport):
        """Test extract content with empty list."""
        result = transport._extract_stdio_content([])
        assert result == []

    def test_extract_stdio_content_multiple_items(self, transport):
        """Test extract content with multiple items in list."""
        content = [{"type": "text", "text": "item1"}, {"type": "text", "text": "item2"}]
        result = transport._extract_stdio_content(content)
        assert result == content  # Should return the list as-is

    def test_extract_stdio_content_non_text_type(self, transport):
        """Test extract content with non-text type."""
        content = [{"type": "image", "data": "base64data"}]
        result = transport._extract_stdio_content(content)
        assert result == {"type": "image", "data": "base64data"}

    @pytest.mark.asyncio
    async def test_list_resources_timeout(self, transport):
        """Test list_resources with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.list_resources()
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_list_prompts_timeout(self, transport):
        """Test list_prompts with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_list",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.list_prompts()
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_read_resource_timeout(self, transport):
        """Test read_resource with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_resources_read",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.read_resource("test://uri")
            assert result == {}
            assert transport._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_get_prompt_timeout(self, transport):
        """Test get_prompt with timeout."""
        transport._initialized = True
        transport._streams = (Mock(), Mock())

        with patch(
            "chuk_tool_processor.mcp.transport._stream_resource_methods.send_prompts_get",
            AsyncMock(side_effect=asyncio.TimeoutError),
        ):
            result = await transport.get_prompt("test_prompt")
            assert result == {}
            assert transport._consecutive_failures == 1
