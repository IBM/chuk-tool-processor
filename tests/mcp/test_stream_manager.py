"""Tests for the actual StreamManager API."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from chuk_tool_processor.mcp.stream_manager import StreamManager
from chuk_tool_processor.mcp.transport import MCPBaseTransport

# Tests in this module use a mix of sync and async
# pytestmark = pytest.mark.asyncio  # Removed - mark individual tests instead


class TestStreamManager:
    """Tests for StreamManager class."""

    @pytest.fixture
    def stream_manager(self):
        """Create a stream manager instance."""
        return StreamManager()

    @pytest.fixture
    def mock_transport(self):
        """Create a mock transport."""
        transport = AsyncMock(spec=MCPBaseTransport)
        transport.initialize = AsyncMock()
        transport.get_tools = AsyncMock(
            return_value=[{"name": "tool1", "description": "Tool 1"}, {"name": "tool2", "description": "Tool 2"}]
        )
        transport.call_tool = AsyncMock()
        transport.close = AsyncMock()
        transport.is_connected = MagicMock(return_value=True)
        return transport

    @pytest.mark.asyncio
    async def test_init(self, stream_manager):
        """Test StreamManager initialization."""
        assert stream_manager.transports == {}
        assert stream_manager.server_info == []
        assert stream_manager.tool_to_server_map == {}
        assert stream_manager.server_names == {}
        assert stream_manager.all_tools == []
        assert stream_manager._closed is False
        assert stream_manager.timeout_config.shutdown == 2.0

    @pytest.mark.asyncio
    async def test_add_server_stdio(self, stream_manager):
        """Test initializing a STDIO server."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            # Mock config load
            mock_load.return_value = {"command": "python", "args": ["-m", "tool_server"]}

            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[{"name": "stdio_tool", "description": "STDIO tool"}])
            mock_stdio.return_value = mock_transport

            # Initialize the stream manager with servers
            await stream_manager.initialize(
                config_file="",
                servers=["python -m tool_server"],
                server_names={0: "test_stdio"},
                transport_type="stdio",
            )

            # The server name comes from the servers list itself, not server_names
            # servers[0] = "python -m tool_server", so that becomes the key
            assert len(stream_manager.transports) == 1
            # Check that a transport was added with the server name as key
            assert "python -m tool_server" in stream_manager.transports
            assert len(stream_manager.server_info) == 1

    @pytest.mark.asyncio
    async def test_add_server_sse(self, stream_manager):
        """Test initializing with SSE server."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock()
            mock_transport.get_tools = AsyncMock(return_value=[{"name": "sse_tool", "description": "SSE tool"}])
            mock_sse.return_value = mock_transport

            # Use initialize_with_sse method - pass list of dicts with name field
            await stream_manager.initialize_with_sse(
                servers=[
                    {
                        "name": "test_sse",
                        "url": "https://api.example.com/sse",
                        "headers": {"Authorization": "Bearer token"},
                    }
                ],
                server_names={0: "test_sse"},
            )

            assert len(stream_manager.transports) == 1
            assert len(stream_manager.server_info) == 1

    @pytest.mark.asyncio
    async def test_add_server_http_streamable(self, stream_manager):
        """Test initializing with HTTP Streamable server."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock()
            mock_transport.get_tools = AsyncMock(return_value=[{"name": "http_tool", "description": "HTTP tool"}])
            mock_http.return_value = mock_transport

            # Use initialize_with_http_streamable method - pass list of dicts with name field
            await stream_manager.initialize_with_http_streamable(
                servers=[{"name": "test_http", "url": "https://api.example.com/stream"}], server_names={0: "test_http"}
            )

            assert len(stream_manager.transports) == 1
            assert len(stream_manager.server_info) == 1

    @pytest.mark.asyncio
    async def test_add_server_invalid_transport(self, stream_manager):
        """Test initializing with invalid transport type."""
        # Initialize with invalid transport should not raise but log warning
        await stream_manager.initialize(config_file="", servers=[], server_names={}, transport_type="invalid_type")

        # Should have no transports
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_add_server_duplicate_name(self, stream_manager):
        """Test that duplicate server names are handled."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}

            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_stdio.return_value = mock_transport

            # Initialize with duplicate names (last one wins)
            await stream_manager.initialize(
                config_file="",
                servers=["python", "python2"],
                server_names={0: "duplicate", 1: "duplicate"},
                transport_type="stdio",
            )

            # Should have handled the duplicates gracefully
            assert len(stream_manager.transports) <= 2

    @pytest.mark.asyncio
    async def test_call_tool(self, stream_manager, mock_transport):
        """Test calling a tool."""
        stream_manager.transports["test_server"] = mock_transport
        stream_manager.tool_to_server_map["test_tool"] = "test_server"

        mock_transport.call_tool.return_value = {"result": "success"}

        result = await stream_manager.call_tool("test_tool", {"arg": "value"})

        assert result["result"] == "success"
        mock_transport.call_tool.assert_called_once_with("test_tool", {"arg": "value"})

    @pytest.mark.asyncio
    async def test_call_tool_not_found(self, stream_manager):
        """Test calling a tool that doesn't exist."""
        result = await stream_manager.call_tool("nonexistent_tool", {})

        # The actual error message says "No server found for tool"
        assert result["isError"] is True
        assert "No server found for tool" in result["error"]

    def test_get_all_tools(self, stream_manager, mock_transport):
        """Test getting all tools."""
        from chuk_tool_processor.mcp.transport.models import MCPToolDefinition

        stream_manager.transports["server1"] = mock_transport
        stream_manager.all_tools = [
            MCPToolDefinition(name="tool1", description="Tool 1"),
            MCPToolDefinition(name="tool2", description="Tool 2"),
        ]

        # get_all_tools is not async
        tools = stream_manager.get_all_tools()

        assert len(tools) == 2
        assert tools[0]["name"] == "tool1"
        assert tools[1]["name"] == "tool2"

    def test_get_server_info(self, stream_manager):
        """Test getting server info."""
        from chuk_tool_processor.mcp.transport.models import ServerInfo

        stream_manager.server_info = [
            ServerInfo(id=0, name="server1", tools=1, status="Up"),
            ServerInfo(id=1, name="server2", tools=2, status="Up"),
        ]

        # get_server_info is not async
        info = stream_manager.get_server_info()

        assert len(info) == 2
        assert info[0]["name"] == "server1"
        assert info[1]["name"] == "server2"

    @pytest.mark.asyncio
    async def test_close(self, stream_manager, mock_transport):
        """Test closing the stream manager."""
        stream_manager.transports["test_server"] = mock_transport

        await stream_manager.close()

        assert stream_manager._closed is True
        mock_transport.close.assert_called_once()
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_close_with_timeout(self, stream_manager):
        """Test closing with slow transport shutdown."""
        slow_transport = AsyncMock(spec=MCPBaseTransport)

        async def slow_close():
            await asyncio.sleep(5)  # Longer than timeout

        slow_transport.close = slow_close
        stream_manager.transports["slow"] = slow_transport

        # Should timeout and continue
        await stream_manager.close()
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_context_manager(self, stream_manager):
        """Test using StreamManager as context manager."""
        async with stream_manager as sm:
            assert sm is stream_manager
            assert sm._closed is False

        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_tool_registration_from_multiple_servers(self, stream_manager):
        """Test tools from multiple servers."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}

            # Create two different transports
            transport1 = AsyncMock(spec=MCPBaseTransport)
            transport1.initialize = AsyncMock(return_value=True)
            transport1.get_tools = AsyncMock(return_value=[{"name": "tool_a", "description": "Tool A"}])

            transport2 = AsyncMock(spec=MCPBaseTransport)
            transport2.initialize = AsyncMock(return_value=True)
            transport2.get_tools = AsyncMock(return_value=[{"name": "tool_b", "description": "Tool B"}])

            mock_stdio.side_effect = [transport1, transport2]

            # Initialize with multiple servers
            await stream_manager.initialize(
                config_file="",
                servers=["cmd1", "cmd2"],
                server_names={0: "server1", 1: "server2"},
                transport_type="stdio",
            )

            # Check tools were registered
            assert "tool_a" in stream_manager.tool_to_server_map
            assert "tool_b" in stream_manager.tool_to_server_map
            # The server names are the actual server strings from the list
            assert stream_manager.tool_to_server_map["tool_a"] == "cmd1"
            assert stream_manager.tool_to_server_map["tool_b"] == "cmd2"

    @pytest.mark.asyncio
    async def test_load_config_integration(self, stream_manager):
        """Test loading configuration."""
        # StreamManager doesn't have a load_from_config method
        # Instead it initializes with a config file
        with patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load:
            mock_load.return_value = {"command": "python", "args": ["-m", "server"]}

            with patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio:
                mock_transport = AsyncMock(spec=MCPBaseTransport)
                mock_transport.initialize = AsyncMock(return_value=True)
                mock_transport.get_tools = AsyncMock(return_value=[])
                mock_stdio.return_value = mock_transport

                await stream_manager.initialize(
                    config_file="test_config.json", servers=["test_server"], transport_type="stdio"
                )

                # Check that config was loaded
                assert mock_load.called

    @pytest.mark.asyncio
    async def test_concurrent_tool_calls(self, stream_manager, mock_transport):
        """Test concurrent tool calls to same server."""
        stream_manager.transports["server"] = mock_transport
        stream_manager.tool_to_server_map["tool1"] = "server"
        stream_manager.tool_to_server_map["tool2"] = "server"

        async def delayed_response(tool_name, args):
            await asyncio.sleep(0.1)
            return {"tool": tool_name, "result": "done"}

        mock_transport.call_tool = delayed_response

        # Call tools concurrently
        results = await asyncio.gather(stream_manager.call_tool("tool1", {}), stream_manager.call_tool("tool2", {}))

        assert results[0]["tool"] == "tool1"
        assert results[1]["tool"] == "tool2"

    @pytest.mark.asyncio
    async def test_server_initialization_failure(self, stream_manager):
        """Test handling server initialization failure."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "bad_command", "args": []}

            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(side_effect=Exception("Init failed"))
            mock_stdio.return_value = mock_transport

            # Initialize should handle errors gracefully
            await stream_manager.initialize(
                config_file="", servers=["bad_command"], server_names={0: "failing_server"}, transport_type="stdio"
            )

            # Should not have added the failing server
            assert "bad_command" not in stream_manager.transports

    # ------------------------------------------------------------------ #
    # Factory method tests                                               #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_create_factory_method(self):
        """Test create factory method."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_stdio.return_value = mock_transport

            stream_manager = await StreamManager.create(
                config_file="test.json", servers=["test"], transport_type="stdio", default_timeout=30.0
            )

            assert stream_manager is not None
            assert isinstance(stream_manager, StreamManager)

    @pytest.mark.asyncio
    async def test_create_with_timeout(self):
        """Test create factory method with initialization timeout - graceful handling."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            # Simulate slow initialization
            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_stdio.return_value = mock_transport

            # Should handle timeout gracefully (no exception, but no tools)
            stream_manager = await StreamManager.create(
                config_file="test.json",
                servers=["test"],
                transport_type="stdio",
                initialization_timeout=0.1,
            )

            # Manager should be created but have no tools due to timeout
            assert isinstance(stream_manager, StreamManager)
            assert len(stream_manager.get_all_tools()) == 0

    @pytest.mark.asyncio
    async def test_create_with_sse_factory(self):
        """Test create_with_sse factory method."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            stream_manager = await StreamManager.create_with_sse(servers=[{"name": "test", "url": "http://test.com"}])

            assert stream_manager is not None
            assert isinstance(stream_manager, StreamManager)

    @pytest.mark.asyncio
    async def test_create_with_sse_timeout(self):
        """Test create_with_sse with timeout - graceful handling."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_sse.return_value = mock_transport

            # Should handle timeout gracefully (no exception, but no tools)
            stream_manager = await StreamManager.create_with_sse(
                servers=[{"name": "test", "url": "http://test.com"}], initialization_timeout=0.1
            )

            # Manager should be created but have no tools due to timeout
            assert isinstance(stream_manager, StreamManager)
            assert len(stream_manager.get_all_tools()) == 0

    @pytest.mark.asyncio
    async def test_create_with_http_streamable_factory(self):
        """Test create_with_http_streamable factory method."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            stream_manager = await StreamManager.create_with_http_streamable(
                servers=[{"name": "test", "url": "http://test.com"}]
            )

            assert stream_manager is not None
            assert isinstance(stream_manager, StreamManager)

    @pytest.mark.asyncio
    async def test_create_with_http_streamable_timeout(self):
        """Test create_with_http_streamable with timeout - graceful handling."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_http.return_value = mock_transport

            # Should handle timeout gracefully (no exception, but no tools)
            stream_manager = await StreamManager.create_with_http_streamable(
                servers=[{"name": "test", "url": "http://test.com"}], initialization_timeout=0.1
            )

            # Manager should be created but have no tools due to timeout
            assert isinstance(stream_manager, StreamManager)
            assert len(stream_manager.get_all_tools()) == 0

    @pytest.mark.asyncio
    async def test_create_managed_context_manager(self):
        """Test create_managed factory method."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_transport.close = AsyncMock()
            mock_stdio.return_value = mock_transport

            async with StreamManager.create_managed(
                config_file="test.json", servers=["test"], transport_type="stdio"
            ) as sm:
                assert sm is not None
                assert isinstance(sm, StreamManager)
                assert sm._closed is False

            # Should be closed after exiting context
            assert sm._closed is True

    # ------------------------------------------------------------------ #
    # Error handling and edge cases                                      #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_initialize_when_closed(self):
        """Test that initialize raises error when StreamManager is closed."""
        stream_manager = StreamManager()
        await stream_manager.close()

        with pytest.raises(RuntimeError, match="Cannot initialize a closed StreamManager"):
            await stream_manager.initialize(config_file="test.json", servers=["test"])

    @pytest.mark.asyncio
    async def test_initialize_with_sse_when_closed(self):
        """Test initialize_with_sse raises error when closed."""
        stream_manager = StreamManager()
        await stream_manager.close()

        with pytest.raises(RuntimeError, match="Cannot initialize a closed StreamManager"):
            await stream_manager.initialize_with_sse(servers=[{"name": "test", "url": "http://test.com"}])

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_when_closed(self):
        """Test initialize_with_http_streamable raises error when closed."""
        stream_manager = StreamManager()
        await stream_manager.close()

        with pytest.raises(RuntimeError, match="Cannot initialize a closed StreamManager"):
            await stream_manager.initialize_with_http_streamable(servers=[{"name": "test", "url": "http://test.com"}])

    @pytest.mark.asyncio
    async def test_initialize_with_sse_type_warning(self, stream_manager):
        """Test that using SSE in initialize() logs warning."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse,
        ):
            mock_load.return_value = ({"url": "http://test.com"}, None)
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            # Should work but log warning
            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="sse", default_timeout=30.0
            )

            assert len(stream_manager.transports) == 1

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_type_warning(self, stream_manager):
        """Test that using http_streamable in initialize() logs warning."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http,
        ):
            mock_load.return_value = ({"url": "http://test.com"}, None)
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="http_streamable", default_timeout=30.0
            )

            assert len(stream_manager.transports) == 1

    @pytest.mark.asyncio
    async def test_initialize_sse_with_headers(self, stream_manager):
        """Test initialize with SSE and headers."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse,
        ):
            mock_load.return_value = ({"url": "http://test.com", "headers": {"Auth": "Bearer token"}}, None)
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="sse", default_timeout=30.0
            )

            # Verify SSETransport was called with headers
            call_kwargs = mock_sse.call_args[1]
            assert "headers" in call_kwargs
            assert call_kwargs["headers"]["Auth"] == "Bearer token"

    @pytest.mark.asyncio
    async def test_initialize_http_streamable_with_headers_warning(self, stream_manager):
        """Test initialize with HTTP streamable and headers logs warning."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http,
        ):
            mock_load.return_value = (
                {
                    "url": "http://test.com",
                    "headers": {"Auth": "Bearer token"},
                    "session_id": "test-session",
                },
                None,
            )
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="http_streamable", default_timeout=30.0
            )

            # Should work but headers not passed (not supported yet)
            call_kwargs = mock_http.call_args[1]
            assert "headers" not in call_kwargs

    @pytest.mark.asyncio
    async def test_initialize_sse_default_url(self, stream_manager):
        """Test initialize with SSE and no URL uses default."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse,
        ):
            mock_load.return_value = ("not_a_dict", None)  # Invalid config
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="sse", default_timeout=30.0
            )

            # Should use default URL
            call_kwargs = mock_sse.call_args[1]
            assert call_kwargs["url"] == "http://localhost:8000"

    @pytest.mark.asyncio
    async def test_initialize_http_streamable_default_url(self, stream_manager):
        """Test initialize with HTTP streamable and no URL uses default."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http,
        ):
            mock_load.return_value = ("not_a_dict", None)
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="http_streamable", default_timeout=30.0
            )

            call_kwargs = mock_http.call_args[1]
            assert call_kwargs["url"] == "http://localhost:8000"

    @pytest.mark.asyncio
    async def test_initialize_transport_init_failure(self, stream_manager):
        """Test when transport.initialize() returns False."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=False)  # Init fails
            mock_stdio.return_value = mock_transport

            await stream_manager.initialize(config_file="test.json", servers=["test"], transport_type="stdio")

            # Should not add the transport
            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_timeout_on_init(self, stream_manager):
        """Test timeout during transport initialization."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
            patch("chuk_tool_processor.mcp.stream_manager.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = {"command": "python", "args": []}
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_stdio.return_value = mock_transport

            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="stdio", initialization_timeout=0.1
            )

            # Should not add the transport due to timeout
            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_sse_bad_config(self, stream_manager):
        """Test initialize_with_sse with missing name or URL."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport"):
            # Missing name
            await stream_manager.initialize_with_sse(servers=[{"url": "http://test.com"}])
            assert len(stream_manager.transports) == 0

            # Missing URL
            await stream_manager.initialize_with_sse(servers=[{"name": "test"}])
            assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_with_sse_init_failure(self, stream_manager):
        """Test initialize_with_sse when transport init fails."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=False)
            mock_sse.return_value = mock_transport

            await stream_manager.initialize_with_sse(servers=[{"name": "test", "url": "http://test.com"}])

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_sse_timeout(self, stream_manager):
        """Test initialize_with_sse with timeout."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow
            mock_sse.return_value = mock_transport

            await stream_manager.initialize_with_sse(
                servers=[{"name": "test", "url": "http://test.com"}], initialization_timeout=0.1
            )

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_sse_exception(self, stream_manager):
        """Test initialize_with_sse handles exceptions."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_sse.side_effect = Exception("Connection failed")

            await stream_manager.initialize_with_sse(servers=[{"name": "test", "url": "http://test.com"}])

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_sse_headers_support(self, stream_manager):
        """Test initialize_with_sse with headers."""
        with patch("chuk_tool_processor.mcp.stream_manager.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            await stream_manager.initialize_with_sse(
                servers=[
                    {"name": "test", "url": "http://test.com", "api_key": "key123", "headers": {"Custom": "Header"}}
                ]
            )

            # Verify headers were passed
            call_kwargs = mock_sse.call_args[1]
            assert "headers" in call_kwargs
            assert call_kwargs["headers"]["Custom"] == "Header"

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_bad_config(self, stream_manager):
        """Test initialize_with_http_streamable with missing name or URL."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport"):
            await stream_manager.initialize_with_http_streamable(servers=[{"url": "http://test.com"}])
            assert len(stream_manager.transports) == 0

            await stream_manager.initialize_with_http_streamable(servers=[{"name": "test"}])
            assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_init_failure(self, stream_manager):
        """Test initialize_with_http_streamable when transport init fails."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=False)
            mock_http.return_value = mock_transport

            await stream_manager.initialize_with_http_streamable(servers=[{"name": "test", "url": "http://test.com"}])

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_timeout(self, stream_manager):
        """Test initialize_with_http_streamable with timeout."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow
            mock_http.return_value = mock_transport

            await stream_manager.initialize_with_http_streamable(
                servers=[{"name": "test", "url": "http://test.com"}], initialization_timeout=0.1
            )

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_exception(self, stream_manager):
        """Test initialize_with_http_streamable handles exceptions."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_http.side_effect = Exception("Connection failed")

            await stream_manager.initialize_with_http_streamable(servers=[{"name": "test", "url": "http://test.com"}])

            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_headers_support(self, stream_manager):
        """Test initialize_with_http_streamable passes headers correctly."""
        with patch("chuk_tool_processor.mcp.stream_manager.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            await stream_manager.initialize_with_http_streamable(
                servers=[
                    {
                        "name": "test",
                        "url": "http://test.com",
                        "headers": {"Custom": "Header", "Authorization": "Bearer token"},
                    }
                ]
            )

            # Headers should now be passed
            call_kwargs = mock_http.call_args[1]
            assert "headers" in call_kwargs
            assert call_kwargs["headers"]["Custom"] == "Header"
            assert call_kwargs["headers"]["Authorization"] == "Bearer token"

    # ------------------------------------------------------------------ #
    # Query methods tests                                                #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_get_server_for_tool(self, stream_manager):
        """Test get_server_for_tool method."""
        stream_manager.tool_to_server_map["tool1"] = "server1"
        stream_manager.tool_to_server_map["tool2"] = "server2"

        assert stream_manager.get_server_for_tool("tool1") == "server1"
        assert stream_manager.get_server_for_tool("tool2") == "server2"
        assert stream_manager.get_server_for_tool("nonexistent") is None

    @pytest.mark.asyncio
    async def test_list_tools(self, stream_manager, mock_transport):
        """Test list_tools method."""
        stream_manager.transports["test_server"] = mock_transport
        mock_transport.get_tools = AsyncMock(return_value=[{"name": "tool1"}, {"name": "tool2"}])

        tools = await stream_manager.list_tools("test_server")

        assert len(tools) == 2
        assert tools[0]["name"] == "tool1"
        assert tools[1]["name"] == "tool2"

    @pytest.mark.asyncio
    async def test_list_tools_when_closed(self, stream_manager):
        """Test list_tools returns empty when closed."""
        await stream_manager.close()

        tools = await stream_manager.list_tools("test_server")

        assert tools == []

    @pytest.mark.asyncio
    async def test_list_tools_server_not_found(self, stream_manager):
        """Test list_tools with non-existent server."""
        tools = await stream_manager.list_tools("nonexistent_server")

        assert tools == []

    @pytest.mark.asyncio
    async def test_list_tools_timeout(self, stream_manager):
        """Test list_tools with timeout."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def slow_get_tools():
            await asyncio.sleep(20)
            return []

        mock_transport.get_tools = slow_get_tools
        stream_manager.transports["slow_server"] = mock_transport

        tools = await stream_manager.list_tools("slow_server")

        assert tools == []

    @pytest.mark.asyncio
    async def test_list_tools_exception(self, stream_manager):
        """Test list_tools handles exceptions."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.get_tools = AsyncMock(side_effect=Exception("Failed"))
        stream_manager.transports["failing_server"] = mock_transport

        tools = await stream_manager.list_tools("failing_server")

        assert tools == []

    # ------------------------------------------------------------------ #
    # Ping, resources, prompts tests                                    #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_ping_servers(self, stream_manager, mock_transport):
        """Test ping_servers method."""
        stream_manager.transports["server1"] = mock_transport
        mock_transport.send_ping = AsyncMock(return_value=True)

        results = await stream_manager.ping_servers()

        assert len(results) == 1
        assert results[0]["server"] == "server1"
        assert results[0]["ok"] is True

    @pytest.mark.asyncio
    async def test_ping_servers_when_closed(self, stream_manager):
        """Test ping_servers returns empty when closed."""
        await stream_manager.close()

        results = await stream_manager.ping_servers()

        assert results == []

    @pytest.mark.asyncio
    async def test_ping_servers_exception(self, stream_manager):
        """Test ping_servers handles exceptions."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.send_ping = AsyncMock(side_effect=Exception("Ping failed"))
        stream_manager.transports["failing_server"] = mock_transport

        results = await stream_manager.ping_servers()

        assert len(results) == 1
        assert results[0]["server"] == "failing_server"
        assert results[0]["ok"] is False

    @pytest.mark.asyncio
    async def test_list_resources(self, stream_manager, mock_transport):
        """Test list_resources method."""
        stream_manager.transports["server1"] = mock_transport
        mock_transport.list_resources = AsyncMock(return_value={"resources": [{"name": "res1"}]})

        resources = await stream_manager.list_resources()

        assert len(resources) == 1
        assert resources[0]["name"] == "res1"
        assert resources[0]["server"] == "server1"

    @pytest.mark.asyncio
    async def test_list_resources_when_closed(self, stream_manager):
        """Test list_resources returns empty when closed."""
        await stream_manager.close()

        resources = await stream_manager.list_resources()

        assert resources == []

    @pytest.mark.asyncio
    async def test_list_resources_exception(self, stream_manager):
        """Test list_resources handles exceptions."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.list_resources = AsyncMock(side_effect=Exception("Failed"))
        stream_manager.transports["failing_server"] = mock_transport

        resources = await stream_manager.list_resources()

        assert resources == []

    @pytest.mark.asyncio
    async def test_list_prompts(self, stream_manager, mock_transport):
        """Test list_prompts method."""
        stream_manager.transports["server1"] = mock_transport
        mock_transport.list_prompts = AsyncMock(return_value={"prompts": [{"name": "prompt1"}]})

        prompts = await stream_manager.list_prompts()

        assert len(prompts) == 1
        assert prompts[0]["name"] == "prompt1"
        assert prompts[0]["server"] == "server1"

    @pytest.mark.asyncio
    async def test_list_prompts_when_closed(self, stream_manager):
        """Test list_prompts returns empty when closed."""
        await stream_manager.close()

        prompts = await stream_manager.list_prompts()

        assert prompts == []

    @pytest.mark.asyncio
    async def test_list_prompts_exception(self, stream_manager):
        """Test list_prompts handles exceptions."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.list_prompts = AsyncMock(side_effect=Exception("Failed"))
        stream_manager.transports["failing_server"] = mock_transport

        prompts = await stream_manager.list_prompts()

        assert prompts == []

    @pytest.mark.asyncio
    async def test_get_prompt(self, stream_manager, mock_transport):
        """get_prompt returns the prompt result from the transport."""
        stream_manager.transports["server1"] = mock_transport
        mock_transport.get_prompt = AsyncMock(
            return_value={"messages": [{"role": "user", "content": {"type": "text", "text": "hi"}}]}
        )

        result = await stream_manager.get_prompt("greet", {"who": "world"})

        assert result["messages"][0]["role"] == "user"
        mock_transport.get_prompt.assert_awaited_once_with("greet", {"who": "world"})

    @pytest.mark.asyncio
    async def test_get_prompt_specific_server(self, stream_manager, mock_transport):
        """get_prompt targets a named server and skips the others."""
        other = AsyncMock(spec=MCPBaseTransport)
        other.get_prompt = AsyncMock(return_value={"messages": []})
        stream_manager.transports["s1"] = other
        stream_manager.transports["s2"] = mock_transport
        mock_transport.get_prompt = AsyncMock(return_value={"messages": [{"role": "assistant", "content": {}}]})

        result = await stream_manager.get_prompt("p", server_name="s2")

        assert result["messages"][0]["role"] == "assistant"
        mock_transport.get_prompt.assert_awaited_once()
        other.get_prompt.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_prompt_when_closed(self, stream_manager):
        """get_prompt returns empty when the manager is closed."""
        await stream_manager.close()

        assert await stream_manager.get_prompt("p") == {}

    @pytest.mark.asyncio
    async def test_get_prompt_exception(self, stream_manager):
        """get_prompt swallows transport errors and returns {}."""
        tr = AsyncMock(spec=MCPBaseTransport)
        tr.get_prompt = AsyncMock(side_effect=Exception("Failed"))
        stream_manager.transports["s"] = tr

        assert await stream_manager.get_prompt("p") == {}

    # ------------------------------------------------------------------ #
    # call_tool method with timeout tests                               #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_call_tool_when_closed(self, stream_manager):
        """Test call_tool returns error when closed."""
        await stream_manager.close()

        result = await stream_manager.call_tool("tool1", {})

        assert result["isError"] is True
        assert "closed" in result["error"]

    @pytest.mark.asyncio
    async def test_call_tool_with_explicit_server(self, stream_manager, mock_transport):
        """Test call_tool with explicit server_name parameter."""
        stream_manager.transports["test_server"] = mock_transport
        mock_transport.call_tool = AsyncMock(return_value={"result": "success"})

        result = await stream_manager.call_tool("tool1", {"arg": "value"}, server_name="test_server")

        assert result["result"] == "success"

    @pytest.mark.asyncio
    async def test_call_tool_with_timeout_param(self, stream_manager, mock_transport):
        """Test call_tool with timeout parameter."""
        stream_manager.transports["test_server"] = mock_transport
        stream_manager.tool_to_server_map["test_tool"] = "test_server"
        mock_transport.call_tool = AsyncMock(return_value={"result": "success"})

        result = await stream_manager.call_tool("test_tool", {"arg": "value"}, timeout=30.0)

        assert result["result"] == "success"

    @pytest.mark.asyncio
    async def test_call_tool_timeout_expires(self, stream_manager):
        """Test call_tool when timeout expires."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def slow_call(tool_name, args):
            await asyncio.sleep(10)
            return {"result": "done"}

        mock_transport.call_tool = slow_call
        stream_manager.transports["test_server"] = mock_transport
        stream_manager.tool_to_server_map["test_tool"] = "test_server"

        result = await stream_manager.call_tool("test_tool", {}, timeout=0.1)

        assert result["isError"] is True
        assert "timed out" in result["error"]

    @pytest.mark.asyncio
    async def test_call_tool_transport_supports_timeout(self, stream_manager):
        """Test call_tool when transport has timeout parameter."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        # Create a method that accepts timeout parameter
        async def call_with_timeout(tool_name, args, timeout=None):
            return {"result": "success", "timeout": timeout}

        mock_transport.call_tool = call_with_timeout
        stream_manager.transports["test_server"] = mock_transport
        stream_manager.tool_to_server_map["test_tool"] = "test_server"

        result = await stream_manager.call_tool("test_tool", {}, timeout=15.0)

        assert result["result"] == "success"
        assert result["timeout"] == 15.0

    # ------------------------------------------------------------------ #
    # Close method tests                                                 #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_close_already_closed(self, stream_manager):
        """Test close when already closed."""
        await stream_manager.close()

        # Should not raise, just return
        await stream_manager.close()

        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_close_no_transports(self, stream_manager):
        """Test close when no transports."""
        await stream_manager.close()

        assert stream_manager._closed is True
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_close_with_exception_during_cleanup(self, stream_manager):
        """Test close handles exceptions during cleanup."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.close = AsyncMock(side_effect=Exception("Close failed"))
        stream_manager.transports["failing_server"] = mock_transport

        # Should not raise
        await stream_manager.close()

        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_close_with_cancelled_error(self, stream_manager):
        """Test close handles CancelledError."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def cancel_close():
            raise asyncio.CancelledError()

        mock_transport.close = cancel_close
        stream_manager.transports["cancelling_server"] = mock_transport

        # Should handle cancellation gracefully
        await stream_manager.close()

        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_list_resources_non_dict_response(self, stream_manager):
        """Test list_resources when response is not dict."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        # Return list directly instead of dict
        mock_transport.list_resources = AsyncMock(return_value=[{"name": "res1"}])
        stream_manager.transports["server1"] = mock_transport

        resources = await stream_manager.list_resources()

        assert len(resources) == 1
        assert resources[0]["name"] == "res1"

    @pytest.mark.asyncio
    async def test_list_prompts_non_dict_response(self, stream_manager):
        """Test list_prompts when response is not dict."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        # Return list directly instead of dict
        mock_transport.list_prompts = AsyncMock(return_value=[{"name": "prompt1"}])
        stream_manager.transports["server1"] = mock_transport

        prompts = await stream_manager.list_prompts()

        assert len(prompts) == 1
        assert prompts[0]["name"] == "prompt1"

    # ------------------------------------------------------------------ #
    # Stream and diagnostic methods                                      #
    # ------------------------------------------------------------------ #
    def test_get_streams(self, stream_manager):
        """Test get_streams method."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.get_streams = lambda: [(None, None)]
        stream_manager.transports["server1"] = mock_transport

        streams = stream_manager.get_streams()

        assert len(streams) == 1

    def test_get_streams_when_closed(self, stream_manager):
        """Test get_streams returns empty when closed."""
        stream_manager._closed = True

        streams = stream_manager.get_streams()

        assert streams == []

    def test_get_streams_fallback_to_attributes(self, stream_manager):
        """Test get_streams falls back to read/write stream attributes."""
        # Create mock that doesn't have get_streams but has read/write_stream attributes
        mock_transport = type("MockTransport", (), {"read_stream": "read", "write_stream": "write"})()
        stream_manager.transports["server1"] = mock_transport

        streams = stream_manager.get_streams()

        assert len(streams) == 1
        assert streams[0] == ("read", "write")

    def test_streams_property(self, stream_manager):
        """Test streams property is an alias for get_streams."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.get_streams = lambda: [(None, None)]
        stream_manager.transports["server1"] = mock_transport

        streams = stream_manager.streams

        assert len(streams) == 1

    def test_is_closed(self, stream_manager):
        """Test is_closed method."""
        assert stream_manager.is_closed() is False

        stream_manager._closed = True

        assert stream_manager.is_closed() is True

    def test_get_transport_count(self, stream_manager, mock_transport):
        """Test get_transport_count method."""
        assert stream_manager.get_transport_count() == 0

        stream_manager.transports["server1"] = mock_transport
        assert stream_manager.get_transport_count() == 1

        stream_manager.transports["server2"] = mock_transport
        assert stream_manager.get_transport_count() == 2

    @pytest.mark.asyncio
    async def test_health_check_when_closed(self, stream_manager):
        """Test health_check when closed."""
        stream_manager._closed = True

        health = await stream_manager.health_check()

        assert health["status"] == "closed"
        assert health["transports"] == {}

    @pytest.mark.asyncio
    async def test_health_check_healthy_transports(self, stream_manager):
        """Test health_check with healthy transports."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.send_ping = AsyncMock(return_value=True)
        stream_manager.transports["server1"] = mock_transport

        health = await stream_manager.health_check()

        assert health["status"] == "active"
        assert health["transport_count"] == 1
        assert health["transports"]["server1"]["status"] == "healthy"
        assert health["transports"]["server1"]["ping_success"] is True

    @pytest.mark.asyncio
    async def test_health_check_unhealthy_transports(self, stream_manager):
        """Test health_check with unhealthy transports."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.send_ping = AsyncMock(return_value=False)
        stream_manager.transports["server1"] = mock_transport

        health = await stream_manager.health_check()

        assert health["transports"]["server1"]["status"] == "unhealthy"
        assert health["transports"]["server1"]["ping_success"] is False

    @pytest.mark.asyncio
    async def test_health_check_timeout(self, stream_manager):
        """Test health_check when ping times out."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def slow_ping():
            await asyncio.sleep(10)
            return True

        mock_transport.send_ping = slow_ping
        stream_manager.transports["server1"] = mock_transport

        health = await stream_manager.health_check()

        assert health["transports"]["server1"]["status"] == "timeout"

    @pytest.mark.asyncio
    async def test_health_check_exception(self, stream_manager):
        """Test health_check when ping raises exception."""
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.send_ping = AsyncMock(side_effect=Exception("Ping failed"))
        stream_manager.transports["server1"] = mock_transport

        health = await stream_manager.health_check()

        assert health["transports"]["server1"]["status"] == "error"
        assert "error" in health["transports"]["server1"]

    @pytest.mark.asyncio
    async def test_call_tool_no_hasattr(self, stream_manager):
        """Test call_tool when transport has no call_tool attribute."""
        # Create object without call_tool method
        mock_transport = AsyncMock(spec=[])
        stream_manager.transports["test_server"] = mock_transport
        stream_manager.tool_to_server_map["test_tool"] = "test_server"

        # Should still work with timeout wrapper
        mock_transport.call_tool = AsyncMock(return_value={"result": "success"})

        result = await stream_manager.call_tool("test_tool", {}, timeout=1.0)

        assert result["result"] == "success"

    @pytest.mark.asyncio
    async def test_close_single_transport_no_close_method(self, stream_manager):
        """Test _close_single_transport when transport has no close method."""
        mock_transport = AsyncMock(spec=[])  # No close method
        stream_manager.transports["no_close"] = mock_transport

        # Should handle gracefully
        await stream_manager.close()

        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_close_concurrent_timeout(self, stream_manager):
        """Test close with concurrent timeout."""
        # Create multiple slow transports
        for i in range(3):
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_close():
                await asyncio.sleep(10)

            mock_transport.close = slow_close
            stream_manager.transports[f"slow_{i}"] = mock_transport

        # Set a very short shutdown timeout
        stream_manager.timeout_config.shutdown = 0.1

        await stream_manager.close()

        assert stream_manager._closed is True
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_invalid_transport_type(self, stream_manager):
        """Test initialize with completely invalid transport type."""
        with (
            patch("chuk_tool_processor.mcp.stream_manager.load_config") as mock_load,
        ):
            mock_load.return_value = {"command": "test"}

            await stream_manager.initialize(
                config_file="test.json",
                servers=["test"],
                transport_type="completely_invalid",
            )

            # Should not add any transports
            assert len(stream_manager.transports) == 0
