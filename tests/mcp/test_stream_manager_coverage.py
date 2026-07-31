"""
Additional tests for stream_manager.py to improve code coverage to above 90%.

These tests target specific uncovered lines identified by coverage analysis.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from chuk_tool_processor.mcp.stream_manager import StreamManager
from chuk_tool_processor.mcp.transport import MCPBaseTransport


class TestStreamManagerAdditionalCoverage:
    """Additional tests to improve stream_manager coverage."""

    # ------------------------------------------------------------------ #
    # create_with_stdio factory method coverage (lines 108-115)         #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_create_with_stdio_factory(self):
        """Test create_with_stdio factory method."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_stdio.return_value = mock_transport

            stream_manager = await StreamManager.create_with_stdio(
                servers=[{"name": "test", "command": "python", "args": ["-m", "test"]}],
                default_timeout=30.0,
                initialization_timeout=60.0,
            )

            assert stream_manager is not None
            assert isinstance(stream_manager, StreamManager)
            assert len(stream_manager.transports) == 1

    @pytest.mark.asyncio
    async def test_create_with_stdio_timeout(self):
        """Test create_with_stdio with initialization timeout."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_stdio.return_value = mock_transport

            stream_manager = await StreamManager.create_with_stdio(
                servers=[{"name": "test", "command": "python"}], initialization_timeout=0.1
            )

            # Should handle timeout gracefully
            assert isinstance(stream_manager, StreamManager)
            assert len(stream_manager.get_all_tools()) == 0

    # ------------------------------------------------------------------ #
    # General timeout/exception handling in initialize (line 305)       #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_initialize_general_timeout(self):
        """Test general timeout handling in initialize - covers line 305."""
        with (
            patch("chuk_tool_processor.mcp._stream_manager_init.load_config") as mock_load,
            patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio,
        ):
            mock_load.return_value = ({"command": "python", "args": []}, None)
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)

            # Make send_ping timeout
            async def timeout_ping():
                raise TimeoutError("Ping timeout")

            mock_transport.send_ping = timeout_ping
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="stdio", initialization_timeout=60.0
            )

            # Should handle timeout in ping gracefully (line 305)
            # Transport added but server_info might not be complete

    # ------------------------------------------------------------------ #
    # OAuth callback support (lines 353-354, 511-512)                   #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_initialize_with_sse_oauth_callback(self):
        """Test initialize_with_sse with OAuth refresh callback - covers lines 353-354."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_sse.return_value = mock_transport

            # Create OAuth callback
            oauth_callback = Mock()

            stream_manager = StreamManager()
            await stream_manager.initialize_with_sse(
                servers=[{"name": "test", "url": "http://test.com"}], oauth_refresh_callback=oauth_callback
            )

            # Verify OAuth callback was passed to transport
            call_kwargs = mock_sse.call_args[1]
            assert "oauth_refresh_callback" in call_kwargs
            assert call_kwargs["oauth_refresh_callback"] == oauth_callback

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_oauth_callback(self):
        """Test initialize_with_http_streamable with OAuth callback - covers lines 511-512."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_http.return_value = mock_transport

            oauth_callback = Mock()

            stream_manager = StreamManager()
            await stream_manager.initialize_with_http_streamable(
                servers=[{"name": "test", "url": "http://test.com"}], oauth_refresh_callback=oauth_callback
            )

            # Verify OAuth callback was passed
            call_kwargs = mock_http.call_args[1]
            assert "oauth_refresh_callback" in call_kwargs
            assert call_kwargs["oauth_refresh_callback"] == oauth_callback

    # ------------------------------------------------------------------ #
    # Timeout handling in SSE/HTTP (lines 384, 546)                     #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_initialize_with_sse_timeout_on_ping_or_tools(self):
        """Test timeout during ping/tools retrieval in initialize_with_sse - line 384."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.SSETransport") as mock_sse:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            # Make initialize itself raise TimeoutError in the outer try block
            async def timeout_init():
                raise TimeoutError("Init timeout")

            mock_transport.initialize = timeout_init
            mock_sse.return_value = mock_transport

            stream_manager = StreamManager()
            # This should trigger the except TimeoutError on line 384
            await stream_manager.initialize_with_sse(servers=[{"name": "test", "url": "http://test.com"}])

            # Server should not be added due to timeout in outer exception handler
            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_http_streamable_timeout_on_ping_or_tools(self):
        """Test timeout during ping/tools in initialize_with_http_streamable - line 546."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.HTTPStreamableTransport") as mock_http:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            # Make initialization raise TimeoutError in outer try block
            async def timeout_init():
                raise TimeoutError("Init timeout")

            mock_transport.initialize = timeout_init
            mock_http.return_value = mock_transport

            stream_manager = StreamManager()
            # This should trigger the except TimeoutError on line 546
            await stream_manager.initialize_with_http_streamable(servers=[{"name": "test", "url": "http://test.com"}])

            # Server should not be added due to timeout in outer exception handler
            assert "test" not in stream_manager.transports

    # ------------------------------------------------------------------ #
    # initialize_with_stdio comprehensive coverage (lines 402-464)      #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_initialize_with_stdio_when_closed(self):
        """Test initialize_with_stdio raises error when closed - line 402."""
        stream_manager = StreamManager()
        await stream_manager.close()

        with pytest.raises(RuntimeError, match="Cannot initialize a closed StreamManager"):
            await stream_manager.initialize_with_stdio(servers=[{"name": "test", "command": "python"}])

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_bad_config_missing_name(self):
        """Test initialize_with_stdio with missing name - line 414-415."""
        stream_manager = StreamManager()
        # Missing name
        await stream_manager.initialize_with_stdio(servers=[{"command": "python", "args": []}])

        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_bad_config_missing_command(self):
        """Test initialize_with_stdio with missing command - line 414-415."""
        stream_manager = StreamManager()
        # Missing command
        await stream_manager.initialize_with_stdio(servers=[{"name": "test", "args": []}])

        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_with_env(self):
        """Test initialize_with_stdio with environment variables - lines 424-425."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize_with_stdio(
                servers=[{"name": "test", "command": "python", "args": [], "env": {"VAR": "value"}}]
            )

            # Verify env was passed to transport
            call_args = mock_stdio.call_args[0]
            transport_params = call_args[0]
            assert "env" in transport_params
            assert transport_params["env"]["VAR"] == "value"

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_init_failure(self):
        """Test initialize_with_stdio when transport init fails - line 435-436."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=False)  # Init fails
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize_with_stdio(servers=[{"name": "test", "command": "python"}])

            # Should not add transport
            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_init_timeout(self):
        """Test initialize_with_stdio timeout - lines 437-439."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def slow_init():
                await asyncio.sleep(10)
                return True

            mock_transport.initialize = slow_init
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize_with_stdio(
                servers=[{"name": "test", "command": "python"}], initialization_timeout=0.1
            )

            # Should not add transport due to timeout
            assert "test" not in stream_manager.transports

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_general_timeout(self):
        """Test initialize_with_stdio general timeout - line 460."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)

            # Make ping timeout
            async def timeout_ping():
                raise TimeoutError("Ping timeout")

            mock_transport.send_ping = timeout_ping
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize_with_stdio(servers=[{"name": "test", "command": "python"}])

            # Should handle timeout gracefully (line 460)

    @pytest.mark.asyncio
    async def test_initialize_with_stdio_general_exception(self):
        """Test initialize_with_stdio general exception - line 461-462."""
        with patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio:
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(side_effect=RuntimeError("Init error"))
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            # Should handle exception gracefully
            await stream_manager.initialize_with_stdio(servers=[{"name": "test", "command": "python"}])

            assert "test" not in stream_manager.transports

    # ------------------------------------------------------------------ #
    # Close method comprehensive coverage (lines 719-838)               #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_close_with_cancelled_error_in_shield(self):
        """Test close handles CancelledError in shield - lines 719-722."""
        stream_manager = StreamManager()

        # Add a transport
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.close = AsyncMock()
        stream_manager.transports["test"] = mock_transport

        # Mock shield to raise CancelledError
        with patch("asyncio.shield", side_effect=asyncio.CancelledError()):
            await stream_manager.close()

        # Should be closed despite error
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_close_with_general_exception_in_shield(self):
        """Test close handles general exception in shield - lines 723-725."""
        stream_manager = StreamManager()

        # Add a transport
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.close = AsyncMock()
        stream_manager.transports["test"] = mock_transport

        # Mock shield to raise exception
        with patch("asyncio.shield", side_effect=RuntimeError("Shield failed")):
            await stream_manager.close()

        # Should be closed despite error
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_concurrent_close_fallback_to_sequential(self):
        """Test concurrent close falls back to sequential - lines 737-740."""
        stream_manager = StreamManager()

        # Add transports
        for i in range(2):
            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.close = AsyncMock()
            stream_manager.transports[f"test_{i}"] = mock_transport

        # Mock _concurrent_close to raise exception, forcing fallback
        async def failing_concurrent_close(*args, **kwargs):
            raise RuntimeError("Concurrent close failed")

        stream_manager._concurrent_close = failing_concurrent_close

        await stream_manager.close()

        # Should be closed via sequential fallback
        assert stream_manager._closed is True
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_concurrent_close_with_timeout(self):
        """Test concurrent close with timeout - lines 775-787."""
        stream_manager = StreamManager()

        # Add slow transports
        for i in range(2):
            mock_transport = AsyncMock(spec=MCPBaseTransport)

            async def very_slow_close():
                await asyncio.sleep(100)  # Very long

            mock_transport.close = very_slow_close
            stream_manager.transports[f"test_{i}"] = mock_transport

        # Set very short timeout
        stream_manager.timeout_config.shutdown = 0.05

        await stream_manager.close()

        # Should timeout and cancel tasks
        assert stream_manager._closed is True
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_sequential_close_with_timeout(self):
        """Test sequential close with timeout - lines 799-801."""
        stream_manager = StreamManager()

        # Add a slow transport
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def slow_close():
            await asyncio.sleep(10)

        mock_transport.close = slow_close
        stream_manager.transports["slow"] = mock_transport

        # Force sequential close by making concurrent fail
        stream_manager._concurrent_close = AsyncMock(side_effect=RuntimeError("Concurrent failed"))

        # Set short timeout
        stream_manager.timeout_config.shutdown = 0.05

        await stream_manager.close()

        # Should handle timeout in sequential close (line 800)
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_sequential_close_with_cancelled_error(self):
        """Test sequential close with CancelledError - lines 802-804."""
        stream_manager = StreamManager()

        # Add transport that raises CancelledError
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def cancel_close():
            raise asyncio.CancelledError()

        mock_transport.close = cancel_close
        stream_manager.transports["cancel"] = mock_transport

        # Force sequential close
        stream_manager._concurrent_close = AsyncMock(side_effect=RuntimeError("Force sequential"))

        await stream_manager.close()

        # Should handle CancelledError gracefully (lines 802-804)
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_sequential_close_with_general_exception(self):
        """Test sequential close with general exception - lines 805-807."""
        stream_manager = StreamManager()

        # Add transport that raises exception
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.close = AsyncMock(side_effect=RuntimeError("Close failed"))
        stream_manager.transports["fail"] = mock_transport

        # Force sequential close
        stream_manager._concurrent_close = AsyncMock(side_effect=RuntimeError("Force sequential"))

        await stream_manager.close()

        # Should handle exception gracefully (lines 805-807)
        assert stream_manager._closed is True

    @pytest.mark.asyncio
    async def test_sync_cleanup_with_exception(self):
        """Test _sync_cleanup handles exceptions - lines 822-827."""
        stream_manager = StreamManager()
        stream_manager.transports["test"] = Mock()

        # Mock _cleanup_state to raise exception
        original_cleanup = stream_manager._cleanup_state

        def failing_cleanup():
            raise RuntimeError("Cleanup failed")

        stream_manager._cleanup_state = failing_cleanup

        # Should not raise
        stream_manager._sync_cleanup()

        # Restore for proper cleanup
        stream_manager._cleanup_state = original_cleanup

    @pytest.mark.asyncio
    async def test_cleanup_state_with_exception(self):
        """Test _cleanup_state handles exceptions - lines 837-838."""
        stream_manager = StreamManager()

        # Create a dict that raises on clear
        class FailingDict(dict):
            def clear(self):
                raise RuntimeError("Clear failed")

        stream_manager.transports = FailingDict({"test": Mock()})

        # Should handle exception gracefully
        stream_manager._cleanup_state()

    # ------------------------------------------------------------------ #
    # call_tool coverage for line 685 (no hasattr path)                 #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_call_tool_transport_no_hasattr_with_timeout(self):
        """Test call_tool when hasattr check fails but method exists - line 685."""
        stream_manager = StreamManager()

        # Create transport where hasattr fails but call_tool exists
        class TransportWithoutHasattr:
            async def call_tool(self, tool_name, args):
                return {"result": "success"}

            def __getattr__(self, name):
                # Make hasattr return False for call_tool
                raise AttributeError(f"No attribute {name}")

        # But we need to set it directly to avoid the __getattr__
        mock_transport = TransportWithoutHasattr()
        # Manually assign to bypass __getattr__
        object.__setattr__(mock_transport, "call_tool", AsyncMock(return_value={"result": "success"}))

        stream_manager.transports["test"] = mock_transport
        stream_manager.tool_to_server_map["tool1"] = "test"

        # Call with timeout - should hit line 685
        result = await stream_manager.call_tool("tool1", {}, timeout=5.0)

        assert result["result"] == "success"

    # ------------------------------------------------------------------ #
    # list_tools timeout and exception coverage (lines 585-586)         #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_list_tools_timeout_error(self):
        """Test list_tools when timeout occurs - lines 584-586."""
        stream_manager = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)

        async def timeout_get_tools():
            await asyncio.sleep(100)
            return []

        mock_transport.get_tools = timeout_get_tools
        stream_manager.transports["slow"] = mock_transport

        # Set very short timeout
        stream_manager.timeout_config.operation = 0.05

        tools = await stream_manager.list_tools("slow")

        # Should return empty list on timeout (lines 585-586)
        assert tools == []

    @pytest.mark.asyncio
    async def test_list_tools_general_exception(self):
        """Test list_tools handles general exceptions - lines 587-589."""
        stream_manager = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.get_tools = AsyncMock(side_effect=RuntimeError("Get tools failed"))
        stream_manager.transports["fail"] = mock_transport

        tools = await stream_manager.list_tools("fail")

        # Should return empty list on exception (lines 588-589)
        assert tools == []

    # ------------------------------------------------------------------ #
    # Edge cases and additional scenarios                               #
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_close_single_transport_with_exception(self):
        """Test _close_single_transport raises exception properly."""
        stream_manager = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport.close = AsyncMock(side_effect=RuntimeError("Close error"))

        # Should raise the exception
        with pytest.raises(RuntimeError, match="Close error"):
            await stream_manager._close_single_transport("test", mock_transport)

    @pytest.mark.asyncio
    async def test_concurrent_close_with_exception_results(self):
        """Test concurrent close handles exception results properly."""
        stream_manager = StreamManager()

        # Add transports - some succeed, some fail
        good_transport = AsyncMock(spec=MCPBaseTransport)
        good_transport.close = AsyncMock()

        bad_transport = AsyncMock(spec=MCPBaseTransport)
        bad_transport.close = AsyncMock(side_effect=RuntimeError("Close failed"))

        stream_manager.transports["good"] = good_transport
        stream_manager.transports["bad"] = bad_transport

        await stream_manager.close()

        # Should handle mixed results
        assert stream_manager._closed is True
        assert len(stream_manager.transports) == 0

    @pytest.mark.asyncio
    async def test_initialize_with_per_server_timeout(self):
        """Test initialize uses per-server timeout when available."""
        with (
            patch("chuk_tool_processor.mcp._stream_manager_init.load_config") as mock_load,
            patch("chuk_tool_processor.mcp._stream_manager_init.StdioTransport") as mock_stdio,
        ):
            # Return per-server timeout
            mock_load.return_value = ({"command": "python", "args": []}, 45.0)  # 45s per-server timeout

            mock_transport = AsyncMock(spec=MCPBaseTransport)
            mock_transport.initialize = AsyncMock(return_value=True)
            mock_transport.send_ping = AsyncMock(return_value=True)
            mock_transport.get_tools = AsyncMock(return_value=[])
            mock_stdio.return_value = mock_transport

            stream_manager = StreamManager()
            await stream_manager.initialize(
                config_file="test.json", servers=["test"], transport_type="stdio", default_timeout=30.0
            )

            # Verify per-server timeout was used (45.0 instead of default 30.0)
            call_kwargs = mock_stdio.call_args[1]
            assert call_kwargs["default_timeout"] == 45.0

    @pytest.mark.asyncio
    async def test_call_tool_with_timeout_and_signature_check(self):
        """Test call_tool checks signature for timeout parameter."""
        stream_manager = StreamManager()

        # Create transport with call_tool that has timeout in signature
        mock_transport = Mock(spec=MCPBaseTransport)

        async def call_with_timeout(tool_name, args, timeout=None):
            return {"result": "success", "timeout_used": timeout}

        mock_transport.call_tool = call_with_timeout

        stream_manager.transports["test"] = mock_transport
        stream_manager.tool_to_server_map["tool1"] = "test"

        result = await stream_manager.call_tool("tool1", {}, timeout=20.0)

        # Should pass timeout directly to transport
        assert result["result"] == "success"
        assert result["timeout_used"] == 20.0


class TestStreamManagerReconnect:
    """Tests for StreamManager.reconnect() method."""

    @pytest.mark.asyncio
    async def test_reconnect_success(self):
        """Test successful reconnect delegates to transport._attempt_recovery."""
        sm = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport._attempt_recovery = AsyncMock(return_value=True)
        sm.transports["server1"] = mock_transport

        result = await sm.reconnect("server1")

        assert result is True
        mock_transport._attempt_recovery.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reconnect_failure(self):
        """Test reconnect when _attempt_recovery returns False."""
        sm = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport._attempt_recovery = AsyncMock(return_value=False)
        sm.transports["server1"] = mock_transport

        result = await sm.reconnect("server1")

        assert result is False
        mock_transport._attempt_recovery.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reconnect_unknown_server(self):
        """Test reconnect with unknown server name returns False."""
        sm = StreamManager()

        result = await sm.reconnect("nonexistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_reconnect_closed_manager(self):
        """Test reconnect on closed StreamManager returns False."""
        sm = StreamManager()
        sm._closed = True

        result = await sm.reconnect("any_server")

        assert result is False

    @pytest.mark.asyncio
    async def test_reconnect_exception(self):
        """Test reconnect handles exception from _attempt_recovery."""
        sm = StreamManager()
        mock_transport = AsyncMock(spec=MCPBaseTransport)
        mock_transport._attempt_recovery = AsyncMock(side_effect=RuntimeError("recovery boom"))
        sm.transports["server1"] = mock_transport

        result = await sm.reconnect("server1")

        assert result is False
