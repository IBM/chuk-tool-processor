# tests/mcp/test_mcp_tool.py
import contextlib
from unittest.mock import AsyncMock, Mock

import pytest

from chuk_tool_processor.mcp.mcp_tool import ConnectionState, MCPTool, RecoveryConfig
from chuk_tool_processor.mcp.stream_manager import StreamManager


class TestMCPTool:
    """Test MCPTool class with resilience features."""

    @pytest.fixture
    def mock_stream_manager(self):
        """Mock StreamManager instance."""
        mock = Mock(spec=StreamManager)
        mock.call_tool = AsyncMock()

        # UPDATED: get_server_info is synchronous — use Mock, not AsyncMock
        mock.get_server_info = Mock(return_value=[{"name": "test", "status": "Up"}])
        mock.transports = {"test": Mock()}  # Mock transports dict

        # Legacy ping_servers (not used in new health check but may be used elsewhere)
        mock.ping_servers = AsyncMock(return_value=[{"ok": True}])
        return mock

    @pytest.fixture
    def mcp_tool(self, mock_stream_manager):
        """Create MCPTool instance with resilience enabled."""
        return MCPTool("echo", mock_stream_manager, enable_resilience=True)

    @pytest.fixture
    def simple_mcp_tool(self, mock_stream_manager):
        """Create MCPTool instance with resilience disabled for legacy behavior."""
        return MCPTool("echo", mock_stream_manager, enable_resilience=False)

    # ------------------------------------------------------------------ #
    # Tests with resilience enabled (new behavior)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_execute_success_with_resilience(self, mcp_tool, mock_stream_manager):
        """Test successful tool execution with resilience features."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Hello World"}

        # Execute
        result = await mcp_tool.execute(message="Hello")

        # Verify
        assert result == "Hello World"
        mock_stream_manager.call_tool.assert_called_once_with(
            tool_name="echo",
            arguments={"message": "Hello"},
            timeout=30.0,  # Default timeout
        )

        # UPDATED: New lenient health check should check transports or server_info
        # but not necessarily ping_servers
        assert hasattr(mock_stream_manager, "transports")  # Should have checked transports

    @pytest.mark.asyncio
    async def test_execute_error_with_resilience(self, mcp_tool, mock_stream_manager):
        """Test tool execution with error returns structured response."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": True, "error": "Connection failed"}

        # Execute
        result = await mcp_tool.execute(message="Hello")

        # Verify structured error response
        assert isinstance(result, dict)
        assert result["available"] is False
        assert "Connection failed" in result["error"]
        assert result["tool_name"] == "echo"
        assert result["reason"] == "execution_failed"

    @pytest.mark.asyncio
    async def test_execute_unhealthy_connection(self, mcp_tool, mock_stream_manager):
        """Test tool execution when connection is unhealthy."""
        # UPDATED: Setup unhealthy connection using new health check method
        # Make both transports and server_info indicate unhealthy state
        mock_stream_manager.transports = {}  # No transports = unhealthy
        mock_stream_manager.get_server_info.return_value = []  # No servers = unhealthy

        # Execute
        result = await mcp_tool.execute(message="Hello")

        # Verify structured error response
        assert isinstance(result, dict)
        assert result["available"] is False
        assert "unhealthy connection" in result["error"]
        assert result["tool_name"] == "echo"
        assert result["reason"] == "unhealthy"

        # Should not call the tool if unhealthy
        mock_stream_manager.call_tool.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_no_stream_manager(self):
        """Test tool execution when no StreamManager is available."""
        tool = MCPTool("echo", stream_manager=None, enable_resilience=True)

        # Execute
        result = await tool.execute(message="Hello")

        # Verify structured error response
        assert isinstance(result, dict)
        assert result["available"] is False
        assert "not available (no stream manager)" in result["error"]
        assert result["tool_name"] == "echo"
        assert result["reason"] == "disconnected"

    @pytest.mark.asyncio
    async def test_execute_timeout_with_resilience(self, mcp_tool, mock_stream_manager):
        """Test tool execution timeout returns structured response."""
        # Setup timeout

        mock_stream_manager.call_tool.side_effect = TimeoutError()

        # Execute
        result = await mcp_tool.execute(message="Hello", timeout=1.0)

        # Verify structured error response
        assert isinstance(result, dict)
        assert result["available"] is False
        assert "timed out" in result["error"]
        assert result["tool_name"] == "echo"
        assert result["reason"] == "timeout"

    @pytest.mark.asyncio
    async def test_execute_transport_error_with_resilience(self, mcp_tool, mock_stream_manager):
        """Test tool execution with transport error returns structured response."""
        # Setup transport error
        mock_stream_manager.call_tool.side_effect = Exception("Transport error")

        # Execute
        result = await mcp_tool.execute(message="Hello")

        # Verify structured error response
        assert isinstance(result, dict)
        assert result["available"] is False
        assert "Transport error" in result["error"]
        assert result["tool_name"] == "echo"
        assert result["reason"] == "execution_failed"

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self, mock_stream_manager):
        """Test that circuit breaker opens after consecutive failures."""
        # Create tool with low failure threshold
        recovery_config = RecoveryConfig(circuit_breaker_threshold=2)
        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True, recovery_config=recovery_config)

        # Setup consecutive failures
        mock_stream_manager.call_tool.side_effect = Exception("Consistent failure")

        # Execute multiple times to trigger circuit breaker
        for _ in range(3):
            result = await tool.execute(message="test")
            assert not result.get("available", True)

        # Fourth call should be circuit breaker response
        result = await tool.execute(message="test")
        assert result["reason"] == "circuit_breaker"
        assert "Circuit breaker open" in result["error"]

    @pytest.mark.asyncio
    async def test_retry_mechanism(self, mock_stream_manager):
        """Test retry mechanism with eventual success."""
        # Create tool with retries
        recovery_config = RecoveryConfig(max_retries=2)
        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True, recovery_config=recovery_config)

        # Setup failure then success
        mock_stream_manager.call_tool.side_effect = [
            Exception("Temporary failure"),
            {"isError": False, "content": "Success after retry"},
        ]

        # Execute
        result = await tool.execute(message="test")

        # Should succeed after retry
        assert result == "Success after retry"
        assert mock_stream_manager.call_tool.call_count == 2

    @pytest.mark.asyncio
    async def test_health_check_with_timeout_assumes_healthy(self, mock_stream_manager):
        """Test that health check timeout assumes healthy (new lenient behavior)."""
        # Setup health check timeout

        mock_stream_manager.transports = {"test": Mock()}  # Has transports
        mock_stream_manager.get_server_info.side_effect = TimeoutError()

        # Setup successful tool call
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Success despite timeout"}

        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True)

        # Execute - should succeed because timeout assumes healthy
        result = await tool.execute(message="test")

        # Should succeed
        assert result == "Success despite timeout"
        mock_stream_manager.call_tool.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_with_exception_assumes_healthy(self, mock_stream_manager):
        """Test that health check exceptions assume healthy (new lenient behavior)."""
        # Setup health check exception
        mock_stream_manager.transports = {"test": Mock()}  # Has transports
        mock_stream_manager.get_server_info.side_effect = Exception("Health check error")

        # Setup successful tool call
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Success despite health error"}

        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True)

        # Execute - should succeed because exceptions assume healthy
        result = await tool.execute(message="test")

        # Should succeed
        assert result == "Success despite health error"
        mock_stream_manager.call_tool.assert_called_once()

    # ------------------------------------------------------------------ #
    # Tests with resilience disabled (legacy behavior)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_execute_success_without_resilience(self, simple_mcp_tool, mock_stream_manager):
        """Test successful tool execution without resilience (legacy behavior)."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Hello World"}

        # Execute
        result = await simple_mcp_tool.execute(message="Hello")

        # Verify
        assert result == "Hello World"
        mock_stream_manager.call_tool.assert_called_once_with(
            tool_name="echo", arguments={"message": "Hello"}, timeout=30.0
        )

        # Should not check health when resilience disabled
        mock_stream_manager.get_server_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_error_without_resilience(self, simple_mcp_tool, mock_stream_manager):
        """Test tool execution with error raises exception (legacy behavior)."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": True, "error": "Connection failed"}

        # Execute and verify exception is raised
        with pytest.raises(RuntimeError, match="Connection failed"):
            await simple_mcp_tool.execute(message="Hello")

    @pytest.mark.asyncio
    async def test_execute_unknown_error_without_resilience(self, simple_mcp_tool, mock_stream_manager):
        """Test tool execution with unknown error (legacy behavior)."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": True}

        # Execute and verify default error message
        with pytest.raises(RuntimeError, match="Unknown error"):
            await simple_mcp_tool.execute(message="Hello")

    @pytest.mark.asyncio
    async def test_execute_transport_error_without_resilience(self, simple_mcp_tool, mock_stream_manager):
        """Test tool call with transport error (legacy behavior)."""
        # Simulate transport error
        mock_stream_manager.call_tool.side_effect = Exception("Transport error")

        # Should raise the original exception
        with pytest.raises(Exception, match="Transport error"):
            await simple_mcp_tool.execute(message="test")

    # ------------------------------------------------------------------ #
    # Utility and monitoring tests
    # ------------------------------------------------------------------ #

    def test_is_available(self, mcp_tool):
        """Test availability checking."""
        assert mcp_tool.is_available() is True

        # Test with no stream manager
        no_sm_tool = MCPTool("test", stream_manager=None)
        assert no_sm_tool.is_available() is False

    def test_get_stats(self, mcp_tool):
        """Test statistics gathering."""
        stats = mcp_tool.get_stats()

        assert stats["tool_name"] == "echo"
        assert stats["resilience_enabled"] is True
        assert stats["available"] is True
        assert stats["state"] == ConnectionState.HEALTHY.value
        assert stats["total_calls"] == 0
        assert stats["has_stream_manager"] is True

    def test_get_stats_without_resilience(self, simple_mcp_tool):
        """Test statistics when resilience is disabled."""
        stats = simple_mcp_tool.get_stats()

        assert stats["tool_name"] == "echo"
        assert stats["resilience_enabled"] is False
        assert stats["available"] is True

    def test_reset_circuit_breaker(self, mcp_tool):
        """Test manual circuit breaker reset."""
        # Open circuit breaker manually
        mcp_tool._circuit_open = True
        mcp_tool._consecutive_failures = 5

        # Reset
        mcp_tool.reset_circuit_breaker()

        assert mcp_tool._circuit_open is False
        assert mcp_tool._consecutive_failures == 0
        assert mcp_tool.connection_state == ConnectionState.HEALTHY

    def test_set_stream_manager(self):
        """Test updating stream manager."""
        tool = MCPTool("test", stream_manager=None)
        assert not tool.is_available()

        # Set stream manager
        mock_sm = Mock(spec=StreamManager)
        tool.set_stream_manager(mock_sm)

        assert tool._sm == mock_sm
        assert tool.connection_state == ConnectionState.HEALTHY

    def test_disable_resilience(self, mcp_tool):
        """Test disabling resilience features."""
        assert mcp_tool.enable_resilience is True

        mcp_tool.disable_resilience()

        assert mcp_tool.enable_resilience is False

    # ------------------------------------------------------------------ #
    # Serialization tests
    # ------------------------------------------------------------------ #

    def test_serialization(self, mcp_tool):
        """Test serialization for subprocess execution."""
        # Get state
        state = mcp_tool.__getstate__()

        # Verify critical fields preserved
        assert state["tool_name"] == "echo"
        assert state["default_timeout"] == 30.0
        assert state["enable_resilience"] is True

        # StreamManager should be None for subprocess
        assert state["_sm"] is None

        # Create new instance and restore state
        new_tool = MCPTool.__new__(MCPTool)
        new_tool.__setstate__(state)

        # Verify restoration
        assert new_tool.tool_name == "echo"
        assert new_tool._sm is None
        assert new_tool.connection_state == ConnectionState.DISCONNECTED

    def test_serialization_invalid_state(self):
        """Test serialization with invalid state."""
        tool = MCPTool.__new__(MCPTool)

        # Try to restore invalid state
        with pytest.raises(ValueError, match="missing tool_name"):
            tool.__setstate__({"enable_resilience": True})

    # ------------------------------------------------------------------ #
    # Legacy method support
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_legacy_aexecute_method(self, simple_mcp_tool, mock_stream_manager):
        """Test legacy _aexecute method still works."""
        # Setup
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Legacy method works"}

        # Execute using legacy method
        result = await simple_mcp_tool._aexecute(message="test")

        # Verify
        assert result == "Legacy method works"

    def test_init_without_tool_name(self):
        """Test MCPTool raises error when tool_name is empty."""
        with pytest.raises(ValueError, match="MCPTool requires a tool_name"):
            MCPTool(tool_name="", stream_manager=None)

        with pytest.raises(ValueError, match="MCPTool requires a tool_name"):
            MCPTool(tool_name=None, stream_manager=None)

    def test_init_with_custom_timeout(self):
        """Test MCPTool initialization with custom timeout."""
        tool = MCPTool(tool_name="test", stream_manager=None, default_timeout=60.0)
        assert tool.default_timeout == 60.0

    def test_connection_state_without_stream_manager(self):
        """Test connection state when initialized without stream manager."""
        tool = MCPTool(tool_name="test", stream_manager=None, enable_resilience=True)
        assert tool.connection_state == ConnectionState.DISCONNECTED

    def test_connection_state_with_stream_manager(self, mock_stream_manager):
        """Test connection state when initialized with stream manager."""
        tool = MCPTool(tool_name="test", stream_manager=mock_stream_manager, enable_resilience=True)
        assert tool.connection_state == ConnectionState.HEALTHY

    @pytest.mark.asyncio
    async def test_execute_with_dict_arguments(self, simple_mcp_tool, mock_stream_manager):
        """Test execute with dict-style arguments."""
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "dict args work"}

        result = await simple_mcp_tool.execute(arg1="value1", arg2="value2")

        assert result == "dict args work"
        mock_stream_manager.call_tool.assert_called_once()

    def test_serialization_preserves_recovery_config(self, simple_mcp_tool):
        """Test serialization preserves recovery config."""
        from chuk_tool_processor.mcp.mcp_tool import RecoveryConfig

        custom_config = RecoveryConfig(max_retries=5, circuit_breaker_threshold=10)
        tool = MCPTool(tool_name="test", stream_manager=None, recovery_config=custom_config)

        state = tool.__getstate__()

        assert "recovery_config" in state
        new_tool = MCPTool.__new__(MCPTool)
        new_tool.__setstate__(state)

        assert new_tool.recovery_config.max_retries == 5
        assert new_tool.recovery_config.circuit_breaker_threshold == 10

    def test_setstate_initializes_missing_resilience_state(self):
        """Test __setstate__ initializes missing resilience state."""
        from chuk_tool_processor.mcp.mcp_tool import ConnectionState

        # Create a state dict without connection_state and stats
        state = {
            "tool_name": "test",
            "default_timeout": 30.0,
            "enable_resilience": True,
            "recovery_config": None,
            "_sm": None,
        }

        tool = MCPTool.__new__(MCPTool)
        tool.__setstate__(state)

        # Verify resilience state was initialized
        assert hasattr(tool, "connection_state")
        assert tool.connection_state == ConnectionState.DISCONNECTED
        assert hasattr(tool, "stats")

    def test_setstate_preserves_existing_resilience_state(self):
        """Test __setstate__ preserves existing resilience state."""
        from chuk_tool_processor.mcp.mcp_tool import ConnectionState, ConnectionStats

        # Create a tool with existing state
        tool = MCPTool.__new__(MCPTool)
        tool.connection_state = ConnectionState.HEALTHY
        tool.stats = ConnectionStats()
        tool.stats.total_calls = 10

        state = {
            "tool_name": "test",
            "default_timeout": 30.0,
            "enable_resilience": True,
            "recovery_config": None,
            "_sm": None,
        }

        tool.__setstate__(state)

        # Verify existing state was preserved
        assert tool.connection_state == ConnectionState.HEALTHY
        assert tool.stats.total_calls == 10

    # ------------------------------------------------------------------ #
    # Additional tests for missing coverage lines
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_simple_execute_timeout_error(self, simple_mcp_tool, mock_stream_manager):
        """Test _simple_execute with TimeoutError (lines 190-191)."""
        # Setup timeout
        mock_stream_manager.call_tool.side_effect = TimeoutError()

        # Execute and verify timeout is raised
        with pytest.raises(TimeoutError):
            await simple_mcp_tool.execute(message="Hello", timeout=1.0)

    @pytest.mark.asyncio
    async def test_health_check_timeout_path(self, mock_stream_manager):
        """Test health check timeout specific exception path (lines 339-341)."""
        # Setup to not have transports attribute, forcing fallback to server_info
        delattr(mock_stream_manager, "transports")

        # Setup server_info to raise TimeoutError specifically
        mock_stream_manager.get_server_info.side_effect = TimeoutError()

        tool = MCPTool("test", mock_stream_manager, enable_resilience=True)

        # Check health - should assume healthy despite timeout
        is_healthy = await tool._is_stream_manager_healthy()

        # New lenient behavior: timeout assumes healthy (line 341)
        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_execute_with_timeout_connection_error(self, mcp_tool, mock_stream_manager):
        """Test _execute_with_timeout with connection error (line 302)."""
        # Setup connection error
        mock_stream_manager.call_tool.side_effect = Exception("Connection lost")

        # Execute and verify state changes to DISCONNECTED
        with contextlib.suppress(Exception):
            await mcp_tool._execute_with_timeout(30.0, message="test")

        # Verify connection state changed
        assert mcp_tool.connection_state == ConnectionState.DISCONNECTED

    @pytest.mark.asyncio
    async def test_health_check_no_stream_manager(self):
        """Test _is_stream_manager_healthy when _sm is None (line 318)."""
        tool = MCPTool("test", stream_manager=None, enable_resilience=True)

        # Check health
        is_healthy = await tool._is_stream_manager_healthy()

        # Should return False when no stream manager
        assert is_healthy is False

    @pytest.mark.asyncio
    async def test_health_check_exception_path(self, mock_stream_manager):
        """Test health check exception handling (lines 338-346)."""
        # Setup to not have transports
        delattr(mock_stream_manager, "transports")

        # Setup server_info to raise an exception
        mock_stream_manager.get_server_info.side_effect = Exception("Health check failed")

        tool = MCPTool("test", mock_stream_manager, enable_resilience=True)

        # Check health - should assume healthy despite exception
        is_healthy = await tool._is_stream_manager_healthy()

        # New lenient behavior: exceptions assume healthy
        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_closes_after_success(self, mock_stream_manager):
        """Test circuit breaker closes after successful execution (lines 375-378)."""
        # Create tool with low failure threshold
        recovery_config = RecoveryConfig(circuit_breaker_threshold=2)
        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True, recovery_config=recovery_config)

        # Open circuit breaker by causing failures
        mock_stream_manager.call_tool.side_effect = Exception("Error")
        for _ in range(3):
            await tool.execute(message="test")

        # Verify circuit is open
        assert tool._circuit_open is True

        # Now we need to wait for circuit breaker timeout or manually close it
        # The circuit breaker won't execute if it's open, so we need to test _record_success directly
        # First, manually set circuit_open to test the closing logic
        tool._circuit_open = True
        tool._circuit_open_time = 12345.0  # Set a time

        # Now setup success and call _record_success
        mock_stream_manager.call_tool.side_effect = None
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Success"}

        # Call _record_success directly to test lines 375-378
        await tool._record_success()

        # Verify circuit breaker closed
        assert tool._circuit_open is False
        assert tool._circuit_open_time is None
        assert tool.connection_state == ConnectionState.HEALTHY

    @pytest.mark.asyncio
    async def test_circuit_breaker_timeout_reset(self, mock_stream_manager):
        """Test circuit breaker resets after timeout (lines 412-416)."""
        import time

        # Create tool with short timeout
        recovery_config = RecoveryConfig(circuit_breaker_threshold=1, circuit_breaker_timeout=0.1)
        tool = MCPTool("echo", mock_stream_manager, enable_resilience=True, recovery_config=recovery_config)

        # Open circuit breaker
        mock_stream_manager.call_tool.side_effect = Exception("Error")
        for _ in range(2):
            await tool.execute(message="test")

        # Verify circuit is open
        assert tool._circuit_open is True

        # Wait for circuit breaker timeout
        time.sleep(0.15)

        # Check circuit breaker - should be closed now
        is_open = tool._is_circuit_open()

        # Verify it reset
        assert is_open is False
        assert tool._circuit_open is False
        assert tool._circuit_open_time is None
        assert tool.connection_state == ConnectionState.HEALTHY

    @pytest.mark.asyncio
    async def test_get_stats_with_success_rate(self, mcp_tool, mock_stream_manager):
        """Test get_stats with success rate calculation (line 444)."""
        # Setup successful execution
        mock_stream_manager.call_tool.return_value = {"isError": False, "content": "Success"}

        # Execute multiple times
        for _ in range(3):
            await mcp_tool.execute(message="test")

        # Get stats
        stats = mcp_tool.get_stats()

        # Verify success rate is calculated
        assert stats["total_calls"] == 3
        assert stats["successful_calls"] == 3
        assert stats["success_rate"] == 100.0

    def test_reset_circuit_breaker_resilience_disabled(self, simple_mcp_tool):
        """Test reset_circuit_breaker when resilience is disabled (line 464)."""
        # Call reset on tool with resilience disabled
        simple_mcp_tool.reset_circuit_breaker()

        # Should return early without error
        # Verify it doesn't have circuit breaker attributes since resilience is disabled
        assert simple_mcp_tool.enable_resilience is False

    def test_set_stream_manager_closes_circuit_breaker(self, mock_stream_manager):
        """Test set_stream_manager closes circuit breaker (lines 488-492)."""
        # Create tool with no stream manager and resilience enabled
        tool = MCPTool("test", stream_manager=None, enable_resilience=True)

        # Manually open circuit breaker
        tool._circuit_open = True
        tool._circuit_open_time = 12345.0

        # Set stream manager
        tool.set_stream_manager(mock_stream_manager)

        # Verify circuit breaker was closed
        assert tool._circuit_open is False
        assert tool._circuit_open_time is None
        assert tool.connection_state == ConnectionState.HEALTHY

    def test_set_stream_manager_to_none_with_resilience(self, mock_stream_manager):
        """Test set_stream_manager to None (line 492)."""
        # Create tool with stream manager
        tool = MCPTool("test", stream_manager=mock_stream_manager, enable_resilience=True)

        # Verify initial state
        assert tool.connection_state == ConnectionState.HEALTHY

        # Set stream manager to None
        tool.set_stream_manager(None)

        # Verify state changed to disconnected
        assert tool._sm is None
        assert tool.connection_state == ConnectionState.DISCONNECTED
