"""Coverage for StreamManager resource/prompt fan-out, session-id propagation,
the direct call_tool timeout branches, and middleware enable/disable.

These paths were previously exercised only indirectly; this suite pins them.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from chuk_tool_processor.mcp.stream_manager import StreamManager


def _sm_with(transports: dict) -> StreamManager:
    sm = StreamManager()
    sm.transports = transports
    return sm


# --------------------------------------------------------------------------- #
#  read_resource
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_read_resource_closed_returns_empty():
    sm = _sm_with({})
    sm._closed = True
    assert await sm.read_resource("demo://r") == {}


@pytest.mark.asyncio
async def test_read_resource_specific_server_success():
    tr = Mock()
    tr.read_resource = AsyncMock(return_value={"contents": [{"text": "hi"}]})
    sm = _sm_with({"s1": tr})
    result = await sm.read_resource("demo://r", server_name="s1")
    assert result == {"contents": [{"text": "hi"}]}
    tr.read_resource.assert_awaited_once_with("demo://r")


@pytest.mark.asyncio
async def test_read_resource_specific_server_exception_returns_empty():
    tr = Mock()
    tr.read_resource = AsyncMock(side_effect=RuntimeError("boom"))
    sm = _sm_with({"s1": tr})
    assert await sm.read_resource("demo://r", server_name="s1") == {}


@pytest.mark.asyncio
async def test_read_resource_tries_all_until_success():
    empty = Mock()
    empty.read_resource = AsyncMock(return_value={})  # falsy -> keep trying
    good = Mock()
    good.read_resource = AsyncMock(return_value={"contents": [{"text": "found"}]})
    sm = _sm_with({"a": empty, "b": good})
    result = await sm.read_resource("demo://r")
    assert result == {"contents": [{"text": "found"}]}


@pytest.mark.asyncio
async def test_read_resource_all_fail_returns_empty():
    bad = Mock()
    bad.read_resource = AsyncMock(side_effect=RuntimeError("nope"))
    sm = _sm_with({"a": bad, "b": bad})
    assert await sm.read_resource("demo://r") == {}


# --------------------------------------------------------------------------- #
#  get_prompt
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_get_prompt_closed_returns_empty():
    sm = _sm_with({})
    sm._closed = True
    assert await sm.get_prompt("greet") == {}


@pytest.mark.asyncio
async def test_get_prompt_specific_server_success():
    tr = Mock()
    tr.get_prompt = AsyncMock(return_value={"messages": [{"role": "user"}]})
    sm = _sm_with({"s1": tr})
    result = await sm.get_prompt("greet", {"x": 1}, server_name="s1")
    assert result == {"messages": [{"role": "user"}]}
    tr.get_prompt.assert_awaited_once_with("greet", {"x": 1})


@pytest.mark.asyncio
async def test_get_prompt_specific_server_exception_returns_empty():
    tr = Mock()
    tr.get_prompt = AsyncMock(side_effect=RuntimeError("boom"))
    sm = _sm_with({"s1": tr})
    assert await sm.get_prompt("greet", server_name="s1") == {}


@pytest.mark.asyncio
async def test_get_prompt_tries_all_until_success():
    empty = Mock()
    empty.get_prompt = AsyncMock(return_value={})
    good = Mock()
    good.get_prompt = AsyncMock(return_value={"messages": [{"role": "assistant"}]})
    sm = _sm_with({"a": empty, "b": good})
    result = await sm.get_prompt("greet")
    assert result == {"messages": [{"role": "assistant"}]}


@pytest.mark.asyncio
async def test_get_prompt_all_fail_returns_empty():
    bad = Mock()
    bad.get_prompt = AsyncMock(side_effect=RuntimeError("nope"))
    sm = _sm_with({"a": bad})
    assert await sm.get_prompt("greet") == {}


# --------------------------------------------------------------------------- #
#  set_session_id
# --------------------------------------------------------------------------- #
def test_set_session_id_propagates_to_transports():
    tr = Mock()
    sm = _sm_with({"s1": tr})
    sm.set_session_id("sess-123")
    tr.set_session_id.assert_called_once_with("sess-123")


# --------------------------------------------------------------------------- #
#  call_tool / _direct_call_tool timeout branches
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_call_tool_routes_through_middleware_stack():
    sm = _sm_with({})
    sm._middleware_stack = Mock()
    sm._middleware_stack.call_tool = AsyncMock(return_value={"ok": True})
    result = await sm.call_tool("t", {"a": 1}, timeout=5)
    assert result == {"ok": True}
    sm._middleware_stack.call_tool.assert_awaited_once_with("t", {"a": 1}, 5)


@pytest.mark.asyncio
async def test_direct_call_tool_timeout_passes_timeout_when_supported():
    class Transport:
        async def call_tool(self, tool_name, arguments, timeout=None):
            return {"tool": tool_name, "timeout": timeout}

    sm = _sm_with({"s1": Transport()})
    result = await sm.call_tool("t", {}, server_name="s1", timeout=7)
    assert result == {"tool": "t", "timeout": 7}


@pytest.mark.asyncio
async def test_direct_call_tool_timeout_wraps_when_unsupported():
    class Transport:
        async def call_tool(self, tool_name, arguments):
            return {"tool": tool_name}

    sm = _sm_with({"s1": Transport()})
    result = await sm.call_tool("t", {}, server_name="s1", timeout=7)
    assert result == {"tool": "t"}


@pytest.mark.asyncio
async def test_direct_call_tool_timeout_error_returns_iserror():
    import asyncio

    class Transport:
        async def call_tool(self, tool_name, arguments, timeout=None):
            raise TimeoutError

    sm = _sm_with({"s1": Transport()})
    result = await sm.call_tool("t", {}, server_name="s1", timeout=3)
    assert result["isError"] is True
    assert "timed out" in result["error"]
    # keep asyncio import meaningful for linters that flag unused
    assert asyncio is asyncio


@pytest.mark.asyncio
async def test_direct_call_tool_no_timeout_calls_directly():
    tr = Mock()
    tr.call_tool = AsyncMock(return_value={"ok": 1})
    sm = _sm_with({"s1": tr})
    result = await sm.call_tool("t", {}, server_name="s1")
    assert result == {"ok": 1}


# --------------------------------------------------------------------------- #
#  middleware enable/disable/status
# --------------------------------------------------------------------------- #
def test_middleware_enable_disable_and_status():
    sm = StreamManager()
    assert sm.middleware_enabled is False
    assert sm.get_middleware_status() is None

    sm.enable_middleware()
    assert sm.middleware_enabled is True
    status = sm.get_middleware_status()
    assert status is not None

    sm.disable_middleware()
    assert sm.middleware_enabled is False
    assert sm.get_middleware_status() is None
