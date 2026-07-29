"""Regression tests for MCP tool-name shadowing across servers.

A bare tool name is not a trust boundary: if two configured MCP servers advertise
the same tool name, the earlier (already-trusted) server must keep default
routing, and the collision must be visible — never a silent last-writer-wins
hijack of every future unpinned call.
"""

from __future__ import annotations

import logging

import pytest

from chuk_tool_processor.mcp.stream_manager import StreamManager
from chuk_tool_processor.mcp.transport.models import MCPToolDefinition


class _FakeTransport:
    def __init__(self, label: str) -> None:
        self.label = label

    async def call_tool(self, name, arguments):
        return {"content": [{"type": "text", "text": f"from {self.label}"}]}


def _tools(*names: str) -> list[MCPToolDefinition]:
    return [MCPToolDefinition(name=n, description=n) for n in names]


def _manager_with(server_label_map: dict[str, str]) -> StreamManager:
    sm = StreamManager()
    for server, label in server_label_map.items():
        sm.transports[server] = _FakeTransport(label)
    return sm


class TestToolShadowing:
    def test_first_server_owns_default_routing(self):
        sm = _manager_with({"trusted": "T", "later": "L"})
        sm._register_tools(_tools("read_file"), "trusted")
        sm._register_tools(_tools("read_file"), "later")
        # First-wins: the trusted server keeps the name for unpinned routing.
        assert sm.get_server_for_tool("read_file") == "trusted"
        assert sm.get_servers_for_tool("read_file") == ["trusted", "later"]
        assert sm.get_tool_collisions() == {"read_file": ["trusted", "later"]}

    def test_collision_logs_warning(self, caplog):
        sm = _manager_with({"trusted": "T", "later": "L"})
        sm._register_tools(_tools("read_file"), "trusted")
        with caplog.at_level(logging.WARNING):
            sm._register_tools(_tools("read_file"), "later")
        assert any("collision" in r.getMessage() and "read_file" in r.getMessage() for r in caplog.records), (
            "expected a collision warning"
        )

    def test_no_warning_without_collision(self, caplog):
        sm = _manager_with({"a": "A", "b": "B"})
        with caplog.at_level(logging.WARNING):
            sm._register_tools(_tools("read_file"), "a")
            sm._register_tools(_tools("search"), "b")
        assert sm.get_tool_collisions() == {}
        assert not [r for r in caplog.records if "collision" in r.getMessage()]

    def test_reregistering_same_server_is_noop(self, caplog):
        sm = _manager_with({"a": "A"})
        sm._register_tools(_tools("read_file"), "a")
        with caplog.at_level(logging.WARNING):
            sm._register_tools(_tools("read_file"), "a")  # e.g. reconnect
        assert sm.get_servers_for_tool("read_file") == ["a"]
        assert not [r for r in caplog.records if "collision" in r.getMessage()]

    def test_tools_without_name_are_skipped(self):
        sm = _manager_with({"a": "A"})
        sm._register_tools([MCPToolDefinition(name="", description="x")], "a")
        assert sm.tool_to_server_map == {}
        assert sm.tool_to_servers == {}

    @pytest.mark.asyncio
    async def test_unpinned_call_stays_with_first_server(self):
        sm = _manager_with({"trusted": "TRUSTED", "later": "UNTRUSTED"})
        sm._register_tools(_tools("read_file"), "trusted")
        sm._register_tools(_tools("read_file"), "later")
        result = await sm.call_tool("read_file", {})
        assert result["content"][0]["text"] == "from TRUSTED"

    @pytest.mark.asyncio
    async def test_pinned_call_reaches_shadowed_server(self):
        sm = _manager_with({"trusted": "TRUSTED", "later": "UNTRUSTED"})
        sm._register_tools(_tools("read_file"), "trusted")
        sm._register_tools(_tools("read_file"), "later")
        result = await sm.call_tool("read_file", {}, server_name="later")
        assert result["content"][0]["text"] == "from UNTRUSTED"

    def test_reset_clears_collision_map(self):
        sm = _manager_with({"a": "A"})
        sm._register_tools(_tools("read_file"), "a")
        sm._sync_cleanup()
        assert sm.tool_to_servers == {}
        assert sm.tool_to_server_map == {}
