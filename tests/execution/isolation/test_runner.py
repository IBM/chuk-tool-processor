# tests/execution/isolation/test_runner.py
"""
Tests for isolated code execution (IsolatedCodeRunner + broker + backends).

The Local backend (no isolation) exercises the whole core machinery on any OS.
The Seatbelt backend tests are skipped unless running on macOS with
``sandbox-exec`` available; where they run, they assert the boundary actually
blocks network and filesystem access.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from chuk_tool_processor.execution.isolation import (
    IsolatedCodeRunner,
    IsolationError,
    IsolationLimits,
    LocalProcessBackend,
    SeatbeltBackend,
    _wire,
)


# --------------------------------------------------------------------------- #
# Stub registry
# --------------------------------------------------------------------------- #
@dataclass
class _Info:
    namespace: str
    name: str


class _AddTool:
    async def execute(self, a, b):
        return {"sum": int(a) + int(b)}


class _EchoTool:
    async def execute(self, text=""):
        return {"text": text}


class StubRegistry:
    async def list_tools(self, namespace=None):
        return [_Info("math", "add"), _Info("math", "echo")]

    async def get_tool(self, name, namespace="default"):
        return {"add": _AddTool(), "echo": _EchoTool()}.get(name)


@pytest.fixture
def registry():
    return StubRegistry()


def _local_runner(registry, **kw):
    kw.setdefault("allow_no_isolation", True)
    kw.setdefault("namespace", "math")
    return IsolatedCodeRunner(LocalProcessBackend(), registry=registry, **kw)


ADD_LOOP = """
total = 0
for i in range(1, 6):
    r = await add(a=str(total), b=str(i))
    total = r["sum"]
return total
"""


# --------------------------------------------------------------------------- #
# Wire framing
# --------------------------------------------------------------------------- #
class TestWire:
    @pytest.mark.asyncio
    async def test_roundtrip(self):
        reader = asyncio.StreamReader()
        reader.feed_data(_wire.encode({"method": "hello", "n": 5}))
        reader.feed_eof()
        msg = await _wire.recv(reader)
        assert msg == {"method": "hello", "n": 5}

    @pytest.mark.asyncio
    async def test_eof_raises(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        with pytest.raises((EOFError, asyncio.IncompleteReadError)):
            await _wire.recv(reader)

    def test_oversize_rejected(self):
        with pytest.raises(ValueError):
            _wire.encode({"x": "a" * (_wire.MAX_FRAME_BYTES + 1)})


# --------------------------------------------------------------------------- #
# Limits validation
# --------------------------------------------------------------------------- #
class TestLimits:
    def test_defaults_ok(self):
        lim = IsolationLimits()
        assert lim.wall_timeout > 0
        assert lim.allow_network is False

    @pytest.mark.parametrize("kw", [{"wall_timeout": 0}, {"max_output_bytes": -1}, {"memory_bytes": 0}])
    def test_invalid(self, kw):
        with pytest.raises(ValueError):
            IsolationLimits(**kw)


# --------------------------------------------------------------------------- #
# Fail-closed
# --------------------------------------------------------------------------- #
class TestFailClosed:
    def test_non_isolating_backend_refused(self):
        with pytest.raises(IsolationError):
            IsolatedCodeRunner(LocalProcessBackend())

    def test_non_isolating_backend_allowed_with_optin(self):
        # Should not raise.
        IsolatedCodeRunner(LocalProcessBackend(), allow_no_isolation=True)


# --------------------------------------------------------------------------- #
# Core execution via the Local backend (works everywhere)
# --------------------------------------------------------------------------- #
class TestLocalExecution:
    @pytest.mark.asyncio
    async def test_add_loop(self, registry):
        r = await _local_runner(registry).run(ADD_LOOP)
        assert r.ok is True
        assert r.value == 15
        assert r.tool_calls == 5

    @pytest.mark.asyncio
    async def test_return_value_and_initial_vars(self, registry):
        r = await _local_runner(registry).run("return base * 2", initial_vars={"base": 21})
        assert r.ok and r.value == 42

    @pytest.mark.asyncio
    async def test_guest_exception_reported(self, registry):
        r = await _local_runner(registry).run("return 1 / 0")
        assert r.ok is False
        assert r.error_type == "guest_exception"
        assert "ZeroDivisionError" in (r.error or "")

    @pytest.mark.asyncio
    async def test_max_tool_calls_enforced(self, registry):
        runner = _local_runner(registry, limits=IsolationLimits(max_tool_calls=2))
        r = await runner.run(ADD_LOOP)
        assert r.ok is False
        assert r.tool_calls == 2  # third call rejected by the broker

    @pytest.mark.asyncio
    async def test_allowed_tools_filter(self, registry):
        runner = _local_runner(registry, allowed_tools={"echo"})
        # 'add' is not in the allowlist, so it is not bound in the guest globals.
        r = await runner.run("return add(a='1', b='2')")
        assert r.ok is False

    @pytest.mark.asyncio
    async def test_timeout(self, registry):
        runner = _local_runner(registry, limits=IsolationLimits(wall_timeout=1.0))
        r = await runner.run("while True:\n    pass\nreturn 1")
        assert r.ok is False
        assert r.timed_out is True
        assert r.error_type == "timeout"

    @pytest.mark.asyncio
    async def test_output_truncated(self, registry):
        runner = _local_runner(registry, limits=IsolationLimits(max_output_bytes=256))
        r = await runner.run("print('x' * 100000)\nreturn 1")
        assert len(r.stdout.encode()) <= 256 + 64


# --------------------------------------------------------------------------- #
# Seatbelt backend (macOS only) — asserts the boundary really blocks things
# --------------------------------------------------------------------------- #
_SEATBELT = SeatbeltBackend()


@pytest.mark.skipif(not _SEATBELT.is_available(), reason="requires macOS sandbox-exec")
class TestSeatbelt:
    def _runner(self, registry, **kw):
        kw.setdefault("namespace", "math")
        kw.setdefault("limits", IsolationLimits(wall_timeout=10.0))
        return IsolatedCodeRunner(SeatbeltBackend(), registry=registry, **kw)

    @pytest.mark.asyncio
    async def test_add_loop_and_tools_work(self, registry):
        r = await self._runner(registry).run(ADD_LOOP)
        assert r.ok is True and r.value == 15 and r.tool_calls == 5

    @pytest.mark.asyncio
    async def test_network_blocked(self, registry):
        code = (
            "import socket\n"
            "s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
            "s.settimeout(3)\n"
            "s.connect(('1.1.1.1', 53))\n"
            "return 'NET_OK'"
        )
        r = await self._runner(registry).run(code)
        assert r.ok is False  # inet connect denied by the sandbox

    @pytest.mark.asyncio
    async def test_filesystem_write_blocked(self, registry):
        code = "open('/etc/ctp_escape_test', 'w').write('x')\nreturn 'WROTE'"
        r = await self._runner(registry).run(code)
        assert r.ok is False  # write outside the sandbox denied

    @pytest.mark.asyncio
    async def test_timeout(self, registry):
        runner = self._runner(registry, limits=IsolationLimits(wall_timeout=2.0))
        r = await runner.run("while True:\n    pass\nreturn 1")
        assert r.timed_out is True
