# tests/execution/isolation/test_broker_protocol.py
"""
Adversarial, protocol-boundary tests for the host tool broker.

These deliberately bypass the friendly guest proxies and drive ``ToolBroker``
with raw frames — because the guest runs untrusted code with the broker token
and can craft any request it likes. A cooperative-client test would never expose
a namespace-substitution or allowlist bypass; these do.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.broker import ToolBroker, _dbg, _to_jsonable
from chuk_tool_processor.execution.isolation.limits import IsolationLimits


# --------------------------------------------------------------------------- #
# A registry with the SAME tool name in two namespaces, plus a privileged tool
# that lives only in the "admin" namespace. This is what a namespace-confused
# broker would leak.
# --------------------------------------------------------------------------- #
@dataclass
class _Info:
    namespace: str
    name: str


class _Tool:
    def __init__(self, label: str) -> None:
        self._label = label

    async def execute(self, **kwargs):
        return {"tool": self._label, "args": kwargs}


_TABLE = {
    ("safe", "add"): _Tool("safe.add"),
    ("admin", "add"): _Tool("admin.add"),
    ("admin", "danger"): _Tool("admin.danger"),
}


class MultiNsRegistry:
    async def list_tools(self, namespace=None):
        infos = [_Info(ns, name) for (ns, name) in _TABLE]
        if namespace is None:
            return infos
        return [i for i in infos if i.namespace == namespace]

    async def get_tool(self, name, namespace="default"):
        return _TABLE.get((namespace, name))

    async def list_namespaces(self):
        return ["safe", "admin"]


class _CollectChannel:
    """Minimal MessageChannel that records what the broker sends back."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def recv(self) -> dict:  # pragma: no cover - not used in these tests
        raise ConnectionError("no incoming frames")

    async def send(self, obj: dict) -> None:
        self.sent.append(obj)

    async def aclose(self) -> None:
        pass


def _make_broker(**kw) -> ToolBroker:
    kw.setdefault("token", "tok")
    kw.setdefault("limits", IsolationLimits())
    return ToolBroker(MultiNsRegistry(), **kw)


async def _dispatch(broker: ToolBroker, method: str, *, params=None, msg_id: int = 1) -> dict:
    """Push one raw frame through the broker's dispatch and return the reply."""
    channel = _CollectChannel()
    frame = {_wire.KEY_ID: msg_id, _wire.KEY_METHOD: method}
    if params is not None:
        frame[_wire.KEY_PARAMS] = params
    await broker._dispatch(frame, channel)
    assert channel.sent, "broker sent no reply"
    return channel.sent[-1]


def _call(name: str, namespace: str | None = None, arguments: dict | None = None) -> dict:
    params: dict = {_wire.KEY_NAME: name, _wire.KEY_ARGUMENTS: arguments or {}}
    if namespace is not None:
        params[_wire.KEY_NAMESPACE] = namespace
    return params


# --------------------------------------------------------------------------- #
# P0: namespace authority
# --------------------------------------------------------------------------- #
class TestNamespaceAuthority:
    @pytest.mark.asyncio
    async def test_guest_cannot_override_pinned_namespace(self):
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("danger", namespace="admin"))
            assert reply[_wire.KEY_OK] is False
            assert "namespace not allowed" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_pinned_namespace_resolves_locally_not_cross_namespace(self):
        # Host pins "safe"; a call for "add" must hit safe.add, never admin.add,
        # even though the strategy has cross-namespace fuzzy fallback.
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add", arguments={"x": 1}))
            assert reply[_wire.KEY_OK] is True
            assert reply[_wire.KEY_VALUE]["tool"] == "safe.add"
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_privileged_tool_in_other_namespace_unreachable(self):
        # "danger" exists only in admin. With host pinned to safe and no explicit
        # namespace, it must NOT fuzzy-resolve to admin.danger.
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("danger"))
            assert reply[_wire.KEY_OK] is False
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_matching_namespace_is_allowed(self):
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add", namespace="safe"))
            assert reply[_wire.KEY_OK] is True
        finally:
            await broker.aclose()


# --------------------------------------------------------------------------- #
# Namespace-qualified allowlist
# --------------------------------------------------------------------------- #
class TestQualifiedAllowlist:
    @pytest.mark.asyncio
    async def test_qualified_entry_pins_to_namespace(self):
        # Allow only safe.add. With no pinned namespace, a call to admin.add must
        # be refused even though the bare name "add" exists there.
        broker = _make_broker(allowed_tools={"safe.add"})
        try:
            ok = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add", namespace="safe"))
            assert ok[_wire.KEY_OK] is True
            blocked = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add", namespace="admin"), msg_id=2)
            assert blocked[_wire.KEY_OK] is False
            assert "not allowed" in blocked[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_bare_entry_matches_any_namespace(self):
        broker = _make_broker(allowed_tools={"add"})
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add", namespace="admin"))
            assert reply[_wire.KEY_OK] is True
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_list_tools_filtered_by_allowlist(self):
        broker = _make_broker(allowed_tools={"safe.add"})
        try:
            reply = await _dispatch(broker, _wire.METHOD_LIST_TOOLS)
            names = {(t[_wire.KEY_NAMESPACE], t[_wire.KEY_NAME]) for t in reply[_wire.KEY_VALUE]}
            assert names == {("safe", "add")}
        finally:
            await broker.aclose()


# --------------------------------------------------------------------------- #
# Lifecycle: no privileged work after the run ends; in-flight calls cancelled
# --------------------------------------------------------------------------- #
class TestLifecycle:
    @pytest.mark.asyncio
    async def test_call_after_result_refused(self):
        broker = _make_broker(namespace="safe")
        try:
            broker._result_future.set_result("done")
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"))
            assert reply[_wire.KEY_OK] is False
            assert "run already completed" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_aclose_cancels_in_flight_tasks(self):
        broker = _make_broker(namespace="safe")

        async def _slow() -> None:
            await asyncio.sleep(30)

        task = asyncio.create_task(_slow())
        broker._tasks.add(task)
        task.add_done_callback(broker._tasks.discard)

        await broker.aclose()
        assert task.cancelled() or task.done()

    @pytest.mark.asyncio
    async def test_max_tool_calls_enforced_at_protocol_level(self):
        broker = _make_broker(namespace="safe", limits=IsolationLimits(max_tool_calls=1))
        try:
            first = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"))
            assert first[_wire.KEY_OK] is True
            second = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"), msg_id=2)
            assert second[_wire.KEY_OK] is False
            assert "limit" in second[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_concurrent_calls_counted_once_each(self):
        broker = _make_broker(namespace="safe")
        try:
            replies = await asyncio.gather(
                _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"), msg_id=1),
                _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"), msg_id=2),
                _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"), msg_id=3),
            )
            assert all(r[_wire.KEY_OK] for r in replies)
            assert broker.tool_calls == 3
        finally:
            await broker.aclose()


# --------------------------------------------------------------------------- #
# Malformed / hostile frames
# --------------------------------------------------------------------------- #
class TestMalformedFrames:
    @pytest.mark.asyncio
    async def test_unknown_method(self):
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, "please_escape")
            assert reply[_wire.KEY_OK] is False
            assert "unknown method" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_call_tool_requires_string_name(self):
        broker = _make_broker(namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params={_wire.KEY_NAME: 123})
            assert reply[_wire.KEY_OK] is False
            assert "string 'name'" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_call_tool_arguments_must_be_object(self):
        broker = _make_broker(namespace="safe")
        try:
            params = {_wire.KEY_NAME: "add", _wire.KEY_ARGUMENTS: [1, 2, 3]}
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=params)
            assert reply[_wire.KEY_OK] is False
            assert "object" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_non_string_namespace_rejected_when_unpinned(self):
        broker = _make_broker()  # no pinned namespace
        try:
            params = {_wire.KEY_NAME: "add", _wire.KEY_NAMESPACE: 999}
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=params)
            assert reply[_wire.KEY_OK] is False
        finally:
            await broker.aclose()


# --------------------------------------------------------------------------- #
# Handshake: bad token is rejected
# --------------------------------------------------------------------------- #
class TestHandshake:
    @pytest.mark.asyncio
    async def test_bad_token_rejected(self):
        broker = _make_broker(namespace="safe")

        class _Script:
            def __init__(self) -> None:
                self.sent: list[dict] = []
                self._frames = [{_wire.KEY_METHOD: _wire.METHOD_HELLO, _wire.KEY_TOKEN: "wrong", _wire.KEY_ID: 0}]

            async def recv(self) -> dict:
                if self._frames:
                    return self._frames.pop(0)
                raise ConnectionError("eof")

            async def send(self, obj: dict) -> None:
                self.sent.append(obj)

            async def aclose(self) -> None:
                pass

        channel = _Script()
        try:
            await broker._handle_client(channel)
            assert channel.sent[-1][_wire.KEY_OK] is False
            assert channel.sent[-1][_wire.KEY_ERROR] == "unauthorized"
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_wire_recv_rejects_non_object(self):
        # A well-framed but non-object JSON payload ("x") must be refused: the
        # broker only ever exchanges JSON objects.
        reader = asyncio.StreamReader()
        body = b'"x"'
        reader.feed_data(_wire._LEN.pack(len(body)) + body)
        reader.feed_eof()
        with pytest.raises(ValueError):
            await _wire.recv(reader)


# --------------------------------------------------------------------------- #
# Edge cases (kept the broker file's own coverage honest)
# --------------------------------------------------------------------------- #
class _Boom:
    async def execute(self, **kwargs):
        raise RuntimeError("kaboom")


class _EmptyExecutor:
    """Stands in for a ToolExecutor that returns no results."""

    async def execute(self, calls):
        return []

    async def shutdown(self):  # pragma: no cover - not exercised (injected, not owned)
        pass


class TestBrokerEdges:
    def test_to_jsonable_passthrough_and_fallbacks(self):
        from dataclasses import dataclass

        assert _to_jsonable({"a": 1}) == {"a": 1}
        # pydantic model -> model_dump
        assert _to_jsonable(IsolationLimits(max_tool_calls=3))["max_tool_calls"] == 3
        # set is not JSON-serialisable -> repr fallback
        assert isinstance(_to_jsonable({1, 2, 3}), str)
        # dict that isn't directly serialisable (non-str key + set value) is
        # rebuilt with stringified keys and jsonable values.
        out = _to_jsonable({1: {5, 6}})
        assert list(out.keys()) == ["1"] and isinstance(out["1"], str)
        # nested list of non-serialisable
        assert isinstance(_to_jsonable([{1, 2}]), list)

        @dataclass
        class _DC:
            a: int
            b: set

        assert _to_jsonable(_DC(a=1, b={9}))["a"] == 1

    def test_dbg_enabled(self, monkeypatch, capsys):
        monkeypatch.setenv(_wire.GUEST_DEBUG_ENV, _wire.GUEST_DEBUG_ON)
        _dbg("hello-checkpoint")
        assert "hello-checkpoint" in capsys.readouterr().err

    @pytest.mark.asyncio
    async def test_endpoint_and_transport_none_before_start(self):
        broker = _make_broker(namespace="safe")
        assert broker.endpoint is None
        assert broker.transport is None
        assert broker.result("fallback") == "fallback"
        assert broker.has_result() is False
        await broker.aclose()

    @pytest.mark.asyncio
    async def test_tool_exception_surfaced_as_error(self):
        class Reg(MultiNsRegistry):
            async def get_tool(self, name, namespace="default"):
                return _Boom() if name == "boom" else await super().get_tool(name, namespace)

            async def list_tools(self, namespace=None):
                return [_Info("safe", "boom")]

        broker = ToolBroker(Reg(), token="t", limits=IsolationLimits(), namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("boom"))
            assert reply[_wire.KEY_OK] is False
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_list_tools_error_is_caught(self):
        class Reg(MultiNsRegistry):
            async def list_tools(self, namespace=None):
                raise RuntimeError("registry down")

        broker = ToolBroker(Reg(), token="t", limits=IsolationLimits(), namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_LIST_TOOLS)
            assert reply[_wire.KEY_OK] is False
            assert "registry down" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_list_tools_skips_non_string_name(self):
        class Reg(MultiNsRegistry):
            async def list_tools(self, namespace=None):
                return [_Info("safe", None), _Info("safe", "add")]  # type: ignore[list-item]

        broker = ToolBroker(Reg(), token="t", limits=IsolationLimits(), namespace="safe")
        try:
            reply = await _dispatch(broker, _wire.METHOD_LIST_TOOLS)
            names = [t[_wire.KEY_NAME] for t in reply[_wire.KEY_VALUE]]
            assert names == ["add"]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_injected_executor_with_no_result(self):
        broker = ToolBroker(
            MultiNsRegistry(), token="t", limits=IsolationLimits(), namespace="safe", executor=_EmptyExecutor()
        )
        try:
            reply = await _dispatch(broker, _wire.METHOD_CALL_TOOL, params=_call("add"))
            assert reply[_wire.KEY_OK] is False
            assert "no result" in reply[_wire.KEY_ERROR]
        finally:
            await broker.aclose()

    @pytest.mark.asyncio
    async def test_handshake_recv_error_is_swallowed(self):
        broker = _make_broker(namespace="safe")

        class _Bad:
            async def recv(self):
                raise ValueError("corrupt")

            async def send(self, obj):  # pragma: no cover - never reached
                pass

            async def aclose(self):
                pass

        # Must not raise: the broker must never crash the host.
        await broker._handle_client(_Bad())
        await broker.aclose()


@pytest.mark.skipif(not hasattr(__import__("os"), "getuid"), reason="POSIX unix-socket listener only")
class TestBrokerListenerPosix:
    @pytest.mark.asyncio
    async def test_start_exposes_unix_endpoint(self):
        broker = _make_broker(namespace="safe")
        endpoint = await broker.start()
        try:
            assert endpoint and broker.endpoint == endpoint
            assert broker.transport == _wire.TRANSPORT_UNIX
        finally:
            await broker.aclose()
            assert broker.endpoint is None
