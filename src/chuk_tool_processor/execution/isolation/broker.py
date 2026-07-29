# chuk_tool_processor/execution/isolation/broker.py
"""
Host-side tool broker for isolated code execution.

The broker is the *only* channel the isolated guest has back into the host. It
listens on a unix-domain socket, authenticates the guest with a per-run token,
and exposes exactly two capabilities:

    list_tools(namespace)   -> the names of tools the guest may call
    call_tool(name, args)   -> run a registered tool ON THE HOST and return JSON

Tools run in the trusted host process (with their real credentials); only their
JSON-encoded results cross back to the guest. Everything the broker enforces —
the tool allowlist, the per-run call ceiling, the token, JSON-only framing — is
enforced here on the host, never in the guest.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
from typing import Any

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.limits import IsolationLimits
from chuk_tool_processor.execution.isolation.transport import BrokerListener, MessageChannel, start_listener
from chuk_tool_processor.execution.tool_executor import ToolExecutor
from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.models.tool_call import ToolCall
from chuk_tool_processor.registry.interface import ToolRegistryInterface

logger = get_logger("chuk_tool_processor.execution.isolation.broker")


def _dbg(msg: str) -> None:
    import os

    if os.environ.get(_wire.GUEST_DEBUG_ENV) == _wire.GUEST_DEBUG_ON:
        import sys

        print(f"[host] {msg}", file=sys.stderr, flush=True)


def _to_jsonable(obj: Any) -> Any:
    """Best-effort conversion of a tool result into JSON-serialisable data."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        pass
    # Pydantic v2 / v1
    for attr in ("model_dump", "dict"):
        method = getattr(obj, attr, None)
        if callable(method):
            with contextlib.suppress(Exception):
                return _to_jsonable(method())
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _to_jsonable(dataclasses.asdict(obj))
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_to_jsonable(v) for v in obj]
    return repr(obj)


class ToolBroker:
    """Serves tool calls to a single isolated guest over a unix socket."""

    def __init__(
        self,
        registry: ToolRegistryInterface,
        *,
        token: str,
        limits: IsolationLimits,
        namespace: str | None = None,
        allowed_tools: set[str] | None = None,
        executor: ToolExecutor | None = None,
    ) -> None:
        self._registry = registry
        self._token = token
        self._limits = limits
        self._namespace = namespace
        self._allowed_tools = allowed_tools

        # Tool calls run through the canonical processor path so sandboxed calls
        # get the same wrappers/guards/observability as any other call. Callers
        # may inject a fully-wrapped executor; otherwise we build (and own) a
        # default one over the same registry.
        self._executor = executor or ToolExecutor(registry=registry, default_timeout=limits.wall_timeout)
        self._owns_executor = executor is None

        self._listener: BrokerListener | None = None

        self._call_count = 0
        self._count_lock = asyncio.Lock()

        # In-flight dispatch tasks, tracked so a guest that goes away (or times
        # out) cannot leave privileged host tool calls running unattended.
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closing = False

        self._result_future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    # -- lifecycle --------------------------------------------------------- #

    async def start(self) -> str:
        """Start the platform broker server. Returns the endpoint the guest connects to."""
        self._listener = await start_listener(self._handle_client)
        logger.debug("Tool broker listening (%s) at %s", self._listener.transport, self._listener.endpoint)
        return self._listener.endpoint

    async def aclose(self) -> None:
        """Stop the server, refuse new calls, and cancel any host calls still running."""
        self._closing = True
        if self._listener is not None:
            await self._listener.aclose()
            self._listener = None
        # Cancel outstanding tool calls: once the sandbox run is over (guest
        # finished, died, or timed out) no privileged host call should keep
        # running. Cancellation is cooperative — a tool that ignores it may still
        # finish its own side effect — but we no longer wait on it.
        tasks = list(self._tasks)
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self._owns_executor:
            with contextlib.suppress(Exception):
                await self._executor.shutdown()
        if not self._result_future.done():
            self._result_future.cancel()

    # -- accessors --------------------------------------------------------- #

    @property
    def endpoint(self) -> str | None:
        return self._listener.endpoint if self._listener else None

    @property
    def transport(self) -> str | None:
        return self._listener.transport if self._listener else None

    @property
    def tool_calls(self) -> int:
        return self._call_count

    def result(self, default: Any = None) -> Any:
        """The value the guest reported, or ``default`` if it never reported one."""
        if self._result_future.done() and not self._result_future.cancelled():
            return self._result_future.result()
        return default

    def has_result(self) -> bool:
        return self._result_future.done() and not self._result_future.cancelled()

    # -- connection handling ----------------------------------------------- #

    async def _handle_client(self, channel: MessageChannel) -> None:
        try:
            hello = await channel.recv()
            if hello.get(_wire.KEY_METHOD) != _wire.METHOD_HELLO or hello.get(_wire.KEY_TOKEN) != self._token:
                logger.warning("Rejected guest connection: bad handshake")
                await self._reply(channel, self._err(hello.get(_wire.KEY_ID), "unauthorized"))
                return
            await self._reply(channel, {_wire.KEY_ID: hello.get(_wire.KEY_ID), _wire.KEY_OK: True})

            while not self._closing:
                try:
                    msg = await channel.recv()
                except (EOFError, asyncio.IncompleteReadError, ConnectionError):
                    break
                _dbg(f"recv {msg.get(_wire.KEY_METHOD)} id={msg.get(_wire.KEY_ID)}")
                # Each request handled concurrently so guest asyncio.gather works;
                # tracked so aclose() can cancel calls a departing guest left running.
                task = asyncio.create_task(self._dispatch(msg, channel))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        except Exception as exc:  # noqa: BLE001 - broker must never crash the host
            logger.debug("Broker connection error: %s", exc)
        finally:
            await channel.aclose()

    async def _dispatch(self, msg: dict[str, Any], channel: MessageChannel) -> None:
        method = msg.get(_wire.KEY_METHOD)
        msg_id = msg.get(_wire.KEY_ID)
        _dbg(f"dispatch {method} id={msg_id}")
        try:
            if method == _wire.METHOD_LIST_TOOLS:
                await self._reply(channel, self._ok(msg_id, await self._list_tools()))
                _dbg(f"replied {method} id={msg_id}")
            elif method == _wire.METHOD_CALL_TOOL:
                # Stop honouring tool calls once the run has produced its result
                # or the broker is shutting down — no privileged work after the end.
                if self._closing or self._result_future.done():
                    await self._reply(channel, self._err(msg_id, "run already completed"))
                    return
                value = await self._call_tool(msg.get(_wire.KEY_PARAMS) or {})
                await self._reply(channel, self._ok(msg_id, value))
            elif method == _wire.METHOD_RESULT:
                if not self._result_future.done():
                    self._result_future.set_result((msg.get(_wire.KEY_PARAMS) or {}).get(_wire.KEY_VALUE))
                await self._reply(channel, {_wire.KEY_ID: msg_id, _wire.KEY_OK: True})
            else:
                await self._reply(channel, self._err(msg_id, f"unknown method: {method}"))
        except _BrokerReject as exc:
            await self._reply(channel, self._err(msg_id, str(exc)))
        except Exception as exc:  # noqa: BLE001 - surface as tool error, never crash host
            await self._reply(channel, self._err(msg_id, f"{type(exc).__name__}: {exc}"))

    @staticmethod
    def _ok(msg_id: Any, value: Any) -> dict[str, Any]:
        return {_wire.KEY_ID: msg_id, _wire.KEY_OK: True, _wire.KEY_VALUE: value}

    @staticmethod
    def _err(msg_id: Any, error: str) -> dict[str, Any]:
        return {_wire.KEY_ID: msg_id, _wire.KEY_OK: False, _wire.KEY_ERROR: error}

    async def _reply(self, channel: MessageChannel, obj: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            await channel.send(obj)

    # -- capabilities ------------------------------------------------------ #

    def _is_allowed(self, namespace: str, name: str) -> bool:
        """Whether ``namespace.name`` is permitted by the host allowlist.

        Entries may be bare (``"name"``) or namespace-qualified (``"ns.name"``).
        Qualified entries pin the tool to one namespace; bare entries are the
        convenience form and match the name in whatever namespace policy resolved.
        """
        if self._allowed_tools is None:
            return True
        return name in self._allowed_tools or f"{namespace}.{name}" in self._allowed_tools

    def _resolve_namespace(self, requested: Any) -> str:
        """Resolve the namespace, with host policy authoritative.

        The guest holds the broker token and can craft raw frames, so a guest-
        supplied namespace must never widen access: if the runner pinned a
        namespace, any other value is rejected rather than honoured.
        """
        if self._namespace is not None:
            if requested is not None and requested != self._namespace:
                raise _BrokerReject(f"namespace not allowed: {requested}")
            return self._namespace
        if requested is not None and not isinstance(requested, str):
            raise _BrokerReject("namespace must be a string")
        return requested or _wire.DEFAULT_NAMESPACE

    async def _list_tools(self) -> list[dict[str, str]]:
        tools = await self._registry.list_tools(namespace=self._namespace)
        out = []
        for info in tools:
            name = getattr(info, "name", None)
            ns = getattr(info, "namespace", None) or _wire.DEFAULT_NAMESPACE
            if not isinstance(name, str):
                continue
            if not self._is_allowed(ns, name):
                continue
            out.append({_wire.KEY_NAME: name, _wire.KEY_NAMESPACE: ns})
        return out

    async def _call_tool(self, params: dict[str, Any]) -> Any:
        name = params.get(_wire.KEY_NAME)
        arguments = params.get(_wire.KEY_ARGUMENTS) or {}

        if not isinstance(name, str):
            raise _BrokerReject("call_tool requires a string 'name'")
        namespace = self._resolve_namespace(params.get(_wire.KEY_NAMESPACE))
        if not self._is_allowed(namespace, name):
            raise _BrokerReject(f"tool not allowed: {namespace}.{name}")
        if not isinstance(arguments, dict):
            raise _BrokerReject("tool arguments must be an object")

        async with self._count_lock:
            if self._call_count >= self._limits.max_tool_calls:
                raise _BrokerReject(f"tool-call limit exceeded ({self._limits.max_tool_calls})")
            self._call_count += 1

        return _to_jsonable(await self._execute_tool(namespace, name, arguments))

    async def _execute_tool(self, namespace: str, name: str, arguments: dict[str, Any]) -> Any:
        # Route through the canonical executor. Use the dotted ``namespace.name``
        # form: the strategy resolves that strictly in the named namespace with NO
        # cross-namespace fallback, so the host's namespace decision is preserved
        # all the way to invocation (a bare name could fuzzy-match elsewhere).
        call = ToolCall(tool=f"{namespace}.{name}", namespace=namespace, arguments=arguments)
        results = await self._executor.execute([call])
        if not results:
            raise _BrokerReject(f"tool produced no result: {namespace}.{name}")
        result = results[0]
        if result.error is not None:
            raise _BrokerReject(result.error)
        return result.result


class _BrokerReject(Exception):
    """Internal: a request rejected for policy reasons (reported to the guest)."""
