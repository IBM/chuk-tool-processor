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

from chuk_tool_processor.execution.isolation.limits import IsolationLimits
from chuk_tool_processor.execution.isolation.transport import BrokerListener, MessageChannel, start_listener
from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.registry.interface import ToolRegistryInterface

logger = get_logger("chuk_tool_processor.execution.isolation.broker")


def _dbg(msg: str) -> None:
    import os

    if os.environ.get("CTP_GUEST_DEBUG") == "1":
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
    ) -> None:
        self._registry = registry
        self._token = token
        self._limits = limits
        self._namespace = namespace
        self._allowed_tools = allowed_tools

        self._listener: BrokerListener | None = None

        self._call_count = 0
        self._count_lock = asyncio.Lock()

        self._result_future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    # -- lifecycle --------------------------------------------------------- #

    async def start(self) -> str:
        """Start the platform broker server. Returns the endpoint the guest connects to."""
        self._listener = await start_listener(self._handle_client)
        logger.debug("Tool broker listening (%s) at %s", self._listener.transport, self._listener.endpoint)
        return self._listener.endpoint

    async def aclose(self) -> None:
        """Stop the server and clean up its endpoint."""
        if self._listener is not None:
            await self._listener.aclose()
            self._listener = None
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
            if hello.get("method") != "hello" or hello.get("token") != self._token:
                logger.warning("Rejected guest connection: bad handshake")
                await self._reply(channel, {"id": hello.get("id"), "ok": False, "error": "unauthorized"})
                return
            await self._reply(channel, {"id": hello.get("id"), "ok": True})

            while True:
                try:
                    msg = await channel.recv()
                except (EOFError, asyncio.IncompleteReadError, ConnectionError):
                    break
                _dbg(f"recv {msg.get('method')} id={msg.get('id')}")
                # Each request handled concurrently so guest asyncio.gather works.
                asyncio.create_task(self._dispatch(msg, channel))
        except Exception as exc:  # noqa: BLE001 - broker must never crash the host
            logger.debug("Broker connection error: %s", exc)
        finally:
            await channel.aclose()

    async def _dispatch(self, msg: dict[str, Any], channel: MessageChannel) -> None:
        method = msg.get("method")
        msg_id = msg.get("id")
        _dbg(f"dispatch {method} id={msg_id}")
        try:
            if method == "list_tools":
                await self._reply(channel, {"id": msg_id, "ok": True, "value": await self._list_tools()})
                _dbg(f"replied {method} id={msg_id}")
            elif method == "call_tool":
                value = await self._call_tool(msg.get("params") or {})
                await self._reply(channel, {"id": msg_id, "ok": True, "value": value})
            elif method == "result":
                if not self._result_future.done():
                    self._result_future.set_result((msg.get("params") or {}).get("value"))
                await self._reply(channel, {"id": msg_id, "ok": True})
            else:
                await self._reply(channel, {"id": msg_id, "ok": False, "error": f"unknown method: {method}"})
        except _BrokerReject as exc:
            await self._reply(channel, {"id": msg_id, "ok": False, "error": str(exc)})
        except Exception as exc:  # noqa: BLE001 - surface as tool error, never crash host
            await self._reply(channel, {"id": msg_id, "ok": False, "error": f"{type(exc).__name__}: {exc}"})

    async def _reply(self, channel: MessageChannel, obj: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            await channel.send(obj)

    # -- capabilities ------------------------------------------------------ #

    async def _list_tools(self) -> list[dict[str, str]]:
        tools = await self._registry.list_tools(namespace=self._namespace)
        out = []
        for info in tools:
            name = getattr(info, "name", None)
            ns = getattr(info, "namespace", None) or "default"
            if not isinstance(name, str):
                continue
            if self._allowed_tools is not None and name not in self._allowed_tools:
                continue
            out.append({"name": name, "namespace": ns})
        return out

    async def _call_tool(self, params: dict[str, Any]) -> Any:
        name = params.get("name")
        namespace = params.get("namespace") or self._namespace or "default"
        arguments = params.get("arguments") or {}

        if not isinstance(name, str):
            raise _BrokerReject("call_tool requires a string 'name'")
        if self._allowed_tools is not None and name not in self._allowed_tools:
            raise _BrokerReject(f"tool not allowed: {name}")
        if not isinstance(arguments, dict):
            raise _BrokerReject("tool arguments must be an object")

        async with self._count_lock:
            if self._call_count >= self._limits.max_tool_calls:
                raise _BrokerReject(f"tool-call limit exceeded ({self._limits.max_tool_calls})")
            self._call_count += 1

        tool = await self._registry.get_tool(name, namespace)
        if tool is None:
            raise _BrokerReject(f"tool not found: {namespace}.{name}")

        result = await tool.execute(**arguments)
        return _to_jsonable(result)


class _BrokerReject(Exception):
    """Internal: a request rejected for policy reasons (reported to the guest)."""
