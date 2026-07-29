# chuk_tool_processor/execution/isolation/guest_bootstrap.py
"""
Guest-side entrypoint for isolated code execution.

This script runs INSIDE the isolation boundary (subprocess / container / etc.).
It must stay dependency-free — stdlib plus a sibling copy of ``_wire.py`` only —
because the chuk_tool_processor package is not installed in the guest.

It reads a JSON job file, applies best-effort in-process resource limits, opens
the tool-broker socket, executes the user code with async tool proxies bound in
its globals, and reports the return value back over the socket. Because the code
runs behind a real OS/runtime boundary, the guest deliberately does NOT restrict
builtins — containment is the boundary's job, not a curated namespace's.

Usage: python guest_bootstrap.py <job.json>
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import _wire  # sibling copy placed next to this file by the backend


def _apply_limits(limits: dict) -> None:
    """Best-effort in-process resource limits (defense in depth; POSIX only)."""
    try:
        import resource
    except ImportError:
        return

    def _set(res: int, value) -> None:
        try:
            _soft, hard = resource.getrlimit(res)
            cap = value if hard == resource.RLIM_INFINITY else min(value, hard)
            resource.setrlimit(res, (cap, hard))
        except (ValueError, OSError):
            pass

    cpu = limits.get("cpu_timeout")
    if cpu:
        _set(resource.RLIMIT_CPU, int(cpu) + 1)
    mem = limits.get("memory_bytes")
    if mem and hasattr(resource, "RLIMIT_AS"):
        _set(resource.RLIMIT_AS, int(mem))
    procs = limits.get("max_processes")
    if procs and hasattr(resource, "RLIMIT_NPROC"):
        _set(resource.RLIMIT_NPROC, int(procs))


def _jsonable(obj):
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        if isinstance(obj, dict):
            return {str(k): _jsonable(v) for k, v in obj.items()}
        if isinstance(obj, list | tuple):
            return [_jsonable(v) for v in obj]
        for attr in ("model_dump", "dict"):
            method = getattr(obj, attr, None)
            if callable(method):
                try:
                    return _jsonable(method())
                except Exception:
                    pass
        return repr(obj)


class _RpcClient:
    """Multiplexes request/response frames over the broker socket by message id."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None

    def start(self) -> None:
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            while True:
                msg = await _wire.recv(self._reader)
                fut = self._pending.pop(msg.get("id"), None)
                if fut and not fut.done():
                    fut.set_result(msg)
        except (EOFError, asyncio.IncompleteReadError):
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("broker closed the connection"))

    async def _roundtrip(self, frame: dict):
        msg_id = self._next_id
        self._next_id += 1
        frame["id"] = msg_id
        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        await _wire.send(self._writer, frame)
        reply = await fut
        if not reply.get("ok"):
            raise RuntimeError(reply.get("error") or "broker rejected request")
        return reply.get("value")

    async def hello(self, token: str) -> None:
        await self._roundtrip({"method": "hello", "token": token})

    async def request(self, method: str, params: dict):
        return await self._roundtrip({"method": method, "params": params})


def _build_tool_proxy(client: _RpcClient, name: str, namespace: str):
    async def _proxy(**kwargs):
        return await client.request("call_tool", {"name": name, "namespace": namespace, "arguments": kwargs})

    _proxy.__name__ = name
    return _proxy


async def _run_user_code(code: str, exec_globals: dict):
    """Execute user code, mirroring CodeSandbox's await/return wrapping."""
    needs_wrapping = "await " in code or "return " in code
    local_scope: dict = {}
    if needs_wrapping:
        header = "async def __guest_main__():\n" if "await " in code else "def __guest_main__():\n"
        wrapped = header + "".join(f"    {line}\n" for line in code.split("\n"))
        exec(compile(wrapped, "<guest>", "exec"), exec_globals, local_scope)
        fn = local_scope["__guest_main__"]
        return await fn() if "await " in code else fn()
    exec(compile(code, "<guest>", "exec"), exec_globals, local_scope)
    return local_scope.get("__result__")


async def _connect(job: dict):
    """Connect to the broker endpoint per the job's transport."""
    transport = job.get("transport", "unix")
    endpoint = job.get("endpoint") or job.get("socket_path")
    if transport == "pipe":
        return await _open_pipe(endpoint)
    return await asyncio.open_unix_connection(endpoint)


async def _open_pipe(name: str):
    """Windows named-pipe client -> (StreamReader, StreamWriter). Retries while busy."""
    import time

    loop = asyncio.get_running_loop()
    last = None
    for _ in range(50):  # ~5s of retries for pipe-busy / not-yet-listening
        try:
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            transport, _proto = await loop.create_pipe_connection(lambda p=protocol: p, name)  # type: ignore[attr-defined]  # noqa: B023
            writer = asyncio.StreamWriter(transport, protocol, reader, loop)
            return reader, writer
        except (FileNotFoundError, OSError) as exc:  # pipe not ready / all instances busy
            last = exc
            time.sleep(0.1)
    raise last or ConnectionError(f"could not connect to pipe {name}")


def _dbg(msg: str) -> None:
    if os.environ.get("CTP_GUEST_DEBUG") == "1":
        print(f"[guest] {msg}", file=sys.stderr, flush=True)


async def _main(job: dict) -> int:
    _dbg(f"connecting transport={job.get('transport')} endpoint={job.get('endpoint')}")
    reader, writer = await _connect(job)
    _dbg("connected; starting rpc client")
    client = _RpcClient(reader, writer)
    client.start()
    _dbg("sending hello")
    await client.hello(job["token"])
    _dbg("hello ack; requesting list_tools")

    tools = await client.request("list_tools", {})
    _dbg(f"got {len(tools)} tools; running user code")
    exec_globals: dict = dict(job.get("initial_vars") or {})
    for meta in tools:
        exec_globals[meta["name"]] = _build_tool_proxy(client, meta["name"], meta["namespace"])

    try:
        value = await _run_user_code(job["code"], exec_globals)
        await client.request("result", {"value": _jsonable(value)})
        writer.close()
        return 0
    except Exception as exc:  # report guest exceptions as structured output
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: guest_bootstrap.py <job.json>", file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as fh:
        job = json.load(fh)
    _apply_limits(job.get("limits") or {})
    try:
        return asyncio.run(_main(job))
    except Exception as exc:  # noqa: BLE001 - top-level guest failure
        print(f"guest failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
