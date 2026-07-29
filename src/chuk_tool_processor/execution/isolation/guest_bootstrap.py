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
import contextlib
import json
import os
import struct
import sys

import _wire  # sibling copy placed next to this file by the backend

# Windows named-pipe connect retry budget (~10s) while the host pipe comes up.
_PIPE_CONNECT_ATTEMPTS = 100
_PIPE_CONNECT_DELAY_S = 0.1
# CreateFile flag for overlapped (async) I/O — see _PipeChannel for why it matters.
_FILE_FLAG_OVERLAPPED = 0x40000000
# 4-byte big-endian length prefix, matching _wire's on-the-wire framing.
_HEADER = struct.Struct(">I")


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

    cpu = limits.get(_wire.KEY_CPU_TIMEOUT)
    if cpu:
        _set(resource.RLIMIT_CPU, int(cpu) + 1)
    mem = limits.get(_wire.KEY_MEMORY_BYTES)
    if mem and hasattr(resource, "RLIMIT_AS"):
        _set(resource.RLIMIT_AS, int(mem))
    procs = limits.get(_wire.KEY_MAX_PROCESSES)
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

    def __init__(self, channel):
        self._channel = channel
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None

    def start(self) -> None:
        self._reader_task = asyncio.create_task(self._read_loop())

    def stop(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            self._reader_task = None

    async def _read_loop(self) -> None:
        try:
            while True:
                msg = await self._channel.recv()
                fut = self._pending.pop(msg.get(_wire.KEY_ID), None)
                if fut and not fut.done():
                    fut.set_result(msg)
        except (EOFError, asyncio.IncompleteReadError, ConnectionError, OSError):
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("broker closed the connection"))

    async def _roundtrip(self, frame: dict):
        msg_id = self._next_id
        self._next_id += 1
        frame[_wire.KEY_ID] = msg_id
        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        await self._channel.send(frame)
        reply = await fut
        if not reply.get(_wire.KEY_OK):
            raise RuntimeError(reply.get(_wire.KEY_ERROR) or "broker rejected request")
        return reply.get(_wire.KEY_VALUE)

    async def hello(self, token: str) -> None:
        await self._roundtrip({_wire.KEY_METHOD: _wire.METHOD_HELLO, _wire.KEY_TOKEN: token})

    async def request(self, method: str, params: dict):
        return await self._roundtrip({_wire.KEY_METHOD: method, _wire.KEY_PARAMS: params})


def _build_tool_proxy(client: _RpcClient, name: str, namespace: str):
    async def _proxy(**kwargs):
        params = {_wire.KEY_NAME: name, _wire.KEY_NAMESPACE: namespace, _wire.KEY_ARGUMENTS: kwargs}
        return await client.request(_wire.METHOD_CALL_TOOL, params)

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


class _StreamChannel:
    """Framed-JSON channel over an asyncio (reader, writer) pair (unix socket)."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer

    async def recv(self) -> dict:
        return await _wire.recv(self._reader)

    async def send(self, obj: dict) -> None:
        await _wire.send(self._writer, obj)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._writer.close()


def _ov_read_exact(handle, n: int) -> bytes:
    """Read exactly ``n`` bytes with overlapped I/O.

    The handle is opened overlapped so a pending read and a concurrent write don't
    serialize (a synchronous handle deadlocks: the request write would block behind
    the read that is waiting for that request's reply).
    """
    import pywintypes
    import win32event
    import win32file
    import winerror

    chunks = bytearray()
    while len(chunks) < n:
        want = n - len(chunks)
        ov = pywintypes.OVERLAPPED()
        ov.hEvent = win32event.CreateEvent(None, True, False, None)
        try:
            buf = win32file.AllocateReadBuffer(want)
            try:
                win32file.ReadFile(handle, buf, ov)
                nread = win32file.GetOverlappedResult(handle, ov, True)
            except pywintypes.error as exc:
                if exc.winerror == winerror.ERROR_IO_PENDING:
                    nread = win32file.GetOverlappedResult(handle, ov, True)
                else:
                    # broken pipe / handle closed while a read was pending
                    # (ERROR_OPERATION_ABORTED on shutdown) -> treat as EOF.
                    raise ConnectionError(f"pipe read failed: {exc}") from exc
            if nread == 0:
                raise ConnectionError("pipe closed")
            chunks.extend(memoryview(buf)[:nread])
        finally:
            win32file.CloseHandle(ov.hEvent)
    return bytes(chunks)


def _ov_write_all(handle, data: bytes) -> None:
    """Write all of ``data`` with overlapped I/O (see :func:`_ov_read_exact`)."""
    import pywintypes
    import win32event
    import win32file
    import winerror

    ov = pywintypes.OVERLAPPED()
    ov.hEvent = win32event.CreateEvent(None, True, False, None)
    try:
        try:
            win32file.WriteFile(handle, data, ov)
        except pywintypes.error as exc:
            if exc.winerror != winerror.ERROR_IO_PENDING:
                raise
        win32file.GetOverlappedResult(handle, ov, True)
    finally:
        win32file.CloseHandle(ov.hEvent)


class _PipeChannel:
    """Framed-JSON channel over a connected Windows named-pipe handle.

    Uses overlapped win32 ReadFile/WriteFile in the default executor. Overlapped
    I/O is required: a synchronous handle serializes a pending read against a
    concurrent write, which deadlocks the second request/reply round-trip. Writes
    are serialized by a lock so concurrent tool calls don't interleave frames.
    """

    def __init__(self, handle):
        self._handle = handle
        self._write_lock = asyncio.Lock()

    def _recv_blocking(self) -> dict:
        (length,) = _HEADER.unpack(_ov_read_exact(self._handle, _HEADER.size))
        return json.loads(_ov_read_exact(self._handle, length).decode("utf-8"))

    async def recv(self) -> dict:
        return await asyncio.get_running_loop().run_in_executor(None, self._recv_blocking)

    async def send(self, obj: dict) -> None:
        data = _wire.encode(obj)
        async with self._write_lock:
            await asyncio.get_running_loop().run_in_executor(None, _ov_write_all, self._handle, data)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            import win32file

            win32file.CloseHandle(self._handle)


async def _connect(job: dict):
    """Connect to the broker endpoint per the job's transport. Returns a channel."""
    transport = job.get(_wire.KEY_TRANSPORT, _wire.TRANSPORT_UNIX)
    endpoint = job.get(_wire.KEY_ENDPOINT)
    if transport == _wire.TRANSPORT_PIPE:
        return _PipeChannel(_open_pipe_handle(endpoint))
    reader, writer = await asyncio.open_unix_connection(endpoint)
    return _StreamChannel(reader, writer)


def _open_pipe_handle(name: str):
    """Open the named pipe (blocking) with retries while it's not yet listening/busy."""
    import time

    import pywintypes
    import win32con
    import win32file

    last = None
    for _ in range(_PIPE_CONNECT_ATTEMPTS):
        try:
            return win32file.CreateFile(
                name,
                win32con.GENERIC_READ | win32con.GENERIC_WRITE,
                0,
                None,
                win32con.OPEN_EXISTING,
                _FILE_FLAG_OVERLAPPED,
                None,
            )
        except pywintypes.error as exc:
            last = exc
            time.sleep(_PIPE_CONNECT_DELAY_S)
    raise ConnectionError(f"could not connect to pipe {name}: {last}")


def _dbg(msg: str) -> None:
    if os.environ.get(_wire.GUEST_DEBUG_ENV) == _wire.GUEST_DEBUG_ON:
        print(f"[guest] {msg}", file=sys.stderr, flush=True)


async def _main(job: dict) -> int:
    _dbg(f"connecting transport={job.get(_wire.KEY_TRANSPORT)} endpoint={job.get(_wire.KEY_ENDPOINT)}")
    channel = await _connect(job)
    _dbg("connected; starting rpc client")
    client = _RpcClient(channel)
    client.start()
    _dbg("sending hello")
    await client.hello(job[_wire.KEY_TOKEN])
    _dbg("hello ack; requesting list_tools")

    try:
        tools = await client.request(_wire.METHOD_LIST_TOOLS, {})
        _dbg(f"got {len(tools)} tools; running user code")
        exec_globals: dict = dict(job.get(_wire.KEY_INITIAL_VARS) or {})
        for meta in tools:
            name, namespace = meta[_wire.KEY_NAME], meta[_wire.KEY_NAMESPACE]
            exec_globals[name] = _build_tool_proxy(client, name, namespace)

        value = await _run_user_code(job[_wire.KEY_CODE], exec_globals)
        await client.request(_wire.METHOD_RESULT, {_wire.KEY_VALUE: _jsonable(value)})
        return 0
    except Exception as exc:  # report guest exceptions as structured output
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
    finally:
        # Always tear the channel down. On Windows the reader parks in a blocking
        # overlapped ReadFile in an executor thread; closing the handle aborts it
        # so the interpreter can exit instead of hanging on executor shutdown.
        client.stop()
        channel.close()


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: guest_bootstrap.py <job.json>", file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as fh:
        job = json.load(fh)
    _apply_limits(job.get(_wire.KEY_LIMITS) or {})
    try:
        return asyncio.run(_main(job))
    except Exception as exc:  # noqa: BLE001 - top-level guest failure
        print(f"guest failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
