# chuk_tool_processor/execution/isolation/_winpipe.py
"""
Windows named-pipe broker transport (EXPERIMENTAL).

Provides the host side of the tool-broker channel on Windows, where unix sockets
don't exist and TCP loopback is blocked for AppContainers without a network hole.
A named pipe is created with a security descriptor that grants:

    * the current user + SYSTEM + Administrators full control, and
    * ``ALL APPLICATION PACKAGES`` (AC) read/write, plus a **low integrity**
      label (``S:(ML;;NW;;;LW)``)

so a low-integrity AppContainer child can connect to it by name — over a local
IPC object, not the network. Uses ``pywin32``; imported lazily and only on
Windows.

.. warning::
    Experimental and verified only via Windows CI. Blocking pipe I/O runs in the
    default thread-pool executor; each message is a 4-byte big-endian length
    prefix followed by JSON (same framing as ``_wire``).
"""

from __future__ import annotations

import asyncio
import struct
import threading
import uuid
from typing import Any

import pywintypes
import win32api
import win32con
import win32file
import win32pipe
import win32security

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.transport import BrokerListener, ClientHandler, MessageChannel

_LEN = struct.Struct(">I")
_BUF = 65536


def _pipe_security_attributes() -> Any:
    """SECURITY_ATTRIBUTES granting the user + ALL APPLICATION PACKAGES at low IL."""
    token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32con.TOKEN_QUERY)
    user_sid = win32security.GetTokenInformation(token, win32security.TokenUser)[0]
    user = win32security.ConvertSidToStringSid(user_sid)
    # GA=GENERIC_ALL, GRGW=read+write; AC=ALL APPLICATION PACKAGES; ML/LW=low integrity.
    sddl = (
        "D:"
        "(A;;GA;;;SY)"  # SYSTEM
        "(A;;GA;;;BA)"  # Administrators
        f"(A;;GA;;;{user})"  # current user
        "(A;;GRGW;;;AC)"  # ALL APPLICATION PACKAGES: read/write
        "S:(ML;;NW;;;LW)"  # low integrity label, no write-up restriction lifted for AC
    )
    sd = win32security.ConvertStringSecurityDescriptorToSecurityDescriptor(sddl, win32security.SDDL_REVISION_1)
    sa = win32security.SECURITY_ATTRIBUTES()
    sa.SECURITY_DESCRIPTOR = sd
    sa.bInheritHandle = False
    return sa


class _PipeChannel(MessageChannel):
    """MessageChannel over a connected named-pipe instance handle (blocking I/O in executor)."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle
        self._write_lock = asyncio.Lock()

    def _read_exact(self, n: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < n:
            _hr, data = win32file.ReadFile(self._handle, n - len(chunks))
            if not data:
                raise ConnectionError("pipe closed")
            chunks.extend(data)
        return bytes(chunks)

    def _recv_blocking(self) -> dict[str, Any]:
        (length,) = _LEN.unpack(self._read_exact(_LEN.size))
        body = self._read_exact(length)
        import json

        return json.loads(body.decode("utf-8"))

    async def recv(self) -> dict[str, Any]:
        try:
            return await asyncio.get_running_loop().run_in_executor(None, self._recv_blocking)
        except pywintypes.error as exc:
            raise ConnectionError(str(exc)) from exc

    async def send(self, obj: dict[str, Any]) -> None:
        data = _wire.encode(obj)
        async with self._write_lock:
            await asyncio.get_running_loop().run_in_executor(None, win32file.WriteFile, self._handle, data)

    async def aclose(self) -> None:
        import contextlib

        with contextlib.suppress(Exception):
            win32pipe.DisconnectNamedPipe(self._handle)
        with contextlib.suppress(Exception):
            win32file.CloseHandle(self._handle)


class _PipeServer:
    """Accept loop for one broker; creates pipe instances and hands each to the handler."""

    def __init__(self, name: str, handle_client: ClientHandler, loop: asyncio.AbstractEventLoop) -> None:
        self.name = name
        self._handle_client = handle_client
        self._loop = loop
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="ctiso-pipe", daemon=True)
        self._first = True

    def start(self) -> None:
        self._thread.start()

    def _make_instance(self) -> Any:
        sa = _pipe_security_attributes()
        open_mode = win32con.PIPE_ACCESS_DUPLEX
        if self._first:
            open_mode |= win32con.FILE_FLAG_FIRST_PIPE_INSTANCE
            self._first = False
        pipe_mode = win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT
        pipe_mode |= getattr(win32pipe, "PIPE_REJECT_REMOTE_CLIENTS", 0)
        return win32pipe.CreateNamedPipe(
            self.name, open_mode, pipe_mode, win32pipe.PIPE_UNLIMITED_INSTANCES, _BUF, _BUF, 0, sa
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                handle = self._make_instance()
            except Exception:  # noqa: BLE001 - can't create pipe; stop
                break
            try:
                win32pipe.ConnectNamedPipe(handle, None)
            except pywintypes.error as exc:
                # ERROR_PIPE_CONNECTED (535): a client connected before we waited.
                if getattr(exc, "winerror", None) != 535:
                    win32file.CloseHandle(handle)
                    if self._stop.is_set():
                        break
                    continue
            if self._stop.is_set():
                win32file.CloseHandle(handle)
                break
            import functools

            channel = _PipeChannel(handle)
            self._loop.call_soon_threadsafe(functools.partial(self._spawn, channel))

    def _spawn(self, channel: MessageChannel) -> None:
        asyncio.ensure_future(self._handle_client(channel), loop=self._loop)  # noqa: RUF006

    def close(self) -> None:
        self._stop.set()
        # Unblock a pending ConnectNamedPipe by connecting a throwaway client.
        try:
            h = win32file.CreateFile(
                self.name, win32con.GENERIC_READ | win32con.GENERIC_WRITE, 0, None, win32con.OPEN_EXISTING, 0, None
            )
            win32file.CloseHandle(h)
        except Exception:  # noqa: BLE001
            pass


async def start_pipe_listener(handle_client: ClientHandler) -> BrokerListener:
    name = r"\\.\pipe\ctiso-" + uuid.uuid4().hex
    server = _PipeServer(name, handle_client, asyncio.get_running_loop())
    server.start()
    return BrokerListener(endpoint=name, transport="pipe", _server=server, _cleanup=None)
