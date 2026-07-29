# chuk_tool_processor/execution/isolation/backends/windows.py
"""
Windows AppContainer backend (EXPERIMENTAL).

The Windows analogue of the macOS Seatbelt backend. It launches the guest as an
**AppContainer** process (capability-restricted, low integrity by construction),
placed inside a **Job Object** that caps memory / active processes and kills the
whole tree on close. With no capabilities granted, the AppContainer has no
network and no access to the user's files; the guest reaches the host only via
the broker **named pipe**, whose ACL grants ``ALL APPLICATION PACKAGES``.

    macOS Seatbelt profile  ≈  Windows AppContainer + Job Object (+ low integrity)

.. warning::
    **Experimental and verified only via Windows CI.** This module uses the
    Win32 AppContainer / Job Object / CreateProcess APIs through ``ctypes`` and
    is import-safe on non-Windows (``is_available()`` returns False). The
    staging dir is granted to ``ALL APPLICATION PACKAGES`` (via ``icacls``) so
    the AppContainer can read the bootstrap and write its output; everything
    else is denied by the container. Details (exact capability set, stdout
    capture) may need adjustment as CI exercises it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from typing import Any

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.backend import GuestJob, GuestOutcome
from chuk_tool_processor.logging import get_logger

logger = get_logger("chuk_tool_processor.execution.isolation.windows")

_IS_WINDOWS = sys.platform == "win32"

_ISO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BOOTSTRAP_SRC = os.path.join(_ISO_DIR, "guest_bootstrap.py")
_WIRE_SRC = os.path.join(_ISO_DIR, "_wire.py")

# ALL APPLICATION PACKAGES — every AppContainer can be granted via this SID.
_ALL_APP_PACKAGES = "*S-1-15-2-1"

_MAX_OUTPUT = 64 * 1024


class WindowsBackend:
    """AppContainer + Job Object isolated guest (experimental)."""

    name = "windows"
    provides_isolation = True

    def is_available(self) -> bool:
        if not _IS_WINDOWS:
            return False
        try:
            import ctypes

            # AppContainer profile APIs live in userenv.dll.
            ctypes.WinDLL("userenv")
            ctypes.WinDLL("kernel32")
            return True
        except Exception:  # noqa: BLE001
            return False

    async def run_guest(
        self, job: GuestJob, *, host_endpoint: str, transport: str = _wire.TRANSPORT_PIPE
    ) -> GuestOutcome:
        # Deferred so this module imports cleanly on non-Windows.
        import asyncio

        return await asyncio.get_running_loop().run_in_executor(None, self._run_blocking, job, host_endpoint, transport)

    # -- blocking Win32 launch (runs in an executor thread) ---------------- #

    def _run_blocking(self, job: GuestJob, host_endpoint: str, transport: str) -> GuestOutcome:
        import json

        workdir = tempfile.mkdtemp(prefix="ctiso-")
        container_name = "ctiso-" + job.token[:24]
        stdout_path = os.path.join(workdir, "stdout.txt")
        stderr_path = os.path.join(workdir, "stderr.txt")
        sid = None
        try:
            shutil.copy2(_BOOTSTRAP_SRC, os.path.join(workdir, "guest_bootstrap.py"))
            shutil.copy2(_WIRE_SRC, os.path.join(workdir, "_wire.py"))
            # Pipe names are machine-global, so the guest endpoint == host endpoint.
            payload = job.payload(endpoint=host_endpoint, transport=transport)
            with open(os.path.join(workdir, "job.json"), "w", encoding="utf-8") as fh:
                json.dump(payload, fh)

            # Let the AppContainer read the staging dir and write its output there.
            self._grant_app_packages(workdir)
            # ...and read the interpreter (venv + base install) so Python can
            # start (it must read pyvenv.cfg + the stdlib). NB: this modifies the
            # interpreter dir's ACL persistently — fine for CI/throwaway envs.
            for prefix in {os.path.realpath(sys.base_prefix), os.path.realpath(sys.prefix)}:
                self._grant_read(prefix)

            sid = self._create_app_container(container_name)
            timed_out, exit_code = self._launch(job, workdir, sid, stdout_path, stderr_path)

            return GuestOutcome(
                exit_code=exit_code,
                stdout=_read_capped(stdout_path),
                stderr=_read_capped(stderr_path),
                timed_out=timed_out,
            )
        finally:
            if sid is not None:
                self._delete_app_container(container_name)
            shutil.rmtree(workdir, ignore_errors=True)

    def _grant_app_packages(self, path: str) -> None:
        # icacls is the pragmatic way to add an inheritable ACE for AppContainers.
        subprocess.run(
            ["icacls", path, "/grant", f"{_ALL_APP_PACKAGES}:(OI)(CI)(F)", "/T", "/Q"],
            capture_output=True,
            check=False,
        )

    def _grant_read(self, path: str) -> None:
        # Read+execute (not full) for read-only trees like the interpreter.
        subprocess.run(
            ["icacls", path, "/grant", f"{_ALL_APP_PACKAGES}:(OI)(CI)(RX)", "/T", "/Q"],
            capture_output=True,
            check=False,
        )

    # -- AppContainer profile ---------------------------------------------- #

    def _create_app_container(self, name: str) -> Any:
        import ctypes
        from ctypes import wintypes

        userenv = ctypes.WinDLL("userenv")
        psid = ctypes.c_void_p()
        # CreateAppContainerProfile(name, displayName, desc, capabilities, count, *sid)
        hr = userenv.CreateAppContainerProfile(
            ctypes.c_wchar_p(name),
            ctypes.c_wchar_p(name),
            ctypes.c_wchar_p("chuk-tool-processor isolated guest"),
            None,
            wintypes.DWORD(0),
            ctypes.byref(psid),
        )
        if hr != 0:
            # 0x800700B7 = already exists -> derive the SID instead.
            derive = userenv.DeriveAppContainerSidFromAppContainerName
            sid2 = ctypes.c_void_p()
            hr2 = derive(ctypes.c_wchar_p(name), ctypes.byref(sid2))
            if hr2 != 0:
                raise OSError(f"CreateAppContainerProfile failed (0x{hr & 0xFFFFFFFF:08x})")
            return sid2
        return psid

    def _delete_app_container(self, name: str) -> None:
        import contextlib
        import ctypes

        with contextlib.suppress(Exception):
            ctypes.WinDLL("userenv").DeleteAppContainerProfile(ctypes.c_wchar_p(name))

    # -- process launch + job object --------------------------------------- #

    def _launch(self, job: GuestJob, workdir: str, sid: Any, stdout_path: str, stderr_path: str) -> tuple[bool, int]:
        import ctypes

        from chuk_tool_processor.execution.isolation import _winproc

        argv = [sys.executable, os.path.join(workdir, "guest_bootstrap.py"), os.path.join(workdir, "job.json")]
        return _winproc.launch_appcontainer(
            argv=argv,
            sid=sid,
            limits=job.limits,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            cwd=workdir,
            _ctypes=ctypes,
        )


def _read_capped(path: str) -> str:
    try:
        with open(path, "rb") as fh:
            data = fh.read(_MAX_OUTPUT + 1)
    except OSError:
        return ""
    if len(data) > _MAX_OUTPUT:
        data = data[:_MAX_OUTPUT] + b"\n...[truncated]"
    return data.decode("utf-8", errors="replace")
