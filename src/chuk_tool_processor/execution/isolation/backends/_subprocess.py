# chuk_tool_processor/execution/isolation/backends/_subprocess.py
"""
Shared launcher base for backends that run the guest as a local child process.

Local (no isolation), Seatbelt (macOS ``sandbox-exec``), bubblewrap (Linux
namespaces), and Docker all follow the same recipe: stage the guest bootstrap
into a work dir, build ``<sandbox wrapper> python guest_bootstrap.py job.json``,
run it with a wall-clock kill, and capture output. They differ only in the
wrapper argv and (for containers) how host paths map into the guest — expressed
here as override hooks.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass

from chuk_tool_processor.execution.isolation.backend import GuestJob, GuestOutcome
from chuk_tool_processor.logging import get_logger

logger = get_logger("chuk_tool_processor.execution.isolation.subprocess")

_ISO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BOOTSTRAP_SRC = os.path.join(_ISO_DIR, "guest_bootstrap.py")
_WIRE_SRC = os.path.join(_ISO_DIR, "_wire.py")

_POSIX = os.name == "posix"


@dataclass
class _LaunchCtx:
    """Paths for one launch, both host-side and guest-visible."""

    workdir: str  # host path of the staging dir
    host_socket_path: str  # broker socket on the host
    bootstrap_guest: str  # bootstrap path as the guest sees it
    job_guest: str  # job.json path as the guest sees it
    socket_guest: str  # broker socket path as the guest sees it


class SubprocessBackend:
    """Base class for local-child-process isolation backends."""

    name = "subprocess"
    provides_isolation = False

    # -- hooks subclasses override ----------------------------------------- #

    def is_available(self) -> bool:
        return _POSIX

    def _python_exe(self) -> str:
        """Interpreter that runs the guest bootstrap (guest-visible)."""
        return sys.executable

    def _wrapper_argv(self, ctx: _LaunchCtx, job: GuestJob) -> list[str]:  # noqa: ARG002 - override hook
        """Sandbox launcher prefix, e.g. ['sandbox-exec', '-p', profile]."""
        return []

    def _guest_ctx(self, workdir: str, host_socket_path: str) -> _LaunchCtx:
        """Map host paths to guest-visible paths (identity for same-fs backends)."""
        return _LaunchCtx(
            workdir=workdir,
            host_socket_path=host_socket_path,
            bootstrap_guest=os.path.join(workdir, "guest_bootstrap.py"),
            job_guest=os.path.join(workdir, "job.json"),
            socket_guest=host_socket_path,
        )

    def _apply_rlimits_in_preexec(self) -> bool:
        """Whether to set RLIMITs via preexec_fn (skip when the sandbox does it)."""
        return _POSIX

    def _extra_env(self) -> dict[str, str]:
        """Extra environment variables for the guest (merged over os.environ)."""
        return {}

    # -- main flow --------------------------------------------------------- #

    async def run_guest(self, job: GuestJob, *, host_socket_path: str) -> GuestOutcome:
        workdir = tempfile.mkdtemp(prefix="ctiso-")
        os.chmod(workdir, 0o700)
        try:
            shutil.copy2(_BOOTSTRAP_SRC, os.path.join(workdir, "guest_bootstrap.py"))
            shutil.copy2(_WIRE_SRC, os.path.join(workdir, "_wire.py"))
            ctx = self._guest_ctx(workdir, host_socket_path)

            import json

            payload = job.payload(socket_path=ctx.socket_guest)
            with open(os.path.join(workdir, "job.json"), "w", encoding="utf-8") as fh:
                json.dump(payload, fh)

            argv = [
                *self._wrapper_argv(ctx, job),
                self._python_exe(),
                ctx.bootstrap_guest,
                ctx.job_guest,
            ]
            logger.debug("[%s] launching guest: %s", self.name, " ".join(argv))
            return await self._spawn(argv, job)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    async def _spawn(self, argv: list[str], job: GuestJob) -> GuestOutcome:
        preexec = self._make_preexec(job) if self._apply_rlimits_in_preexec() else None
        extra_env = self._extra_env()
        env = {**os.environ, **extra_env} if extra_env else None
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=preexec,  # noqa: PLW1509 - intentional per-child limits (POSIX)
            start_new_session=_POSIX,
            env=env,
        )
        timed_out = False
        stdout: bytes = b""
        stderr: bytes = b""
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=job.limits.wall_timeout)
        except TimeoutError:
            timed_out = True
            self._kill(proc)
            with contextlib.suppress(Exception):
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=5.0)

        cap = job.limits.max_output_bytes
        return GuestOutcome(
            exit_code=proc.returncode,
            stdout=_decode(stdout, cap),
            stderr=_decode(stderr, cap),
            timed_out=timed_out,
        )

    def _kill(self, proc: asyncio.subprocess.Process) -> None:
        with contextlib.suppress(ProcessLookupError, Exception):
            if _POSIX:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            else:
                proc.kill()

    def _make_preexec(self, job: GuestJob) -> Callable[[], None] | None:
        if not _POSIX:
            return None
        import resource

        limits = job.limits

        def _preexec() -> None:  # pragma: no cover - runs in the child
            # NB: no os.setsid() here — start_new_session=True already makes the
            # child a session leader; a second setsid would raise.
            if limits.cpu_timeout:
                _try_rlimit(resource.RLIMIT_CPU, int(limits.cpu_timeout) + 1)
            if limits.memory_bytes and hasattr(resource, "RLIMIT_AS"):
                _try_rlimit(resource.RLIMIT_AS, int(limits.memory_bytes))
            if limits.max_processes and hasattr(resource, "RLIMIT_NPROC"):
                _try_rlimit(resource.RLIMIT_NPROC, int(limits.max_processes))

        return _preexec


def _try_rlimit(res: int, value: int) -> None:  # pragma: no cover - child process
    import resource

    try:
        _soft, hard = resource.getrlimit(res)
        cap = value if hard == resource.RLIM_INFINITY else min(value, hard)
        resource.setrlimit(res, (cap, hard))
    except (ValueError, OSError):
        pass


def _decode(data: bytes | None, cap: int) -> str:
    if not data:
        return ""
    if len(data) > cap:
        data = data[:cap] + b"\n...[truncated]"
    return data.decode("utf-8", errors="replace")
