# chuk_tool_processor/execution/isolation/backends/docker.py
"""
Docker backend — runs the guest in a throwaway container.

Each run launches one ``docker run --rm`` container with no network, a read-only
root, dropped capabilities, and memory/pids limits. The broker unix socket is
bind-mounted into the container so the guest can still call host tools; nothing
else crosses the boundary. Works anywhere a Docker/Podman-compatible ``docker``
CLI reaches a daemon; the container only needs a stock ``python`` image because
all tool execution happens back on the host.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.backend import GuestJob, GuestOutcome
from chuk_tool_processor.execution.isolation.backends._subprocess import SubprocessBackend, _LaunchCtx

_GUEST_MOUNT = "/ctguest"
_SOCK_MOUNT = "/ctsock"


class DockerBackend(SubprocessBackend):
    """Container-isolated guest via the ``docker`` CLI."""

    name = "docker"
    provides_isolation = True

    def __init__(self, image: str = "python:3.12-slim", *, cpus: float = 1.0, docker_bin: str = "docker") -> None:
        self.image = image
        self.cpus = cpus
        self.docker_bin = docker_bin

    def is_available(self) -> bool:
        return shutil.which(self.docker_bin) is not None

    # Container manages limits/interpreter; no host-side preexec or env.
    def _python_exe(self) -> str:
        return "python"

    def _apply_rlimits_in_preexec(self) -> bool:
        return False

    def _guest_ctx(self, workdir: str, host_endpoint: str) -> _LaunchCtx:
        socket_name = os.path.basename(host_endpoint)
        return _LaunchCtx(
            workdir=workdir,
            host_endpoint=host_endpoint,
            bootstrap_guest=f"{_GUEST_MOUNT}/guest_bootstrap.py",
            job_guest=f"{_GUEST_MOUNT}/job.json",
            endpoint_guest=f"{_SOCK_MOUNT}/{socket_name}",
        )

    def _container_name(self, job: GuestJob) -> str:
        return f"ctiso-{job.token[:24]}"

    def _wrapper_argv(self, ctx: _LaunchCtx, job: GuestJob) -> list[str]:
        socket_dir = os.path.dirname(ctx.host_endpoint)
        lim = job.limits
        argv = [
            self.docker_bin,
            "run",
            "--rm",
            "-i",
            # Image is pre-pulled in run_guest; never pull during the sandboxed
            # run (a --network none run can't reach a registry anyway).
            "--pull",
            "never",
            "--name",
            self._container_name(job),
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--cpus",
            str(self.cpus),
            # Run as the host uid so the container (with no CAP_DAC_OVERRIDE) can
            # read the 0700 staging dir and reach the broker socket dir, and so
            # the guest runs unprivileged. POSIX host only.
            *(["--user", f"{os.getuid()}:{os.getgid()}"] if hasattr(os, "getuid") else []),
            "-e",
            "PYTHONDONTWRITEBYTECODE=1",
        ]
        if not lim.allow_network:
            argv += ["--network", "none"]
        if lim.memory_bytes:
            argv += ["--memory", str(lim.memory_bytes), "--memory-swap", str(lim.memory_bytes)]
        if lim.max_processes:
            argv += ["--pids-limit", str(lim.max_processes)]
        argv += [
            "-v",
            f"{ctx.workdir}:{_GUEST_MOUNT}:ro",
            "-v",
            f"{socket_dir}:{_SOCK_MOUNT}",
            self.image,
        ]
        return argv

    async def run_guest(
        self, job: GuestJob, *, host_endpoint: str, transport: str = _wire.TRANSPORT_UNIX
    ) -> GuestOutcome:
        # Acquire the image up front (with the daemon's network) so the sandboxed
        # `docker run --network none --pull never` never has to reach a registry.
        pull_error = await self._ensure_image()
        if pull_error:
            return GuestOutcome(exit_code=1, stderr=pull_error, timed_out=False)
        # Killing the docker client on timeout does not stop the container, so
        # force-remove by name in a finally (name is derived from the unique
        # per-run token, making this concurrency-safe).
        try:
            return await super().run_guest(job, host_endpoint=host_endpoint, transport=transport)
        finally:
            await self._force_remove(self._container_name(job))

    async def _ensure_image(self) -> str | None:
        """Ensure ``self.image`` is present locally; pull it if not. Returns an error string on failure."""
        inspect = await asyncio.create_subprocess_exec(
            self.docker_bin,
            "image",
            "inspect",
            self.image,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if await inspect.wait() == 0:
            return None
        pull = await asyncio.create_subprocess_exec(
            self.docker_bin,
            "pull",
            self.image,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _out, err = await pull.communicate()
        if pull.returncode != 0:
            detail = (err or b"").decode(errors="replace").strip()[:400]
            return f"failed to pull image {self.image!r}: {detail}"
        return None

    async def _force_remove(self, name: str) -> None:
        with contextlib.suppress(Exception):
            proc = await asyncio.create_subprocess_exec(
                self.docker_bin,
                "rm",
                "-f",
                name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=10.0)
