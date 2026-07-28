# chuk_tool_processor/execution/isolation/backends/bubblewrap.py
"""
bubblewrap backend — runs the guest in a Linux namespace sandbox via ``bwrap``.

bubblewrap builds an unprivileged sandbox using user/mount/pid/net namespaces.
We give the guest a read-only view of the system + interpreter, a private
tmpfs, a fresh /proc and /dev, no network (unless explicitly allowed), and
bind-mount only the broker socket for host tool access. The staging dir and the
socket are bound at their original paths, so guest-visible paths match host
paths (the default identity mapping).

Requires the ``bwrap`` binary (package ``bubblewrap``) on Linux.
"""

from __future__ import annotations

import shutil
import sys

from chuk_tool_processor.execution.isolation.backend import GuestJob
from chuk_tool_processor.execution.isolation.backends._subprocess import SubprocessBackend, _LaunchCtx

# System roots the interpreter needs, bind-mounted read-only when present.
_SYSTEM_ROOTS = ("/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc")


class BubblewrapBackend(SubprocessBackend):
    """Linux namespace-isolated guest via ``bwrap``."""

    name = "bubblewrap"
    provides_isolation = True

    def is_available(self) -> bool:
        return sys.platform.startswith("linux") and shutil.which("bwrap") is not None

    def _extra_env(self) -> dict[str, str]:
        return {"PYTHONDONTWRITEBYTECODE": "1"}

    def _wrapper_argv(self, ctx: _LaunchCtx, job: GuestJob) -> list[str]:
        import os

        argv = ["bwrap", "--die-with-parent", "--new-session", "--unshare-user", "--unshare-pid", "--unshare-ipc"]
        if not job.limits.allow_network:
            argv += ["--unshare-net"]

        for root in _SYSTEM_ROOTS:
            if os.path.exists(root):
                argv += ["--ro-bind", root, root]
        # Interpreter prefixes (venv/pyenv installs live outside /usr).
        for prefix in {os.path.realpath(sys.base_prefix), os.path.realpath(sys.prefix)}:
            argv += ["--ro-bind-try", prefix, prefix]

        argv += [
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            # Staging dir read-only; broker socket dir writable for connect().
            "--ro-bind",
            ctx.workdir,
            ctx.workdir,
            "--bind",
            os.path.dirname(ctx.socket_guest),
            os.path.dirname(ctx.socket_guest),
            "--chdir",
            "/",
            "--",
        ]
        return argv
