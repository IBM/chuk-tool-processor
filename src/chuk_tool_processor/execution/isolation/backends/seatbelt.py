# chuk_tool_processor/execution/isolation/backends/seatbelt.py
"""
macOS Seatbelt backend — runs the guest under ``sandbox-exec``.

Seatbelt is Apple's kernel sandbox (the mechanism behind App Sandbox). We drive
it with a generated SBPL profile that denies everything by default and then
grants exactly what a short guest run needs:

    * network: inet is denied by default; only the unix broker socket is allowed
    * filesystem writes: denied except the staging dir, the socket dir, and tmp
    * filesystem reads: broadly allowed (CPython/dyld abort hard if a needed
      library read is denied), but with well-known secret directories under
      $HOME explicitly denied as hardening

The reliably strong properties here are **no outbound network** and **no
filesystem writes** outside the sandboxed work/tmp dirs. Read confinement is
best-effort (a denylist, not an allowlist) because a strict read allowlist
breaks the interpreter. ``sandbox-exec`` is deprecated by Apple but still
functional and is the only built-in OS sandbox on macOS.
"""

from __future__ import annotations

import os
import shutil
import sys

from chuk_tool_processor.execution.isolation.backend import GuestJob
from chuk_tool_processor.execution.isolation.backends._subprocess import SubprocessBackend, _LaunchCtx

# Well-known secret locations under $HOME denied to the guest (hardening).
_SECRET_SUBPATHS = (
    ".ssh",
    ".aws",
    ".config/gcloud",
    ".kube",
    ".gnupg",
    ".docker",
    ".netrc",
    ".git-credentials",
    "Library/Keychains",
    "Library/Application Support/com.apple.TCC",
)


def _q(path: str) -> str:
    """Quote a path for an SBPL string literal."""
    return path.replace("\\", "\\\\").replace('"', '\\"')


class SeatbeltBackend(SubprocessBackend):
    """OS-sandboxed guest via macOS ``sandbox-exec``."""

    name = "seatbelt"
    provides_isolation = True

    def is_available(self) -> bool:
        return sys.platform == "darwin" and shutil.which("sandbox-exec") is not None

    def _extra_env(self) -> dict[str, str]:
        # Avoid .pyc writes next to (read-only) stdlib modules.
        return {"PYTHONDONTWRITEBYTECODE": "1"}

    def _profile(self, ctx: _LaunchCtx, job: GuestJob) -> str:  # noqa: ARG002 - uniform hook signature
        socket_dir = os.path.dirname(ctx.socket_guest)
        home = os.path.realpath(os.path.expanduser("~"))
        write_roots = [ctx.workdir, socket_dir, "/private/tmp", "/tmp"]

        lines = [
            "(version 1)",
            "(deny default)",
            "(allow process-fork)",
            "(allow process-exec*)",
            "(allow sysctl-read)",
            "(allow mach-lookup)",
            "(allow signal (target self))",
            # Reads: broad (dyld/CPython abort if a needed lib read is denied)...
            "(allow file-read*)",
            # ...but deny well-known secret dirs under $HOME.
            *[f'(deny file-read* (subpath "{_q(os.path.join(home, s))}"))' for s in _SECRET_SUBPATHS],
            # Writes: only the staging dir, socket dir, tmp, and /dev/null.
            *[f'(allow file-write* (subpath "{_q(os.path.realpath(p))}"))' for p in write_roots],
            '(allow file-write* (literal "/dev/null"))',
            # Network: only the unix broker socket. Inet stays denied by default.
            "(allow network-outbound (remote unix-socket))",
        ]
        return "\n".join(lines) + "\n"

    def _wrapper_argv(self, ctx: _LaunchCtx, job: GuestJob) -> list[str]:
        return ["sandbox-exec", "-p", self._profile(ctx, job)]
