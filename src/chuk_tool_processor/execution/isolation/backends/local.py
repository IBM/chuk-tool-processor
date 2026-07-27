# chuk_tool_processor/execution/isolation/backends/local.py
"""
Local subprocess backend — runs the guest in a plain child process.

.. warning::
    This backend provides **no isolation boundary**. It runs the guest as an
    ordinary child of the host with the same user and privileges; only the
    resource limits (rlimits, timeout) and the broker's tool allowlist apply.
    It exists for development, testing, and as the launcher base the real
    isolation backends build on. ``IsolatedCodeRunner`` refuses to use it unless
    the caller passes ``allow_no_isolation=True``.
"""

from __future__ import annotations

from chuk_tool_processor.execution.isolation.backends._subprocess import SubprocessBackend


class LocalProcessBackend(SubprocessBackend):
    """Non-isolating local child process (dev/testing only)."""

    name = "local"
    provides_isolation = False

    def is_available(self) -> bool:
        return True
