# chuk_tool_processor/execution/isolation/__init__.py
"""
Isolated code execution — run untrusted/LLM-generated code behind a real
OS/runtime boundary, with tool access brokered back to the trusted host.

This is the safe counterpart to
:class:`~chuk_tool_processor.execution.code_sandbox.CodeSandbox`, which runs code
in-process with no isolation and is trusted-code-only. See ``docs/security.md``.
"""

from chuk_tool_processor.execution.isolation.backend import (
    BackendUnavailableError,
    GuestJob,
    GuestOutcome,
    IsolationBackend,
    IsolationError,
)
from chuk_tool_processor.execution.isolation.backends import LocalProcessBackend, SeatbeltBackend
from chuk_tool_processor.execution.isolation.limits import IsolationLimits
from chuk_tool_processor.execution.isolation.result import IsolatedResult
from chuk_tool_processor.execution.isolation.runner import IsolatedCodeRunner

__all__ = [
    # Runner + data types
    "IsolatedCodeRunner",
    "IsolationLimits",
    "IsolatedResult",
    # Backend protocol + errors
    "IsolationBackend",
    "IsolationError",
    "BackendUnavailableError",
    "GuestJob",
    "GuestOutcome",
    # Backends
    "LocalProcessBackend",
    "SeatbeltBackend",
]
