# chuk_tool_processor/execution/isolation/result.py
"""Result of an isolated code execution."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class IsolatedResult(BaseModel):
    """
    Outcome of running code through an :class:`IsolatedCodeRunner`.

    Attributes:
        ok: True if the guest code ran to completion and returned a value.
        value: The return value of the code (JSON round-tripped from the guest).
            This is UNTRUSTED data produced by untrusted code — validate before
            acting on it.
        error: Error message if execution failed (guest exception, timeout,
            limit exceeded, or backend failure).
        error_type: Short classifier — e.g. "guest_exception", "timeout",
            "limit_exceeded", "backend_error", "protocol_error".
        stdout: Captured guest stdout (truncated to ``max_output_bytes``).
        stderr: Captured guest stderr (truncated to ``max_output_bytes``).
        tool_calls: Number of tool calls the guest made through the broker.
        duration: Wall-clock seconds spent running the guest.
        backend: Name of the isolation backend used.
        timed_out: True if the guest was killed for exceeding the wall timeout.
    """

    model_config = ConfigDict(extra="forbid")

    ok: bool
    value: Any = None
    error: str | None = None
    error_type: str | None = None
    stdout: str = ""
    stderr: str = ""
    tool_calls: int = 0
    duration: float = 0.0
    backend: str = ""
    timed_out: bool = False
    meta: dict[str, Any] = Field(default_factory=dict)
