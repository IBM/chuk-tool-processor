# chuk_tool_processor/execution/isolation/limits.py
"""
Resource limits applied to isolated code execution.

These are enforced by the isolation backend (OS rlimits, container flags, or a
WASM runtime's fuel/epoch limits) and by the host-side tool broker (tool-call
count, output size). A backend applies as many of these as its mechanism allows;
see each backend for which limits it can enforce.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# Sensible defaults for running a short orchestration snippet.
DEFAULT_WALL_TIMEOUT = 30.0
DEFAULT_CPU_TIMEOUT = 15.0
DEFAULT_MEMORY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_OUTPUT_BYTES = 64 * 1024
DEFAULT_MAX_TOOL_CALLS = 100
DEFAULT_MAX_PROCESSES = 64


class IsolationLimits(BaseModel):
    """
    Resource ceilings for a single isolated execution.

    Frozen and validated: field constraints reject non-positive limits at
    construction (``pydantic.ValidationError`` is a ``ValueError``), and unknown
    fields are refused so a typo in a security limit fails loudly rather than
    being silently ignored.

    Attributes:
        wall_timeout: Hard wall-clock ceiling in seconds. The backend kills the
            guest when it is exceeded. Always enforced.
        cpu_timeout: CPU-seconds ceiling (RLIMIT_CPU / container/runtime limit).
            ``None`` disables it. Guards against busy loops that don't trip the
            wall clock (e.g. while sleeping).
        memory_bytes: Address-space / memory ceiling in bytes. ``None`` disables.
        max_output_bytes: Maximum captured stdout/stderr bytes kept from the
            guest. Output beyond this is truncated.
        max_tool_calls: Maximum number of tool calls the guest may make through
            the broker before further calls are rejected.
        max_processes: Maximum number of processes/threads the guest may spawn
            (RLIMIT_NPROC / container ``--pids-limit``). ``None`` disables.
        allow_network: If ``False`` (default) the backend denies the guest all
            network access except the single tool-broker channel. Set ``True``
            only when the isolated code legitimately needs outbound network.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    wall_timeout: float = Field(default=DEFAULT_WALL_TIMEOUT, gt=0)
    cpu_timeout: float | None = Field(default=DEFAULT_CPU_TIMEOUT, gt=0)
    memory_bytes: int | None = Field(default=DEFAULT_MEMORY_BYTES, gt=0)
    max_output_bytes: int = Field(default=DEFAULT_MAX_OUTPUT_BYTES, gt=0)
    max_tool_calls: int = Field(default=DEFAULT_MAX_TOOL_CALLS, ge=0)
    max_processes: int | None = Field(default=DEFAULT_MAX_PROCESSES, gt=0)
    allow_network: bool = False
