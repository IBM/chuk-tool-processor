# chuk_tool_processor/execution/isolation/limits.py
"""
Resource limits applied to isolated code execution.

These are enforced by the isolation backend (OS rlimits, container flags, or a
WASM runtime's fuel/epoch limits) and by the host-side tool broker (tool-call
count, output size). A backend applies as many of these as its mechanism allows;
see each backend for which limits it can enforce.
"""

from __future__ import annotations

from dataclasses import dataclass

# Sensible defaults for running a short orchestration snippet.
DEFAULT_WALL_TIMEOUT = 30.0
DEFAULT_CPU_TIMEOUT = 15.0
DEFAULT_MEMORY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_OUTPUT_BYTES = 64 * 1024
DEFAULT_MAX_TOOL_CALLS = 100
DEFAULT_MAX_PROCESSES = 64


@dataclass(frozen=True)
class IsolationLimits:
    """
    Resource ceilings for a single isolated execution.

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

    wall_timeout: float = DEFAULT_WALL_TIMEOUT
    cpu_timeout: float | None = DEFAULT_CPU_TIMEOUT
    memory_bytes: int | None = DEFAULT_MEMORY_BYTES
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES
    max_tool_calls: int = DEFAULT_MAX_TOOL_CALLS
    max_processes: int | None = DEFAULT_MAX_PROCESSES
    allow_network: bool = False

    def __post_init__(self) -> None:
        if self.wall_timeout <= 0:
            raise ValueError("wall_timeout must be positive")
        if self.cpu_timeout is not None and self.cpu_timeout <= 0:
            raise ValueError("cpu_timeout must be positive or None")
        if self.memory_bytes is not None and self.memory_bytes <= 0:
            raise ValueError("memory_bytes must be positive or None")
        if self.max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")
        if self.max_tool_calls < 0:
            raise ValueError("max_tool_calls must be >= 0")
        if self.max_processes is not None and self.max_processes <= 0:
            raise ValueError("max_processes must be positive or None")
