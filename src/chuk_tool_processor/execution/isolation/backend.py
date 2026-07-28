# chuk_tool_processor/execution/isolation/backend.py
"""
Isolation backend protocol and the value types passed across it.

A backend's single job: take a :class:`GuestJob` (untrusted code + limits +
the address of the host tool-broker socket), run it inside whatever isolation
mechanism the backend implements, and return a :class:`GuestOutcome`. Backends
never touch the tool registry — all tool access flows back to the host broker
over the broker channel.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from chuk_tool_processor.execution.isolation.limits import IsolationLimits


class IsolationError(Exception):
    """Base class for isolation backend failures."""


class BackendUnavailableError(IsolationError):
    """Raised when a backend is selected but its runtime is not available."""


@dataclass
class GuestJob:
    """Everything the guest needs to run one snippet."""

    code: str
    token: str
    limits: IsolationLimits
    namespace: str | None = None
    initial_vars: dict[str, Any] = field(default_factory=dict)

    def payload(self, *, endpoint: str, transport: str) -> dict[str, Any]:
        """
        Build the JSON job payload handed to the guest bootstrap.

        Args:
            endpoint: The broker endpoint *as seen from inside the guest* — a unix
                socket path (``transport="unix"``) or a named pipe name
                (``transport="pipe"``). A backend that remaps paths (e.g. a
                container bind mount) passes the guest-side value here.
            transport: ``"unix"`` or ``"pipe"``; tells the guest how to connect.
        """
        lim = self.limits
        return {
            "code": self.code,
            "namespace": self.namespace,
            "token": self.token,
            "endpoint": endpoint,
            "transport": transport,
            "initial_vars": self.initial_vars,
            "limits": {
                "cpu_timeout": lim.cpu_timeout,
                "memory_bytes": lim.memory_bytes,
                "max_processes": lim.max_processes,
                "max_output_bytes": lim.max_output_bytes,
            },
        }


@dataclass
class GuestOutcome:
    """Raw result of the guest process, before combining with broker state."""

    exit_code: int | None
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False


@runtime_checkable
class IsolationBackend(Protocol):
    """
    A mechanism for running untrusted code away from the host process.

    Attributes:
        name: Stable short identifier (e.g. "docker", "seatbelt").
        provides_isolation: True for backends that impose a real OS/runtime
            boundary. False only for development/reference launchers that run
            code with no containment — the runner refuses those unless the
            caller explicitly opts in.
    """

    name: str
    provides_isolation: bool

    def is_available(self) -> bool:
        """Return True if this backend's runtime is present and usable now."""
        ...

    async def run_guest(self, job: GuestJob, *, host_endpoint: str, transport: str) -> GuestOutcome:
        """
        Run ``job`` inside the isolation boundary.

        Args:
            job: The code, limits, and broker token to run.
            host_endpoint: The broker endpoint on the host (unix socket path or
                named pipe name). The backend makes it reachable from inside the
                guest (bind mount, shared namespace, pipe ACL, etc.) and tells the
                guest via ``job.payload(endpoint=..., transport=...)``.
            transport: ``"unix"`` or ``"pipe"`` — the broker's transport kind.
        """
        ...
