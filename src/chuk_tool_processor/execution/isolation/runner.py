# chuk_tool_processor/execution/isolation/runner.py
"""
IsolatedCodeRunner — run untrusted/LLM-generated code behind a real boundary.

Unlike :class:`~chuk_tool_processor.execution.code_sandbox.CodeSandbox` (which
runs code in-process with no isolation and is trusted-code-only), this runner
executes code inside an isolation backend (container, macOS Seatbelt, Linux
bubblewrap, WASM, ...). The code reaches registered tools only through the
host-side broker, over a single audited channel; everything else — network,
filesystem, host process — is denied by the backend.
"""

from __future__ import annotations

import secrets
import time
from typing import Any

from chuk_tool_processor.execution.isolation import _wire
from chuk_tool_processor.execution.isolation.backend import (
    BackendUnavailableError,
    GuestJob,
    GuestOutcome,
    IsolationBackend,
    IsolationError,
)
from chuk_tool_processor.execution.isolation.broker import ToolBroker
from chuk_tool_processor.execution.isolation.limits import IsolationLimits
from chuk_tool_processor.execution.isolation.result import IsolatedResult
from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.registry import get_default_registry
from chuk_tool_processor.registry.interface import ToolRegistryInterface

logger = get_logger("chuk_tool_processor.execution.isolation.runner")


class IsolatedCodeRunner:
    """Runs code inside an isolation backend with brokered tool access."""

    def __init__(
        self,
        backend: IsolationBackend,
        *,
        registry: ToolRegistryInterface | None = None,
        limits: IsolationLimits | None = None,
        namespace: str | None = None,
        allowed_tools: set[str] | None = None,
        allow_no_isolation: bool = False,
    ) -> None:
        """
        Args:
            backend: The isolation backend to run code in.
            registry: Tool registry (default: the global registry).
            limits: Resource ceilings (default: :class:`IsolationLimits`).
            namespace: Restrict brokered tools to this namespace.
            allowed_tools: If set, only these tool names may be called.
            allow_no_isolation: Required to use a backend whose
                ``provides_isolation`` is False (e.g. the local dev launcher).
                Refuses otherwise, so a non-isolating backend cannot be used to
                run untrusted code by accident.
        """
        if not backend.provides_isolation and not allow_no_isolation:
            raise IsolationError(
                f"backend '{backend.name}' provides no isolation boundary; it must not be used "
                "for untrusted code. Pass allow_no_isolation=True only for trusted code/testing."
            )
        self.backend = backend
        self.registry = registry
        self.limits = limits or IsolationLimits()
        self.namespace = namespace
        self.allowed_tools = allowed_tools

    async def run(
        self,
        code: str,
        *,
        namespace: str | None = None,
        initial_vars: dict[str, Any] | None = None,
    ) -> IsolatedResult:
        """Execute ``code`` in the backend and return an :class:`IsolatedResult`."""
        if not self.backend.is_available():
            raise BackendUnavailableError(
                f"isolation backend '{self.backend.name}' is not available in this environment"
            )

        registry = self.registry or await get_default_registry()
        ns = namespace if namespace is not None else self.namespace
        token = secrets.token_hex(16)

        broker = ToolBroker(
            registry,
            token=token,
            limits=self.limits,
            namespace=ns,
            allowed_tools=self.allowed_tools,
        )
        endpoint = await broker.start()
        try:
            job = GuestJob(
                code=code,
                token=token,
                limits=self.limits,
                namespace=ns,
                initial_vars=initial_vars or {},
            )
            started = time.monotonic()
            outcome = await self.backend.run_guest(
                job, host_endpoint=endpoint, transport=broker.transport or _wire.TRANSPORT_UNIX
            )
            duration = time.monotonic() - started
            return self._assemble(outcome, broker, duration)
        finally:
            await broker.aclose()

    def _assemble(self, outcome: GuestOutcome, broker: ToolBroker, duration: float) -> IsolatedResult:
        common: dict[str, Any] = {
            "stdout": outcome.stdout,
            "stderr": outcome.stderr,
            "tool_calls": broker.tool_calls,
            "duration": duration,
            "backend": self.backend.name,
            "timed_out": outcome.timed_out,
        }
        if broker.has_result():
            return IsolatedResult(ok=True, value=broker.result(), **common)

        if outcome.timed_out:
            return IsolatedResult(
                ok=False,
                error=f"execution timed out after {self.limits.wall_timeout}s",
                error_type="timeout",
                **common,
            )
        detail = outcome.stderr.strip()
        if outcome.exit_code not in (0, None):
            error = detail or f"guest exited with code {outcome.exit_code} without returning a result"
            error_type = "guest_exception"
        else:
            error = detail or "guest ended without returning a result"
            error_type = "protocol_error"
        return IsolatedResult(ok=False, error=error, error_type=error_type, **common)
