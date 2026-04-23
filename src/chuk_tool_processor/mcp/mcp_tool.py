#!/usr/bin/env python
# chuk_tool_processor/mcp/mcp_tool.py
"""
MCP tool shim that delegates execution to a StreamManager.

FIXED: Removed config file management - MCPTool should only handle execution,
not configuration or bootstrapping. Configuration is handled at registration time.

CORE PRINCIPLE: MCPTool wraps a StreamManager and delegates calls to it.
If the StreamManager becomes unavailable, return graceful errors rather than
trying to recreate it with config files.

HEALTH MONITORING FIX: Updated health checking to be more lenient and trust
the underlying transport's health monitoring instead of doing aggressive
ping tests that create false negatives.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.stream_manager import StreamManager

logger = get_logger("chuk_tool_processor.mcp.mcp_tool")


class ConnectionState(Enum):
    """Connection states for the MCP tool."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"
    FAILED = "failed"


@dataclass
class RecoveryConfig:
    """Configuration for connection recovery behavior."""

    max_retries: int = 3
    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    backoff_multiplier: float = 2.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0


@dataclass
class ConnectionStats:
    """Statistics for connection monitoring."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    connection_errors: int = 0
    last_success_time: float | None = None
    last_failure_time: float | None = None


class MCPTool:
    """
    Wrap a remote MCP tool so it can be called like a local tool.

    SIMPLIFIED: This class now focuses only on execution delegation.
    It does NOT handle configuration files or StreamManager bootstrapping.

    FIXED: Health monitoring is now more lenient and trusts the underlying
    transport's health reporting instead of doing aggressive health checks.
    """

    def __init__(
        self,
        tool_name: str = "",
        stream_manager: StreamManager | None = None,
        *,
        default_timeout: float | None = None,
        enable_resilience: bool = True,
        recovery_config: RecoveryConfig | None = None,
    ) -> None:
        if not tool_name:
            raise ValueError("MCPTool requires a tool_name")

        self.tool_name = tool_name
        self._sm: StreamManager | None = stream_manager
        self.default_timeout = default_timeout or 30.0

        # Resilience features
        self.enable_resilience = enable_resilience
        self.recovery_config = recovery_config or RecoveryConfig()

        # State tracking (only if resilience enabled)
        if self.enable_resilience:
            self.connection_state = ConnectionState.HEALTHY if stream_manager else ConnectionState.DISCONNECTED
            self.stats = ConnectionStats()

            # Circuit breaker state
            self._circuit_open = False
            self._circuit_open_time: float | None = None
            self._consecutive_failures = 0

    # ------------------------------------------------------------------ #
    # Serialization support for subprocess execution
    # ------------------------------------------------------------------ #
    def __getstate__(self) -> dict[str, Any]:
        """
        Serialize for subprocess execution.

        SIMPLIFIED: Only preserve essential execution state, not configuration.
        The StreamManager will be None after deserialization - that's expected.
        """
        state = self.__dict__.copy()

        # Remove non-serializable items
        state["_sm"] = None  # StreamManager will be None in subprocess

        # Reset connection state for subprocess
        if self.enable_resilience:
            state["connection_state"] = ConnectionState.DISCONNECTED

        logger.debug(f"Serializing MCPTool '{self.tool_name}' for subprocess")
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        Deserialize after subprocess execution.

        SIMPLIFIED: Just restore state. StreamManager will be None and that's fine.
        """
        self.__dict__.update(state)

        # Ensure critical fields exist
        if not hasattr(self, "tool_name") or not self.tool_name:
            raise ValueError("Invalid MCPTool state: missing tool_name")

        # StreamManager will be None in subprocess - that's expected
        self._sm = None

        # Initialize resilience state if enabled
        if self.enable_resilience:
            if not hasattr(self, "connection_state"):
                self.connection_state = ConnectionState.DISCONNECTED
            if not hasattr(self, "stats"):
                self.stats = ConnectionStats()

        logger.debug(f"Deserialized MCPTool '{self.tool_name}' in subprocess")

    # ------------------------------------------------------------------ #
    async def execute(self, timeout: float | None = None, **kwargs: Any) -> Any:
        """
        Execute the tool, returning graceful errors if StreamManager unavailable.

        SIMPLIFIED: If no StreamManager, return a structured error response
        instead of trying to bootstrap one.
        """
        # Check if we have a StreamManager
        if self._sm is None:
            return {
                "error": f"Tool '{self.tool_name}' is not available (no stream manager)",
                "tool_name": self.tool_name,
                "available": False,
                "reason": "disconnected",
            }

        # If resilience is disabled, use simple execution
        if not self.enable_resilience:
            return await self._simple_execute(timeout, **kwargs)

        # Resilient execution
        return await self._resilient_execute(timeout, **kwargs)

    async def _simple_execute(self, timeout: float | None = None, **kwargs: Any) -> Any:
        """Simple execution without resilience features."""
        effective_timeout = timeout if timeout is not None else self.default_timeout

        call_kwargs: dict[str, Any] = {
            "tool_name": self.tool_name,
            "arguments": kwargs,
        }
        if effective_timeout is not None:
            call_kwargs["timeout"] = effective_timeout

        try:
            result = await self._sm.call_tool(**call_kwargs)  # type: ignore[union-attr]
        except TimeoutError:
            logger.warning(f"MCP tool '{self.tool_name}' timed out after {effective_timeout}s")
            raise

        if result.get("isError"):
            err = result.get("error", "Unknown error")
            logger.error(f"Remote MCP error from '{self.tool_name}': {err}")
            raise RuntimeError(err)

        return result.get("content")

    async def _resilient_execute(self, timeout: float | None = None, **kwargs: Any) -> Any:
        """Resilient execution with circuit breaker and health checks."""
        # Check circuit breaker
        if self._is_circuit_open():
            return {
                "error": f"Circuit breaker open for tool '{self.tool_name}' - too many recent failures",
                "tool_name": self.tool_name,
                "available": False,
                "reason": "circuit_breaker",
            }

        effective_timeout = timeout if timeout is not None else self.default_timeout
        self.stats.total_calls += 1

        # FIXED: More lenient health check - trust the transport layer
        if not await self._is_stream_manager_healthy():
            await self._record_failure(is_connection_error=True)
            return {
                "error": f"Tool '{self.tool_name}' is not available (unhealthy connection)",
                "tool_name": self.tool_name,
                "available": False,
                "reason": "unhealthy",
            }

        # Try execution with retries
        max_attempts = self.recovery_config.max_retries + 1
        backoff = self.recovery_config.initial_backoff

        for attempt in range(max_attempts):
            try:
                result = await self._execute_with_timeout(effective_timeout, **kwargs)
                await self._record_success()
                return result

            except TimeoutError:
                error_msg = f"Tool '{self.tool_name}' timed out after {effective_timeout}s"
                logger.warning(error_msg)
                await self._record_failure()

                if attempt == max_attempts - 1:
                    return {
                        "error": error_msg,
                        "tool_name": self.tool_name,
                        "available": False,
                        "reason": "timeout",
                    }

            except Exception as e:
                error_str = str(e)
                is_connection_error = self._is_connection_error(e)

                logger.warning(f"Tool '{self.tool_name}' attempt {attempt + 1} failed: {error_str}")
                await self._record_failure(is_connection_error)

                if attempt == max_attempts - 1:
                    return {
                        "error": f"Tool execution failed after {max_attempts} attempts: {error_str}",
                        "tool_name": self.tool_name,
                        "available": False,
                        "reason": "execution_failed",
                    }

                # Exponential backoff
                if attempt < max_attempts - 1:
                    logger.debug(f"Waiting {backoff:.1f}s before retry {attempt + 2}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * self.recovery_config.backoff_multiplier, self.recovery_config.max_backoff)

        # Should never reach here, but return error if we do
        return {
            "error": f"Tool '{self.tool_name}' failed after all attempts",
            "tool_name": self.tool_name,
            "available": False,
            "reason": "execution_failed",
        }

    async def _execute_with_timeout(self, timeout: float, **kwargs: Any) -> Any:
        """Execute the tool with timeout."""
        call_kwargs: dict[str, Any] = {
            "tool_name": self.tool_name,
            "arguments": kwargs,
        }
        if timeout is not None:
            call_kwargs["timeout"] = timeout

        try:
            result = await asyncio.wait_for(
                self._sm.call_tool(**call_kwargs),  # type: ignore[union-attr]
                timeout=(timeout + 5.0) if timeout else None,
            )

            if result.get("isError"):
                error = result.get("error", "Unknown error")
                raise RuntimeError(f"Tool execution failed: {error}")

            return result.get("content")

        except TimeoutError:
            self.connection_state = ConnectionState.DEGRADED
            raise
        except Exception as e:
            if self._is_connection_error(e):
                self.connection_state = ConnectionState.DISCONNECTED
            else:
                self.connection_state = ConnectionState.DEGRADED
            raise

    async def _is_stream_manager_healthy(self) -> bool:
        """
        FIXED: Much more lenient health check.

        The diagnostic proves SSE transport is healthy, so we should trust it
        instead of doing aggressive health checking that creates false negatives.

        This replaces the previous ping_servers() call which was too aggressive
        and caused "unhealthy connection" false positives.
        """
        if self._sm is None:
            return False

        # FIXED: Simple check - if we have a StreamManager, assume it's available
        # The underlying SSE transport now has proper health monitoring
        try:
            # Just check if the StreamManager has basic functionality
            if hasattr(self._sm, "transports") and self._sm.transports:
                logger.debug(
                    f"StreamManager healthy for '{self.tool_name}' - has {len(self._sm.transports)} transports"
                )
                return True

            # Fallback - check server_info synchronously (get_server_info is not a coroutine)
            server_info: list[dict[str, Any]] = self._sm.get_server_info()
            healthy = len(server_info) > 0
            logger.debug(f"StreamManager health for '{self.tool_name}': {healthy} (via server_info)")
            return healthy

        except TimeoutError:
            logger.debug(f"StreamManager health check timed out for '{self.tool_name}' - assuming healthy")
            # FIXED: Timeout doesn't mean unavailable, just slow
            return True
        except Exception as e:
            logger.debug(f"StreamManager health check failed for '{self.tool_name}': {e}")
            # FIXED: Most exceptions don't mean the StreamManager is unavailable
            # The transport layer handles real connectivity issues
            return True

    def _is_connection_error(self, exception: Exception) -> bool:
        """Determine if an exception indicates a connection problem."""
        error_str = str(exception).lower()
        connection_indicators = [
            "connection lost",
            "connection closed",
            "connection refused",
            "broken pipe",
            "timeout",
            "eof",
            "pipe closed",
            "process died",
            "no route to host",
            "no server found",
            "transport not initialized",
            "stream manager unavailable",
        ]
        return any(indicator in error_str for indicator in connection_indicators)

    async def _record_success(self) -> None:
        """Record a successful execution."""
        self.stats.successful_calls += 1
        self.stats.last_success_time = time.time()
        self._consecutive_failures = 0

        # Close circuit breaker if it was open
        if self._circuit_open:
            self._circuit_open = False
            self._circuit_open_time = None
            self.connection_state = ConnectionState.HEALTHY
            logger.debug(f"Circuit breaker closed for tool '{self.tool_name}' after successful execution")

    async def _record_failure(self, is_connection_error: bool = False) -> None:
        """Record a failed execution."""
        self.stats.failed_calls += 1
        self.stats.last_failure_time = time.time()

        if is_connection_error:
            self.stats.connection_errors += 1
            self.connection_state = ConnectionState.DISCONNECTED
        else:
            self.connection_state = ConnectionState.DEGRADED

        self._consecutive_failures += 1

        # Check if we should open the circuit breaker
        if self._consecutive_failures >= self.recovery_config.circuit_breaker_threshold and not self._circuit_open:
            self._circuit_open = True
            self._circuit_open_time = time.monotonic()
            self.connection_state = ConnectionState.FAILED
            logger.error(
                f"Circuit breaker opened for tool '{self.tool_name}' after {self._consecutive_failures} consecutive failures"
            )

    def _is_circuit_open(self) -> bool:
        """Check if the circuit breaker is currently open."""
        if not self._circuit_open:
            return False

        # Check if enough time has passed to close the circuit
        if (
            self._circuit_open_time
            and time.monotonic() - self._circuit_open_time >= self.recovery_config.circuit_breaker_timeout
        ):
            self._circuit_open = False
            self._circuit_open_time = None
            self.connection_state = ConnectionState.HEALTHY
            logger.debug(f"Circuit breaker reset for tool '{self.tool_name}' after timeout")
            return False

        return True

    # ------------------------------------------------------------------ #
    # Legacy method name support
    async def _aexecute(self, timeout: float | None = None, **kwargs: Any) -> Any:
        """Legacy alias for execute() method."""
        return await self.execute(timeout=timeout, **kwargs)

    # ------------------------------------------------------------------ #
    # Utility and monitoring methods
    # ------------------------------------------------------------------ #
    def is_available(self) -> bool:
        """Check if this tool is currently available."""
        return (
            self._sm is not None
            and not self._is_circuit_open()
            and self.connection_state in [ConnectionState.HEALTHY, ConnectionState.DEGRADED]
        )

    def get_stats(self) -> dict[str, Any]:
        """Get connection and execution statistics."""
        if not self.enable_resilience:
            return {"tool_name": self.tool_name, "resilience_enabled": False, "available": self._sm is not None}

        success_rate = 0.0
        if self.stats.total_calls > 0:
            success_rate = (self.stats.successful_calls / self.stats.total_calls) * 100

        return {
            "tool_name": self.tool_name,
            "resilience_enabled": True,
            "available": self.is_available(),
            "state": self.connection_state.value,
            "circuit_open": self._circuit_open,
            "total_calls": self.stats.total_calls,
            "successful_calls": self.stats.successful_calls,
            "failed_calls": self.stats.failed_calls,
            "connection_errors": self.stats.connection_errors,
            "success_rate": success_rate,
            "consecutive_failures": self._consecutive_failures,
            "has_stream_manager": self._sm is not None,
        }

    def reset_circuit_breaker(self) -> None:
        """Manually reset the circuit breaker."""
        if not self.enable_resilience:
            return

        self._circuit_open = False
        self._circuit_open_time = None
        self._consecutive_failures = 0
        self.connection_state = ConnectionState.HEALTHY
        logger.debug(f"Circuit breaker manually reset for tool '{self.tool_name}'")

    def disable_resilience(self) -> None:
        """Disable resilience features for this tool instance."""
        self.enable_resilience = False
        logger.debug(f"Resilience features disabled for tool '{self.tool_name}'")

    def set_stream_manager(self, stream_manager: StreamManager | None) -> None:
        """
        Set or update the StreamManager for this tool.

        This can be used by external systems to reconnect tools after
        StreamManager recovery at a higher level.
        """
        self._sm = stream_manager
        if stream_manager is not None:
            self.connection_state = ConnectionState.HEALTHY
            if self._circuit_open:
                self._circuit_open = False
                self._circuit_open_time = None
                logger.debug(f"Circuit breaker closed for tool '{self.tool_name}' due to new stream manager")
        else:
            self.connection_state = ConnectionState.DISCONNECTED

        logger.debug(f"StreamManager {'set' if stream_manager else 'cleared'} for tool '{self.tool_name}'")
