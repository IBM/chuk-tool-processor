# chuk_tool_processor/exceptions.py
"""
Structured error taxonomy for tool execution.

This module provides machine-readable error codes and structured error types
that enable planners to make intelligent retry and fallback decisions.
"""

from __future__ import annotations

from difflib import get_close_matches
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    pass  # For future type hints if needed


class ErrorCode(StrEnum):
    """Machine-readable error codes for tool processor errors."""

    # Tool registry errors
    TOOL_NOT_FOUND = "TOOL_NOT_FOUND"
    TOOL_REGISTRATION_FAILED = "TOOL_REGISTRATION_FAILED"

    # Execution errors
    TOOL_EXECUTION_FAILED = "TOOL_EXECUTION_FAILED"
    TOOL_TIMEOUT = "TOOL_TIMEOUT"
    TOOL_CANCELLED = "TOOL_CANCELLED"

    # Validation errors
    TOOL_VALIDATION_ERROR = "TOOL_VALIDATION_ERROR"
    TOOL_ARGUMENT_ERROR = "TOOL_ARGUMENT_ERROR"
    TOOL_RESULT_ERROR = "TOOL_RESULT_ERROR"

    # Rate limiting and circuit breaker
    TOOL_RATE_LIMITED = "TOOL_RATE_LIMITED"
    TOOL_CIRCUIT_OPEN = "TOOL_CIRCUIT_OPEN"

    # Bulkhead errors
    BULKHEAD_FULL = "BULKHEAD_FULL"

    # Parser errors
    PARSER_ERROR = "PARSER_ERROR"
    PARSER_INVALID_FORMAT = "PARSER_INVALID_FORMAT"

    # MCP errors
    MCP_CONNECTION_FAILED = "MCP_CONNECTION_FAILED"
    MCP_TRANSPORT_ERROR = "MCP_TRANSPORT_ERROR"
    MCP_SERVER_ERROR = "MCP_SERVER_ERROR"
    MCP_TIMEOUT = "MCP_TIMEOUT"

    # System errors
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"


class ErrorCategory(StrEnum):
    """
    High-level error categories for planner decision-making.

    These categories help planners distinguish between different failure modes
    and make appropriate decisions about retries, fallbacks, and backpressure.

    Categories:
        RATE_LIMIT: Too many requests - slow down and retry after delay
        CIRCUIT_OPEN: Service unhealthy - use fallback or wait for recovery
        BULKHEAD_FULL: Concurrency limit hit - backpressure signal
        TIMEOUT: Operation took too long - may retry with longer timeout
        VALIDATION: Bad input/output - do not retry, fix the request
        NOT_FOUND: Tool doesn't exist - do not retry
        EXECUTION: Tool logic failed - may retry if transient
        CANCELLED: Operation cancelled - do not retry
        CONNECTION: Network/transport error - may retry
        CONFIGURATION: System misconfigured - do not retry
    """

    RATE_LIMIT = "rate_limit"
    CIRCUIT_OPEN = "circuit_open"
    BULKHEAD_FULL = "bulkhead_full"
    TIMEOUT = "timeout"
    VALIDATION = "validation"
    NOT_FOUND = "not_found"
    EXECUTION = "execution"
    CANCELLED = "cancelled"
    CONNECTION = "connection"
    CONFIGURATION = "configuration"


# Mapping from ErrorCode to ErrorCategory for automatic categorization
ERROR_CODE_TO_CATEGORY: dict[ErrorCode, ErrorCategory] = {
    ErrorCode.TOOL_NOT_FOUND: ErrorCategory.NOT_FOUND,
    ErrorCode.TOOL_REGISTRATION_FAILED: ErrorCategory.CONFIGURATION,
    ErrorCode.TOOL_EXECUTION_FAILED: ErrorCategory.EXECUTION,
    ErrorCode.TOOL_TIMEOUT: ErrorCategory.TIMEOUT,
    ErrorCode.TOOL_CANCELLED: ErrorCategory.CANCELLED,
    ErrorCode.TOOL_VALIDATION_ERROR: ErrorCategory.VALIDATION,
    ErrorCode.TOOL_ARGUMENT_ERROR: ErrorCategory.VALIDATION,
    ErrorCode.TOOL_RESULT_ERROR: ErrorCategory.VALIDATION,
    ErrorCode.TOOL_RATE_LIMITED: ErrorCategory.RATE_LIMIT,
    ErrorCode.TOOL_CIRCUIT_OPEN: ErrorCategory.CIRCUIT_OPEN,
    ErrorCode.BULKHEAD_FULL: ErrorCategory.BULKHEAD_FULL,
    ErrorCode.PARSER_ERROR: ErrorCategory.VALIDATION,
    ErrorCode.PARSER_INVALID_FORMAT: ErrorCategory.VALIDATION,
    ErrorCode.MCP_CONNECTION_FAILED: ErrorCategory.CONNECTION,
    ErrorCode.MCP_TRANSPORT_ERROR: ErrorCategory.CONNECTION,
    ErrorCode.MCP_SERVER_ERROR: ErrorCategory.EXECUTION,
    ErrorCode.MCP_TIMEOUT: ErrorCategory.TIMEOUT,
    ErrorCode.RESOURCE_EXHAUSTED: ErrorCategory.BULKHEAD_FULL,
    ErrorCode.CONFIGURATION_ERROR: ErrorCategory.CONFIGURATION,
}


# Categories that are generally retryable (after appropriate delay)
RETRYABLE_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.RATE_LIMIT,
        ErrorCategory.CIRCUIT_OPEN,  # Retryable after reset_timeout
        ErrorCategory.BULKHEAD_FULL,  # Retryable after backpressure clears
        ErrorCategory.TIMEOUT,
        ErrorCategory.EXECUTION,
        ErrorCategory.CONNECTION,
    }
)

# Categories that should never be retried
NON_RETRYABLE_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.VALIDATION,
        ErrorCategory.NOT_FOUND,
        ErrorCategory.CANCELLED,
        ErrorCategory.CONFIGURATION,
    }
)


def is_retryable_category(category: ErrorCategory) -> bool:
    """Check if an error category is generally retryable."""
    return category in RETRYABLE_CATEGORIES


def get_category_for_code(code: ErrorCode) -> ErrorCategory:
    """Get the error category for a given error code."""
    return ERROR_CODE_TO_CATEGORY.get(code, ErrorCategory.EXECUTION)


class ToolProcessorError(Exception):
    """Base exception for all tool processor errors with machine-readable codes."""

    def __init__(
        self,
        message: str,
        code: ErrorCode | None = None,
        details: dict[str, Any] | None = None,
        original_error: Exception | None = None,
        retry_after_ms: int | None = None,
    ):
        super().__init__(message)
        self.code = code or ErrorCode.TOOL_EXECUTION_FAILED
        self.details = details or {}
        self.original_error = original_error
        self._retry_after_ms = retry_after_ms

    @property
    def category(self) -> ErrorCategory:
        """Get the high-level error category for this error."""
        return get_category_for_code(self.code)

    @property
    def retryable(self) -> bool:
        """Check if this error is generally retryable."""
        return is_retryable_category(self.category)

    @property
    def retry_after_ms(self) -> int | None:
        """
        Get the suggested retry delay in milliseconds.

        Returns None if no specific delay is suggested.
        For rate limits and circuit breakers, this provides
        a hint about when to retry.
        """
        return self._retry_after_ms

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to a structured dictionary for logging/monitoring."""
        result = {
            "error": self.__class__.__name__,
            "code": self.code.value,
            "category": self.category.value,
            "message": str(self),
            "retryable": self.retryable,
            "retry_after_ms": self.retry_after_ms,
            "details": self.details,
        }
        if self.original_error:
            result["original_error"] = {
                "type": type(self.original_error).__name__,
                "message": str(self.original_error),
            }
        return result

    def to_error_info(self) -> ErrorInfo:
        """
        Convert this exception to an ErrorInfo for ToolResult.

        This enables structured error information to flow through
        the ToolResult without losing type information.
        """
        return ErrorInfo(
            code=self.code,
            category=self.category,
            message=str(self),
            retryable=self.retryable,
            retry_after_ms=self.retry_after_ms,
            details=self.details,
        )


class ToolNotFoundError(ToolProcessorError):
    """Raised when a requested tool is not found in the registry."""

    def __init__(
        self,
        tool_name: str,
        namespace: str = "default",
        available_tools: list[tuple[str, str]] | list[str] | None = None,
        available_namespaces: list[str] | None = None,
    ):
        self.tool_name = tool_name
        self.namespace = namespace

        # Build helpful error message
        message_parts = [f"Tool '{tool_name}' not found in namespace '{namespace}'"]

        # Find similar tool names using fuzzy matching
        similar_tools: list[str] = []
        if available_tools:
            # Handle both tuple format (namespace, name) and string format
            if isinstance(available_tools[0], tuple):
                # mypy narrows available_tools to list[tuple[str, str]] here
                all_tool_names = [name for _, name in available_tools]
                # Also check for namespace:name format
                full_names = [f"{ns}:{name}" for ns, name in available_tools]
                similar_in_namespace = get_close_matches(tool_name, all_tool_names, n=3, cutoff=0.6)
                similar_full = get_close_matches(f"{namespace}:{tool_name}", full_names, n=3, cutoff=0.6)
                similar_tools = list(similar_in_namespace) + list(similar_full)
            else:
                # mypy narrows available_tools to list[str] here
                str_tools = available_tools
                similar_tools = list(get_close_matches(tool_name, str_tools, n=3, cutoff=0.6))

        if similar_tools:
            message_parts.append(f"\n\nDid you mean: {', '.join(similar_tools)}?")

        # Add available namespaces
        if available_namespaces:
            message_parts.append(f"\n\nAvailable namespaces: {', '.join(available_namespaces)}")

        # Add helpful tip
        message_parts.append(
            "\n\nTip: Use `await registry.list_tools()` to see all registered tools, "
            "or `await registry.list_namespaces()` to see available namespaces."
        )

        message = "".join(message_parts)

        # Store details
        details: dict[str, Any] = {"tool_name": tool_name, "namespace": namespace}
        if available_tools:
            details["available_tools"] = available_tools
        if available_namespaces:
            details["available_namespaces"] = available_namespaces
        if similar_tools:
            details["suggestions"] = similar_tools

        super().__init__(
            message,
            code=ErrorCode.TOOL_NOT_FOUND,
            details=details,
        )


class ToolExecutionError(ToolProcessorError):
    """Raised when a tool execution fails."""

    def __init__(
        self,
        tool_name: str,
        original_error: Exception | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.tool_name = tool_name
        message = f"Tool '{tool_name}' execution failed"
        if original_error:
            message += f": {str(original_error)}"

        error_details = {"tool_name": tool_name}
        if details:
            error_details.update(details)

        super().__init__(
            message,
            code=ErrorCode.TOOL_EXECUTION_FAILED,
            details=error_details,
            original_error=original_error,
        )


class ToolTimeoutError(ToolExecutionError):
    """Raised when a tool execution times out."""

    def __init__(self, tool_name: str, timeout: float, attempts: int = 1):
        self.timeout = timeout
        self.attempts = attempts
        # Call ToolProcessorError.__init__ directly to set the right code
        ToolProcessorError.__init__(
            self,
            f"Tool '{tool_name}' timed out after {timeout}s (attempts: {attempts})",
            code=ErrorCode.TOOL_TIMEOUT,
            details={"tool_name": tool_name, "timeout": timeout, "attempts": attempts},
        )
        self.tool_name = tool_name


class ToolValidationError(ToolProcessorError):
    """Raised when tool arguments or results fail validation."""

    def __init__(
        self,
        tool_name: str,
        errors: dict[str, Any],
        validation_type: str = "arguments",
    ):
        self.tool_name = tool_name
        self.errors = errors
        self.validation_type = validation_type
        super().__init__(
            f"Validation failed for tool '{tool_name}' {validation_type}: {errors}",
            code=ErrorCode.TOOL_VALIDATION_ERROR,
            details={"tool_name": tool_name, "validation_type": validation_type, "errors": errors},
        )


class ParserError(ToolProcessorError):
    """Raised when parsing tool calls from raw input fails."""

    def __init__(
        self,
        message: str,
        parser_name: str | None = None,
        input_sample: str | None = None,
    ):
        self.parser_name = parser_name
        self.input_sample = input_sample
        details = {}
        if parser_name:
            details["parser_name"] = parser_name
        if input_sample:
            # Truncate sample for logging
            details["input_sample"] = input_sample[:200] + "..." if len(input_sample) > 200 else input_sample
        super().__init__(
            message,
            code=ErrorCode.PARSER_ERROR,
            details=details,
        )


class ToolRateLimitedError(ToolProcessorError):
    """Raised when a tool call is rate limited."""

    def __init__(
        self,
        tool_name: str,
        retry_after: float | None = None,
        limit: int | None = None,
        period: float | None = None,
    ):
        self.tool_name = tool_name
        self.retry_after = retry_after
        self.limit = limit
        self.period = period
        message = f"Tool '{tool_name}' rate limited"
        if retry_after:
            message += f" (retry after {retry_after}s)"

        # Convert retry_after to milliseconds for structured error info
        retry_after_ms = int(retry_after * 1000) if retry_after else None

        super().__init__(
            message,
            code=ErrorCode.TOOL_RATE_LIMITED,
            details={
                "tool_name": tool_name,
                "retry_after": retry_after,
                "retry_after_ms": retry_after_ms,
                "limit": limit,
                "period": period,
            },
            retry_after_ms=retry_after_ms,
        )


class ToolCircuitOpenError(ToolProcessorError):
    """Raised when a tool circuit breaker is open."""

    def __init__(
        self,
        tool_name: str,
        failure_count: int,
        reset_timeout: float | None = None,
    ):
        self.tool_name = tool_name
        self.failure_count = failure_count
        self.reset_timeout = reset_timeout
        message = f"Tool '{tool_name}' circuit breaker is open (failures: {failure_count})"
        if reset_timeout:
            message += f" (reset in {reset_timeout}s)"

        # Convert reset_timeout to milliseconds for structured error info
        retry_after_ms = int(reset_timeout * 1000) if reset_timeout else None

        super().__init__(
            message,
            code=ErrorCode.TOOL_CIRCUIT_OPEN,
            details={
                "tool_name": tool_name,
                "failure_count": failure_count,
                "reset_timeout": reset_timeout,
                "retry_after_ms": retry_after_ms,
            },
            retry_after_ms=retry_after_ms,
        )


class MCPError(ToolProcessorError):
    """Base class for MCP-related errors."""

    def __init__(
        self,
        message: str,
        code: ErrorCode,
        server_name: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        error_details = details or {}
        if server_name:
            error_details["server_name"] = server_name
        super().__init__(message, code=code, details=error_details)


class MCPConnectionError(MCPError):
    """Raised when MCP connection fails."""

    def __init__(self, server_name: str, reason: str | None = None):
        message = f"Failed to connect to MCP server '{server_name}'"
        if reason:
            message += f": {reason}"
        super().__init__(
            message,
            code=ErrorCode.MCP_CONNECTION_FAILED,
            server_name=server_name,
            details={"reason": reason} if reason else None,
        )


class MCPTimeoutError(MCPError):
    """Raised when MCP operation times out."""

    def __init__(self, server_name: str, operation: str, timeout: float):
        super().__init__(
            f"MCP operation '{operation}' on server '{server_name}' timed out after {timeout}s",
            code=ErrorCode.MCP_TIMEOUT,
            server_name=server_name,
            details={"operation": operation, "timeout": timeout},
        )


class BulkheadFullError(ToolProcessorError):
    """
    Raised when a bulkhead cannot acquire a slot within timeout.

    This indicates backpressure - the system is at capacity for this
    tool/namespace/global limit.
    """

    def __init__(
        self,
        tool_name: str,
        namespace: str = "default",
        limit_type: str = "tool",
        limit: int = 0,
        timeout: float | None = None,
    ):
        self.tool_name = tool_name
        self.namespace = namespace
        self.limit_type = limit_type
        self.limit = limit
        self.timeout = timeout

        message = f"Bulkhead full for '{tool_name}' ({limit_type} limit: {limit})"
        if timeout:
            message += f" after {timeout}s timeout"

        super().__init__(
            message,
            code=ErrorCode.BULKHEAD_FULL,
            details={
                "tool_name": tool_name,
                "namespace": namespace,
                "limit_type": limit_type,
                "limit": limit,
                "timeout": timeout,
            },
        )


# ---------------------------------------------------------------------------
# ErrorInfo: Structured error information for ToolResult (Pydantic model)
# ---------------------------------------------------------------------------


class ErrorInfo(BaseModel):
    """
    Structured error information for ToolResult.

    This Pydantic model provides machine-readable error details that enable
    planners to make intelligent decisions about retries, fallbacks, and
    backpressure.

    Example usage in a planner:

        result = await processor.process(calls)
        for r in result:
            if r.error_info:
                if r.error_info.category == ErrorCategory.RATE_LIMIT:
                    await asyncio.sleep(r.error_info.retry_after_ms / 1000)
                    return await retry()
                elif r.error_info.category == ErrorCategory.CIRCUIT_OPEN:
                    return await use_fallback_tool()
                elif not r.error_info.retryable:
                    return await report_permanent_failure()

    Attributes:
        code: Machine-readable error code (ErrorCode enum)
        category: High-level error category for decision-making
        message: Human-readable error message
        retryable: Whether this error is generally retryable
        retry_after_ms: Suggested delay before retry (milliseconds)
        details: Additional structured error context
    """

    model_config = ConfigDict(
        extra="ignore",
        frozen=True,  # Immutable for safety
        use_enum_values=False,  # Keep enums as enums, not strings
    )

    code: ErrorCode = Field(
        ...,
        description="Machine-readable error code",
    )
    category: ErrorCategory = Field(
        ...,
        description="High-level error category for planner decision-making",
    )
    message: str = Field(
        ...,
        description="Human-readable error message",
    )
    retryable: bool = Field(
        default=True,
        description="Whether this error is generally retryable",
    )
    retry_after_ms: int | None = Field(
        default=None,
        ge=0,
        description="Suggested delay before retry in milliseconds",
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional structured error context",
    )

    @classmethod
    def from_exception(cls, exc: Exception) -> ErrorInfo:
        """
        Create ErrorInfo from any exception.

        For ToolProcessorError subclasses, extracts structured information.
        For other exceptions, creates a generic execution error.
        """
        if isinstance(exc, ToolProcessorError):
            return exc.to_error_info()

        # Generic exception - wrap as execution error
        return cls(
            code=ErrorCode.TOOL_EXECUTION_FAILED,
            category=ErrorCategory.EXECUTION,
            message=str(exc),
            retryable=True,
            details={"exception_type": type(exc).__name__},
        )

    @classmethod
    def from_error_string(cls, error: str, tool_name: str | None = None) -> ErrorInfo:
        """
        Create ErrorInfo by parsing an error string.

        This is a best-effort parser for legacy error strings that attempts
        to extract structured information. Used for backwards compatibility.
        """
        error_lower = error.lower()

        # Detect error category from string patterns
        if "rate limit" in error_lower:
            return cls(
                code=ErrorCode.TOOL_RATE_LIMITED,
                category=ErrorCategory.RATE_LIMIT,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "circuit" in error_lower and "open" in error_lower:
            return cls(
                code=ErrorCode.TOOL_CIRCUIT_OPEN,
                category=ErrorCategory.CIRCUIT_OPEN,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "bulkhead" in error_lower or "concurrency" in error_lower:
            return cls(
                code=ErrorCode.BULKHEAD_FULL,
                category=ErrorCategory.BULKHEAD_FULL,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "timeout" in error_lower or "timed out" in error_lower:
            return cls(
                code=ErrorCode.TOOL_TIMEOUT,
                category=ErrorCategory.TIMEOUT,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "not found" in error_lower:
            return cls(
                code=ErrorCode.TOOL_NOT_FOUND,
                category=ErrorCategory.NOT_FOUND,
                message=error,
                retryable=False,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "validation" in error_lower or "invalid" in error_lower:
            return cls(
                code=ErrorCode.TOOL_VALIDATION_ERROR,
                category=ErrorCategory.VALIDATION,
                message=error,
                retryable=False,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "cancel" in error_lower:
            return cls(
                code=ErrorCode.TOOL_CANCELLED,
                category=ErrorCategory.CANCELLED,
                message=error,
                retryable=False,
                details={"tool_name": tool_name} if tool_name else {},
            )
        elif "connection" in error_lower or "connect" in error_lower:
            return cls(
                code=ErrorCode.MCP_CONNECTION_FAILED,
                category=ErrorCategory.CONNECTION,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
        else:
            # Default to generic execution error
            return cls(
                code=ErrorCode.TOOL_EXECUTION_FAILED,
                category=ErrorCategory.EXECUTION,
                message=error,
                retryable=True,
                details={"tool_name": tool_name} if tool_name else {},
            )
