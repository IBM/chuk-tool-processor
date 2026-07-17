# chuk_tool_processor/mcp/transport/base_transport.py
"""
Abstract base class for MCP transports with complete interface definition.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class MCPBaseTransport(ABC):
    """
    Abstract base class for all MCP transport implementations.

    Defines the complete interface that all transports must implement
    for consistency across stdio, SSE, and HTTP streamable transports.
    """

    # ------------------------------------------------------------------ #
    #  Core connection lifecycle                                         #
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the transport connection.

        Returns:
            True if initialization was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Close the transport connection and clean up all resources."""
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    #  Health and diagnostics                                            #
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def send_ping(self) -> bool:
        """
        Send a ping to verify the connection is alive.

        Returns:
            True if ping was successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the transport is connected and ready for operations.

        Returns:
            True if connected, False otherwise.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    #  Core MCP operations                                               #
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def get_tools(self) -> list[dict[str, Any]]:
        """
        Get the list of available tools from the server.

        Returns:
            List of tool definitions.
        """
        raise NotImplementedError

    @abstractmethod
    async def call_tool(
        self, tool_name: str, arguments: dict[str, Any], timeout: float | None = None
    ) -> dict[str, Any]:
        """
        Call a tool with the given arguments.

        Args:
            tool_name: Name of the tool to call
            arguments: Arguments to pass to the tool
            timeout: Optional timeout for the operation

        Returns:
            Dictionary with 'isError' boolean and either 'content' or 'error'
        """
        raise NotImplementedError

    @abstractmethod
    async def list_resources(self) -> dict[str, Any]:
        """
        List available resources from the server.

        Returns:
            Dictionary containing resources list or empty dict if not supported.
        """
        raise NotImplementedError

    async def read_resource(self, uri: str) -> dict[str, Any]:
        """
        Read a specific resource by URI.

        Args:
            uri: Resource URI to read.

        Returns:
            Dictionary containing resource content or empty dict if not supported.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_prompts(self) -> dict[str, Any]:
        """
        List available prompts from the server.

        Returns:
            Dictionary containing prompts list or empty dict if not supported.
        """
        raise NotImplementedError

    async def get_prompt(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Get a specific prompt by name.

        Args:
            name: Prompt name to fetch.
            arguments: Optional arguments used to render the prompt template.

        Returns:
            Dictionary containing the prompt result or empty dict if not supported.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    #  Metrics and monitoring (all transports should support these)     #
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_metrics(self) -> dict[str, Any]:
        """
        Get performance and connection metrics.

        Returns:
            Dictionary containing metrics data.
        """
        raise NotImplementedError

    @abstractmethod
    def reset_metrics(self) -> None:
        """Reset performance metrics to initial state."""
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    #  Recovery                                                          #
    # ------------------------------------------------------------------ #
    async def _attempt_recovery(self) -> bool:
        """Attempt to recover from connection issues.

        Default implementation: cleanup then reinitialize.
        Subclasses may override for transport-specific recovery.
        """
        try:
            await self.close()
            return await self.initialize()
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Backward compatibility and utility methods                       #
    # ------------------------------------------------------------------ #
    def get_streams(self) -> list[tuple]:
        """
        Get underlying stream objects for backward compatibility.

        Returns:
            List of (read_stream, write_stream) tuples, empty if not applicable.
        """
        return []

    # ------------------------------------------------------------------ #
    #  Context manager support (all transports should support this)     #
    # ------------------------------------------------------------------ #
    async def __aenter__(self):
        """Context manager entry."""
        success = await self.initialize()
        if not success:
            raise RuntimeError(f"Failed to initialize {self.__class__.__name__}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        await self.close()

    # ------------------------------------------------------------------ #
    #  Shared OAuth and auth helpers                                     #
    # ------------------------------------------------------------------ #
    def _is_oauth_error(self, error_msg: str) -> bool:
        """
        Detect if error is OAuth-related per RFC 6750 and MCP OAuth spec.
        """
        if not error_msg:
            return False

        error_lower = error_msg.lower()
        oauth_indicators = [
            "invalid_token",
            "insufficient_scope",
            "invalid_grant",
            "oauth validation",
            "unauthorized",
            "expired token",
            "token expired",
            "authentication failed",
            "invalid access token",
        ]

        return any(indicator in error_lower for indicator in oauth_indicators)

    def _build_auth_headers(self, base_headers: dict[str, str]) -> dict[str, str]:
        """
        Apply configured headers and API key auth to transport-specific base headers.

        Merges configured_headers (e.g., from OAuth) and adds Bearer token
        from api_key if no Authorization header exists.
        """
        headers = dict(base_headers)

        if hasattr(self, "configured_headers") and self.configured_headers:
            headers.update(self.configured_headers)

        if hasattr(self, "api_key") and self.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {self.api_key}"

        return headers

    # ------------------------------------------------------------------ #
    #  Shared helper methods for response normalization                  #
    # ------------------------------------------------------------------ #
    def _normalize_mcp_response(self, response: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize MCP response to consistent format.

        This provides shared logic for all transports to ensure consistent
        response format regardless of transport type.
        """
        # Handle explicit error in response
        if "error" in response:
            error_info = response["error"]
            error_msg = error_info.get("message", "Unknown error") if isinstance(error_info, dict) else str(error_info)

            return {"isError": True, "error": error_msg}

        # Handle successful response with result
        if "result" in response:
            result = response["result"]

            if isinstance(result, dict) and "content" in result:
                return {"isError": False, "content": self._extract_mcp_content(result["content"])}
            else:
                return {"isError": False, "content": result}

        # Handle direct content-based response
        if "content" in response:
            return {"isError": False, "content": self._extract_mcp_content(response["content"])}

        # Fallback
        return {"isError": False, "content": response}

    def _extract_mcp_content(self, content_list: Any) -> Any:
        """
        Extract content from MCP content format.

        Handles the standard MCP content format where content is a list
        of content items with type and data.
        """
        if not isinstance(content_list, list) or not content_list:
            return content_list

        # Handle single content item
        if len(content_list) == 1:
            content_item = content_list[0]
            if isinstance(content_item, dict):
                if content_item.get("type") == "text":
                    text_content = content_item.get("text", "")
                    # Try to parse JSON, fall back to plain text
                    try:
                        import json

                        return json.loads(text_content)
                    except json.JSONDecodeError:
                        return text_content
                else:
                    return content_item

        # Multiple content items - return as-is
        return content_list

    # ------------------------------------------------------------------ #
    #  Standard string representation                                    #
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:
        """Standard string representation for all transports."""
        status = "initialized" if getattr(self, "_initialized", False) else "not initialized"

        # Add metrics info if available
        metrics_info = ""
        if hasattr(self, "enable_metrics") and getattr(self, "enable_metrics", False):
            metrics = self.get_metrics()
            if metrics.get("total_calls", 0) > 0:
                success_rate = (metrics.get("successful_calls", 0) / metrics["total_calls"]) * 100
                metrics_info = f", calls: {metrics['total_calls']}, success: {success_rate:.1f}%"

        # Add transport-specific info - FIXED FORMAT
        transport_info = ""
        if hasattr(self, "url"):
            transport_info = f", url={self.url}"  # Fixed: was "url: "
        elif hasattr(self, "server_params") and hasattr(self.server_params, "command"):
            transport_info = f", command={self.server_params.command}"  # Fixed: was "command: "

        return f"{self.__class__.__name__}(status={status}{transport_info}{metrics_info})"
