"""Custom exceptions for micromech."""


class MechError(Exception):
    """Base exception for all micromech errors."""


class ConfigError(MechError):
    """Invalid or missing configuration."""


class ToolExecutionError(MechError):
    """Tool failed to execute."""

    def __init__(self, tool_id: str, message: str):
        self.tool_id = tool_id
        super().__init__(f"Tool '{tool_id}' failed: {message}")


class DeliveryError(MechError):
    """Failed to deliver response on-chain."""

    def __init__(self, request_id: str, message: str):
        self.request_id = request_id
        super().__init__(f"Delivery failed for request {request_id}: {message}")


class RequestError(MechError):
    """Invalid or unparseable request."""


class PersistenceError(MechError):
    """Database operation failed."""
