"""Custom exceptions for micromech."""


class MechError(Exception):
    """Base exception for all micromech errors."""


class ToolExecutionError(MechError):
    """Tool failed to execute."""

    def __init__(self, tool_id: str, message: str):
        self.tool_id = tool_id
        super().__init__(f"Tool '{tool_id}' failed: {message}")


class PersistenceError(MechError):
    """Database operation failed."""
