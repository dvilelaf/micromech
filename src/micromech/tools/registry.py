"""Tool registry: discover, load, and manage tools."""

from typing import Optional

from loguru import logger

from micromech.core.errors import MechError
from micromech.tools.base import Tool


class ToolNotFoundError(MechError):
    """Requested tool is not registered."""

    def __init__(self, tool_id: str):
        self.tool_id = tool_id
        super().__init__(f"Tool '{tool_id}' not found in registry")


class ToolRegistry:
    """Registry for discovering and managing tools."""

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        """Register a tool instance."""
        tool_id = tool.metadata.id
        if tool_id in self._tools:
            logger.warning("Overwriting existing tool: {}", tool_id)
        self._tools[tool_id] = tool
        logger.info("Registered tool: {} (v{})", tool_id, tool.metadata.version)

    def get(self, tool_id: str) -> Tool:
        """Get a registered tool by ID. Raises ToolNotFoundError if not found."""
        tool = self._tools.get(tool_id)
        if tool is None:
            raise ToolNotFoundError(tool_id)
        return tool

    def get_optional(self, tool_id: str) -> Optional[Tool]:
        """Get a registered tool by ID, or None."""
        return self._tools.get(tool_id)

    def list_tools(self) -> list[Tool]:
        """List all registered tools."""
        return list(self._tools.values())

    def has(self, tool_id: str) -> bool:
        """Check if a tool is registered."""
        return tool_id in self._tools

    @property
    def tool_ids(self) -> list[str]:
        """List all registered tool IDs."""
        return list(self._tools.keys())

    def load_builtins(self) -> None:
        """Load all built-in tools."""
        from micromech.tools.builtin.echo import EchoTool

        self.register(EchoTool())

        try:
            from micromech.tools.builtin.llm import LLMTool

            self.register(LLMTool())
        except ImportError:
            logger.info("LLM tool not available (install micromech[llm])")
