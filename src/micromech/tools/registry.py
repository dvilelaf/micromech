"""Tool registry: discover, load, and manage tools."""

import importlib

from loguru import logger

from micromech.core.errors import MechError
from micromech.tools.base import Tool, ToolMetadata


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

    def _load_tool_package(self, module_path: str, tool_id: str, **meta_kwargs) -> None:
        """Load a tool from a Valory-format package module."""
        try:
            mod = importlib.import_module(module_path)
            run_fn = getattr(mod, "run")
            allowed = getattr(mod, "ALLOWED_TOOLS", [tool_id])
            metadata = ToolMetadata(id=tool_id, **meta_kwargs)
            tool = Tool(metadata=metadata, run_fn=run_fn, allowed_tools=allowed)
            self.register(tool)
            # Register aliases (other ALLOWED_TOOLS names)
            for alias in allowed:
                if alias != tool_id and alias not in self._tools:
                    self._tools[alias] = tool
        except ImportError:
            logger.info("Tool {} not available (missing dependencies)", tool_id)
        except Exception as e:
            logger.error("Failed to load tool {}: {}", tool_id, e)

    def load_builtins(self) -> None:
        """Load all built-in tools."""
        self._load_tool_package(
            "micromech.tools.echo_tool.echo_tool",
            "echo",
            name="Echo",
            description="Returns default prediction. For testing.",
            timeout=5,
        )

        self._load_tool_package(
            "micromech.tools.llm_tool.llm_tool",
            "llm",
            name="Local LLM",
            description="General-purpose local LLM (Qwen 0.5B, CPU).",
            timeout=120,
        )

        self._load_tool_package(
            "micromech.tools.prediction_request.prediction_request",
            "prediction-offline",
            name="Prediction Offline (Local LLM)",
            description="Prediction market analysis using local LLM.",
            timeout=120,
        )
