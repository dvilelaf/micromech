"""Tool registry: discover, load, and manage tools."""

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml
from loguru import logger

from micromech.core.errors import MechError
from micromech.tools.base import Tool, ToolMetadata

# Built-in tools directory (inside the package)
_BUILTINS_DIR = Path(__file__).parent

# Default timeout for tools whose component.yaml doesn't specify one
_DEFAULT_TIMEOUT = 60

# Mapping of known tool packages to their default timeout (seconds).
# component.yaml can override this via a "timeout" field.
_KNOWN_TIMEOUTS: dict[str, int] = {
    "echo_tool": 5,
    "local_llm": 120,
    "prediction_request": 120,
    "gemma4_api_tool": 60,
}


class ToolNotFoundError(MechError):
    """Requested tool is not registered."""

    def __init__(self, tool_id: str):
        self.tool_id = tool_id
        super().__init__(f"Tool '{tool_id}' not found in registry")


def _load_module_from_file(module_name: str, file_path: Path) -> ModuleType:
    """Load a Python module from a filesystem path (no sys.path needed)."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        msg = f"Cannot create module spec for {file_path}"
        raise ImportError(msg)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


class ToolRegistry:
    """Registry for discovering and managing tools."""

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool, *, allow_override: bool = True) -> bool:
        """Register a tool instance.

        Args:
            allow_override: If False, skip registration when tool_id already exists.
                Used for custom tools to prevent overwriting builtins.

        Returns:
            True if registered, False if skipped due to collision.
        """
        tool_id = tool.metadata.id
        if tool_id in self._tools:
            if not allow_override:
                logger.warning(
                    "Custom tool '{}' conflicts with existing — skipping",
                    tool_id,
                )
                return False
            logger.warning("Overwriting existing tool: {}", tool_id)
        self._tools[tool_id] = tool
        logger.info(
            "Registered tool: {} (v{}, {})",
            tool_id,
            tool.metadata.version,
            tool.metadata.origin,
        )
        return True

    def get(self, tool_id: str) -> Tool:
        """Get a registered tool by ID. Raises ToolNotFoundError if not found."""
        tool = self._tools.get(tool_id)
        if tool is None:
            raise ToolNotFoundError(tool_id)
        return tool

    def list_tools(self) -> list[Tool]:
        """List all registered tools (deduplicated, aliases excluded)."""
        seen: set[int] = set()
        result: list[Tool] = []
        for tool in self._tools.values():
            if id(tool) not in seen:
                seen.add(id(tool))
                result.append(tool)
        return result

    def list_packages(self) -> list[dict]:
        """List tools grouped by package with their accepted tool IDs."""
        packages = []
        seen: set[int] = set()
        for tool in self._tools.values():
            if id(tool) not in seen:
                seen.add(id(tool))
                packages.append(
                    {
                        "name": tool.metadata.name,
                        "version": tool.metadata.version,
                        "tools": list(tool.ALLOWED_TOOLS),
                        "origin": tool.metadata.origin,
                    }
                )
        return packages

    def has(self, tool_id: str) -> bool:
        """Check if a tool is registered."""
        return tool_id in self._tools

    @property
    def tool_ids(self) -> list[str]:
        """List all registered tool IDs."""
        return list(self._tools.keys())

    def _load_tool_package(
        self,
        tool_id: str,
        run_fn: Any,
        allowed: list[str],
        allow_override: bool = True,
        **meta_kwargs: Any,
    ) -> bool:
        """Register a tool from its run function and metadata.

        Returns True if registered successfully.
        """
        metadata = ToolMetadata(id=tool_id, **meta_kwargs)
        tool = Tool(metadata=metadata, run_fn=run_fn, allowed_tools=allowed)
        if not self.register(tool, allow_override=allow_override):
            return False
        # Register aliases (other ALLOWED_TOOLS names)
        for alias in allowed:
            if alias != tool_id and alias not in self._tools:
                self._tools[alias] = tool
        return True

    def _discover_directory(
        self,
        tools_dir: Path,
        origin: str = "builtin",
        module_prefix: str | None = None,
        skip_packages: set[str] | None = None,
    ) -> int:
        """Auto-discover and load all tool packages in a directory.

        For builtins (origin="builtin"), uses importlib.import_module with module_prefix.
        For custom tools (origin="custom"), uses file-based import (no sys.path needed).

        Returns the number of tools successfully loaded.
        """
        if not tools_dir.exists():
            return 0

        from micromech.ipfs.metadata import _parse_allowed_tools

        allow_override = origin == "builtin"
        loaded = 0

        for tool_dir in sorted(tools_dir.iterdir()):
            if not tool_dir.is_dir():
                continue

            component_yaml = tool_dir / "component.yaml"
            if not component_yaml.exists():
                continue

            try:
                spec = yaml.safe_load(component_yaml.read_text())
            except Exception as e:
                logger.warning("Failed to parse {}: {}", component_yaml, e)
                continue

            name = spec.get("name", tool_dir.name)
            if skip_packages and name in skip_packages:
                logger.info("Skipping disabled tool package: {}", name)
                continue

            entry_point = spec.get("entry_point", f"{name}.py")
            entry_module = entry_point.removesuffix(".py")
            description = spec.get("description", "")
            version = spec.get("version", "0.1.0")
            timeout = spec.get("timeout", _KNOWN_TIMEOUTS.get(tool_dir.name, _DEFAULT_TIMEOUT))

            # Get ALLOWED_TOOLS via AST (safe, no code execution)
            module_file = tool_dir / entry_point
            if not module_file.exists():
                logger.warning("Entry point {} not found in {}", entry_point, tool_dir)
                continue

            allowed = _parse_allowed_tools(module_file)
            tool_id = allowed[0] if allowed else tool_dir.name

            # Import the module to get run()
            try:
                if origin == "builtin" and module_prefix:
                    module_path = f"{module_prefix}.{tool_dir.name}.{entry_module}"
                    mod = importlib.import_module(module_path)
                else:
                    module_name = f"micromech._custom_tools.{tool_dir.name}.{entry_module}"
                    mod = _load_module_from_file(module_name, module_file)
            except ImportError:
                logger.info("Tool {} not available (missing dependencies)", name)
                continue
            except Exception as e:
                logger.error("Failed to load tool {}: {}", name, e)
                continue

            run_fn = getattr(mod, "run", None)
            if run_fn is None:
                logger.error("Tool {} has no run() function", name)
                continue

            # Re-read ALLOWED_TOOLS from loaded module (may differ from AST if dynamic)
            allowed = getattr(mod, "ALLOWED_TOOLS", allowed)
            tool_id = allowed[0] if allowed else tool_dir.name

            success = self._load_tool_package(
                tool_id,
                run_fn,
                allowed,
                allow_override=allow_override,
                name=name,
                description=description,
                version=version,
                timeout=timeout,
                origin=origin,
            )
            if success:
                loaded += 1

        return loaded

    def load_builtins(self, disabled: set[str] | None = None) -> None:
        """Auto-discover and load all built-in tools from the tools directory."""
        loaded = self._discover_directory(
            _BUILTINS_DIR,
            origin="builtin",
            module_prefix="micromech.tools",
            skip_packages=disabled,
        )
        logger.info("Auto-discovered {} built-in tool package(s)", loaded)

    def load_custom(
        self,
        custom_dir: Path,
        disabled: set[str] | None = None,
    ) -> None:
        """Auto-discover and load custom tools from an external directory.

        Custom tools are loaded via file-based import (no sys.path manipulation).
        They cannot override builtin tools with the same tool ID.
        """
        if not custom_dir.exists():
            return
        loaded = self._discover_directory(
            custom_dir,
            origin="custom",
            skip_packages=disabled,
        )
        if loaded:
            logger.info("Loaded {} custom tool package(s) from {}", loaded, custom_dir)
