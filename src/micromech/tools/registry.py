"""Tool registry: discover, load, and manage tools."""

import importlib
from pathlib import Path

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
        """List all registered tools (deduplicated, aliases excluded)."""
        seen: set[int] = set()
        result: list[Tool] = []
        for tool in self._tools.values():
            if id(tool) not in seen:
                seen.add(id(tool))
                result.append(tool)
        return result

    def list_packages(self) -> list[dict]:
        """List tools grouped by package with their accepted tool IDs.

        Returns list of dicts:
            {"name": "local_llm", "version": "0.1.0", "tools": ["local-llm"]}
        """
        packages = []
        seen: set[int] = set()
        for tool in self._tools.values():
            if id(tool) not in seen:
                seen.add(id(tool))
                packages.append({
                    "name": tool.metadata.name,
                    "version": tool.metadata.version,
                    "tools": list(tool.ALLOWED_TOOLS),
                })
        return packages

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

    def _discover_directory(
        self, tools_dir: Path, module_prefix: str,
        skip_packages: set[str] | None = None,
    ) -> int:
        """Auto-discover and load all tool packages in a directory.

        Each subdirectory with a component.yaml is treated as a tool package.
        The component.yaml provides metadata; the entry_point Python module
        provides ALLOWED_TOOLS and the run() function.

        Returns the number of tools successfully loaded.
        """
        if not tools_dir.exists():
            return 0

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

            # Build importable module path
            module_path = f"{module_prefix}.{tool_dir.name}.{entry_module}"

            # Determine primary tool ID: first ALLOWED_TOOLS entry or package name
            # We do a quick pre-import to get ALLOWED_TOOLS for the primary ID
            try:
                mod = importlib.import_module(module_path)
                allowed = getattr(mod, "ALLOWED_TOOLS", [])
                tool_id = allowed[0] if allowed else tool_dir.name
            except ImportError:
                logger.info("Tool {} not available (missing dependencies)", name)
                continue
            except Exception as e:
                logger.error("Failed to import {}: {}", module_path, e)
                continue

            self._load_tool_package(
                module_path, tool_id,
                name=name, description=description,
                version=version, timeout=timeout,
            )
            loaded += 1

        return loaded

    def load_builtins(self, disabled: set[str] | None = None) -> None:
        """Auto-discover and load all built-in tools from the tools directory."""
        loaded = self._discover_directory(
            _BUILTINS_DIR, "micromech.tools", skip_packages=disabled,
        )
        logger.info("Auto-discovered {} built-in tool package(s)", loaded)

    def load_custom(self, custom_dir: Path) -> None:
        """Auto-discover and load custom tools from an external directory.

        Custom tool packages must be importable — either installed or on sys.path.
        For packages under ~/.micromech/tools/, callers should add that path to
        sys.path before calling this method.
        """
        if not custom_dir.exists():
            return
        loaded = self._discover_directory(custom_dir, "custom_tools")
        if loaded:
            logger.info("Loaded {} custom tool package(s) from {}", loaded, custom_dir)
