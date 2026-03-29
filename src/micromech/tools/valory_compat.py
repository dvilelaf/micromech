"""Valory mech tool compatibility layer.

Loads Valory-format tools (component.yaml + Python module with run() function)
from local filesystem. Uses importlib instead of exec() for safety.
Synchronous tool execution is offloaded to a thread pool.
"""

import asyncio
import importlib.util
import re
import sys
import uuid
from pathlib import Path
from typing import Any, Optional

import yaml
from loguru import logger

from micromech.tools.base import Tool, ToolMetadata, ValoryResponse

_VALID_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class ValoryTool(Tool):
    """Wrapper around a Valory-format tool module."""

    def __init__(self, metadata: ToolMetadata, module: Any, callable_name: str):
        self.metadata = metadata
        self._module = module
        self._callable_name = callable_name

    def _sync_run(self, prompt: str, **kwargs: Any) -> str:
        """Synchronous execution — called from thread pool."""
        fn = getattr(self._module, self._callable_name)
        result = fn(prompt=prompt, tool=self.metadata.id, **kwargs)
        if isinstance(result, tuple) and len(result) >= 1:
            return str(result[0])
        return str(result)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Execute the Valory tool's run() function in a thread."""
        return await asyncio.to_thread(self._sync_run, prompt, **kwargs)

    def _sync_run_valory(self, **kwargs: Any) -> ValoryResponse:
        """Synchronous Valory execution — called from thread pool."""
        fn = getattr(self._module, self._callable_name)
        result = fn(**kwargs)
        if isinstance(result, tuple) and len(result) == 5:
            return result
        return str(result), kwargs.get("prompt"), None, None, None

    async def execute_valory(self, **kwargs: Any) -> ValoryResponse:
        """Execute returning the full Valory 5-tuple, in a thread."""
        return await asyncio.to_thread(self._sync_run_valory, **kwargs)


def load_valory_tool(tool_dir: Path) -> Optional[Tool]:
    """Load a Valory-format tool from a local directory.

    Expected structure:
        tool_dir/
            component.yaml   # name, entry_point, callable
            <tool_name>.py   # Python module with run() function

    Returns:
        Tool instance, or None if loading fails.
    """
    component_yaml = tool_dir / "component.yaml"
    if not component_yaml.exists():
        logger.error("No component.yaml found in {}", tool_dir)
        return None

    try:
        spec = yaml.safe_load(component_yaml.read_text())
    except Exception as e:
        logger.error("Failed to parse component.yaml in {}: {}", tool_dir, e)
        return None

    name = spec.get("name", tool_dir.name)
    entry_point = spec.get("entry_point", f"{name}.py")
    callable_name = spec.get("callable", "run")
    version = spec.get("version", "0.1.0")
    description = spec.get("description", "")

    # Validate tool name as safe identifier
    if not _VALID_NAME_RE.match(name):
        logger.error("Invalid tool name '{}' in {}", name, tool_dir)
        return None

    module_path = tool_dir / entry_point
    if not module_path.exists():
        logger.error("Entry point {} not found in {}", entry_point, tool_dir)
        return None

    # Use unique module name to avoid collisions in sys.modules
    unique_id = uuid.uuid4().hex[:8]
    module_name = f"_micromech_ext_{name}_{unique_id}"

    spec_obj = importlib.util.spec_from_file_location(module_name, module_path)
    if spec_obj is None or spec_obj.loader is None:
        logger.error("Failed to create module spec for {}", module_path)
        return None

    module = importlib.util.module_from_spec(spec_obj)
    sys.modules[module_name] = module
    try:
        spec_obj.loader.exec_module(module)
    except Exception as e:
        logger.error("Failed to load module {}: {}", module_path, e)
        del sys.modules[module_name]
        return None

    if not hasattr(module, callable_name):
        logger.error("Module {} has no callable '{}'", module_path, callable_name)
        del sys.modules[module_name]
        return None

    # Convert name to valid tool ID (lowercase, hyphens ok)
    tool_id = name.lower().replace("_", "-")

    metadata = ToolMetadata(
        id=tool_id,
        name=name,
        description=description,
        version=version,
    )

    logger.info("Loaded Valory tool: {} v{} from {}", tool_id, version, tool_dir)
    return ValoryTool(metadata=metadata, module=module, callable_name=callable_name)
