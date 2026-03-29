"""Valory mech tool compatibility layer.

Loads Valory-format tools (component.yaml + Python module with run() function)
from local filesystem. Uses importlib instead of exec() for safety.
"""

import importlib.util
import re
import sys
import uuid
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger

from micromech.tools.base import Tool, ToolMetadata

_VALID_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def load_valory_tool(tool_dir: Path) -> Optional[Tool]:
    """Load a Valory-format tool from a local directory.

    Expected structure:
        tool_dir/
            component.yaml   # name, entry_point, callable
            <tool_name>.py   # Python module with ALLOWED_TOOLS + run()

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

    # Validate tool name
    if not _VALID_NAME_RE.match(name):
        logger.error("Invalid tool name '{}' in {}", name, tool_dir)
        return None

    module_path = tool_dir / entry_point
    if not module_path.exists():
        logger.error("Entry point {} not found in {}", entry_point, tool_dir)
        return None

    # Use unique module name to avoid sys.modules collisions
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

    run_fn = getattr(module, callable_name, None)
    if run_fn is None:
        logger.error("Module {} has no callable '{}'", module_path, callable_name)
        del sys.modules[module_name]
        return None

    # Convert name to valid tool ID (lowercase, hyphens ok)
    tool_id = name.lower().replace("_", "-")
    allowed = getattr(module, "ALLOWED_TOOLS", [tool_id])

    metadata = ToolMetadata(
        id=tool_id,
        name=name,
        description=description,
        version=version,
    )

    logger.info("Loaded Valory tool: {} v{} from {}", tool_id, version, tool_dir)
    return Tool(metadata=metadata, run_fn=run_fn, allowed_tools=allowed)
