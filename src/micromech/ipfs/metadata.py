"""Metadata generation — scan tools, build metadata.json, compute TOOLS_TO_PACKAGE_HASH.

Generates the on-chain metadata document that describes the mech's available tools.
This metadata is pushed to IPFS and its hash stored on-chain via changeHash().
"""

import importlib
import json
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid, compute_cid_hex


def scan_tool_packages(tools_dir: Path) -> list[dict[str, Any]]:
    """Scan tool package directories and extract metadata.

    Returns list of dicts with: name, description, version, allowed_tools, package_cid.
    """
    tools = []

    if not tools_dir.exists():
        return tools

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
        description = spec.get("description", "")
        version = spec.get("version", "0.1.0")
        entry_point = spec.get("entry_point", f"{name}.py")

        # Extract ALLOWED_TOOLS from the Python module
        allowed_tools = []
        module_path = tool_dir / entry_point
        if module_path.exists():
            try:
                mod_spec = importlib.util.spec_from_file_location(f"_scan_{name}", module_path)
                if mod_spec and mod_spec.loader:
                    mod = importlib.util.module_from_spec(mod_spec)
                    mod_spec.loader.exec_module(mod)
                    allowed_tools = getattr(mod, "ALLOWED_TOOLS", [])
            except Exception as e:
                logger.warning("Failed to import {} for ALLOWED_TOOLS: {}", name, e)

        # Compute package CID from all files
        package_data = b""
        for f in sorted(tool_dir.rglob("*")):
            if f.is_file() and "__pycache__" not in str(f):
                package_data += f.read_bytes()
        package_cid = compute_cid(package_data) if package_data else ""

        tools.append(
            {
                "name": name,
                "description": description,
                "version": version,
                "allowed_tools": allowed_tools,
                "package_cid": package_cid,
                "path": str(tool_dir),
            }
        )

    return tools


def build_metadata(
    tools: list[dict[str, Any]],
    name: str = "micromech",
    description: str = "Lightweight OLAS mech runtime",
) -> dict[str, Any]:
    """Build metadata.json matching the Valory format.

    Returns the metadata dict ready to be pushed to IPFS.
    """
    all_tool_names = []
    tool_metadata = {}

    for tool_info in tools:
        for tool_name in tool_info.get("allowed_tools", []):
            all_tool_names.append(tool_name)
            tool_metadata[tool_name] = {
                "name": tool_info["name"],
                "description": tool_info.get("description", ""),
                "input": {
                    "type": "text",
                    "description": "The prompt or question to process",
                },
                "output": {
                    "type": "object",
                    "description": "JSON result with prediction or response",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "requestId": {"type": "string"},
                            "result": {"type": "string"},
                            "prompt": {"type": "string"},
                        },
                        "required": ["result"],
                    },
                },
            }

    return {
        "name": name,
        "description": description,
        "inputFormat": "ipfs-v0.1",
        "outputFormat": "ipfs-v0.1",
        "image": "tbd",
        "tools": all_tool_names,
        "toolMetadata": tool_metadata,
    }


def build_tools_to_package_hash(
    tools: list[dict[str, Any]],
) -> dict[str, str]:
    """Build TOOLS_TO_PACKAGE_HASH mapping: tool_name → IPFS package CID."""
    mapping = {}
    for tool_info in tools:
        cid = tool_info.get("package_cid", "")
        for tool_name in tool_info.get("allowed_tools", []):
            mapping[tool_name] = cid
    return mapping


def fingerprint_tool_package(tool_dir: Path) -> dict[str, str]:
    """Compute bafkrei... fingerprints for each file in a tool package.

    Returns a dict mapping relative file paths to their CIDv1 base32 hashes.
    Also updates the component.yaml fingerprint field in-place.
    """
    from micromech.ipfs.client import compute_cid

    fingerprints: dict[str, str] = {}
    for f in sorted(tool_dir.rglob("*")):
        if not f.is_file():
            continue
        if "__pycache__" in str(f):
            continue
        if f.name == "component.yaml":
            continue
        rel = str(f.relative_to(tool_dir))
        fingerprints[rel] = compute_cid(f.read_bytes())

    # Update component.yaml
    component_yaml = tool_dir / "component.yaml"
    if component_yaml.exists():
        spec = yaml.safe_load(component_yaml.read_text())
        spec["fingerprint"] = fingerprints
        component_yaml.write_text(yaml.dump(spec, default_flow_style=False, sort_keys=False))

    return fingerprints


def fingerprint_all_builtins() -> dict[str, dict[str, str]]:
    """Compute and write fingerprints for all built-in tool packages.

    Returns a dict mapping tool directory names to their fingerprints.
    """
    tools_dir = Path(__file__).parent.parent / "tools" / "builtin"
    results: dict[str, dict[str, str]] = {}
    for tool_dir in sorted(tools_dir.iterdir()):
        if not tool_dir.is_dir():
            continue
        component_yaml = tool_dir / "component.yaml"
        if not component_yaml.exists():
            continue
        fps = fingerprint_tool_package(tool_dir)
        results[tool_dir.name] = fps
        logger.info("Fingerprinted {}: {} files", tool_dir.name, len(fps))
    return results


def compute_onchain_hash(metadata: dict[str, Any]) -> str:
    """Compute the on-chain hash for a metadata dict.

    Returns the truncated multihash hex (0x1220...) format expected
    by ComplementaryServiceMetadata.changeHash().
    """
    data = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    cid_hex = compute_cid_hex(data)
    multihash = cid_hex_to_multihash_bytes(cid_hex)
    return "0x" + multihash.hex()
