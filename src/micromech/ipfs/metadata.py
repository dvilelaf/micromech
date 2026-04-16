"""Metadata generation — scan tools, build metadata.json, compute TOOLS_TO_PACKAGE_HASH.

Generates the on-chain metadata document that describes the mech's available tools.
This metadata is pushed to IPFS and its hash stored on-chain via changeHash().
"""

import ast
import json
import re
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid, compute_cid_hex

# Accept only simple file names: alnum / underscore / hyphen / dot, ending in .py.
# Rejects path separators, parent refs (..), absolute paths, hidden files,
# and consecutive dots (via negative lookahead).
_SAFE_ENTRY_POINT_RE = re.compile(r"^(?!.*\.\.)[A-Za-z0-9_][A-Za-z0-9_.\-]*\.py$")


def _is_safe_entry_point(entry_point: str, tool_dir: Path) -> bool:
    """Return True if *entry_point* is a plain filename inside *tool_dir*.

    Rejects path separators, parent refs, and symlinks that escape the tool
    directory. This is the last line of defense against a malicious
    ``component.yaml`` crafted to load arbitrary Python files.
    """
    if not entry_point or not _SAFE_ENTRY_POINT_RE.match(entry_point):
        return False
    candidate = (tool_dir / entry_point).resolve()
    try:
        tool_root = tool_dir.resolve()
    except OSError:
        return False
    try:
        candidate.relative_to(tool_root)
    except ValueError:
        return False
    # Reject symlinks that point outside the tool tree even if the resolved
    # target happens to land back inside.
    if (tool_dir / entry_point).is_symlink():
        return False
    return True


def _parse_allowed_tools(module_path: Path) -> list[str]:
    """Extract ALLOWED_TOOLS from a Python file using AST (no code execution).

    Parses the file as an AST and looks for a top-level assignment:
        ALLOWED_TOOLS = ["tool-a", "tool-b"]
    """
    tree = ast.parse(module_path.read_text())
    for node in ast.iter_child_nodes(tree):
        # Handle: ALLOWED_TOOLS = ["a", "b"]
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "ALLOWED_TOOLS":
                    value = ast.literal_eval(node.value)
                    if isinstance(value, list):
                        return [str(v) for v in value]
        # Handle: ALLOWED_TOOLS: list[str] = ["a", "b"]
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "ALLOWED_TOOLS":
                if node.value:
                    value = ast.literal_eval(node.value)
                    if isinstance(value, list):
                        return [str(v) for v in value]
    return []


def compute_tools_fingerprint(tools_dir: Path) -> dict[str, str]:
    """Compute a composite fingerprint per tool package.

    Returns {package_name: package_cid} for change detection.
    """
    tools = scan_tool_packages(tools_dir)
    return {t["name"]: t["package_cid"] for t in tools if t.get("package_cid")}


def scan_tool_packages(
    tools_dir: Path,
    source: str = "builtin",
) -> list[dict[str, Any]]:
    """Scan tool package directories and extract metadata.

    Returns list of dicts with: name, description, version, allowed_tools, package_cid.
    """
    tools = []

    if not tools_dir.exists():
        return tools

    for tool_dir in sorted(tools_dir.iterdir()):
        # Reject symlinked tool directories to prevent an operator (or an
        # attacker with write access to data/tools/) from symlinking
        # arbitrary host paths into the scan.
        if tool_dir.is_symlink():
            logger.warning("Skipping symlinked tool dir: {}", tool_dir)
            continue
        if not tool_dir.is_dir():
            continue

        component_yaml = tool_dir / "component.yaml"
        if not component_yaml.exists() or component_yaml.is_symlink():
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

        # Validate entry_point is a plain filename inside tool_dir (blocks
        # crafted component.yaml with "entry_point: ../../etc/passwd").
        if not _is_safe_entry_point(entry_point, tool_dir):
            logger.warning("Skipping tool {}: unsafe entry_point {!r}", name, entry_point)
            continue

        # Extract ALLOWED_TOOLS via AST (no code execution)
        allowed_tools = []
        module_path = tool_dir / entry_point
        if module_path.exists():
            try:
                allowed_tools = _parse_allowed_tools(module_path)
            except Exception as e:
                logger.warning("Failed to parse ALLOWED_TOOLS from {}: {}", name, e)

        # Compute package CID from all files (with path delimiters to prevent collisions)
        package_data = b""
        for f in sorted(tool_dir.rglob("*")):
            if f.is_file() and "__pycache__" not in str(f):
                rel = str(f.relative_to(tool_dir))
                package_data += f"{rel}\x00".encode() + f.read_bytes() + b"\x00"
        package_cid = compute_cid(package_data) if package_data else ""

        tools.append(
            {
                "name": name,
                "description": description,
                "version": version,
                "allowed_tools": allowed_tools,
                "package_cid": package_cid,
                "path": str(tool_dir),
                "source": source,
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
    tools_dir = Path(__file__).parent.parent / "tools"
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

    Returns bytes32 hex (0x + 32 bytes sha256 digest) for changeHash().
    The multihash prefix (0x1220) is stripped — the contract expects raw bytes32.
    """
    data = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    cid_hex = compute_cid_hex(data)
    multihash = cid_hex_to_multihash_bytes(cid_hex)
    # Strip the 2-byte multihash prefix (0x12 = sha256, 0x20 = 32 bytes length)
    digest = multihash[2:]  # 32 bytes
    return "0x" + digest.hex()
