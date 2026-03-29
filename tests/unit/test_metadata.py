"""Tests for metadata generation."""

from pathlib import Path

from micromech.ipfs.metadata import (
    build_metadata,
    build_tools_to_package_hash,
    compute_onchain_hash,
    scan_tool_packages,
)


class TestScanToolPackages:
    def test_scan_builtin_tools(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools" / "builtin"
        tools = scan_tool_packages(tools_dir)
        assert len(tools) >= 1
        # Echo should always be found
        names = [t["name"] for t in tools]
        assert "echo_tool" in names

    def test_scan_nonexistent_dir(self):
        tools = scan_tool_packages(Path("/nonexistent"))
        assert tools == []

    def test_tool_has_allowed_tools(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools" / "builtin"
        tools = scan_tool_packages(tools_dir)
        echo = [t for t in tools if t["name"] == "echo_tool"][0]
        assert "echo" in echo["allowed_tools"]

    def test_tool_has_package_cid(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools" / "builtin"
        tools = scan_tool_packages(tools_dir)
        for tool in tools:
            if tool["allowed_tools"]:
                assert tool["package_cid"].startswith("bafkrei")


class TestBuildMetadata:
    def test_basic_structure(self):
        tools = [
            {
                "name": "echo",
                "description": "Echo tool",
                "allowed_tools": ["echo"],
                "package_cid": "bafkrei123",
            }
        ]
        meta = build_metadata(tools)
        assert meta["name"] == "micromech"
        assert "echo" in meta["tools"]
        assert "echo" in meta["toolMetadata"]
        assert meta["inputFormat"] == "ipfs-v0.1"

    def test_multiple_allowed_tools(self):
        tools = [
            {
                "name": "prediction",
                "description": "Prediction tool",
                "allowed_tools": ["prediction-offline", "prediction-offline-local"],
                "package_cid": "bafkrei456",
            }
        ]
        meta = build_metadata(tools)
        assert len(meta["tools"]) == 2
        assert "prediction-offline" in meta["tools"]
        assert "prediction-offline-local" in meta["tools"]

    def test_empty_tools(self):
        meta = build_metadata([])
        assert meta["tools"] == []
        assert meta["toolMetadata"] == {}


class TestBuildToolsToPackageHash:
    def test_mapping(self):
        tools = [
            {"allowed_tools": ["echo"], "package_cid": "bafkrei_echo"},
            {"allowed_tools": ["llm"], "package_cid": "bafkrei_llm"},
        ]
        mapping = build_tools_to_package_hash(tools)
        assert mapping["echo"] == "bafkrei_echo"
        assert mapping["llm"] == "bafkrei_llm"


class TestComputeOnchainHash:
    def test_returns_hex_string(self):
        meta = {"name": "test", "tools": []}
        h = compute_onchain_hash(meta)
        assert h.startswith("0x")
        assert len(h) == 70  # 0x + 34 bytes * 2 hex chars

    def test_deterministic(self):
        meta = {"name": "test", "tools": ["echo"]}
        h1 = compute_onchain_hash(meta)
        h2 = compute_onchain_hash(meta)
        assert h1 == h2
