"""Tests for metadata generation."""

from pathlib import Path

import yaml

from micromech.ipfs.metadata import (
    build_metadata,
    build_tools_to_package_hash,
    compute_onchain_hash,
    fingerprint_all_builtins,
    fingerprint_tool_package,
    scan_tool_packages,
)


class TestScanToolPackages:
    def test_scan_builtin_tools(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
        tools = scan_tool_packages(tools_dir)
        assert len(tools) >= 1
        # Echo should always be found
        names = [t["name"] for t in tools]
        assert "echo_tool" in names

    def test_scan_nonexistent_dir(self):
        tools = scan_tool_packages(Path("/nonexistent"))
        assert tools == []

    def test_tool_has_allowed_tools(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
        tools = scan_tool_packages(tools_dir)
        echo = [t for t in tools if t["name"] == "echo_tool"][0]
        assert "echo" in echo["allowed_tools"]

    def test_tool_has_package_cid(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
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


class TestFingerprintToolPackage:
    def test_fingerprint_creates_correct_mapping(self, tmp_path: Path):
        """fingerprint_tool_package returns file->CID mapping and writes component.yaml."""
        tool_dir = tmp_path / "my_tool"
        tool_dir.mkdir()

        # Create component.yaml
        spec = {"name": "my_tool", "version": "0.1.0"}
        (tool_dir / "component.yaml").write_text(yaml.dump(spec))

        # Create a tool module file
        (tool_dir / "my_tool.py").write_text("ALLOWED_TOOLS = ['my-tool']\ndef run(**kw): pass\n")

        fps = fingerprint_tool_package(tool_dir)
        assert "my_tool.py" in fps
        assert fps["my_tool.py"].startswith("bafkrei")

        # Verify component.yaml was updated with fingerprints
        updated = yaml.safe_load((tool_dir / "component.yaml").read_text())
        assert "fingerprint" in updated
        assert updated["fingerprint"]["my_tool.py"] == fps["my_tool.py"]

    def test_fingerprint_skips_pycache(self, tmp_path: Path):
        tool_dir = tmp_path / "my_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(yaml.dump({"name": "my_tool"}))
        (tool_dir / "my_tool.py").write_text("pass")
        pycache = tool_dir / "__pycache__"
        pycache.mkdir()
        (pycache / "my_tool.cpython-312.pyc").write_bytes(b"\x00")

        fps = fingerprint_tool_package(tool_dir)
        assert "my_tool.py" in fps
        assert not any("__pycache__" in k for k in fps)

    def test_fingerprint_no_component_yaml(self, tmp_path: Path):
        """Without component.yaml, fingerprints are computed but not written."""
        tool_dir = tmp_path / "my_tool"
        tool_dir.mkdir()
        (tool_dir / "my_tool.py").write_text("pass")

        fps = fingerprint_tool_package(tool_dir)
        assert "my_tool.py" in fps


class TestFingerprintAllBuiltins:
    def test_fingerprints_echo_tool(self):
        results = fingerprint_all_builtins()
        assert "echo_tool" in results
        assert len(results["echo_tool"]) >= 1

    def test_returns_dict_of_dicts(self):
        results = fingerprint_all_builtins()
        for tool_name, fps in results.items():
            assert isinstance(fps, dict)
            for path, cid in fps.items():
                assert isinstance(path, str)
                assert cid.startswith("bafkrei")


class TestScanToolPackagesErrors:
    def test_scan_handles_bad_yaml(self, tmp_path: Path):
        """Tool with invalid YAML is skipped."""
        tool_dir = tmp_path / "bad_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("{{invalid yaml")

        tools = scan_tool_packages(tmp_path)
        assert tools == []

    def test_scan_skips_non_directories(self, tmp_path: Path):
        """Files in tools_dir are skipped."""
        (tmp_path / "not_a_dir.txt").write_text("hello")
        tools = scan_tool_packages(tmp_path)
        assert tools == []

    def test_scan_handles_import_error_for_allowed_tools(self, tmp_path: Path):
        """Tool that fails to import still gets scanned (without ALLOWED_TOOLS)."""
        tool_dir = tmp_path / "broken_tool"
        tool_dir.mkdir()
        spec = {"name": "broken_tool", "entry_point": "broken_tool.py"}
        (tool_dir / "component.yaml").write_text(yaml.dump(spec))
        (tool_dir / "broken_tool.py").write_text("import nonexistent_module_xyz")

        tools = scan_tool_packages(tmp_path)
        assert len(tools) == 1
        assert tools[0]["name"] == "broken_tool"
        assert tools[0]["allowed_tools"] == []


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
