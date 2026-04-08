"""Tests for MetadataManager and metadata utilities."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.ipfs.metadata import _parse_allowed_tools, compute_tools_fingerprint
from tests.conftest import make_test_config


class TestParseAllowedTools:
    """Test AST-based ALLOWED_TOOLS parsing (no code execution)."""

    def test_simple_list(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text('ALLOWED_TOOLS = ["echo", "test"]\n')
        assert _parse_allowed_tools(module) == ["echo", "test"]

    def test_single_item(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text('ALLOWED_TOOLS = ["prediction-online"]\n')
        assert _parse_allowed_tools(module) == ["prediction-online"]

    def test_no_allowed_tools(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text('OTHER_VAR = "hello"\n')
        assert _parse_allowed_tools(module) == []

    def test_empty_file(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text("")
        assert _parse_allowed_tools(module) == []

    def test_multiline_list(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text(
            'ALLOWED_TOOLS = [\n'
            '    "tool-a",\n'
            '    "tool-b",\n'
            ']\n'
        )
        assert _parse_allowed_tools(module) == ["tool-a", "tool-b"]

    def test_annotated_assignment(self, tmp_path: Path):
        module = tmp_path / "tool.py"
        module.write_text('ALLOWED_TOOLS: list[str] = ["typed-tool"]\n')
        assert _parse_allowed_tools(module) == ["typed-tool"]

    def test_does_not_execute_code(self, tmp_path: Path):
        """Verify dangerous code is NOT executed during parsing."""
        module = tmp_path / "tool.py"
        marker = tmp_path / "executed.txt"
        module.write_text(
            f'open("{marker}", "w").write("pwned")\n'
            'ALLOWED_TOOLS = ["safe"]\n'
        )
        result = _parse_allowed_tools(module)
        assert result == ["safe"]
        assert not marker.exists(), "Code was executed during AST parse!"


class TestComputeToolsFingerprint:
    def test_returns_dict(self):
        from micromech.ipfs.metadata import compute_tools_fingerprint
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
        fps = compute_tools_fingerprint(tools_dir)
        assert isinstance(fps, dict)
        assert len(fps) >= 1
        for name, cid in fps.items():
            assert isinstance(name, str)
            assert cid.startswith("bafkrei")

    def test_deterministic(self):
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
        fps1 = compute_tools_fingerprint(tools_dir)
        fps2 = compute_tools_fingerprint(tools_dir)
        assert fps1 == fps2


class TestMetadataManager:
    def test_get_status(self):
        from micromech.metadata_manager import MetadataManager

        config = make_test_config()
        mm = MetadataManager(config)
        status = mm.get_status()

        assert isinstance(status.tools_fingerprint, dict)
        assert len(status.tools) >= 1
        assert status.needs_update is True  # no metadata published yet
        assert status.computed_hash is not None
        assert status.computed_hash.startswith("0x")

    def test_computed_hash_is_bytes32(self):
        """compute_onchain_hash must return 32 bytes (not 34 multihash)."""
        from micromech.ipfs.metadata import (
            build_metadata,
            compute_onchain_hash,
            scan_tool_packages,
        )
        from pathlib import Path

        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools"
        tools = scan_tool_packages(tools_dir)
        metadata = build_metadata(tools)
        h = compute_onchain_hash(metadata)

        assert h.startswith("0x")
        raw = bytes.fromhex(h[2:])
        assert len(raw) == 32, f"Expected 32 bytes (bytes32), got {len(raw)}"
        # Must NOT start with 0x1220 (that's the multihash prefix)
        assert not h.startswith("0x1220"), "Hash should be raw digest, not multihash"

    def test_get_status_up_to_date(self):
        """When stored hash matches current, needs_update is False."""
        from micromech.metadata_manager import MetadataManager
        from micromech.ipfs.metadata import (
            build_metadata,
            compute_onchain_hash,
            compute_tools_fingerprint,
            scan_tool_packages,
        )

        config = make_test_config()
        mm = MetadataManager(config)

        # Simulate a previous publish
        tools_dir = mm.tools_dir
        tools = scan_tool_packages(tools_dir)
        metadata = build_metadata(tools)
        config.metadata_onchain_hash = compute_onchain_hash(metadata)
        config.metadata_fingerprints = compute_tools_fingerprint(tools_dir)

        status = mm.get_status()
        assert status.needs_update is False
        assert status.changed_packages == []

    @pytest.mark.asyncio
    async def test_publish_ipfs_only(self):
        """Publish with skip_onchain pushes to IPFS but doesn't call changeHash."""
        from micromech.metadata_manager import MetadataManager

        config = make_test_config()
        mm = MetadataManager(config)

        progress_log = []

        with patch(
            "micromech.ipfs.client.push_json_to_ipfs",
            new_callable=AsyncMock,
            return_value=("bafkrei_test_cid", "f01551220abc"),
        ):
            result = await mm.publish(
                update_onchain=False,
                on_progress=lambda step, msg: progress_log.append((step, msg)),
            )

        assert result.success
        assert result.ipfs_cid == "bafkrei_test_cid"
        assert result.onchain_hash.startswith("0x")
        assert len(result.chain_txs) == 0
        assert config.metadata_ipfs_cid == "bafkrei_test_cid"
        assert any(step == "done" for step, _ in progress_log)
