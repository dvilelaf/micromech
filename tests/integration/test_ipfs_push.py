"""Integration test: IPFS push and pull.

Tests both local IPFS node and the Autonolas public gateway.
Skips automatically if the target gateway is unreachable.

Run:
  # Local node tests (needs `ipfs daemon`)
  uv run pytest tests/integration/test_ipfs_push.py -v -s -k local

  # Autonolas gateway tests (needs network)
  uv run pytest tests/integration/test_ipfs_push.py -v -s -k autonolas
"""

import uuid

import pytest

from micromech.ipfs.client import compute_cid, fetch_from_ipfs, push_to_ipfs


class TestIpfsPushPullLocal:
    """Test real IPFS push/pull if local node available."""

    @pytest.mark.asyncio
    async def test_ipfs_push_and_pull_local(self):
        """Push data to local IPFS, then fetch it back and verify roundtrip."""
        try:
            cid, cid_hex = await push_to_ipfs(
                b"test data micromech",
                api_url="http://localhost:5001",
            )
        except Exception:
            pytest.skip("No local IPFS node available")

        data = await fetch_from_ipfs(
            cid,
            gateway="http://localhost:8080/ipfs/",
        )
        assert data == b"test data micromech"

    @pytest.mark.asyncio
    async def test_ipfs_push_json_roundtrip_local(self):
        """Push JSON data and verify it roundtrips correctly."""
        import json

        payload = json.dumps({"prompt": "Will ETH hit 10k?", "tool": "echo"}).encode()

        try:
            cid, cid_hex = await push_to_ipfs(
                payload,
                api_url="http://localhost:5001",
            )
        except Exception:
            pytest.skip("No local IPFS node available")

        data = await fetch_from_ipfs(
            cid,
            gateway="http://localhost:8080/ipfs/",
        )
        assert json.loads(data) == json.loads(payload)

    @pytest.mark.asyncio
    async def test_ipfs_cid_deterministic_local(self):
        """Same data pushed twice yields the same CID."""
        content = b"deterministic test micromech"
        try:
            cid1, _ = await push_to_ipfs(
                content,
                api_url="http://localhost:5001",
            )
            cid2, _ = await push_to_ipfs(
                content,
                api_url="http://localhost:5001",
            )
        except Exception:
            pytest.skip("No local IPFS node available")

        assert cid1 == cid2


class TestIpfsPushPullAutonolas:
    """Test IPFS push/pull via the Autonolas public gateway."""

    @pytest.mark.asyncio
    async def test_ipfs_push_to_autonolas(self):
        """Push to Autonolas IPFS gateway and fetch back."""
        test_data = b'{"test": "micromech-ipfs-' + str(uuid.uuid4()).encode() + b'"}'

        try:
            cid_str, cid_hex = await push_to_ipfs(
                test_data,
                api_url="https://registry.autonolas.tech",
            )
        except Exception:
            pytest.skip("Autonolas IPFS gateway unreachable")

        # Verify local CID computation matches (raw codec)
        local_cid = compute_cid(test_data)
        # Remote may use dag-pb codec, so CIDs may differ — just check both are valid
        assert cid_str.startswith("b") or cid_str.startswith("Qm")
        assert local_cid.startswith("b")

        # Fetch back via public gateway
        fetched = await fetch_from_ipfs(
            cid_str,
            gateway="https://gateway.autonolas.tech/ipfs/",
            timeout=60,
        )
        assert fetched == test_data

    @pytest.mark.asyncio
    async def test_ipfs_autonolas_deterministic(self):
        """Same data pushed twice to Autonolas yields the same CID."""
        content = b"deterministic-autonolas-" + str(uuid.uuid4()).encode()[:8]
        try:
            cid1, _ = await push_to_ipfs(
                content,
                api_url="https://registry.autonolas.tech",
            )
            cid2, _ = await push_to_ipfs(
                content,
                api_url="https://registry.autonolas.tech",
            )
        except Exception:
            pytest.skip("Autonolas IPFS gateway unreachable")

        assert cid1 == cid2
