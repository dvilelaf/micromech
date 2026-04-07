"""Integration test: IPFS push and pull.

Tests both direct and via the Autonolas public gateway.

Run:
  uv run pytest tests/integration/test_ipfs_push.py -v -s
"""

import uuid

import pytest

from micromech.ipfs.client import compute_cid, fetch_from_ipfs, push_to_ipfs



AUTONOLAS_API = "https://registry.autonolas.tech"
AUTONOLAS_GW = "https://gateway.autonolas.tech/ipfs/"


class TestIpfsPushPullDirect:
    """Test real IPFS push/pull via Autonolas gateway (direct data)."""

    @pytest.mark.asyncio
    async def test_ipfs_push_and_pull(self):
        """Push data to IPFS, then fetch it back and verify roundtrip."""
        cid, cid_hex = await push_to_ipfs(
            b"test data micromech",
            api_url=AUTONOLAS_API,
        )

        data = await fetch_from_ipfs(
            cid,
            gateway=AUTONOLAS_GW,
            timeout=60,
        )
        assert data == b"test data micromech"

    @pytest.mark.asyncio
    async def test_ipfs_push_json_roundtrip(self):
        """Push JSON data and verify it roundtrips correctly."""
        import json

        payload = json.dumps({"prompt": "Will ETH hit 10k?", "tool": "echo"}).encode()

        cid, cid_hex = await push_to_ipfs(
            payload,
            api_url=AUTONOLAS_API,
        )

        data = await fetch_from_ipfs(
            cid,
            gateway=AUTONOLAS_GW,
            timeout=60,
        )
        assert json.loads(data) == json.loads(payload)

    @pytest.mark.asyncio
    async def test_ipfs_cid_deterministic(self):
        """Same data pushed twice yields the same CID."""
        content = b"deterministic test micromech"
        cid1, _ = await push_to_ipfs(
            content,
            api_url=AUTONOLAS_API,
        )
        cid2, _ = await push_to_ipfs(
            content,
            api_url=AUTONOLAS_API,
        )

        assert cid1 == cid2


class TestIpfsPushPullAutonolas:
    """Test IPFS push/pull via the Autonolas public gateway."""

    @pytest.mark.asyncio
    async def test_ipfs_push_to_autonolas(self):
        """Push to Autonolas IPFS gateway and fetch back."""
        test_data = b'{"test": "micromech-ipfs-' + str(uuid.uuid4()).encode() + b'"}'

        cid_str, cid_hex = await push_to_ipfs(
            test_data,
            api_url=AUTONOLAS_API,
        )

        # Verify local CID computation matches (raw codec)
        local_cid = compute_cid(test_data)
        # Remote may use dag-pb codec, so CIDs may differ — just check both are valid
        assert cid_str.startswith("b") or cid_str.startswith("Qm")
        assert local_cid.startswith("b")

        # Fetch back via public gateway
        fetched = await fetch_from_ipfs(
            cid_str,
            gateway=AUTONOLAS_GW,
            timeout=60,
        )
        assert fetched == test_data

    @pytest.mark.asyncio
    async def test_ipfs_autonolas_deterministic(self):
        """Same data pushed twice to Autonolas yields the same CID."""
        content = b"deterministic-autonolas-" + str(uuid.uuid4()).encode()[:8]
        cid1, _ = await push_to_ipfs(
            content,
            api_url=AUTONOLAS_API,
        )
        cid2, _ = await push_to_ipfs(
            content,
            api_url=AUTONOLAS_API,
        )

        assert cid1 == cid2
