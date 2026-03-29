"""Integration test: IPFS push and pull with a local node.

Skips automatically if no local IPFS node is available.

Run:
  # Start local IPFS node
  ipfs daemon

  # Run the test
  uv run pytest tests/integration/test_ipfs_push.py -v -s
"""

import pytest

from micromech.ipfs.client import fetch_from_ipfs, push_to_ipfs


class TestIpfsPushPull:
    """Test real IPFS push/pull if local node available."""

    @pytest.mark.asyncio
    async def test_ipfs_push_and_pull(self):
        """Push data to local IPFS, then fetch it back and verify roundtrip."""
        try:
            cid, cid_hex = await push_to_ipfs(
                b"test data micromech",
                api_url="http://localhost:5001",
            )
        except Exception:
            pytest.skip("No local IPFS node available")

        # If push succeeded, try to pull it back
        data = await fetch_from_ipfs(
            cid,
            gateway="http://localhost:8080/ipfs/",
        )
        assert data == b"test data micromech"

    @pytest.mark.asyncio
    async def test_ipfs_push_json_roundtrip(self):
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
    async def test_ipfs_cid_deterministic(self):
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
