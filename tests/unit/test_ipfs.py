"""Tests for IPFS client and CID helpers."""

import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.ipfs.client import (
    cid_hex_to_multihash_bytes,
    compute_cid,
    compute_cid_hex,
    fetch_from_ipfs,
    fetch_json_from_ipfs,
    is_ipfs_multihash,
    multihash_to_cid,
    push_json_to_ipfs,
    push_to_ipfs,
)


class TestComputeCid:
    def test_deterministic(self):
        data = b"hello world"
        cid1 = compute_cid(data)
        cid2 = compute_cid(data)
        assert cid1 == cid2

    def test_starts_with_bafkrei(self):
        cid = compute_cid(b"test data")
        assert cid.startswith("bafkrei")

    def test_different_data_different_cid(self):
        cid1 = compute_cid(b"data1")
        cid2 = compute_cid(b"data2")
        assert cid1 != cid2

    def test_empty_data(self):
        cid = compute_cid(b"")
        assert cid.startswith("bafkrei")


class TestComputeCidHex:
    def test_starts_with_f(self):
        cid_hex = compute_cid_hex(b"test")
        assert cid_hex.startswith("f0155")

    def test_contains_sha256(self):
        data = b"test data"
        sha256 = hashlib.sha256(data).hexdigest()
        cid_hex = compute_cid_hex(data)
        assert sha256 in cid_hex


class TestCidHexToMultihashBytes:
    def test_extracts_multihash(self):
        data = b"test"
        cid_hex = compute_cid_hex(data)
        mh = cid_hex_to_multihash_bytes(cid_hex)
        assert len(mh) == 34
        assert mh[0] == 0x12  # sha2-256 function code
        assert mh[1] == 0x20  # 32 bytes digest length
        assert mh[2:] == hashlib.sha256(data).digest()


class TestIsIpfsMultihash:
    def test_valid_multihash(self):
        digest = hashlib.sha256(b"test").digest()
        mh = bytes([0x12, 0x20]) + digest
        assert is_ipfs_multihash(mh) is True

    def test_too_short(self):
        assert is_ipfs_multihash(b"\x12\x20") is False

    def test_wrong_prefix(self):
        assert is_ipfs_multihash(bytes(34)) is False

    def test_json_data(self):
        data = json.dumps({"prompt": "test"}).encode()
        assert is_ipfs_multihash(data) is False

    def test_empty(self):
        assert is_ipfs_multihash(b"") is False


class TestMultihashToCid:
    def test_roundtrip(self):
        data = b"test data for roundtrip"
        cid_hex = compute_cid_hex(data)
        mh_bytes = cid_hex_to_multihash_bytes(cid_hex)
        cid = multihash_to_cid(mh_bytes)
        assert cid == compute_cid(data)

    def test_invalid_multihash(self):
        with pytest.raises(ValueError, match="Invalid multihash"):
            multihash_to_cid(b"too short")

    def test_wrong_prefix(self):
        with pytest.raises(ValueError, match="Invalid multihash"):
            multihash_to_cid(bytes(34))


class TestFetchFromIpfs:
    @pytest.mark.asyncio
    async def test_fetch_success(self):
        """fetch_from_ipfs returns raw bytes from gateway."""
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.read = AsyncMock(return_value=b'{"result": "hello"}')

        mock_session = AsyncMock()
        mock_session.get = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_resp),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("micromech.ipfs.client.aiohttp.ClientSession", return_value=mock_session_ctx):
            data = await fetch_from_ipfs("bafkrei123", gateway="https://gw.test/ipfs/")

        assert data == b'{"result": "hello"}'

    @pytest.mark.asyncio
    async def test_fetch_json_parses(self):
        """fetch_json_from_ipfs returns parsed dict."""
        payload = {"prompt": "test", "tool": "echo"}

        with patch(
            "micromech.ipfs.client.fetch_from_ipfs",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = json.dumps(payload).encode()
            result = await fetch_json_from_ipfs("bafkrei123")

        assert result == payload


class TestPushToIpfs:
    @pytest.mark.asyncio
    async def test_push_without_iwa(self):
        """Without iwa, computes CID locally."""
        with patch.dict("sys.modules", {"iwa.core.ipfs": None}):
            # Force ImportError path
            with patch("micromech.ipfs.client.push_to_ipfs", wraps=push_to_ipfs):
                cid_str, cid_hex = await push_to_ipfs(b"test data")
        assert cid_str.startswith("bafkrei")
        assert cid_hex.startswith("f0155")

    @pytest.mark.asyncio
    async def test_push_json_to_ipfs(self):
        """push_json_to_ipfs serializes and delegates to push_to_ipfs."""
        with patch("micromech.ipfs.client.push_to_ipfs", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = ("bafkrei_test", "f0155_test")
            cid_str, cid_hex = await push_json_to_ipfs({"key": "value"})

        assert cid_str == "bafkrei_test"
        assert cid_hex == "f0155_test"
        # Verify the data passed is compact JSON
        call_data = mock_push.call_args[0][0]
        assert call_data == b'{"key":"value"}'


class TestPrepareRequestData:
    def test_local_fallback_returns_34_bytes(self):
        """Test the local CID computation path (no IPFS push)."""
        data = b'{"prompt":"test","tool":"echo"}'
        cid_hex = compute_cid_hex(data)
        mh = cid_hex_to_multihash_bytes(cid_hex)
        assert len(mh) == 34
        assert mh[0] == 0x12
        assert mh[1] == 0x20

    def test_local_fallback_deterministic(self):
        data = b'{"prompt":"test","tool":"echo"}'
        mh1 = cid_hex_to_multihash_bytes(compute_cid_hex(data))
        mh2 = cid_hex_to_multihash_bytes(compute_cid_hex(data))
        assert mh1 == mh2


class TestComputeCidHexFormat:
    """Verify CID hex produces correct f01551220... format."""

    def test_exact_prefix(self):
        cid_hex = compute_cid_hex(b"hello")
        assert cid_hex[:9] == "f01551220"

    def test_total_length(self):
        """f(1) + version(2) + codec(2) + hash_fn(2) + hash_len(2) + digest(64) = 73."""
        cid_hex = compute_cid_hex(b"some data")
        assert len(cid_hex) == 73

    def test_digest_matches_sha256(self):
        data = b"verify digest"
        digest_hex = hashlib.sha256(data).hexdigest()
        cid_hex = compute_cid_hex(data)
        assert cid_hex.endswith(digest_hex)


class TestCidHexToMultihashBytesExtraction:
    """Verify extraction of 34-byte multihash from CID hex."""

    def test_prefix_bytes(self):
        mh = cid_hex_to_multihash_bytes(compute_cid_hex(b"data"))
        assert mh[0] == 0x12
        assert mh[1] == 0x20

    def test_digest_content(self):
        data = b"check digest"
        mh = cid_hex_to_multihash_bytes(compute_cid_hex(data))
        assert mh[2:] == hashlib.sha256(data).digest()

    def test_length_is_34(self):
        mh = cid_hex_to_multihash_bytes(compute_cid_hex(b"x"))
        assert len(mh) == 34


class TestMultihashToCidRoundtrip:
    """Verify data -> compute_cid_hex -> cid_hex_to_multihash_bytes -> multihash_to_cid produces valid bafkrei CID."""

    def test_roundtrip_matches_compute_cid(self):
        data = b"roundtrip test"
        cid_direct = compute_cid(data)
        cid_hex = compute_cid_hex(data)
        mh = cid_hex_to_multihash_bytes(cid_hex)
        cid_roundtrip = multihash_to_cid(mh)
        assert cid_roundtrip == cid_direct

    def test_roundtrip_starts_with_bafkrei(self):
        data = b"another roundtrip"
        cid_hex = compute_cid_hex(data)
        mh = cid_hex_to_multihash_bytes(cid_hex)
        cid = multihash_to_cid(mh)
        assert cid.startswith("bafkrei")

    def test_roundtrip_empty_data(self):
        data = b""
        cid_direct = compute_cid(data)
        mh = cid_hex_to_multihash_bytes(compute_cid_hex(data))
        assert multihash_to_cid(mh) == cid_direct


class TestIsIpfsMultihashEdgeCases:
    """Additional edge cases for is_ipfs_multihash."""

    def test_exactly_34_bytes_wrong_function_code(self):
        data = bytes([0x11, 0x20]) + b"\x00" * 32
        assert is_ipfs_multihash(data) is False

    def test_exactly_34_bytes_wrong_length_byte(self):
        data = bytes([0x12, 0x21]) + b"\x00" * 32
        assert is_ipfs_multihash(data) is False

    def test_35_bytes_not_multihash(self):
        data = bytes([0x12, 0x20]) + b"\x00" * 33
        assert is_ipfs_multihash(data) is False

    def test_33_bytes_not_multihash(self):
        data = bytes([0x12, 0x20]) + b"\x00" * 31
        assert is_ipfs_multihash(data) is False


class TestCidCompatibilityWithIwa:
    """Verify micromech CID computation matches iwa's format exactly."""

    def test_manual_computation_matches(self):
        data = b"test data"
        digest = hashlib.sha256(data).digest()
        expected = "f" + bytes([0x01, 0x55, 0x12, 0x20]).hex() + digest.hex()
        assert compute_cid_hex(data) == expected

    def test_various_payloads(self):
        for payload in [b"", b"x", b"\x00" * 100, b'{"key":"value"}']:
            digest = hashlib.sha256(payload).digest()
            expected = "f" + bytes([0x01, 0x55, 0x12, 0x20]).hex() + digest.hex()
            assert compute_cid_hex(payload) == expected, f"Failed for payload {payload!r}"

    def test_json_payload_like_valory(self):
        """JSON response payload produces same CID as manual SHA-256."""
        payload = json.dumps(
            {"requestId": "0xabc", "result": "yes", "prompt": "test", "tool": "echo"},
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        expected = "f" + bytes([0x01, 0x55, 0x12, 0x20]).hex() + digest.hex()
        assert compute_cid_hex(payload) == expected
