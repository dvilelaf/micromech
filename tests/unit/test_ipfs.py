"""Tests for IPFS client and CID helpers."""

import hashlib
import json

import pytest

from micromech.ipfs.client import (
    cid_hex_to_multihash_bytes,
    compute_cid,
    compute_cid_hex,
    is_ipfs_multihash,
    multihash_to_cid,
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
