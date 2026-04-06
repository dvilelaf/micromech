"""Tests for the demo_requests IPFS push function."""

import hashlib
import json
import uuid
from unittest.mock import patch

from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid_hex

# Import the function under test
from scripts.demo_requests import _push_request_to_ipfs


def _call_push(prompt: str, tool: str, nonce: str | None = None) -> bytes:
    """Call _push_request_to_ipfs with mocked network and optionally fixed nonce."""
    patches = [patch("requests.post", side_effect=ConnectionError("no network"))]
    if nonce:
        patches.append(patch("scripts.demo_requests.uuid.uuid4", return_value=uuid.UUID(nonce)))

    result = None
    for p in patches:
        p.__enter__()
    try:
        result = _push_request_to_ipfs(prompt, tool, "http://fake:5001")
    finally:
        for p in patches:
            p.__exit__(None, None, None)
    return result


class TestPushRequestToIpfs:
    """Tests for scripts/demo_requests.py::_push_request_to_ipfs."""

    def test_returns_34_byte_multihash(self):
        mh = _call_push("Will BTC hit 100k?", "echo")
        assert len(mh) == 34
        assert mh[0] == 0x12
        assert mh[1] == 0x20

    def test_multihash_digest_matches_sha256(self):
        """The 32-byte digest in the multihash must match SHA-256 of the serialized JSON."""
        prompt, tool = "test prompt", "llm"
        fixed_nonce = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        mh = _call_push(prompt, tool, nonce=fixed_nonce)

        metadata = {
            "prompt": prompt,
            "tool": tool,
            "nonce": fixed_nonce,
            "schema_version": "2.0",
        }
        expected_bytes = json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8")
        expected_digest = hashlib.sha256(expected_bytes).digest()
        assert mh[2:] == expected_digest

    def test_metadata_format_valory_v2(self):
        """Metadata JSON has required Valory v2 fields: prompt, tool, nonce, schema_version."""
        fixed_nonce = "11111111-2222-3333-4444-555555555555"
        mh = _call_push("q", "echo", nonce=fixed_nonce)

        metadata = {
            "prompt": "q",
            "tool": "echo",
            "nonce": fixed_nonce,
            "schema_version": "2.0",
        }
        json_bytes = json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8")
        expected_mh = cid_hex_to_multihash_bytes(compute_cid_hex(json_bytes))
        assert mh == expected_mh

    def test_json_serialization_format(self):
        """JSON uses ensure_ascii=False and indent=4 (pretty-printed, not compact)."""
        metadata = {
            "prompt": "test with unicode \u00e9",
            "tool": "echo",
            "nonce": "fake-nonce",
            "schema_version": "2.0",
        }
        serialized = json.dumps(metadata, ensure_ascii=False, indent=4)
        # ensure_ascii=False means unicode chars are NOT escaped
        assert "\u00e9" in serialized
        assert "\\u00e9" not in serialized
        # indent=4 means newlines and spaces
        assert "\n" in serialized
        assert "    " in serialized

    def test_schema_version_is_2_0(self):
        """The metadata must include schema_version='2.0' for Valory v2 compatibility."""
        fixed_nonce = "00000000-0000-0000-0000-000000000000"
        metadata = {
            "prompt": "p",
            "tool": "t",
            "nonce": fixed_nonce,
            "schema_version": "2.0",
        }
        json_bytes = json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8")

        mh = _call_push("p", "t", nonce=fixed_nonce)

        expected_digest = hashlib.sha256(json_bytes).digest()
        assert mh[2:] == expected_digest
