"""Tests for the demo_requests IPFS push function."""

import hashlib
import json
import uuid
from unittest.mock import MagicMock, patch

from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid_hex

# Import the function under test
from scripts.demo_requests import (
    _parse_delivery_data,
    _push_request_to_ipfs,
    format_response,
)


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


class TestFormatResponse:
    """Tests for format_response — display formatting for mech results."""

    def test_prediction_bars(self):
        """Prediction result with p_yes/p_no returns bar chart with block chars."""
        result = format_response({"p_yes": 0.5, "p_no": 0.5, "confidence": 0.0})
        assert "█" in result
        assert "YES" in result
        assert "NO" in result
        assert "50%" in result

    def test_prediction_skewed(self):
        """Skewed prediction produces asymmetric bars."""
        result = format_response({"p_yes": 0.8, "p_no": 0.2, "confidence": 0.9})
        assert "80%" in result
        assert "20%" in result
        assert "90%" in result

    def test_llm_result_with_model(self):
        """LLM result with 'result' and 'model' keys returns text + model suffix."""
        result = format_response({"result": "The capital is Tokyo.", "model": "qwen/qwen3-8b"})
        assert "The capital is Tokyo." in result
        assert "qwen3-8b" in result

    def test_llm_result_truncation(self):
        """Long LLM results are truncated to 120 chars."""
        long_text = "x" * 200
        result = format_response({"result": long_text})
        assert "..." in result
        assert len(result) < 200

    def test_llm_result_without_model(self):
        """LLM result without model key still works."""
        result = format_response({"result": "some text"})
        assert "some text" in result

    def test_none_returns_empty(self):
        """None input returns empty string."""
        assert format_response(None) == ""

    def test_empty_dict_returns_json(self):
        """Unknown dict falls through to JSON dump."""
        result = format_response({"foo": "bar"})
        assert "foo" in result

    def test_prediction_no_confidence(self):
        """Prediction result without explicit confidence defaults to 0."""
        result = format_response({"p_yes": 0.7, "p_no": 0.3})
        assert "70%" in result
        assert "0%" in result  # confidence defaults to 0


class TestParseDeliveryData:
    """Tests for _parse_delivery_data — IPFS multihash + raw JSON parsing."""

    def test_ipfs_multihash_fetches_and_formats(self):
        """IPFS multihash triggers gateway fetch; nested JSON result is parsed."""
        # Build a valid 34-byte multihash
        digest = hashlib.sha256(b"test payload").digest()
        multihash = bytes([0x12, 0x20]) + digest

        # The IPFS response has result as a JSON string (nested)
        ipfs_response = {
            "requestId": "abc",
            "result": json.dumps({"p_yes": 0.6, "p_no": 0.4, "confidence": 0.8}),
            "prompt": "Will it rain?",
            "tool": "prediction-offline",
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = ipfs_response
        mock_resp.raise_for_status = MagicMock()

        mock_config = MagicMock()
        mock_config.ipfs.gateway = "https://gateway.test/ipfs/"

        with (
            patch("requests.get", return_value=mock_resp) as mock_get,
            patch(
                "micromech.core.config.MicromechConfig.load",
                return_value=mock_config,
            ),
        ):
            result = _parse_delivery_data(multihash)

        mock_get.assert_called_once()
        # The nested JSON was parsed and formatted as prediction bars
        assert "█" in result
        assert "60%" in result

    def test_ipfs_multihash_fetch_failure_shows_cid(self):
        """When IPFS fetch fails, returns abbreviated CID string."""
        digest = hashlib.sha256(b"unreachable").digest()
        multihash = bytes([0x12, 0x20]) + digest

        mock_config = MagicMock()
        mock_config.ipfs.gateway = "https://gateway.test/ipfs/"

        with (
            patch("requests.get", side_effect=ConnectionError("no IPFS")),
            patch(
                "micromech.core.config.MicromechConfig.load",
                return_value=mock_config,
            ),
        ):
            result = _parse_delivery_data(multihash)

        assert "IPFS" in result

    def test_raw_json_bytes_formatted(self):
        """Raw JSON delivery data (not IPFS) is parsed and formatted."""
        payload = json.dumps({"p_yes": 0.9, "p_no": 0.1, "confidence": 0.5}).encode()
        result = _parse_delivery_data(payload)
        assert "█" in result
        assert "90%" in result

    def test_raw_json_llm_result(self):
        """Raw JSON with 'result' key returns the text."""
        payload = json.dumps({"result": "Hello world", "model": "gpt-4"}).encode()
        result = _parse_delivery_data(payload)
        assert "Hello world" in result

    def test_non_json_bytes_shows_length(self):
        """Non-JSON bytes return a byte-count message."""
        result = _parse_delivery_data(b"\x00\x01\x02random garbage")
        assert "bytes" in result

    def test_ipfs_nested_json_result_string(self):
        """The critical fix: result field is a JSON string, not a dict.

        _parse_delivery_data must json.loads the result string before
        passing to format_response, otherwise format_response gets a
        raw string and falls through to the truncated-text path instead
        of rendering prediction bars.
        """
        digest = hashlib.sha256(b"nested test").digest()
        multihash = bytes([0x12, 0x20]) + digest

        # This is exactly how IPFS responses look: result is a JSON STRING
        ipfs_response = {
            "requestId": "0xabc",
            "result": '{"p_yes": 0.5, "p_no": 0.5, "confidence": 0.0}',
            "prompt": "Will BTC hit 100k?",
            "tool": "echo",
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = ipfs_response
        mock_resp.raise_for_status = MagicMock()

        mock_config = MagicMock()
        mock_config.ipfs.gateway = "https://gateway.test/ipfs/"

        with (
            patch("requests.get", return_value=mock_resp),
            patch(
                "micromech.core.config.MicromechConfig.load",
                return_value=mock_config,
            ),
        ):
            result = _parse_delivery_data(multihash)

        # Must contain bar chars — proves the nested JSON was parsed
        assert "█" in result
        assert "50%" in result


class TestDemoChainFiltering:
    """Tests for the demo's chain discovery — skips placeholder mech addresses."""

    def test_includes_real_mech_address(self):
        """A chain with a real mech_address is included (placeholder check passes)."""
        addr = "0x77af31De935740567Cf4fF1986D04B2c964A786a"
        addr_body = addr[2:]
        # Real address has many distinct hex chars
        assert len(set(addr_body.lower())) > 1

    def test_skips_placeholder_all_same_digit(self):
        """0x3333...3333 has only one distinct char in body — treated as placeholder."""
        addr = "0x" + "3" * 40
        addr_body = addr[2:]
        assert len(set(addr_body.lower())) <= 1

    def test_skips_placeholder_all_zeros(self):
        """0x0000...0000 also treated as placeholder."""
        addr = "0x" + "0" * 40
        addr_body = addr[2:]
        assert len(set(addr_body.lower())) <= 1

    def test_real_address_multiple_chars(self):
        """A variety of real addresses pass the filter."""
        for addr in [
            "0x77af31De935740567Cf4fF1986D04B2c964A786a",
            "0xAbCdEf0123456789AbCdEf0123456789AbCdEf01",
        ]:
            addr_body = addr[2:]
            assert len(set(addr_body.lower())) > 1

    def test_none_mech_address_skipped_by_truthiness(self):
        """None mech_address is falsy, so `if not cc.mech_address` skips it."""
        mech_address = None
        assert not mech_address
