"""Tests for the event listener."""

import json

import pytest

from micromech.core.config import MicromechConfig
from micromech.runtime.listener import EventListener


class TestParseRequestData:
    """Test request data parsing (no chain access needed)."""

    def test_parse_json_payload(self):
        listener = EventListener(MicromechConfig())
        data = json.dumps({"prompt": "Will ETH hit 10k?", "tool": "llm"}).encode()
        prompt, tool, extra = listener._parse_request_data(data)
        assert prompt == "Will ETH hit 10k?"
        assert tool == "llm"
        assert extra == {}

    def test_parse_json_with_extra_params(self):
        listener = EventListener(MicromechConfig())
        data = json.dumps(
            {
                "prompt": "test",
                "tool": "llm",
                "model": "qwen",
                "nonce": 42,
            }
        ).encode()
        prompt, tool, extra = listener._parse_request_data(data)
        assert prompt == "test"
        assert tool == "llm"
        assert extra == {"model": "qwen", "nonce": 42}

    def test_parse_empty_data(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(b"")
        assert prompt == ""
        assert tool == ""

    def test_parse_invalid_json(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(b"not json")
        assert prompt == ""
        assert tool == ""

    def test_parse_binary_data(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(bytes(range(256)))
        assert prompt == ""
        assert tool == ""

    @pytest.mark.asyncio
    async def test_poll_without_bridge_returns_empty(self):
        listener = EventListener(MicromechConfig(), bridge=None)
        result = await listener.poll_once()
        assert result == []

    def test_stop(self):
        listener = EventListener(MicromechConfig())
        listener._running = True
        listener.stop()
        assert listener._running is False
