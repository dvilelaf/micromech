"""Tests for gemma4_api_tool."""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestGemma4ApiTool:
    """Test gemma4_api_tool.run() with mocked Google GenAI client."""

    def _make_mock_response(self, content="Hello!", tokens=20):
        resp = MagicMock()
        resp.text = content
        resp.usage_metadata = MagicMock(total_token_count=tokens)
        return resp

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("micromech.tools.gemma4_api_tool.gemma4_api_tool._get_client")
    def test_run_returns_4_tuple(self, mock_get_client):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import run

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = (
            self._make_mock_response()
        )
        mock_get_client.return_value = mock_client

        result = run(prompt="What is 2+2?")
        assert isinstance(result, tuple)
        assert len(result) == 4
        result_str, prompt_echo, metadata, counter = result
        assert prompt_echo == "What is 2+2?"
        assert metadata is None
        assert counter is None

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("micromech.tools.gemma4_api_tool.gemma4_api_tool._get_client")
    def test_run_returns_valid_json(self, mock_get_client):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import run

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = (
            self._make_mock_response("The answer is 4.", tokens=15)
        )
        mock_get_client.return_value = mock_client

        result_str, _, _, _ = run(prompt="What is 2+2?")
        data = json.loads(result_str)
        assert data["result"] == "The answer is 4."
        assert data["tokens"] == 15
        assert data["model"] == "gemma-4-27b-it"

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("micromech.tools.gemma4_api_tool.gemma4_api_tool._get_client")
    def test_unknown_model_falls_back(self, mock_get_client):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import run

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = (
            self._make_mock_response()
        )
        mock_get_client.return_value = mock_client

        result_str, _, _, _ = run(prompt="test", model="nonexistent")
        data = json.loads(result_str)
        assert data["model"] == "gemma-4-27b-it"

    def test_missing_api_key_raises(self):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import _get_client

        with patch.dict("os.environ", {}, clear=True):
            import os
            os.environ.pop("GOOGLE_API_KEY", None)
            with pytest.raises(RuntimeError, match="GOOGLE_API_KEY"):
                _get_client()

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("micromech.tools.gemma4_api_tool.gemma4_api_tool._get_client")
    def test_counter_callback_passed_through(self, mock_get_client):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import run

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = (
            self._make_mock_response()
        )
        mock_get_client.return_value = mock_client

        mock_cb = MagicMock()
        _, _, _, counter = run(prompt="test", counter_callback=mock_cb)
        assert counter is mock_cb

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("micromech.tools.gemma4_api_tool.gemma4_api_tool._get_client")
    def test_empty_response_text(self, mock_get_client):
        from micromech.tools.gemma4_api_tool.gemma4_api_tool import run

        resp = MagicMock()
        resp.text = None
        resp.usage_metadata = None
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = resp
        mock_get_client.return_value = mock_client

        result_str, _, _, _ = run(prompt="test")
        data = json.loads(result_str)
        assert data["result"] == ""
        assert data["tokens"] == 0
