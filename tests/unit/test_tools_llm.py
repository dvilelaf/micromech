"""Tests for LLM and prediction tools (unit, with mocks)."""

import json
from unittest.mock import MagicMock, patch

import pytest

# lxml (from ddgs) and llama_cpp C extensions segfault in the same process
pytestmark = pytest.mark.forked


class TestLlmTool:
    """Test llm_tool.run() with mocked llama-cpp and huggingface-hub."""

    def _make_mock_llm(self, content: str = "Hello, world!"):
        """Create a mock Llama instance that returns a canned response."""
        mock_llm = MagicMock()
        mock_llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }
        return mock_llm

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_returns_4_tuple(self):
        """run() returns a valid MechResponse 4-tuple."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod

        importlib.reload(llm_mod)

        mock_llm = self._make_mock_llm("Test response")
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            result = llm_mod.run(prompt="What is 2+2?")

        assert isinstance(result, tuple)
        assert len(result) == 4
        result_str, prompt_echo, metadata, counter = result
        assert prompt_echo == "What is 2+2?"
        assert metadata is None
        assert counter is None

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_returns_valid_json_with_result_key(self):
        """run() returns JSON with 'result' key."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod

        importlib.reload(llm_mod)

        mock_llm = self._make_mock_llm("The answer is 4.")
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            result_str, _, _, _ = llm_mod.run(prompt="What is 2+2?")

        data = json.loads(result_str)
        assert "result" in data
        assert data["result"] == "The answer is 4."
        assert "model" in data
        assert "tokens" in data
        assert data["tokens"] == 15

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_calls_create_chat_completion_with_correct_messages(self):
        """run() passes system + user messages to LLM."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod

        importlib.reload(llm_mod)

        mock_llm = self._make_mock_llm("response")
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            llm_mod.run(prompt="Hello", system_prompt="Be concise.")

        mock_llm.create_chat_completion.assert_called_once()
        call_kwargs = mock_llm.create_chat_completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        if messages is None:
            messages = call_kwargs[0][0] if call_kwargs[0] else None
        # Handle both positional and keyword argument styles
        if messages is None:
            messages = call_kwargs.kwargs.get("messages", call_kwargs[0][0])
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "Be concise."
        assert messages[1]["role"] == "user"
        assert messages[1]["content"] == "Hello"

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_passes_counter_callback_through(self):
        """Counter callback is returned as-is in the 4th element."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod

        importlib.reload(llm_mod)

        mock_llm = self._make_mock_llm("ok")
        mock_counter = MagicMock()
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            _, _, _, counter = llm_mod.run(prompt="test", counter_callback=mock_counter)

        assert counter is mock_counter

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_custom_temperature_and_max_tokens(self):
        """Custom temperature and max_tokens are forwarded to LLM."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod

        importlib.reload(llm_mod)

        mock_llm = self._make_mock_llm("ok")
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            llm_mod.run(prompt="test", temperature=0.9, max_tokens=512)

        call_kwargs = mock_llm.create_chat_completion.call_args
        assert (
            call_kwargs.kwargs.get("temperature") == 0.9 or call_kwargs[1].get("temperature") == 0.9
        )
        assert (
            call_kwargs.kwargs.get("max_tokens") == 512 or call_kwargs[1].get("max_tokens") == 512
        )


class TestPredictionRequestTool:
    """Test prediction_request.run() with mocked LLM."""

    def _make_mock_llm(self, content: str):
        mock_llm = MagicMock()
        mock_llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": content}}],
            "usage": {"total_tokens": 20},
        }
        return mock_llm

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_returns_valid_prediction_json(self):
        """run() returns prediction with p_yes, p_no, confidence, info_utility."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod
        import micromech.tools.prediction_request.prediction_request as pred_mod

        importlib.reload(llm_mod)
        importlib.reload(pred_mod)

        llm_response = json.dumps(
            {"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.5}
        )
        mock_llm = self._make_mock_llm(llm_response)

        # prediction_request imports _get_llm from llm_tool at call time
        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            result_str, prompt_echo, metadata, counter = pred_mod.run(
                prompt="Will ETH hit 10k by 2027?"
            )

        data = json.loads(result_str)
        assert "p_yes" in data
        assert "p_no" in data
        assert "confidence" in data
        assert "info_utility" in data
        assert abs(data["p_yes"] + data["p_no"] - 1.0) < 0.02

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_uses_prediction_prompt_template(self):
        """run() formats the PREDICTION_PROMPT with user_prompt."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod
        import micromech.tools.prediction_request.prediction_request as pred_mod

        importlib.reload(llm_mod)
        importlib.reload(pred_mod)

        llm_response = json.dumps(
            {"p_yes": 0.5, "p_no": 0.5, "confidence": 0.5, "info_utility": 0.0}
        )
        mock_llm = self._make_mock_llm(llm_response)

        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            _, prompt_echo, _, _ = pred_mod.run(
                prompt="Will BTC hit 100k?",
                additional_information="Current price is 60k.",
            )

        # The prompt_echo should be the formatted PREDICTION_PROMPT
        assert "Will BTC hit 100k?" in prompt_echo
        assert "Current price is 60k." in prompt_echo
        assert "USER_PROMPT" in prompt_echo
        assert "OUTPUT_FORMAT" in prompt_echo

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_handles_llm_error_gracefully(self):
        """On LLM error, returns default prediction (0.5/0.5/0.0/0.0)."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod
        import micromech.tools.prediction_request.prediction_request as pred_mod

        importlib.reload(llm_mod)
        importlib.reload(pred_mod)

        with patch.object(llm_mod, "_get_llm", side_effect=RuntimeError("Model failed to load")):
            result_str, _, _, _ = pred_mod.run(prompt="Will ETH hit 10k?")

        data = json.loads(result_str)
        assert data["p_yes"] == 0.5
        assert data["p_no"] == 0.5
        assert data["confidence"] == 0.0
        assert data["info_utility"] == 0.0

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_handles_malformed_llm_output(self):
        """If LLM returns non-JSON, returns default prediction."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod
        import micromech.tools.prediction_request.prediction_request as pred_mod

        importlib.reload(llm_mod)
        importlib.reload(pred_mod)

        mock_llm = self._make_mock_llm("I think the probability is about 70%")

        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            result_str, _, _, _ = pred_mod.run(prompt="Will ETH hit 10k?")

        data = json.loads(result_str)
        # Should be valid JSON with required fields (defaults if parse failed)
        assert "p_yes" in data
        assert "p_no" in data
        assert abs(data["p_yes"] + data["p_no"] - 1.0) < 0.02

    @patch.dict("sys.modules", {"llama_cpp": MagicMock(), "huggingface_hub": MagicMock()})
    def test_run_normalizes_probabilities(self):
        """Probabilities that don't sum to 1 get normalized."""
        import importlib

        import micromech.tools.llm_tool.llm_tool as llm_mod
        import micromech.tools.prediction_request.prediction_request as pred_mod

        importlib.reload(llm_mod)
        importlib.reload(pred_mod)

        # Return probabilities that sum to 2.0
        llm_response = json.dumps(
            {"p_yes": 0.8, "p_no": 1.2, "confidence": 0.7, "info_utility": 0.3}
        )
        mock_llm = self._make_mock_llm(llm_response)

        with patch.object(llm_mod, "_get_llm", return_value=mock_llm):
            result_str, _, _, _ = pred_mod.run(prompt="test")

        data = json.loads(result_str)
        assert abs(data["p_yes"] + data["p_no"] - 1.0) < 0.02


class TestPredictionHelpers:
    """Test _extract_json and _validate_prediction directly."""

    def test_extract_json_from_markdown(self):
        from micromech.tools.prediction_request.prediction_request import _extract_json

        text = '```json\n{"p_yes": 0.6, "p_no": 0.4}\n```'
        result = _extract_json(text)
        data = json.loads(result)
        assert data["p_yes"] == 0.6

    def test_extract_json_plain(self):
        from micromech.tools.prediction_request.prediction_request import _extract_json

        text = '{"p_yes": 0.5, "p_no": 0.5}'
        result = _extract_json(text)
        assert json.loads(result)["p_yes"] == 0.5

    def test_validate_prediction_adds_missing_fields(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        raw = json.dumps({"p_yes": 0.6, "p_no": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert "confidence" in result
        assert "info_utility" in result

    def test_validate_prediction_invalid_json(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        result = json.loads(_validate_prediction("not json at all"))
        assert result["p_yes"] == 0.5
        assert result["p_no"] == 0.5
