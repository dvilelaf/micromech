"""Integration test: LLM tool with real model download and inference.

Downloads Qwen 0.5B model (~400MB on first run), then tests:
1. Echo tool (baseline)
2. LLM tool with a simple prompt
3. Prediction tool with a prediction market question

Skips if llama-cpp-python is not installed.

Run:
  uv run pytest tests/integration/test_llm_tool.py -v -s
"""

import json

import pytest


@pytest.fixture(scope="module")
def llm_available():
    """Check if llama-cpp-python is installed."""
    try:
        from llama_cpp import Llama  # noqa: F401

        return True
    except ImportError:
        pytest.skip("llama-cpp-python not installed")


class TestEchoToolBaseline:
    """Baseline: echo tool always works (no LLM needed)."""

    def test_echo_tool(self):
        from micromech.tools.echo_tool.echo_tool import run

        result, prompt, metadata, cb = run(prompt="hello world", tool="echo")
        parsed = json.loads(result)
        assert parsed["p_yes"] == 0.5
        assert parsed["p_no"] == 0.5
        assert prompt == "hello world"


class TestLLMTool:
    """Test the LLM tool with a real model."""

    def test_llm_tool_simple(self, llm_available):
        """LLM tool returns a valid response for a simple prompt."""
        from micromech.tools.local_llm.local_llm import run

        result, prompt, metadata, cb = run(prompt="What is 2 + 2? Answer with just the number.")
        assert result is not None
        parsed = json.loads(result)
        assert "result" in parsed
        assert len(parsed["result"]) > 0

    def test_llm_tool_returns_model_info(self, llm_available):
        """LLM tool response includes model name and token count."""
        from micromech.tools.local_llm.local_llm import run

        result, prompt, metadata, cb = run(prompt="Say hello.")
        parsed = json.loads(result)
        assert "model" in parsed
        assert "tokens" in parsed
        assert parsed["tokens"] >= 0


class TestPredictionTool:
    """Test the prediction tool with a real LLM."""

    def test_prediction_tool_real(self, llm_available):
        """Prediction tool returns valid p_yes/p_no that sum to ~1.0."""
        from micromech.tools.prediction_request.prediction_request import run

        result, prompt, metadata, cb = run(
            prompt="Will Bitcoin exceed $200,000 by December 2026?",
            tool="prediction-offline",
        )
        assert result is not None
        parsed = json.loads(result)
        assert "p_yes" in parsed
        assert "p_no" in parsed
        assert "confidence" in parsed
        assert "info_utility" in parsed
        # p_yes + p_no should be ~1.0 (the tool normalizes them)
        assert abs(parsed["p_yes"] + parsed["p_no"] - 1.0) < 0.1

    def test_prediction_tool_values_in_range(self, llm_available):
        """All prediction values are within [0, 1]."""
        from micromech.tools.prediction_request.prediction_request import run

        result, prompt, metadata, cb = run(
            prompt="Will it rain in London tomorrow?",
            tool="prediction-offline",
        )
        parsed = json.loads(result)
        for field in ("p_yes", "p_no", "confidence", "info_utility"):
            assert 0.0 <= parsed[field] <= 1.0, f"{field} = {parsed[field]} out of range"
