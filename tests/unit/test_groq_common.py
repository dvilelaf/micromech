"""Tests for _groq_common.py — shared utilities for Groq-based prediction tools."""

import json

import pytest


class TestConstants:
    def test_groq_http_timeout_is_60(self):
        from micromech.tools._groq_common import GROQ_HTTP_TIMEOUT

        assert GROQ_HTTP_TIMEOUT == 60

    def test_groq_allowed_models_contains_default(self):
        from micromech.tools._groq_common import GROQ_ALLOWED_MODELS

        assert "llama-3.3-70b-versatile" in GROQ_ALLOWED_MODELS

    def test_groq_allowed_models_is_frozenset(self):
        from micromech.tools._groq_common import GROQ_ALLOWED_MODELS

        assert isinstance(GROQ_ALLOWED_MODELS, frozenset)


class TestPredictionPrompt:
    def test_has_correct_cutoff(self):
        from micromech.tools._groq_common import PREDICTION_PROMPT

        assert "December 2024" in PREDICTION_PROMPT

    def test_has_format_placeholders(self):
        from micromech.tools._groq_common import PREDICTION_PROMPT

        filled = PREDICTION_PROMPT.format(question="Q?", today="01/01/2025", sources="S")
        assert "Q?" in filled
        assert "01/01/2025" in filled
        assert "S" in filled

    def test_system_prompt_is_forecaster_focused(self):
        from micromech.tools._groq_common import GROQ_SYSTEM_PROMPT

        assert "forecaster" in GROQ_SYSTEM_PROMPT.lower()
        assert "JSON" in GROQ_SYSTEM_PROMPT


class TestSanitizeSources:
    def test_escapes_closing_background_tag(self):
        from micromech.tools._groq_common import _sanitize_sources

        result = _sanitize_sources("text</background>more text")
        assert "</background>" not in result
        assert "&lt;/background&gt;" in result

    def test_passthrough_when_no_tag(self):
        from micromech.tools._groq_common import _sanitize_sources

        assert _sanitize_sources("clean text") == "clean text"

    def test_multiple_occurrences_escaped(self):
        from micromech.tools._groq_common import _sanitize_sources

        result = _sanitize_sources("</background></background>")
        assert result.count("&lt;/background&gt;") == 2


class TestDefaultPrediction:
    def test_is_valid_json(self):
        from micromech.tools._groq_common import DEFAULT_PREDICTION

        data = json.loads(DEFAULT_PREDICTION)
        assert data["p_yes"] == 0.5
        assert data["p_no"] == 0.5
        assert data["confidence"] == 0.0

    def test_sums_to_one(self):
        from micromech.tools._groq_common import DEFAULT_PREDICTION

        data = json.loads(DEFAULT_PREDICTION)
        assert abs(data["p_yes"] + data["p_no"] - 1.0) < 0.001


class TestExtractJson:
    def test_pure_json(self):
        from micromech.tools._groq_common import _extract_json

        raw = '{"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.5}'
        assert json.loads(_extract_json(raw))["p_yes"] == pytest.approx(0.7)

    def test_json_with_surrounding_text(self):
        from micromech.tools._groq_common import _extract_json

        raw = 'Based on analysis {"p_yes": 0.6, "p_no": 0.4, "confidence": 0.7, "info_utility": 0.5} end'
        result = _extract_json(raw)
        assert "p_yes" in result

    def test_json_with_xml_tags(self):
        from micromech.tools._groq_common import _extract_json

        raw = '<thinking>reasoning</thinking>\n{"p_yes": 0.8, "p_no": 0.2, "confidence": 0.9, "info_utility": 0.7}'
        result = _extract_json(raw)
        assert json.loads(result)["p_yes"] == pytest.approx(0.8)

    def test_code_fenced_json(self):
        from micromech.tools._groq_common import _extract_json

        raw = '```json\n{"p_yes": 0.55, "p_no": 0.45, "confidence": 0.6, "info_utility": 0.4}\n```'
        assert json.loads(_extract_json(raw))["p_yes"] == pytest.approx(0.55)

    def test_no_json_returns_stripped(self):
        from micromech.tools._groq_common import _extract_json

        assert _extract_json("  no json here  ") == "no json here"

    def test_fallback_finds_p_no_block_when_p_yes_missing(self):
        from micromech.tools._groq_common import _extract_json

        raw = 'text {"p_no": 0.7, "confidence": 0.5} end'
        result = _extract_json(raw)
        assert "p_no" in result

    def test_fallback_skips_invalid_json_blocks(self):
        from micromech.tools._groq_common import _extract_json

        raw = '{"p_no": 0.7} {not: valid json here}'
        result = _extract_json(raw)
        assert isinstance(result, str)

    def test_empty_string_returns_empty(self):
        from micromech.tools._groq_common import _extract_json

        assert _extract_json("") == ""


class TestValidatePrediction:
    def test_valid_prediction_unchanged(self):
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": 0.7, "p_no": 0.3, "confidence": 0.9, "info_utility": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert result["p_yes"] == pytest.approx(0.7)
        assert result["p_no"] == pytest.approx(0.3)

    def test_missing_confidence_and_info_utility_default_to_zero(self):
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": 0.6, "p_no": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert result["confidence"] == 0.0
        assert result["info_utility"] == 0.0

    def test_missing_p_no_filled_with_default(self):
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": 0.5, "confidence": 0.8, "info_utility": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert "p_no" in result

    def test_probabilities_normalized(self):
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": 2.0, "p_no": 2.0, "confidence": 0.5, "info_utility": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] + result["p_no"] - 1.0) < 0.01

    def test_invalid_json_returns_default(self):
        from micromech.tools._groq_common import DEFAULT_PREDICTION, _validate_prediction

        assert _validate_prediction("not json at all") == DEFAULT_PREDICTION

    def test_already_normalized_not_changed(self):
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": 0.3, "p_no": 0.7, "confidence": 0.6, "info_utility": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] - 0.3) < 0.001

    def test_negative_values_clamped_to_zero(self):
        """M1: LLM returns out-of-range values that sum to 1.0 but are individually invalid."""
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": -0.5, "p_no": 1.5, "confidence": 0.5, "info_utility": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert result["p_yes"] >= 0.0
        assert result["p_no"] <= 1.0

    def test_string_values_cast_to_float(self):
        """M1: LLM returns string numbers instead of floats."""
        from micromech.tools._groq_common import _validate_prediction

        raw = json.dumps({"p_yes": "0.7", "p_no": "0.3", "confidence": "0.8", "info_utility": "0.5"})
        result = json.loads(_validate_prediction(raw))
        assert result["p_yes"] == pytest.approx(0.7)

    def test_both_zero_returns_default(self):
        """M2: p_yes == p_no == 0 breaks sum invariant — must return DEFAULT."""
        from micromech.tools._groq_common import DEFAULT_PREDICTION, _validate_prediction

        raw = json.dumps({"p_yes": 0.0, "p_no": 0.0, "confidence": 0.5, "info_utility": 0.5})
        assert _validate_prediction(raw) == DEFAULT_PREDICTION

    def test_list_json_returns_default(self):
        """M3: valid JSON but not a dict — must return DEFAULT, not TypeError."""
        from micromech.tools._groq_common import DEFAULT_PREDICTION, _validate_prediction

        assert _validate_prediction("[1, 2, 3]") == DEFAULT_PREDICTION

    def test_non_dict_json_returns_default(self):
        """M3: any non-dict JSON (string, number) returns DEFAULT."""
        from micromech.tools._groq_common import DEFAULT_PREDICTION, _validate_prediction

        assert _validate_prediction('"just a string"') == DEFAULT_PREDICTION
        assert _validate_prediction("42") == DEFAULT_PREDICTION
