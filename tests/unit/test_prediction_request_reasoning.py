"""Tests for prediction_request_reasoning.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_groq_response(content: str) -> MagicMock:
    choice = MagicMock()
    choice.message.content = content
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def _patch_secrets(groq_key: str | None):
    mock_secrets = MagicMock()
    if groq_key is None:
        mock_secrets.groq_api_key = None
    else:
        mock_val = MagicMock()
        mock_val.get_secret_value.return_value = groq_key
        mock_secrets.groq_api_key = mock_val
    return patch(
        "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.secrets",
        mock_secrets,
    )


def _fake_ddgs_mod(news=None, text=None, exc=None):
    mock_inst = MagicMock()
    if exc:
        mock_inst.news.side_effect = exc
    else:
        mock_inst.news.return_value = news or []
        mock_inst.text.return_value = text or []
    fake_mod = MagicMock()
    fake_mod.DDGS = MagicMock(return_value=mock_inst)
    return fake_mod


_GOOD_JSON = json.dumps(
    {"p_yes": 0.65, "p_no": 0.35, "confidence": 0.75, "info_utility": 0.8}
)


# ===========================================================================
# _generate_queries
# ===========================================================================


class TestGenerateQueries:
    def test_returns_three_variants(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        queries = _generate_queries("Will Bitcoin reach $200k by end of 2025?")
        assert len(queries) == 3

    def test_first_query_is_original(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        question = "Will X happen by 2026?"
        assert _generate_queries(question)[0] == question

    def test_news_variant_included(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        queries = _generate_queries("Will Bitcoin hit 200k?")
        assert any("news" in q.lower() for q in queries)

    def test_forecast_variant_included(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        queries = _generate_queries("Will Bitcoin hit 200k?")
        assert any("forecast" in q.lower() or "probability" in q.lower() for q in queries)

    def test_strips_leading_will(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        queries = _generate_queries("Will Tesla stock rise in 2025?")
        # variants should be based on stripped core, not start with "will"
        assert not any(q.lower().startswith("will") for q in queries[1:])

    def test_simple_question_without_modal(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _generate_queries,
        )

        queries = _generate_queries("Bitcoin price 2025")
        assert len(queries) == 3


# ===========================================================================
# _search_ddgs_multi
# ===========================================================================


class TestSearchDdgsMulti:
    def test_ddgs_not_installed_returns_empty(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        with patch.dict(sys.modules, {"ddgs": None}):
            assert _search_ddgs_multi(["q1", "q2"]) == ""

    def test_collects_results_across_queries(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        call_count = [0]

        def fake_news(query, max_results=5):
            call_count[0] += 1
            return [{"title": f"Result for {query}", "body": f"body {query}", "date": "2025-01-01", "url": ""}]

        mock_inst = MagicMock()
        mock_inst.news.side_effect = fake_news
        mock_inst.text.return_value = []
        fake_mod = MagicMock()
        fake_mod.DDGS = MagicMock(return_value=mock_inst)

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_ddgs_multi(["q1", "q2", "q3"])

        assert call_count[0] == 3
        assert "Result for q1" in result

    def test_deduplicates_across_queries(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        dup_body = "This is the exact same content from multiple queries"
        dup_title = "Same Title"

        mock_inst = MagicMock()
        mock_inst.news.return_value = [
            {"title": dup_title, "body": dup_body, "date": "2025-01-01", "url": "http://x.com"}
        ]
        mock_inst.text.return_value = []
        fake_mod = MagicMock()
        fake_mod.DDGS = MagicMock(return_value=mock_inst)

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_ddgs_multi(["q1", "q2", "q3"])

        # The same content should appear only once despite 3 queries returning it
        assert result.count(dup_title) == 1

    def test_search_exception_per_query_skipped(self):
        """If one query fails, remaining queries still run."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        call_count = [0]

        def fake_news(query, max_results=5):
            call_count[0] += 1
            if query == "q1":
                raise Exception("network error")
            return [{"title": f"OK {query}", "body": "body", "date": "2025-01-01", "url": ""}]

        mock_inst = MagicMock()
        mock_inst.news.side_effect = fake_news
        mock_inst.text.return_value = []
        fake_mod = MagicMock()
        fake_mod.DDGS = MagicMock(return_value=mock_inst)

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_ddgs_multi(["q1", "q2"])

        assert "OK q2" in result

    def test_empty_results_returns_empty(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        fake = _fake_ddgs_mod(news=[], text=[])
        with patch.dict(sys.modules, {"ddgs": fake}):
            assert _search_ddgs_multi(["q1"]) == ""

    def test_context_truncated_at_max_chars(self):
        from micromech.tools.prediction_request_reasoning import (
            prediction_request_reasoning as prr,
        )

        many_news = [
            {"title": f"T{i}", "body": "X" * 400, "date": "2025-01-01", "url": ""}
            for i in range(40)
        ]
        fake = _fake_ddgs_mod(news=many_news, text=[])
        with patch.dict(sys.modules, {"ddgs": fake}):
            result = prr._search_ddgs_multi(["q1"])
        assert result.endswith("...")
        assert len(result) <= prr._MAX_CONTEXT_CHARS + 3

    def test_collects_text_results(self):
        """Lines 163-170: text results are collected alongside news results."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        mock_inst = MagicMock()
        mock_inst.news.return_value = [
            {"title": "News A", "body": "News body A", "date": "2025-01-01", "url": ""}
        ]
        mock_inst.text.return_value = [
            {"title": "Web B", "body": "Web body B completely different", "href": "http://b.com"}
        ]
        fake_mod = MagicMock()
        fake_mod.DDGS = MagicMock(return_value=mock_inst)

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_ddgs_multi(["q1"])

        assert "News A" in result
        assert "Web body B" in result

    def test_ddgs_init_failure_returns_empty(self):
        """Lines 175-177: DDGS() itself raises → returns empty string."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _search_ddgs_multi,
        )

        fake_mod = MagicMock()
        fake_mod.DDGS.side_effect = Exception("DDGS init failed")

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_ddgs_multi(["q1"])

        assert result == ""


# ===========================================================================
# _extract_json
# ===========================================================================


class TestExtractJson:
    def test_pure_json(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        raw = '{"p_yes": 0.6, "p_no": 0.4, "confidence": 0.7, "info_utility": 0.5}'
        assert json.loads(_extract_json(raw))["p_yes"] == pytest.approx(0.6)

    def test_json_after_xml_reasoning(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        raw = "<facts>stuff</facts>\n<thinking>thoughts</thinking>\n" + _GOOD_JSON
        result = _extract_json(raw)
        assert json.loads(result)["p_yes"] == pytest.approx(0.65)

    def test_code_fenced_json(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        raw = "```json\n" + _GOOD_JSON + "\n```"
        assert json.loads(_extract_json(raw))["p_yes"] == pytest.approx(0.65)

    def test_no_json_returns_stripped(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        assert _extract_json("  no json  ") == "no json"

    def test_fallback_finds_p_no_block_when_p_yes_missing(self):
        """Lines 213-216: regex misses p_no-only block; fallback dict scan finds it."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        raw = 'text {"p_no": 0.7, "confidence": 0.5} end'
        result = _extract_json(raw)
        assert "p_no" in result

    def test_fallback_except_branch_on_invalid_block(self):
        """Lines 217-218: bad JSON block triggers except+continue in fallback scan."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _extract_json,
        )

        # Bad block comes after good in reversed scan → except hit, then good found
        raw = '{"p_no": 0.7} {not: valid json}'
        result = _extract_json(raw)
        assert isinstance(result, str)


# ===========================================================================
# _validate_prediction
# ===========================================================================


class TestValidatePrediction:
    def test_valid_unchanged(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _validate_prediction,
        )

        result = json.loads(_validate_prediction(_GOOD_JSON))
        assert result["p_yes"] == pytest.approx(0.65)

    def test_missing_confidence_defaults_to_zero(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _validate_prediction,
        )

        raw = json.dumps({"p_yes": 0.5, "p_no": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert result["confidence"] == 0.0
        assert result["info_utility"] == 0.0

    def test_unnormalized_probabilities_normalized(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            _validate_prediction,
        )

        raw = json.dumps({"p_yes": 3.0, "p_no": 1.0, "confidence": 0.5, "info_utility": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] - 0.75) < 0.01
        assert abs(result["p_no"] - 0.25) < 0.01

    def test_invalid_json_returns_default(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            DEFAULT_PREDICTION,
            _validate_prediction,
        )

        assert _validate_prediction("garbage") == DEFAULT_PREDICTION


# ===========================================================================
# run()
# ===========================================================================


class TestPredictionRequestReasoningRun:
    def test_no_api_key_returns_default(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            DEFAULT_PREDICTION,
            run,
        )

        with _patch_secrets(None):
            result, prompt_used, meta, cb = run(prompt="Will X happen?")

        assert result == DEFAULT_PREDICTION
        assert prompt_used is None

    def test_happy_path(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="context",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            result, prompt_used, meta, cb = run(prompt="Will X happen?")

        data = json.loads(result)
        assert data["p_yes"] == pytest.approx(0.65)
        assert meta is None

    def test_uses_multiple_queries_in_search(self):
        """_generate_queries is called and its output is passed to _search_ddgs_multi."""
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)
        captured_queries = []

        def fake_search(queries):
            captured_queries.extend(queries)
            return "search context"

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                side_effect=fake_search,
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            run(prompt="Will Bitcoin hit 200k?")

        assert len(captured_queries) >= 2
        assert "Will Bitcoin hit 200k?" in captured_queries

    def test_groq_failure_returns_default(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            DEFAULT_PREDICTION,
            run,
        )

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("API error")

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            result, _, _, _ = run(prompt="Will X happen?")

        assert result == DEFAULT_PREDICTION

    def test_garbage_llm_output_still_returns_valid_json(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(
            "Sorry, I cannot make this prediction."
        )

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            result, _, _, _ = run(prompt="Will X happen?")

        data = json.loads(result)
        assert "p_yes" in data
        assert "p_no" in data

    def test_counter_callback_passthrough(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)
        cb = MagicMock()

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            _, _, _, returned_cb = run(prompt="Will X happen?", counter_callback=cb)

        assert returned_cb is cb

    def test_search_context_appears_in_prompt(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="UNIQUE_MARKER_XYZ",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            _, prompt_used, _, _ = run(prompt="Will X happen?")

        assert "UNIQUE_MARKER_XYZ" in prompt_used

    def test_groq_base_url_is_correct(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)
        captured = {}

        def capture(**kwargs):
            captured.update(kwargs)
            return mock_client

        with (
            _patch_secrets("my-key-123"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                side_effect=capture,
            ),
        ):
            run(prompt="Will X happen?")

        assert captured["base_url"] == "https://api.groq.com/openai/v1"
        assert captured["api_key"] == "my-key-123"

    def test_both_tool_aliases_in_allowed_tools(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import (
            ALLOWED_TOOLS,
        )

        assert "prediction-request-reasoning" in ALLOWED_TOOLS
        assert "prediction-request-reasoning-claude" in ALLOWED_TOOLS

    def test_custom_model_forwarded_to_groq(self):
        from micromech.tools.prediction_request_reasoning.prediction_request_reasoning import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning._search_ddgs_multi",
                return_value="",
            ),
            patch(
                "micromech.tools.prediction_request_reasoning.prediction_request_reasoning.OpenAI",
                return_value=mock_client,
            ),
        ):
            run(prompt="Will X happen?", model="llama-3.1-8b-instant")

        call_kwargs = mock_client.chat.completions.create.call_args.kwargs
        assert call_kwargs["model"] == "llama-3.1-8b-instant"
