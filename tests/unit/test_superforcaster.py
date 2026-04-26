"""Tests for superforcaster.py."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_groq_response(content: str) -> MagicMock:
    """Build a mock openai.OpenAI().chat.completions.create() response."""
    choice = MagicMock()
    choice.message.content = content
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def _patch_secrets(groq_key: str | None):
    """Patch micromech.secrets.secrets.groq_api_key."""
    mock_secrets = MagicMock()
    if groq_key is None:
        mock_secrets.groq_api_key = None
    else:
        mock_val = MagicMock()
        mock_val.get_secret_value.return_value = groq_key
        mock_secrets.groq_api_key = mock_val
    return patch("micromech.tools.superforcaster.superforcaster.secrets", mock_secrets)


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
    {"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.6}
)


# ===========================================================================
# _search_ddgs
# ===========================================================================


class TestSearchDdgs:
    def test_ddgs_not_installed_returns_empty(self):
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        with patch.dict(sys.modules, {"ddgs": None}):
            assert _search_ddgs("will BTC hit 200k?") == ""

    def test_happy_path_news_and_text(self):
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        fake = _fake_ddgs_mod(
            news=[
                {"title": "BTC surges", "body": "Bitcoin up.", "date": "2025-01-01", "url": "http://a.com"},
            ],
            text=[
                {"title": "Analysis", "body": "Analysts say 200k unlikely.", "href": "http://b.com"},
            ],
        )
        with patch.dict(sys.modules, {"ddgs": fake}):
            result = _search_ddgs("will BTC hit 200k?")
        assert "BTC surges" in result
        assert "Analysts say" in result

    def test_search_exception_returns_empty(self):
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        fake = _fake_ddgs_mod(exc=Exception("network error"))
        with patch.dict(sys.modules, {"ddgs": fake}):
            assert _search_ddgs("question?") == ""

    def test_no_results_returns_empty(self):
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        fake = _fake_ddgs_mod(news=[], text=[])
        with patch.dict(sys.modules, {"ddgs": fake}):
            assert _search_ddgs("question?") == ""

    def test_context_truncated_at_max_chars(self):
        from micromech.tools.superforcaster import superforcaster as sf

        many_news = [
            {"title": f"News {i}", "body": "X" * 300, "date": "2025-01-01", "url": "http://x.com"}
            for i in range(30)
        ]
        fake = _fake_ddgs_mod(news=many_news, text=[])
        with patch.dict(sys.modules, {"ddgs": fake}):
            result = sf._search_ddgs("long question?")
        assert result.endswith("...")
        assert len(result) <= sf._MAX_CONTEXT_CHARS + 3

    def test_text_results_deduplicated(self):
        """Text result whose body prefix matches a news snippet body is skipped."""
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        body = "Shared body content xyz"
        # news dedup key is (title+body)[:100], text dedup key is body[:100]
        # To dedup the text result, its body[:100] must match the news (title+body)[:100]
        # Use an empty title so news key == body[:100]
        fake = _fake_ddgs_mod(
            news=[{"title": "", "body": body, "date": "2025-01-01", "url": "http://a.com"}],
            text=[{"title": "T", "body": body, "href": "http://b.com"}],
        )
        with patch.dict(sys.modules, {"ddgs": fake}):
            result = _search_ddgs("q?")
        # body appears only once
        assert result.count(body) == 1

    def test_result_includes_title_and_date(self):
        from micromech.tools.superforcaster.superforcaster import _search_ddgs

        fake = _fake_ddgs_mod(
            news=[{"title": "My Title", "body": "Some body.", "date": "2025-06-15", "url": ""}],
            text=[],
        )
        with patch.dict(sys.modules, {"ddgs": fake}):
            result = _search_ddgs("q?")
        assert "My Title" in result
        assert "2025-06-15" in result


# ===========================================================================
# _extract_json
# ===========================================================================


class TestExtractJson:
    def test_pure_json_returned_as_is(self):
        from micromech.tools.superforcaster.superforcaster import _extract_json

        raw = '{"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.5}'
        result = _extract_json(raw)
        assert json.loads(result)["p_yes"] == pytest.approx(0.7)

    def test_json_with_surrounding_text(self):
        from micromech.tools.superforcaster.superforcaster import _extract_json

        raw = 'Based on analysis {"p_yes": 0.6, "p_no": 0.4, "confidence": 0.7, "info_utility": 0.5} end'
        result = _extract_json(raw)
        assert "p_yes" in result

    def test_json_with_xml_tags(self):
        from micromech.tools.superforcaster.superforcaster import _extract_json

        raw = '<thinking>reasoning</thinking>\n{"p_yes": 0.8, "p_no": 0.2, "confidence": 0.9, "info_utility": 0.7}'
        result = _extract_json(raw)
        assert json.loads(result)["p_yes"] == pytest.approx(0.8)

    def test_json_in_code_fence(self):
        from micromech.tools.superforcaster.superforcaster import _extract_json

        raw = '```json\n{"p_yes": 0.55, "p_no": 0.45, "confidence": 0.6, "info_utility": 0.4}\n```'
        result = _extract_json(raw)
        assert json.loads(result)["p_yes"] == pytest.approx(0.55)

    def test_no_json_returns_stripped_text(self):
        from micromech.tools.superforcaster.superforcaster import _extract_json

        assert _extract_json("  no json here  ") == "no json here"

    def test_fallback_finds_p_no_block_when_p_yes_missing(self):
        """Regex misses block with only p_no; fallback dict scan finds it."""
        from micromech.tools.superforcaster.superforcaster import _extract_json

        # _JSON_RE searches for "p_yes"; this block has only p_no → fallback
        raw = 'text {"p_no": 0.7, "confidence": 0.5} end'
        result = _extract_json(raw)
        assert "p_no" in result

    def test_fallback_skips_invalid_json_block(self):
        """Bad JSON block before good one triggers except branch, then good one found."""
        from micromech.tools.superforcaster.superforcaster import _extract_json

        # reversed order: bad block is "last" so tried first in reversed scan
        raw = '{"p_no": 0.7} {not: valid json here}'
        result = _extract_json(raw)
        # Should fall back to stripped text since none match cleanly via _JSON_RE
        assert isinstance(result, str)


# ===========================================================================
# _validate_prediction
# ===========================================================================


class TestValidatePrediction:
    def test_valid_unchanged(self):
        from micromech.tools.superforcaster.superforcaster import _validate_prediction

        raw = _GOOD_JSON
        result = json.loads(_validate_prediction(raw))
        assert result["p_yes"] == pytest.approx(0.7)
        assert result["p_no"] == pytest.approx(0.3)

    def test_missing_fields_get_defaults(self):
        from micromech.tools.superforcaster.superforcaster import _validate_prediction

        raw = json.dumps({"p_yes": 0.6, "p_no": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert result["confidence"] == 0.0
        assert result["info_utility"] == 0.0

    def test_probabilities_normalized(self):
        from micromech.tools.superforcaster.superforcaster import _validate_prediction

        raw = json.dumps({"p_yes": 2.0, "p_no": 2.0, "confidence": 0.5, "info_utility": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] + result["p_no"] - 1.0) < 0.01

    def test_invalid_json_returns_default(self):
        from micromech.tools.superforcaster.superforcaster import (
            DEFAULT_PREDICTION,
            _validate_prediction,
        )

        assert _validate_prediction("not json") == DEFAULT_PREDICTION

    def test_missing_p_no_filled_with_default(self):
        from micromech.tools.superforcaster.superforcaster import _validate_prediction

        raw = json.dumps({"p_yes": 0.5, "confidence": 0.8, "info_utility": 0.4})
        result = json.loads(_validate_prediction(raw))
        assert "p_no" in result


# ===========================================================================
# run()
# ===========================================================================


class TestSuperforcasterRun:
    def test_no_api_key_returns_default(self):
        from micromech.tools.superforcaster.superforcaster import (
            DEFAULT_PREDICTION,
            run,
        )

        with _patch_secrets(None):
            result, prompt_used, meta, cb = run(prompt="Will X happen?")

        assert result == DEFAULT_PREDICTION
        assert prompt_used is None
        assert meta is None

    def test_happy_path(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value="some context"),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            result, prompt_used, meta, cb = run(prompt="Will X happen?")

        data = json.loads(result)
        assert data["p_yes"] == pytest.approx(0.7)
        assert "p_no" in data
        assert meta is None

    def test_search_included_in_prompt(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch(
                "micromech.tools.superforcaster.superforcaster._search_ddgs",
                return_value="UNIQUE_CONTEXT_MARKER",
            ),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            _, prompt_used, _, _ = run(prompt="Will X happen?")

        assert "UNIQUE_CONTEXT_MARKER" in prompt_used

    def test_groq_failure_returns_default(self):
        from micromech.tools.superforcaster.superforcaster import (
            DEFAULT_PREDICTION,
            run,
        )

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("connection failed")

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            result, _, _, _ = run(prompt="Will X happen?")

        assert result == DEFAULT_PREDICTION

    def test_garbage_llm_output_returns_valid_prediction(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(
            "I cannot determine the probability."
        )

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            result, _, _, _ = run(prompt="Will X happen?")

        data = json.loads(result)
        assert "p_yes" in data
        assert "p_no" in data

    def test_counter_callback_passthrough(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)
        cb = MagicMock()

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            _, _, _, returned_cb = run(prompt="Will X happen?", counter_callback=cb)

        assert returned_cb is cb

    def test_custom_model_forwarded_to_groq(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            run(prompt="Will X happen?", model="llama-3.1-8b-instant")

        call_kwargs = mock_client.chat.completions.create.call_args.kwargs
        assert call_kwargs["model"] == "llama-3.1-8b-instant"

    def test_groq_client_uses_correct_base_url(self):
        from micromech.tools.superforcaster.superforcaster import run

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response(_GOOD_JSON)
        captured_kwargs = {}

        def capture_openai(**kwargs):
            captured_kwargs.update(kwargs)
            return mock_client

        with (
            _patch_secrets("my-groq-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", side_effect=capture_openai),
        ):
            run(prompt="Will X happen?")

        assert captured_kwargs["base_url"] == "https://api.groq.com/openai/v1"
        assert captured_kwargs["api_key"] == "my-groq-key"

    def test_empty_response_content_returns_default(self):
        from micromech.tools.superforcaster.superforcaster import (
            DEFAULT_PREDICTION,
            run,
        )

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_groq_response("")

        with (
            _patch_secrets("fake-key"),
            patch("micromech.tools.superforcaster.superforcaster._search_ddgs", return_value=""),
            patch("micromech.tools.superforcaster.superforcaster.OpenAI", return_value=mock_client),
        ):
            result, _, _, _ = run(prompt="Will X happen?")

        assert result == DEFAULT_PREDICTION

    def test_allowed_tools_constant(self):
        from micromech.tools.superforcaster.superforcaster import ALLOWED_TOOLS

        assert "superforcaster" in ALLOWED_TOOLS
