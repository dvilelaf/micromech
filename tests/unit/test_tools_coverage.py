"""Coverage tests for local_llm.py and prediction_request.py.

Covers the lines missed in the existing test suite:
  local_llm.py          — lines 40-44, 53-81, 94-124, 129-134, 150-181
  prediction_request.py — lines 72-110, 148-185
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_llm(content: str = "answer") -> MagicMock:
    """Return a mock Llama instance."""
    mock_llm = MagicMock()
    mock_llm.create_chat_completion.return_value = {
        "choices": [{"message": {"content": content}}],
        "usage": {"total_tokens": 42},
    }
    return mock_llm


# ===========================================================================
# local_llm.py
# ===========================================================================


class TestSha256:
    """Lines 40-44: _sha256."""

    def test_known_content(self, tmp_path: Path):
        import hashlib

        from micromech.tools.local_llm.local_llm import _sha256

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        assert _sha256(f) == hashlib.sha256(data).hexdigest()

    def test_large_file_multi_chunk(self, tmp_path: Path):
        import hashlib

        from micromech.tools.local_llm.local_llm import _sha256

        data = b"x" * (65536 * 3 + 7)
        f = tmp_path / "big.bin"
        f.write_bytes(data)
        assert _sha256(f) == hashlib.sha256(data).hexdigest()


class TestVerifyOrPinHash:
    """Lines 53-81: _verify_or_pin_hash."""

    def test_first_download_creates_manifest(self, tmp_path: Path):
        from micromech.tools.local_llm.local_llm import _verify_or_pin_hash

        model = tmp_path / "model.gguf"
        model.write_bytes(b"fake-model-data")

        result = _verify_or_pin_hash(model)

        assert result is True
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert model.name in manifest

    def test_second_load_passes_verification(self, tmp_path: Path):
        from micromech.tools.local_llm.local_llm import _verify_or_pin_hash

        model = tmp_path / "model.gguf"
        model.write_bytes(b"stable-model")

        _verify_or_pin_hash(model)  # pin
        result = _verify_or_pin_hash(model)  # verify
        assert result is True

    def test_hash_mismatch_returns_false(self, tmp_path: Path):
        from micromech.tools.local_llm.local_llm import _verify_or_pin_hash

        model = tmp_path / "model.gguf"
        model.write_bytes(b"original data")

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps({"model.gguf": "a" * 64}))

        result = _verify_or_pin_hash(model)
        assert result is False

    def test_manifest_without_this_model_pins_it(self, tmp_path: Path):
        from micromech.tools.local_llm.local_llm import _verify_or_pin_hash

        model = tmp_path / "new_model.gguf"
        model.write_bytes(b"new model bytes")

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps({"other_model.gguf": "a" * 64}))

        result = _verify_or_pin_hash(model)

        assert result is True
        manifest = json.loads(manifest_path.read_text())
        assert "new_model.gguf" in manifest

    def test_corrupt_manifest_handled(self, tmp_path: Path):
        from micromech.tools.local_llm.local_llm import _verify_or_pin_hash

        model = tmp_path / "model.gguf"
        model.write_bytes(b"data")
        (tmp_path / "manifest.json").write_text("NOT VALID JSON }{")

        result = _verify_or_pin_hash(model)
        assert result is True


class TestGetLlm:
    """Lines 94-124: _get_llm."""

    def setup_method(self):
        import micromech.tools.local_llm.local_llm as _mod

        _mod._llm_instances.clear()

    def test_returns_cached_instance(self, tmp_path: Path):
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm()
        _mod._llm_instances["model.gguf"] = mock_llm

        result = _mod._get_llm(model_file="model.gguf", models_dir=tmp_path)
        assert result is mock_llm

    def test_downloads_and_loads_model(self, tmp_path: Path):
        """Model file absent → hf_hub_download called, then Llama loaded."""
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm()
        model_path = tmp_path / "dl.gguf"

        mock_dl = MagicMock()

        def fake_download(**kwargs):
            model_path.write_bytes(b"fake-gguf-content")

        mock_dl.side_effect = fake_download
        mock_llama_cls = MagicMock(return_value=mock_llm)

        fake_hf = MagicMock()
        fake_hf.hf_hub_download = mock_dl
        fake_llama_mod = MagicMock()
        fake_llama_mod.Llama = mock_llama_cls

        with patch.dict(
            sys.modules,
            {"huggingface_hub": fake_hf, "llama_cpp": fake_llama_mod},
        ):
            result = _mod._get_llm(
                model_repo="some/repo",
                model_file="dl.gguf",
                models_dir=tmp_path,
            )

        assert result is mock_llm
        assert "dl.gguf" in _mod._llm_instances

    def test_model_already_present_skips_download(self, tmp_path: Path):
        """Model file exists → hf_hub_download NOT called."""
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm()
        model_path = tmp_path / "existing.gguf"
        model_path.write_bytes(b"preloaded")

        mock_dl = MagicMock()
        fake_hf = MagicMock()
        fake_hf.hf_hub_download = mock_dl
        fake_llama_mod = MagicMock()
        fake_llama_mod.Llama = MagicMock(return_value=mock_llm)

        with patch.dict(
            sys.modules,
            {"huggingface_hub": fake_hf, "llama_cpp": fake_llama_mod},
        ):
            _mod._get_llm(
                model_repo="r",
                model_file="existing.gguf",
                models_dir=tmp_path,
            )

        mock_dl.assert_not_called()

    def test_integrity_failure_raises(self, tmp_path: Path):
        """Hash mismatch → RuntimeError, instance not cached."""
        import micromech.tools.local_llm.local_llm as _mod

        model_path = tmp_path / "bad.gguf"
        model_path.write_bytes(b"tampered")
        (tmp_path / "manifest.json").write_text(
            json.dumps({"bad.gguf": "0" * 64})
        )

        fake_hf = MagicMock()
        fake_llama = MagicMock()

        with patch.dict(
            sys.modules,
            {"huggingface_hub": fake_hf, "llama_cpp": fake_llama},
        ):
            with pytest.raises(RuntimeError, match="integrity check failed"):
                _mod._get_llm(
                    model_repo="r",
                    model_file="bad.gguf",
                    models_dir=tmp_path,
                )

        assert "bad.gguf" not in _mod._llm_instances

    def test_thread_safe_double_check(self, tmp_path: Path):
        """Second thread wins inside lock → cached instance returned."""
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm()

        class _FakeLock:
            def __enter__(self):
                # Simulate another thread loading the instance first
                _mod._llm_instances["race.gguf"] = mock_llm
                return self

            def __exit__(self, *a):
                pass

        with patch.object(_mod, "_init_lock", _FakeLock()):
            result = _mod._get_llm(
                model_repo="r",
                model_file="race.gguf",
                models_dir=tmp_path,
            )

        assert result is mock_llm


class TestResolveModel:
    """Lines 129-134: _resolve_model."""

    def test_preset_qwen(self):
        from micromech.core.constants import LLM_MODEL_PRESETS
        from micromech.tools.local_llm.local_llm import _resolve_model

        repo, fname = _resolve_model({"model": "qwen"})
        assert repo == LLM_MODEL_PRESETS["qwen"][0]
        assert fname == LLM_MODEL_PRESETS["qwen"][1]

    def test_preset_gemma4(self):
        from micromech.core.constants import LLM_MODEL_PRESETS
        from micromech.tools.local_llm.local_llm import _resolve_model

        repo, fname = _resolve_model({"model": "gemma4"})
        assert repo == LLM_MODEL_PRESETS["gemma4"][0]

    def test_unknown_preset_falls_through(self):
        from micromech.core.constants import DEFAULT_LLM_FILE, DEFAULT_LLM_MODEL
        from micromech.tools.local_llm.local_llm import _resolve_model

        repo, fname = _resolve_model({"model": "nonexistent"})
        assert repo == DEFAULT_LLM_MODEL
        assert fname == DEFAULT_LLM_FILE

    def test_explicit_repo_and_file(self):
        from micromech.tools.local_llm.local_llm import _resolve_model

        repo, fname = _resolve_model(
            {"model_repo": "custom/repo", "model_file": "custom.gguf"}
        )
        assert repo == "custom/repo"
        assert fname == "custom.gguf"

    def test_empty_kwargs_uses_defaults(self):
        from micromech.core.constants import DEFAULT_LLM_FILE, DEFAULT_LLM_MODEL
        from micromech.tools.local_llm.local_llm import _resolve_model

        repo, fname = _resolve_model({})
        assert repo == DEFAULT_LLM_MODEL
        assert fname == DEFAULT_LLM_FILE


class TestLocalLlmRun:
    """Lines 150-181: run()."""

    def setup_method(self):
        import micromech.tools.local_llm.local_llm as _mod

        _mod._llm_instances.clear()

    def test_run_happy_path(self):
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm("The answer is 42.")

        with patch.object(_mod, "_get_llm", return_value=mock_llm):
            result_str, echoed_prompt, meta, cb = _mod.run(
                prompt="What is the answer?",
                model="qwen",
            )

        data = json.loads(result_str)
        assert data["result"] == "The answer is 42."
        assert "tokens" in data
        assert echoed_prompt == "What is the answer?"
        assert meta is None
        assert cb is None

    def test_run_counter_callback_passthrough(self):
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm("hi")
        cb = MagicMock()

        with patch.object(_mod, "_get_llm", return_value=mock_llm):
            _, _, _, returned_cb = _mod.run(prompt="hello", counter_callback=cb)

        assert returned_cb is cb

    def test_run_custom_params_forwarded(self):
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = _make_mock_llm("ok")

        with patch.object(_mod, "_get_llm", return_value=mock_llm):
            _mod.run(
                prompt="test",
                system_prompt="You are terse.",
                max_tokens=64,
                temperature=0.0,
            )

        kw = mock_llm.create_chat_completion.call_args.kwargs
        assert kw["max_tokens"] == 64
        assert kw["temperature"] == 0.0
        assert kw["messages"][0]["content"] == "You are terse."

    def test_run_missing_usage_defaults_to_zero(self):
        import micromech.tools.local_llm.local_llm as _mod

        mock_llm = MagicMock()
        mock_llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": "resp"}}],
        }

        with patch.object(_mod, "_get_llm", return_value=mock_llm):
            result_str, _, _, _ = _mod.run(prompt="q")

        assert json.loads(result_str)["tokens"] == 0

    def test_allowed_tools_constant(self):
        from micromech.tools.local_llm.local_llm import ALLOWED_TOOLS

        assert "local-llm" in ALLOWED_TOOLS


# ===========================================================================
# prediction_request.py
# ===========================================================================


def _fake_ddgs_mod(news_results=None, text_results=None, exc=None):
    """Build a fake ddgs module with a DDGS mock."""
    mock_inst = MagicMock()
    if exc:
        mock_inst.news.side_effect = exc
    else:
        mock_inst.news.return_value = news_results or []
        mock_inst.text.return_value = text_results or []
    fake_mod = MagicMock()
    fake_mod.DDGS = MagicMock(return_value=mock_inst)
    return fake_mod


class TestSearchContext:
    """Lines 72-110: _search_context."""

    def test_ddgs_not_installed_returns_empty(self):
        """ImportError for ddgs → returns empty string."""
        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        with patch.dict(sys.modules, {"ddgs": None}):
            result = _search_context("will bitcoin rise?")

        assert result == ""

    def test_happy_path_news_and_text(self):
        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        fake_mod = _fake_ddgs_mod(
            news_results=[
                {
                    "title": "BTC hits 100k",
                    "body": "Bitcoin reaches all-time high.",
                    "date": "2025-01-01",
                },
                {"title": "ETH upgrade", "body": "", "date": "2025-01-02"},
            ],
            text_results=[{"body": "Crypto markets surging today."}],
        )

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_context("will bitcoin hit 200k?")

        assert "BTC hits 100k" in result
        assert len(result) > 0

    def test_search_exception_returns_empty(self):
        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        fake_mod = _fake_ddgs_mod(exc=Exception("network error"))

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_context("some question")

        assert result == ""

    def test_no_snippets_returns_empty(self):
        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        fake_mod = _fake_ddgs_mod(news_results=[], text_results=[])

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_context("will X happen?")

        assert result == ""

    def test_context_truncated_at_max_chars(self):
        """Context > _MAX_CONTEXT_CHARS is truncated with '...'."""
        from micromech.tools.prediction_request import prediction_request as pr

        # Build enough snippets to exceed _MAX_CONTEXT_CHARS (2000)
        many_news = [
            {
                "title": f"News {i}",
                "body": "B" * 150,
                "date": "2025-01-01",
            }
            for i in range(20)
        ]
        fake_mod = _fake_ddgs_mod(news_results=many_news, text_results=[])

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = pr._search_context("long question?")

        assert result.endswith("...")
        assert len(result) <= pr._MAX_CONTEXT_CHARS + 3

    def test_text_result_not_deduped_when_different(self):
        """Text snippet different from news snippets is appended."""
        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        fake_mod = _fake_ddgs_mod(
            news_results=[
                {"title": "News A", "body": "First snippet.", "date": "2025-01-01"}
            ],
            text_results=[{"body": "Completely different text result."}],
        )

        with patch.dict(sys.modules, {"ddgs": fake_mod}):
            result = _search_context("question?")

        lines = result.split("\n")
        assert len(lines) == 2

    def test_text_result_deduped_when_same_prefix(self):
        """Text body whose first 200 chars match a news snippet is skipped.

        The dedup set is built from {s[:200] for s in snippets}.
        For a text body[:200] to match, it must equal the news snippet[:200].
        We craft a news body of exactly 150 chars so the formatted snippet
        "[date] title: body[:150]" is long (>200 chars) and its first 200 chars
        can be replicated in the text body.
        """
        import ddgs as ddgs_mod

        from micromech.tools.prediction_request.prediction_request import (
            _search_context,
        )

        # news body long enough so formatted snippet exceeds 200 chars.
        # Format: "[date] title: body[:150]"
        # "[2025-01-01] " = 13 chars, ": " = 2 chars → need title + 150 > 200-15=185
        # Use a 40-char title so total = 13 + 40 + 2 + 150 = 205 > 200
        long_title = "T" * 40
        news_body = "Y" * 150
        news_entry = f"[2025-01-01] {long_title}: {news_body}"
        assert len(news_entry) > 200, f"news_entry len={len(news_entry)}, must be >200"

        # text body whose [:200] equals news_entry[:200] → deduplicated
        dup_text_body = news_entry[:200] + " some extra content"

        mock_inst = MagicMock()
        mock_inst.news.return_value = [
            {"title": long_title, "body": news_body, "date": "2025-01-01"}
        ]
        mock_inst.text.return_value = [{"body": dup_text_body}]

        with patch.object(ddgs_mod, "DDGS", return_value=mock_inst):
            result = _search_context("dup question?")

        lines = [ln for ln in result.split("\n") if ln]
        assert len(lines) == 1


class TestExtractJson:
    def test_extracts_json_from_text(self):
        from micromech.tools.prediction_request.prediction_request import (
            _extract_json,
        )

        text = 'Some text {"p_yes": 0.7, "p_no": 0.3} trailing'
        assert _extract_json(text) == '{"p_yes": 0.7, "p_no": 0.3}'

    def test_returns_stripped_when_no_json(self):
        from micromech.tools.prediction_request.prediction_request import (
            _extract_json,
        )

        assert _extract_json("  no json here  ") == "no json here"


class TestValidatePrediction:
    def test_valid_prediction_unchanged(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        raw = json.dumps(
            {"p_yes": 0.7, "p_no": 0.3, "confidence": 0.9, "info_utility": 0.5}
        )
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] - 0.7) < 0.01

    def test_missing_p_no_gets_default(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        raw = json.dumps(
            {"p_yes": 0.6, "p_no": 0.5, "confidence": 0.8, "info_utility": 0.4}
        )
        result = json.loads(_validate_prediction(raw))
        assert "p_no" in result

    def test_missing_confidence_gets_zero(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        raw = json.dumps({"p_yes": 0.5, "p_no": 0.5})
        result = json.loads(_validate_prediction(raw))
        assert result["confidence"] == 0.0
        assert result["info_utility"] == 0.0

    def test_probabilities_normalized_when_not_summing_to_one(self):
        from micromech.tools.prediction_request.prediction_request import (
            _validate_prediction,
        )

        raw = json.dumps(
            {"p_yes": 2.0, "p_no": 2.0, "confidence": 0.8, "info_utility": 0.5}
        )
        result = json.loads(_validate_prediction(raw))
        assert abs(result["p_yes"] + result["p_no"] - 1.0) < 0.01

    def test_invalid_json_returns_default(self):
        from micromech.tools.prediction_request.prediction_request import (
            DEFAULT_PREDICTION,
            _validate_prediction,
        )

        assert _validate_prediction("not json at all") == DEFAULT_PREDICTION


class TestPredictionRun:
    """Lines 148-185: run() in prediction_request."""

    def _patch_llm(self, content: str):
        """Patch _get_llm in the local_llm module (where it lives)."""
        import micromech.tools.local_llm.local_llm as llm_mod

        mock_llm = _make_mock_llm(content)
        return patch.object(llm_mod, "_get_llm", return_value=mock_llm), mock_llm

    def test_run_happy_path(self):
        from micromech.tools.prediction_request import prediction_request as pr

        good_json = json.dumps(
            {"p_yes": 0.6, "p_no": 0.4, "confidence": 0.8, "info_utility": 0.5}
        )
        ctx, mock_llm = self._patch_llm(good_json)

        with (
            patch.object(pr, "_search_context", return_value="some context"),
            ctx,
        ):
            result_str, prompt_used, meta, cb = pr.run(
                prompt="Will BTC exceed 200k in 2025?"
            )

        data = json.loads(result_str)
        assert "p_yes" in data
        assert "p_no" in data
        assert meta is None
        assert cb is None

    def test_run_uses_provided_additional_info_skips_search(self):
        from micromech.tools.prediction_request import prediction_request as pr

        good_json = json.dumps(
            {"p_yes": 0.7, "p_no": 0.3, "confidence": 0.9, "info_utility": 0.8}
        )
        ctx, _ = self._patch_llm(good_json)

        with (
            patch.object(pr, "_search_context") as mock_search,
            ctx,
        ):
            pr.run(prompt="Will X happen?", additional_information="Expert says yes.")

        mock_search.assert_not_called()

    def test_run_llm_failure_returns_default_prediction(self):
        import micromech.tools.local_llm.local_llm as llm_mod
        from micromech.tools.prediction_request import prediction_request as pr
        from micromech.tools.prediction_request.prediction_request import (
            DEFAULT_PREDICTION,
        )

        with (
            patch.object(pr, "_search_context", return_value=""),
            patch.object(
                llm_mod, "_get_llm", side_effect=RuntimeError("model not found")
            ),
        ):
            result_str, _, _, _ = pr.run(prompt="Will Z happen?")

        assert result_str == DEFAULT_PREDICTION

    def test_run_garbage_llm_output_returns_valid_prediction(self):
        from micromech.tools.prediction_request import prediction_request as pr

        ctx, _ = self._patch_llm("I cannot determine the probability.")

        with (
            patch.object(pr, "_search_context", return_value=""),
            ctx,
        ):
            result_str, _, _, _ = pr.run(prompt="Will Y happen?")

        data = json.loads(result_str)
        assert "p_yes" in data
        assert "p_no" in data

    def test_run_counter_callback_passthrough(self):
        from micromech.tools.prediction_request import prediction_request as pr

        good_json = json.dumps(
            {"p_yes": 0.5, "p_no": 0.5, "confidence": 0.5, "info_utility": 0.0}
        )
        ctx, _ = self._patch_llm(good_json)
        cb = MagicMock()

        with (
            patch.object(pr, "_search_context", return_value=""),
            ctx,
        ):
            _, _, _, returned_cb = pr.run(prompt="test", counter_callback=cb)

        assert returned_cb is cb

    def test_run_no_additional_info_triggers_search(self):
        """Empty additional_info → _search_context is called."""
        from micromech.tools.prediction_request import prediction_request as pr

        good_json = json.dumps(
            {"p_yes": 0.5, "p_no": 0.5, "confidence": 0.5, "info_utility": 0.0}
        )
        ctx, _ = self._patch_llm(good_json)

        with (
            patch.object(pr, "_search_context", return_value="context") as mock_search,
            ctx,
        ):
            pr.run(prompt="Will something happen?")

        mock_search.assert_called_once_with("Will something happen?")

    def test_allowed_tools_constant(self):
        from micromech.tools.prediction_request.prediction_request import ALLOWED_TOOLS

        assert "prediction-online" in ALLOWED_TOOLS
        assert "prediction-offline" in ALLOWED_TOOLS
