"""Local LLM tool using llama-cpp-python.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Supports multiple models via presets (qwen, gemma4) or custom repo/file.
Default model: Qwen2.5-0.5B-Instruct (Q4_K_M). Runs on CPU, ~400MB RAM.

Model integrity: on first download the SHA-256 hash is stored in
data/models/manifest.json.  On every subsequent load the file is re-hashed
and compared against the manifest.  A mismatch aborts loading and logs an
error so the operator can investigate before running untrusted model weights.
"""

import hashlib
import json
import threading
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from micromech.core.constants import (
    DEFAULT_LLM_CONTEXT_SIZE,
    DEFAULT_LLM_FILE,
    DEFAULT_LLM_MAX_TOKENS,
    DEFAULT_LLM_MODEL,
    LLM_MODEL_PRESETS,
)

ALLOWED_TOOLS = ["local-llm"]

_llm_instances: dict[str, Any] = {}
_init_lock = threading.Lock()  # For thread-safe model loading
_llm_lock = threading.Lock()  # For serializing inference (llama-cpp not thread-safe)

_MANIFEST_FILE = "manifest.json"


def _sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _verify_or_pin_hash(model_path: Path) -> bool:
    """Verify model file integrity against manifest; pin hash on first download.

    Returns True if the file is trustworthy, False if a hash mismatch is detected.
    On first download the hash is stored — subsequent loads verify against it.
    """
    manifest_path = model_path.parent / _MANIFEST_FILE
    actual = _sha256(model_path)

    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            manifest = {}
        expected = manifest.get(model_path.name)
        if expected and expected != actual:
            logger.error(
                "Model integrity check FAILED for {} — hash mismatch "
                "(expected {}, got {}). Possible tampering. Refusing to load.",
                model_path.name,
                expected[:16] + "...",
                actual[:16] + "...",
            )
            return False
        if not expected:
            # Hash not yet in manifest — add it
            manifest[model_path.name] = actual
            manifest_path.write_text(json.dumps(manifest, indent=2))
            logger.debug("Model hash added to manifest: {}", model_path.name)
    else:
        # First download — create manifest
        manifest_path.write_text(json.dumps({model_path.name: actual}, indent=2))
        logger.info("Model hash pinned in manifest: {}", model_path.name)

    return True


def _get_llm(
    model_repo: str = DEFAULT_LLM_MODEL,
    model_file: str = DEFAULT_LLM_FILE,
    context_size: int = DEFAULT_LLM_CONTEXT_SIZE,
    models_dir: Optional[Path] = None,
) -> Any:
    """Get or create an LLM instance for the given model (thread-safe).

    Instances are cached by model_file so switching models doesn't reload.
    """
    if model_file in _llm_instances:
        return _llm_instances[model_file]

    with _init_lock:
        if model_file in _llm_instances:
            return _llm_instances[model_file]

        from huggingface_hub import hf_hub_download
        from llama_cpp import Llama

        mdir = models_dir or (Path("data") / "models")
        mdir.mkdir(parents=True, exist_ok=True)
        model_path = mdir / model_file
        if not model_path.exists():
            logger.info("Downloading {} from {}", model_file, model_repo)
            hf_hub_download(
                repo_id=model_repo,
                filename=model_file,
                local_dir=str(mdir),
            )
        if not _verify_or_pin_hash(model_path):
            raise RuntimeError(f"Model integrity check failed for {model_file}")
        logger.info("Loading LLM from {}", model_path)
        instance = Llama(
            model_path=str(model_path),
            n_ctx=context_size,
            n_threads=4,
            verbose=False,
        )
        _llm_instances[model_file] = instance
    return instance


def _resolve_model(kwargs: dict[str, Any]) -> tuple[str, str]:
    """Resolve model_repo and model_file from kwargs (preset or explicit)."""
    model_preset = kwargs.get("model")
    if model_preset and model_preset in LLM_MODEL_PRESETS:
        return LLM_MODEL_PRESETS[model_preset]
    model_repo = kwargs.get("model_repo", DEFAULT_LLM_MODEL)
    model_file = kwargs.get("model_file", DEFAULT_LLM_FILE)
    return model_repo, model_file


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The input text.
        model: Optional preset name ("qwen", "gemma4").
        model_repo: Optional HuggingFace repo (overrides preset).
        model_file: Optional GGUF filename (overrides preset).
        system_prompt: Optional system prompt (default: "You are a helpful assistant.").
        max_tokens: Optional max tokens.
        temperature: Optional temperature.
        counter_callback: Optional token counter.
    """
    prompt = kwargs.get("prompt", "")
    system_prompt = kwargs.get("system_prompt", "You are a helpful assistant.")
    max_tokens = kwargs.get("max_tokens", DEFAULT_LLM_MAX_TOKENS)
    temperature = kwargs.get("temperature", 0.3)
    counter_callback = kwargs.get("counter_callback")

    model_repo, model_file = _resolve_model(kwargs)
    llm = _get_llm(model_repo=model_repo, model_file=model_file)

    # llama-cpp-python is NOT thread-safe — serialize all inference
    with _llm_lock:
        response = llm.create_chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )

    content = response["choices"][0]["message"]["content"]
    usage = response.get("usage", {})

    result = json.dumps(
        {
            "result": content,
            "model": model_repo,
            "tokens": usage.get("total_tokens", 0),
        }
    )

    return result, prompt, None, counter_callback
