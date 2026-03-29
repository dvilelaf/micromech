"""Local LLM tool using llama-cpp-python.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Default model: Qwen2.5-0.5B-Instruct (Q4_K_M). Runs on CPU, ~400MB RAM.
"""

import json
import threading
from pathlib import Path
from typing import Any, Optional

from loguru import logger

try:
    from huggingface_hub import hf_hub_download
    from llama_cpp import Llama
except ImportError as e:
    raise ImportError(
        "LLM tool requires llama-cpp-python and huggingface-hub. "
        "Install with: pip install micromech[llm]"
    ) from e

ALLOWED_TOOLS = ["llm"]

DEFAULT_MODEL_REPO = "Qwen/Qwen2.5-0.5B-Instruct-GGUF"
DEFAULT_MODEL_FILE = "qwen2.5-0.5b-instruct-q4_k_m.gguf"
DEFAULT_MAX_TOKENS = 256
DEFAULT_CONTEXT_SIZE = 2048

_llm_instance: Optional[Llama] = None
_llm_lock = threading.Lock()


def _get_llm(
    model_repo: str = DEFAULT_MODEL_REPO,
    model_file: str = DEFAULT_MODEL_FILE,
) -> Llama:
    """Get or create the singleton LLM instance (thread-safe)."""
    global _llm_instance
    if _llm_instance is None:
        with _llm_lock:
            if _llm_instance is None:
                models_dir = Path.home() / ".micromech" / "models"
                models_dir.mkdir(parents=True, exist_ok=True)
                model_path = models_dir / model_file
                if not model_path.exists():
                    logger.info("Downloading {} from {}", model_file, model_repo)
                    hf_hub_download(
                        repo_id=model_repo,
                        filename=model_file,
                        local_dir=str(models_dir),
                    )
                logger.info("Loading LLM from {}", model_path)
                _llm_instance = Llama(
                    model_path=str(model_path),
                    n_ctx=DEFAULT_CONTEXT_SIZE,
                    n_threads=4,
                    verbose=False,
                )
    return _llm_instance


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The input text.
        system_prompt: Optional system prompt (default: "You are a helpful assistant.").
        max_tokens: Optional max tokens.
        temperature: Optional temperature.
        counter_callback: Optional token counter.
    """
    prompt = kwargs.get("prompt", "")
    system_prompt = kwargs.get("system_prompt", "You are a helpful assistant.")
    max_tokens = kwargs.get("max_tokens", DEFAULT_MAX_TOKENS)
    temperature = kwargs.get("temperature", 0.3)
    counter_callback = kwargs.get("counter_callback")

    llm = _get_llm()

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
            "model": DEFAULT_MODEL_REPO,
            "tokens": usage.get("total_tokens", 0),
        }
    )

    return result, prompt, None, counter_callback
