"""Local LLM tool using llama-cpp-python.

Default model: Qwen2.5-0.5B-Instruct (Q4_K_M quantization).
Runs on CPU, ~400MB RAM, ~60-80 tokens/sec.

All inference is offloaded to a thread pool to avoid blocking the event loop.
"""

import asyncio
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
)
from micromech.tools.base import Tool, ToolMetadata

try:
    from huggingface_hub import hf_hub_download
    from llama_cpp import Llama
except ImportError as e:
    raise ImportError(
        "LLM tool requires llama-cpp-python and huggingface-hub. "
        "Install with: pip install micromech[llm]"
    ) from e


PREDICTION_SYSTEM_PROMPT = (
    "You are a prediction market analyst. Given a yes/no question about future events, "
    "estimate the probability. Respond ONLY with a JSON object containing: "
    '"p_yes" (float 0-1), "p_no" (float 0-1), "confidence" (float 0-1), '
    'and "info_utility" (float 0-1). Example: '
    '{"p_yes": 0.6, "p_no": 0.4, "confidence": 0.7, "info_utility": 0.5}'
)


class LLMTool(Tool):
    """Local LLM tool for prediction market questions."""

    metadata = ToolMetadata(
        id="llm",
        name="Local LLM",
        description="Answers prediction market questions using a local LLM.",
        version="0.1.0",
        timeout=120,
    )

    def __init__(
        self,
        model_repo: str = DEFAULT_LLM_MODEL,
        model_file: str = DEFAULT_LLM_FILE,
        models_dir: Optional[Path] = None,
        max_tokens: int = DEFAULT_LLM_MAX_TOKENS,
        context_size: int = DEFAULT_LLM_CONTEXT_SIZE,
    ):
        self.model_repo = model_repo
        self.model_file = model_file
        self.models_dir = models_dir or Path.home() / ".micromech" / "models"
        self.max_tokens = max_tokens
        self.context_size = context_size
        self._llm: Optional[Llama] = None
        self._init_lock = threading.Lock()

    def _ensure_model(self) -> Path:
        """Download model if not already cached."""
        self.models_dir.mkdir(parents=True, exist_ok=True)
        model_path = self.models_dir / self.model_file
        if not model_path.exists():
            logger.info("Downloading model {} from {}", self.model_file, self.model_repo)
            hf_hub_download(
                repo_id=self.model_repo,
                filename=self.model_file,
                local_dir=str(self.models_dir),
            )
            logger.info("Model downloaded to {}", model_path)
        return model_path

    def _get_llm(self) -> Llama:
        """Lazy-load the LLM instance (thread-safe)."""
        if self._llm is None:
            with self._init_lock:
                if self._llm is None:
                    model_path = self._ensure_model()
                    logger.info("Loading LLM from {}", model_path)
                    self._llm = Llama(
                        model_path=str(model_path),
                        n_ctx=self.context_size,
                        n_threads=4,
                        verbose=False,
                    )
                    logger.info("LLM loaded successfully")
        return self._llm

    def _sync_execute(self, prompt: str, system_prompt: str) -> str:
        """Synchronous inference — called from thread pool."""
        llm = self._get_llm()

        response = llm.create_chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            max_tokens=self.max_tokens,
            temperature=0.3,
        )

        content = response["choices"][0]["message"]["content"]
        usage = response.get("usage", {})

        return json.dumps(
            {
                "result": content,
                "tool": "llm",
                "model": self.model_repo,
                "tokens": usage.get("total_tokens", 0),
            }
        )

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Run prediction market analysis, offloaded to thread pool."""
        system_prompt = kwargs.get("system_prompt", PREDICTION_SYSTEM_PROMPT)
        return await asyncio.to_thread(self._sync_execute, prompt, system_prompt)
