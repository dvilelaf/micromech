"""Local LLM tool using llama-cpp-python.

Valory-compatible: defines ALLOWED_TOOLS and run(**kwargs) -> MechResponse.

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
from micromech.tools.base import MechResponse, Tool, ToolMetadata

try:
    from huggingface_hub import hf_hub_download
    from llama_cpp import Llama
except ImportError as e:
    raise ImportError(
        "LLM tool requires llama-cpp-python and huggingface-hub. "
        "Install with: pip install micromech[llm]"
    ) from e

ALLOWED_TOOLS = ["llm"]


class LLMTool(Tool):
    """Local LLM tool for general-purpose prompts."""

    metadata = ToolMetadata(
        id="llm",
        name="Local LLM",
        description="Answers prompts using a local LLM (Qwen 0.5B, CPU).",
        version="0.1.0",
        timeout=120,
    )
    ALLOWED_TOOLS = ALLOWED_TOOLS

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

    def _sync_chat(
        self, messages: list[dict[str, str]], max_tokens: int, temperature: float
    ) -> dict:
        """Synchronous LLM chat completion — called from thread pool."""
        llm = self._get_llm()
        return llm.create_chat_completion(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Run LLM inference, offloaded to thread pool."""
        system_prompt = kwargs.get("system_prompt", "You are a helpful assistant.")
        max_tokens = kwargs.get("max_tokens", self.max_tokens)
        temperature = kwargs.get("temperature", 0.3)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        response = await asyncio.to_thread(self._sync_chat, messages, max_tokens, temperature)

        content = response["choices"][0]["message"]["content"]
        usage = response.get("usage", {})

        return json.dumps(
            {
                "result": content,
                "model": self.model_repo,
                "tokens": usage.get("total_tokens", 0),
            }
        )


def run(**kwargs: Any) -> MechResponse:
    """Valory-compatible entry point."""
    tool = LLMTool()
    return tool.run(**kwargs)
