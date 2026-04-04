"""Gemma 4 tool via Google AI Studio API.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Calls Gemma 4 models through the Google Generative AI API.
Requires GOOGLE_API_KEY environment variable.
"""

import json
import os
from typing import Any, Optional

from loguru import logger

ALLOWED_TOOLS = ["gemma4-api"]

DEFAULT_MODEL = "gemma-4-27b-it"
AVAILABLE_MODELS = {
    "gemma-4-27b-it",
    "gemma-4-12b-it",
    "gemma-4-4b-it",
    "gemma-4-2b-it",
}


def _get_client() -> Any:
    """Create a Google GenAI client."""
    from google import genai

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        msg = "GOOGLE_API_KEY environment variable is not set"
        raise RuntimeError(msg)
    return genai.Client(api_key=api_key)


def run(
    **kwargs: Any,
) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The input text.
        model: Model name (default: gemma-4-27b-it).
        system_prompt: Optional system prompt.
        max_tokens: Optional max output tokens.
        temperature: Optional temperature.
        counter_callback: Optional token counter.
    """
    prompt = kwargs.get("prompt", "")
    model_name = kwargs.get("model", DEFAULT_MODEL)
    system_prompt = kwargs.get("system_prompt", "You are a helpful assistant.")
    max_tokens = kwargs.get("max_tokens", 256)
    temperature = kwargs.get("temperature", 0.3)
    counter_callback = kwargs.get("counter_callback")

    if model_name not in AVAILABLE_MODELS:
        logger.warning(
            "Unknown model '{}', falling back to {}", model_name, DEFAULT_MODEL
        )
        model_name = DEFAULT_MODEL

    client = _get_client()

    from google.genai import types

    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            max_output_tokens=max_tokens,
            temperature=temperature,
        ),
    )

    content = response.text or ""
    tokens = 0
    if response.usage_metadata:
        tokens = response.usage_metadata.total_token_count or 0

    result = json.dumps(
        {
            "result": content,
            "model": model_name,
            "tokens": tokens,
        }
    )

    return result, prompt, None, counter_callback
