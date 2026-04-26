"""Superforcaster tool: DuckDuckGo web search + Groq Llama for calibrated probability forecasting.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Free-API replacement for Valory's superforcaster (OpenAI gpt-4o + Serper).
Uses Groq Llama-3.3-70b-versatile + DuckDuckGo.
"""

from datetime import date
from typing import Any, Optional

import openai
from loguru import logger
from openai import OpenAI

from micromech.secrets import secrets
from micromech.tools._groq_common import (
    DEFAULT_PREDICTION,
    GROQ_SYSTEM_PROMPT,
    PREDICTION_PROMPT,
    _extract_json,
    _validate_prediction,
)

ALLOWED_TOOLS = ["superforcaster"]
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
_MAX_SEARCH_RESULTS = 5
_MAX_CONTEXT_CHARS = 8000
_GROQ_TIMEOUT = 60


def _search_ddgs(question: str) -> str:
    """Search DuckDuckGo for context relevant to the prediction question."""
    try:
        from ddgs import DDGS
    except ImportError:
        logger.debug("ddgs not installed, skipping web search")
        return ""

    snippets: list[str] = []
    seen_body_prefixes: set[str] = set()
    try:
        ddgs = DDGS()
        for r in ddgs.news(question, max_results=_MAX_SEARCH_RESULTS):
            title = r.get("title", "")
            body = r.get("body", "")
            date_str = str(r.get("date", ""))[:10]
            link = r.get("url", r.get("link", ""))
            if title:
                prefix = body[:100]
                if prefix not in seen_body_prefixes:
                    seen_body_prefixes.add(prefix)
                    snippets.append(f"[{date_str}] {title}\n{body}\n{link}")

        for r in ddgs.text(question, max_results=_MAX_SEARCH_RESULTS):
            body = r.get("body", "")
            title = r.get("title", "")
            link = r.get("href", "")
            if body:
                prefix = body[:100]
                if prefix not in seen_body_prefixes:
                    seen_body_prefixes.add(prefix)
                    snippets.append(f"{title}\n{body[:300]}\n{link}")

    except Exception as e:
        logger.debug("DuckDuckGo search failed: {}", e)
        return ""

    if not snippets:
        return ""

    context = "\n\n".join(snippets)
    if len(context) > _MAX_CONTEXT_CHARS:
        context = context[:_MAX_CONTEXT_CHARS] + "..."
    logger.info("Superforcaster search: {} chars from {} snippets", len(context), len(snippets))
    return context


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The prediction market question.
        tool: Tool name (superforcaster).
        model: Optional Groq model override.
        counter_callback: Optional token counter.
    """
    counter_callback = kwargs.get("counter_callback")
    prompt = kwargs.get("prompt", "")

    groq_key = secrets.groq_api_key
    if groq_key is None:
        logger.warning("groq_api_key not set, returning default prediction")
        return DEFAULT_PREDICTION, None, None, counter_callback

    sources = _search_ddgs(prompt)
    today = date.today().strftime("%d/%m/%Y")
    prediction_prompt = PREDICTION_PROMPT.format(
        question=prompt,
        today=today,
        sources=sources or "No additional information found.",
    )

    try:
        client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key.get_secret_value(),
            timeout=_GROQ_TIMEOUT,
        )
        model = kwargs.get("model", DEFAULT_GROQ_MODEL)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": GROQ_SYSTEM_PROMPT},
                {"role": "user", "content": prediction_prompt},
            ],
            temperature=0,
            max_tokens=3000,
        )
        raw_text = response.choices[0].message.content or ""
    except openai.RateLimitError:
        logger.warning("Groq rate limit hit in superforcaster — free tier may be exhausted")
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback
    except Exception as e:
        logger.error("Groq API call failed in superforcaster: {}", e)
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback

    prediction_json = _extract_json(raw_text)
    result = _validate_prediction(prediction_json)
    return result, prediction_prompt, None, counter_callback
