"""Prediction-request-reasoning tool: multi-query web search + Groq Llama for forecasting.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Free-API replacement for Valory's prediction-request-reasoning (OpenAI + Google + FAISS).
Uses Groq Llama-3.3-70b-versatile + DuckDuckGo multi-query search.

Key differentiator from superforcaster: generates multiple search query variants
and collects more context snippets, leveraging Groq's 128K token context window.
"""

from datetime import date
from typing import Any

import openai
from loguru import logger
from openai import OpenAI

from micromech.secrets import secrets
from micromech.tools._groq_common import (
    DEFAULT_PREDICTION,
    GROQ_ALLOWED_MODELS,
    GROQ_HTTP_TIMEOUT,
    GROQ_SYSTEM_PROMPT,
    PREDICTION_PROMPT,
    MechResponse,
    _extract_json,
    _sanitize_sources,
    _validate_prediction,
)

ALLOWED_TOOLS = [
    "prediction-request-reasoning",
    "prediction-request-reasoning-claude",  # TODO: differentiate when Claude backend available
]
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
_MAX_SEARCH_RESULTS = 8
_MAX_CONTEXT_CHARS = 16000


def _generate_queries(question: str) -> list[str]:
    """Generate multiple search query variants from the original question.

    Produces 3 complementary queries to broaden search coverage:
    - The original question (direct match)
    - A news-focused variant
    - A forecast/probability-focused variant
    """
    core = question.rstrip("?").strip()
    return [
        question,
        f"{core} latest news",
        f"{core} prediction probability",
    ]


def _search_ddgs_multi(queries: list[str]) -> str:
    """Search DuckDuckGo with multiple queries and deduplicate results."""
    try:
        from ddgs import DDGS
    except ImportError:
        logger.debug("ddgs not installed, skipping web search")
        return ""

    snippets: list[str] = []
    seen_prefixes: set[str] = set()

    try:
        ddgs = DDGS(timeout=10)
        for query in queries:
            try:
                for r in ddgs.news(query, max_results=_MAX_SEARCH_RESULTS):
                    title = r.get("title", "")
                    body = r.get("body", "")
                    date_str = str(r.get("date", ""))[:10]
                    link = r.get("url", r.get("link", ""))
                    if title:
                        prefix = body[:100]
                        if prefix not in seen_prefixes:
                            seen_prefixes.add(prefix)
                            snippets.append(f"[{date_str}] {title}\n{body}\n{link}")

                for r in ddgs.text(query, max_results=_MAX_SEARCH_RESULTS):
                    body = r.get("body", "")
                    title = r.get("title", "")
                    link = r.get("href", "")
                    if body:
                        prefix = body[:100]
                        if prefix not in seen_prefixes:
                            seen_prefixes.add(prefix)
                            snippets.append(f"{title}\n{body[:400]}\n{link}")
            except Exception as e:
                logger.debug("DuckDuckGo search failed for query '{}': {}", query, e)
                continue

    except Exception as e:
        logger.debug("DuckDuckGo DDGS init failed: {}", e)
        return ""

    if not snippets:
        return ""

    context = "\n\n".join(snippets)
    if len(context) > _MAX_CONTEXT_CHARS:
        context = context[:_MAX_CONTEXT_CHARS] + "..."
    logger.info(
        "Prediction-request-reasoning search: {} chars from {} snippets across {} queries",
        len(context),
        len(snippets),
        len(queries),
    )
    return context


def run(**kwargs: Any) -> MechResponse:
    """Valory-compatible entry point.

    kwargs:
        prompt: The prediction market question.
        tool: Tool name (prediction-request-reasoning or prediction-request-reasoning-claude).
        model: Optional Groq model override (must be in GROQ_ALLOWED_MODELS).
        counter_callback: Optional token counter.

    Returns:
        (result_json, prompt_used, meta, counter_callback).
        prompt_used is "" (not None) when groq_api_key is unset.
    """
    counter_callback = kwargs.get("counter_callback")
    prompt = kwargs.get("prompt", "")

    groq_key = secrets.groq_api_key
    if groq_key is None:
        logger.warning("groq_api_key not set, returning default prediction")
        return DEFAULT_PREDICTION, "", None, counter_callback

    queries = _generate_queries(prompt)
    sources = _search_ddgs_multi(queries)
    today = date.today().strftime("%d/%m/%Y")
    prediction_prompt = PREDICTION_PROMPT.format(
        question=prompt,
        today=today,
        sources=_sanitize_sources(sources) or "No additional information found.",
    )

    model_req = kwargs.get("model", DEFAULT_GROQ_MODEL)
    model = model_req if model_req in GROQ_ALLOWED_MODELS else DEFAULT_GROQ_MODEL
    if model != model_req:
        logger.warning("Requested model '{}' not in allow-list, using default", model_req)

    try:
        client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key.get_secret_value(),
            timeout=GROQ_HTTP_TIMEOUT,
        )
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": GROQ_SYSTEM_PROMPT},
                {"role": "user", "content": prediction_prompt},
            ],
            temperature=0,
            max_tokens=3000,
        )
        if not response.choices:
            logger.warning("Groq returned empty choices in prediction-request-reasoning")
            return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback
        raw_text = response.choices[0].message.content or ""
    except openai.RateLimitError:
        logger.warning("Groq rate limit hit in prediction-request-reasoning — free tier may be exhausted")
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback
    except Exception as e:
        logger.error("Groq API call failed in prediction-request-reasoning: {} {}", type(e).__name__, e)
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback

    prediction_json = _extract_json(raw_text)
    result = _validate_prediction(prediction_json)
    return result, prediction_prompt, None, counter_callback
