"""Prediction market tool using web search + local LLM.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Equivalent to Valory's prediction-offline: takes a binary prediction market
question and returns p_yes/p_no/confidence/info_utility.
Searches the web for recent news and context before prompting the LLM.
"""

import json
import re
from typing import Any, Optional

from loguru import logger

# Max characters of search context to feed the LLM
_MAX_CONTEXT_CHARS = 2000
_MAX_RESULTS = 5

ALLOWED_TOOLS = [
    "prediction-offline",
    "prediction-offline-local",
]

# Prediction prompt template (matches Valory's format)
PREDICTION_PROMPT = """\
You are an LLM inside a multi-agent system that takes in a prompt \
of a user requesting a probability estimation of whether a given event will happen. \
You are provided with an input under the label "USER_PROMPT". You must follow the \
instructions under the label "INSTRUCTIONS". You must provide your response in the \
format specified under "OUTPUT_FORMAT".

INSTRUCTIONS
* Read the input under "USER_PROMPT" carefully.
* Output ONLY a JSON object in the format specified below.
* Do NOT include any markdown formatting, code fences, or explanation.

USER_PROMPT:
{user_prompt}

ADDITIONAL_INFORMATION:
{additional_information}

OUTPUT_FORMAT
* Your output response must be ONLY a JSON object with the following fields:
  - "p_yes": a float between 0 and 1 representing the probability the event happens.
  - "p_no": a float between 0 and 1 representing the probability the event does not happen.
  - "confidence": a float between 0 and 1 indicating your confidence in the prediction.
  - "info_utility": a float between 0 and 1 representing how useful the additional \
information was (0 if none provided).
* p_yes and p_no MUST sum to 1.
* Output ONLY the JSON object, nothing else."""

# Regex to extract JSON from LLM response (same as Valory)
JSON_EXTRACT_RE = re.compile(r"(\{[^}]*\})")

DEFAULT_PREDICTION = json.dumps(
    {
        "p_yes": 0.5,
        "p_no": 0.5,
        "confidence": 0.0,
        "info_utility": 0.0,
    }
)


def _search_context(query: str) -> str:
    """Search the web for recent info relevant to the prediction question.

    Combines news and text results from DuckDuckGo into a concise context string.
    Returns empty string if search is unavailable or fails.
    """
    try:
        from ddgs import DDGS
    except ImportError:
        logger.debug("ddgs not installed, skipping web search")
        return ""

    snippets = []
    try:
        ddgs = DDGS()

        # News first (most relevant for predictions)
        for r in ddgs.news(query, max_results=_MAX_RESULTS):
            title = r.get("title", "")
            body = r.get("body", "")
            date = r.get("date", "")
            if title:
                entry = f"[{date[:10]}] {title}"
                if body:
                    entry += f": {body[:150]}"
                snippets.append(entry)

        # Supplement with web results
        for r in ddgs.text(query, max_results=_MAX_RESULTS):
            body = r.get("body", "")
            if body and body[:200] not in {s[:200] for s in snippets}:
                snippets.append(body[:200])

    except Exception as e:
        logger.debug("Web search failed: {}", e)
        return ""

    if not snippets:
        return ""

    context = "\n".join(snippets)
    if len(context) > _MAX_CONTEXT_CHARS:
        context = context[:_MAX_CONTEXT_CHARS] + "..."
    logger.info("Search context: {} chars from {} snippets", len(context), len(snippets))
    return context


def _extract_json(text: str) -> str:
    """Extract JSON object from LLM response, stripping markdown etc."""
    match = JSON_EXTRACT_RE.search(text)
    if match:
        return match.group(1)
    return text.strip()


def _validate_prediction(raw: str) -> str:
    """Validate and normalize prediction JSON."""
    try:
        data = json.loads(raw)
        for field in ("p_yes", "p_no", "confidence", "info_utility"):
            if field not in data:
                data[field] = 0.5 if field.startswith("p_") else 0.0
        # Normalize p_yes + p_no = 1
        total = data["p_yes"] + data["p_no"]
        if total > 0 and abs(total - 1.0) > 0.01:
            data["p_yes"] /= total
            data["p_no"] /= total
        return json.dumps(data)
    except (json.JSONDecodeError, KeyError, TypeError):
        logger.warning("Failed to parse prediction JSON, returning defaults")
        return DEFAULT_PREDICTION


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The prediction market question.
        tool: Tool name (prediction-offline, prediction-offline-local).
        additional_information: Optional additional context.
        counter_callback: Optional token counter.
    """
    prompt = kwargs.get("prompt", "")
    counter_callback = kwargs.get("counter_callback")
    additional_info = kwargs.get("additional_information", "")

    # Search the web for recent context if none provided
    if not additional_info:
        additional_info = _search_context(prompt)

    prediction_prompt = PREDICTION_PROMPT.format(
        user_prompt=prompt,
        additional_information=additional_info or "No additional information provided.",
    )

    # Use local LLM for inference
    try:
        from micromech.tools.llm_tool.llm_tool import _get_llm, _llm_lock, _resolve_model

        model_repo, model_file = _resolve_model(kwargs)
        llm = _get_llm(model_repo=model_repo, model_file=model_file)
        with _llm_lock:
            response = llm.create_chat_completion(
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prediction_prompt},
                ],
                max_tokens=256,
                temperature=0.3,
            )
        raw_text = response["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error("LLM inference failed: {}", e)
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback

    # Extract and validate prediction
    prediction_json = _extract_json(raw_text)
    result = _validate_prediction(prediction_json)

    return result, prediction_prompt, None, counter_callback
