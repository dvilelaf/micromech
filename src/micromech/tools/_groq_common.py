"""Shared utilities for Groq-based prediction market tools (superforcaster, prediction-request-reasoning)."""

import json
import re
from typing import Any, Final, Optional

from loguru import logger

# Valory-compatible return type: (result, prompt_used, meta, counter_callback)
MechResponse = tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]

# HTTP timeout (seconds) for Groq API calls — shared by all Groq-based tools.
# The executor-level timeout (component.yaml) is the outer bound.
GROQ_HTTP_TIMEOUT: Final[int] = 60

# Models available on Groq's free tier that we trust for forecasting.
# Rejects unknown values to prevent model-downgrade attacks via untrusted IPFS payloads.
GROQ_ALLOWED_MODELS: Final[frozenset[str]] = frozenset(
    {
        "llama-3.3-70b-versatile",
        "llama-3.1-70b-versatile",
        "llama-3.1-8b-instant",
        "llama3-70b-8192",
        "llama3-8b-8192",
        "gemma2-9b-it",
        "mixtral-8x7b-32768",
    }
)

# Valory's superforecasting prompt (Apache 2.0, Valory AG 2023-2024)
PREDICTION_PROMPT = """
You are an advanced AI system which has been finetuned to provide calibrated probabilistic
forecasts under uncertainty, with your performance evaluated according to the Brier score. When
forecasting, do not treat 0.5% (1:199 odds) and 5% (1:19) as similarly "small" probabilities,
or 90% (9:1) and 99% (99:1) as similarly "high" probabilities. As the odds show, they are
markedly different, so output your probabilities accordingly.

Question:
{question}

Today's date: {today}
Your pretraining knowledge cutoff: December 2024

We have retrieved the following information for this question:
<background>{sources}</background>

Recall the question you are forecasting:
{question}

Instructions:
1. Compress key factual information from the sources, as well as useful background information
which may not be in the sources, into a list of core factual points to reference. Aim for
information which is specific, relevant, and covers the core considerations you'll use to make
your forecast. For this step, do not draw any conclusions about how a fact will influence your
answer or forecast. Place this section of your response in <facts></facts> tags.

2. Provide a few reasons why the answer might be no. Rate the strength of each reason on a
scale of 1-10. Use <no></no> tags.

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason on a
scale of 1-10. Use <yes></yes> tags.

4. Aggregate your considerations. Do not summarize or repeat previous points; instead,
investigate how the competing factors and mechanisms interact and weigh against each other.
Factorize your thinking across (exhaustive, mutually exclusive) cases if and only if it would be
beneficial to your reasoning. We have detected that you overestimate world conflict, drama,
violence, and crises due to news' negativity bias, which doesn't necessarily represent overall
trends or base rates. Similarly, we also have detected you overestimate dramatic, shocking,
or emotionally charged news due to news' sensationalism bias. Therefore adjust for news'
negativity bias and sensationalism bias by considering reasons to why your provided sources
might be biased or exaggerated. Think like a superforecaster. Use <thinking></thinking> tags
for this section of your response.

5. Output an initial probability (prediction) as a single number between 0 and 1 given steps 1-4.
Use <tentative></tentative> tags.

6. Reflect on your answer, performing sanity checks and mentioning any additional knowledge
or background information which may be relevant. Check for over/underconfidence, improper
treatment of conjunctive or disjunctive conditions (only if applicable), and other forecasting
biases when reviewing your reasoning. Consider priors/base rates, and the extent to which
case-specific information justifies the deviation between your tentative forecast and the prior.
Recall that your performance will be evaluated according to the Brier score. Be precise with tail
probabilities. Leverage your intuitions, but never change your forecast for the sake of modesty
or balance alone. Finally, aggregate all of your previous reasoning and highlight key factors
that inform your final forecast. Use <thinking></thinking> tags for this portion of your response.

7. Output your final prediction (a number between 0 and 1 with an asterisk at the beginning and
end of the decimal) in <answer></answer> tags.


OUTPUT_FORMAT
* Your output response must be only a single JSON object to be parsed by Python's "json.loads()".
* The JSON must contain four fields: "p_yes", "p_no", "confidence", and "info_utility".
* Each item in the JSON must have a value between 0 and 1.
   - "p_yes": Estimated probability that the event in the "Question" occurs.
   - "p_no": Estimated probability that the event in the "Question" does not occur.
   - "confidence": A value between 0 and 1 indicating the confidence in the prediction. 0 indicates lowest
     confidence value; 1 maximum confidence value.
   - "info_utility": Utility of the information provided in "sources" to help you make the prediction.
     0 indicates lowest utility; 1 maximum utility.
* The sum of "p_yes" and "p_no" must equal 1.
* Output only the JSON object. Do not include any other contents in your response.
* This is incorrect:"```json{{\\n  \\"p_yes\\": 0.2,\\n  \\"p_no\\": 0.8,\\n  \\"confidence\\": 0.7,\\n  \\"info_utility\\": 0.5\\n}}```"
* This is correct:"{{\\"p_yes\\": 0.2, \\"p_no\\": 0.8, \\"confidence\\": 0.7, \\"info_utility\\": 0.5}}"
"""

GROQ_SYSTEM_PROMPT: Final[str] = (
    "You are an expert forecaster. Respond only with a valid JSON object."
)

DEFAULT_PREDICTION: Final[str] = json.dumps(
    {"p_yes": 0.5, "p_no": 0.5, "confidence": 0.0, "info_utility": 0.0}
)

_JSON_RE = re.compile(r'\{[^{}]*"p_yes"[^{}]*\}', re.DOTALL)


def _sanitize_sources(text: str) -> str:
    """Escape closing background tag to prevent prompt injection via DDG results."""
    return text.replace("</background>", "&lt;/background&gt;")


def _extract_json(text: str) -> str:
    """Extract JSON prediction object from LLM response.

    Handles: pure JSON, JSON with surrounding text/XML tags, code-fenced JSON.
    Note: always chain with _validate_prediction — this function may return non-JSON.
    """
    text = re.sub(r"```(?:json)?\s*", "", text).strip()

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return json.dumps(obj)
    except json.JSONDecodeError:
        pass

    match = _JSON_RE.search(text)
    if match:
        return match.group(0)

    for candidate in reversed(re.findall(r"\{[^{}]+\}", text, re.DOTALL)):
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict) and any(k in obj for k in ("p_yes", "p_no")):
                return candidate
        except json.JSONDecodeError:
            continue

    return text.strip()


def _validate_prediction(raw: str) -> str:
    """Validate and normalize prediction JSON, returning defaults on failure.

    Ensures: values are floats clamped to [0, 1], missing fields default to 0,
    and p_yes + p_no sum to 1.0.
    """
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("prediction must be a JSON object, got %s" % type(data).__name__)

        # Cast and clamp all numeric fields to [0, 1] to guard against LLM returning
        # out-of-range values (e.g. -0.5, 1.5) or string numbers (e.g. "0.7").
        for field in ("p_yes", "p_no", "confidence", "info_utility"):
            raw_val = data.get(field, 0.5 if field.startswith("p_") else 0.0)
            data[field] = max(0.0, min(1.0, float(raw_val)))

        total = data["p_yes"] + data["p_no"]
        if total <= 0:
            # Both zero after clamping — not a valid prediction
            return DEFAULT_PREDICTION
        if abs(total - 1.0) > 0.01:
            data["p_yes"] /= total
            data["p_no"] /= total

        return json.dumps(data)
    except Exception as e:
        logger.warning("Failed to parse Groq prediction JSON: {}", e)
        return DEFAULT_PREDICTION
