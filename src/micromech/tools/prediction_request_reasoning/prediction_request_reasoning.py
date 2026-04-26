"""Prediction-request-reasoning tool: multi-query web search + Groq Llama for forecasting.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Free-API replacement for Valory's prediction-request-reasoning (OpenAI + Google + FAISS).
Uses Groq Llama-3.3-70b-versatile + DuckDuckGo multi-query search.

Key differentiator from superforcaster: generates multiple search query variants
and collects more context snippets, leveraging Groq's 128K token context window.
"""

import json
import re
from datetime import date
from typing import Any, Optional

from loguru import logger
from openai import OpenAI

from micromech.secrets import secrets

ALLOWED_TOOLS = [
    "prediction-request-reasoning",
    "prediction-request-reasoning-claude",
]
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
_MAX_SEARCH_RESULTS = 8
_MAX_CONTEXT_CHARS = 16000

# Valory's superforecasting prompt (Apache 2.0, Valory AG 2024)
PREDICTION_PROMPT = """
You are an advanced AI system which has been finetuned to provide calibrated probabilistic
forecasts under uncertainty, with your performance evaluated according to the Brier score. When
forecasting, do not treat 0.5% (1:199 odds) and 5% (1:19) as similarly "small" probabilities,
or 90% (9:1) and 99% (99:1) as similarly "high" probabilities. As the odds show, they are
markedly different, so output your probabilities accordingly.

Question:
{question}

Today's date: {today}
Your pretraining knowledge cutoff: October 2023

We have retrieved the following information for this question from multiple searches:
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

DEFAULT_PREDICTION = json.dumps(
    {"p_yes": 0.5, "p_no": 0.5, "confidence": 0.0, "info_utility": 0.0}
)

_JSON_RE = re.compile(r'\{[^{}]*"p_yes"[^{}]*\}', re.DOTALL)


def _generate_queries(question: str) -> list[str]:
    """Generate multiple search query variants from the original question.

    Produces 3 complementary queries to broaden search coverage:
    - The original question (direct match)
    - A news-focused variant
    - A forecast/probability-focused variant
    """
    # Strip common question markers to get the core subject
    core = re.sub(r"\?$", "", question.strip())
    core = re.sub(
        r"^(will|would|could|can|is|are|was|were|has|have|did|does|do)\s+",
        "",
        core,
        flags=re.IGNORECASE,
    )
    return [
        question,
        f"{core} latest news",
        f"{core} probability forecast",
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
        ddgs = DDGS()
        for query in queries:
            try:
                for r in ddgs.news(query, max_results=_MAX_SEARCH_RESULTS):
                    title = r.get("title", "")
                    body = r.get("body", "")
                    date_str = str(r.get("date", ""))[:10]
                    link = r.get("url", r.get("link", ""))
                    if title:
                        prefix = (title + body)[:100]
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


def _extract_json(text: str) -> str:
    """Extract JSON prediction object from LLM response.

    Handles: pure JSON, JSON with surrounding text/XML tags, code-fenced JSON.
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
    """Validate and normalize prediction JSON, returning defaults on failure."""
    try:
        data = json.loads(raw)
        for field in ("p_yes", "p_no", "confidence", "info_utility"):
            if field not in data:
                data[field] = 0.5 if field.startswith("p_") else 0.0
        total = data["p_yes"] + data["p_no"]
        if total > 0 and abs(total - 1.0) > 0.01:
            data["p_yes"] /= total
            data["p_no"] /= total
        return json.dumps(data)
    except (json.JSONDecodeError, KeyError, TypeError):
        logger.warning("Failed to parse prediction-request-reasoning JSON")
        return DEFAULT_PREDICTION


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    kwargs:
        prompt: The prediction market question.
        tool: Tool name (prediction-request-reasoning or prediction-request-reasoning-claude).
        model: Optional Groq model override.
        counter_callback: Optional token counter.
    """
    counter_callback = kwargs.get("counter_callback")
    prompt = kwargs.get("prompt", "")

    groq_key = secrets.groq_api_key
    if groq_key is None:
        logger.warning("groq_api_key not set, returning default prediction")
        return DEFAULT_PREDICTION, None, None, counter_callback

    queries = _generate_queries(prompt)
    sources = _search_ddgs_multi(queries)
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
        )
        model = kwargs.get("model", DEFAULT_GROQ_MODEL)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prediction_prompt},
            ],
            temperature=0,
            max_tokens=1500,
        )
        raw_text = response.choices[0].message.content or ""
    except Exception as e:
        logger.error("Groq API call failed in prediction-request-reasoning: {}", e)
        return DEFAULT_PREDICTION, prediction_prompt, None, counter_callback

    prediction_json = _extract_json(raw_text)
    result = _validate_prediction(prediction_json)
    return result, prediction_prompt, None, counter_callback
