"""Prediction market tool using local LLM.

Valory-compatible: defines ALLOWED_TOOLS and run(**kwargs) -> MechResponse.

Equivalent to Valory's prediction-offline tool: takes a binary prediction
market question, runs it through a local LLM, and returns p_yes/p_no/confidence.
Uses the same prompt template as Valory's mech-predict-markets package.
"""

import json
import re
from typing import Any

from loguru import logger

from micromech.tools.base import MechResponse, Tool, ToolMetadata

ALLOWED_TOOLS = ["prediction-offline", "prediction-offline-local"]

# Valory's prediction prompt template (simplified from mech-predict-markets)
PREDICTION_PROMPT = """You are an LLM inside a multi-agent system that takes in a prompt \
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


def _extract_prediction_json(text: str) -> str:
    """Extract JSON object from LLM response, stripping markdown etc."""
    match = JSON_EXTRACT_RE.search(text)
    if match:
        return match.group(1)
    return text.strip()


class PredictionTool(Tool):
    """Prediction market tool using local LLM (prediction-offline equivalent)."""

    metadata = ToolMetadata(
        id="prediction-offline",
        name="Prediction Offline (Local LLM)",
        description=(
            "Estimates probabilities for binary prediction market questions "
            "using a local LLM. Equivalent to Valory's prediction-offline."
        ),
        version="0.1.0",
        timeout=120,
    )
    ALLOWED_TOOLS = ALLOWED_TOOLS

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Run prediction analysis using local LLM."""
        from micromech.tools.builtin.llm import LLMTool

        additional_info = kwargs.get("additional_information", "")

        prediction_prompt = PREDICTION_PROMPT.format(
            user_prompt=prompt,
            additional_information=additional_info or "No additional information provided.",
        )

        # Use local LLM for inference
        llm_tool = LLMTool()
        llm_result = await llm_tool.execute(
            prediction_prompt,
            system_prompt="You are a helpful assistant.",
            max_tokens=256,
            temperature=0.3,
        )

        # Extract the LLM's raw text response
        try:
            llm_data = json.loads(llm_result)
            raw_text = llm_data.get("result", llm_result)
        except json.JSONDecodeError:
            raw_text = llm_result

        # Extract and validate prediction JSON
        prediction_json = _extract_prediction_json(raw_text)

        try:
            prediction = json.loads(prediction_json)
            # Validate required fields
            for field in ("p_yes", "p_no", "confidence", "info_utility"):
                if field not in prediction:
                    prediction[field] = 0.5 if field.startswith("p_") else 0.0
            # Ensure p_yes + p_no = 1
            total = prediction["p_yes"] + prediction["p_no"]
            if total > 0 and abs(total - 1.0) > 0.01:
                prediction["p_yes"] /= total
                prediction["p_no"] /= total
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Failed to parse prediction JSON, returning defaults")
            prediction = {
                "p_yes": 0.5,
                "p_no": 0.5,
                "confidence": 0.0,
                "info_utility": 0.0,
            }

        return json.dumps(prediction)


def run(**kwargs: Any) -> MechResponse:
    """Valory-compatible entry point."""
    tool = PredictionTool()
    return tool.run(**kwargs)
