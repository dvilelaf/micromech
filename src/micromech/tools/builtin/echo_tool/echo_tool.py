"""Echo tool — returns a default prediction with p_yes=0.5.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Useful for testing the full request-execute-deliver pipeline without LLM.
"""

import json
from typing import Any, Optional

ALLOWED_TOOLS = ["echo"]


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    Returns a default 50/50 prediction regardless of input.
    """
    prompt = kwargs.get("prompt", "")
    counter_callback = kwargs.get("counter_callback")

    result = json.dumps(
        {
            "p_yes": 0.5,
            "p_no": 0.5,
            "confidence": 0.0,
            "info_utility": 0.0,
        }
    )

    return result, prompt, None, counter_callback
