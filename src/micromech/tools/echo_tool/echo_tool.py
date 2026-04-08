"""Echo tool — returns the prompt back as-is.

Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.
Useful for testing the full request-execute-deliver pipeline without LLM.
"""

import json
from typing import Any, Optional

ALLOWED_TOOLS = ["echo"]


def run(**kwargs: Any) -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:
    """Valory-compatible entry point.

    Returns the input prompt as the result.
    """
    prompt = kwargs.get("prompt", "")
    counter_callback = kwargs.get("counter_callback")

    result = json.dumps({"result": prompt})

    return result, prompt, None, counter_callback
