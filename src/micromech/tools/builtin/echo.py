"""Echo tool — returns the prompt as-is.

Valory-compatible: defines ALLOWED_TOOLS and run(**kwargs) -> MechResponse.
Useful for testing the full request-execute-deliver pipeline.
"""

import json
from typing import Any

from micromech.tools.base import MechResponse, Tool, ToolMetadata

ALLOWED_TOOLS = ["echo"]


class EchoTool(Tool):
    """Simple echo tool that returns the input prompt."""

    metadata = ToolMetadata(
        id="echo",
        name="Echo",
        description="Returns the input prompt as-is. For testing.",
        version="0.1.0",
        timeout=5,
    )
    ALLOWED_TOOLS = ALLOWED_TOOLS

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        return json.dumps(
            {
                "p_yes": 0.5,
                "p_no": 0.5,
                "confidence": 0.0,
                "info_utility": 0.0,
            }
        )


def run(**kwargs: Any) -> MechResponse:
    """Valory-compatible entry point."""
    tool = EchoTool()
    return tool.run(**kwargs)
