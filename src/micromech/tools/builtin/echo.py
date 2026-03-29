"""Echo tool — returns the prompt as-is. Useful for testing."""

import json
from typing import Any

from micromech.tools.base import Tool, ToolMetadata


class EchoTool(Tool):
    """Simple echo tool that returns the input prompt."""

    metadata = ToolMetadata(
        id="echo",
        name="Echo",
        description="Returns the input prompt as-is. For testing.",
        version="0.1.0",
        timeout=5,
    )

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        return json.dumps(
            {
                "result": prompt,
                "tool": "echo",
            }
        )
