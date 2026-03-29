"""Abstract base class for mech tools.

Tool interface follows the Valory mech standard:
- Each tool defines ALLOWED_TOOLS (list of tool names it handles)
- run(**kwargs) -> Tuple[str, Optional[str], Optional[Dict], Any]
  Returns: (result_json, prompt_used, transaction, counter_callback)
- The framework wraps this into a 5-tuple adding api_keys as the 5th element

micromech tools use an async execute() method internally, with a Valory-compatible
wrapper for interop.
"""

import asyncio
import re
from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from micromech.core.errors import ToolExecutionError

TOOL_ID_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Valory run() return type: (result, prompt_used, transaction, counter_callback)
MechResponse = tuple[str, Optional[str], Optional[dict[str, Any]], Any]


class ToolMetadata(BaseModel):
    """Tool metadata for registration and discovery."""

    id: str = Field(min_length=1)
    name: str = ""
    description: str = ""
    version: str = "0.1.0"
    timeout: int = Field(default=60, ge=1, le=3600)

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not TOOL_ID_RE.match(v):
            msg = f"Tool ID must match {TOOL_ID_RE.pattern}, got: {v!r}"
            raise ValueError(msg)
        return v

    def model_post_init(self, __context: Any) -> None:
        if not self.name:
            self.name = self.id


class Tool(ABC):
    """Abstract base class for all mech tools.

    Subclasses implement execute() which must return a JSON string.
    They also define ALLOWED_TOOLS for Valory compatibility.
    """

    metadata: ToolMetadata
    ALLOWED_TOOLS: list[str] = []

    @abstractmethod
    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Execute the tool with the given prompt.

        Args:
            prompt: The main input (question/prompt).
            **kwargs: Tool-specific parameters (model, api_keys, etc.)

        Returns:
            JSON string with the result.
        """

    async def execute_with_timeout(self, prompt: str, **kwargs: Any) -> str:
        """Execute with timeout enforcement from metadata."""
        try:
            return await asyncio.wait_for(
                self.execute(prompt, **kwargs),
                timeout=self.metadata.timeout,
            )
        except asyncio.TimeoutError:
            raise ToolExecutionError(self.metadata.id, f"Timed out after {self.metadata.timeout}s")

    def run(self, **kwargs: Any) -> MechResponse:
        """Valory-compatible synchronous entry point.

        This is the function that Valory's mech framework calls.
        Returns: (result_json, prompt_used, transaction, counter_callback)
        """
        prompt = kwargs.get("prompt", "")
        counter_callback = kwargs.get("counter_callback")
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(self.execute(prompt, **kwargs))
        except Exception as e:
            return str(e), prompt, None, counter_callback
        finally:
            loop.close()
        return result, prompt, None, counter_callback
