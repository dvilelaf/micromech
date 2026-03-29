"""Abstract base class for mech tools.

Tool interface is compatible with Valory mech tools:
- run(**kwargs) -> Tuple[str, Optional[str], Optional[Dict], Any, Any]
- ALLOWED_TOOLS list defines which tool names this module handles

micromech tools extend this with a cleaner async interface while maintaining
backward compatibility with Valory's format.

All execute() implementations must return a JSON string.
"""

import asyncio
import re
from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from micromech.core.errors import ToolExecutionError

# Tool ID must be a valid identifier: lowercase alphanumeric + underscore/hyphen
TOOL_ID_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


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


# Valory-compatible response type
# (result, prompt, context/transaction, artifact, callback)
ValoryResponse = tuple[str, Optional[str], Optional[dict], Any, Any]


class Tool(ABC):
    """Abstract base class for all mech tools.

    Subclasses implement execute() which must return a JSON string.
    The timeout from metadata is enforced by execute_with_timeout().
    """

    metadata: ToolMetadata

    @abstractmethod
    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Execute the tool with the given prompt.

        Args:
            prompt: The main input (question/prompt).
            **kwargs: Tool-specific parameters.

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

    async def execute_valory(self, **kwargs: Any) -> ValoryResponse:
        """Execute in Valory-compatible format.

        This wraps execute() to return the 5-tuple that Valory mech expects.
        Override for full Valory compatibility if needed.
        """
        prompt = kwargs.pop("prompt", "")
        result = await self.execute_with_timeout(prompt, **kwargs)
        return result, prompt, None, None, None
