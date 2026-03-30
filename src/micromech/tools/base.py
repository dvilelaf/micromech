"""Tool base classes and types.

Valory mech tool format:
- Each tool is a package with __init__.py, component.yaml, and <name>.py
- The .py file defines ALLOWED_TOOLS (list[str]) and run(**kwargs) -> MechResponse
- MechResponse = (result_json, prompt_used, transaction, counter_callback)

micromech wraps these as Tool instances for the async runtime.
"""

import asyncio
import re
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from micromech.core.errors import ToolExecutionError

TOOL_ID_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Valory run() return type: (result, prompt_used, transaction, counter_callback)
MechResponse = tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]


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


class Tool:
    """Wrapper around a Valory-format tool's run() function.

    The run() function is the Valory-compatible synchronous entry point.
    execute() wraps it in asyncio.to_thread() for the async runtime.
    """

    def __init__(
        self,
        metadata: ToolMetadata,
        run_fn: Any,
        allowed_tools: list[str] | None = None,
    ):
        self.metadata = metadata
        self._run_fn = run_fn
        self.ALLOWED_TOOLS = allowed_tools or [metadata.id]

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Execute the tool asynchronously (offloaded to thread pool).

        Catches all exceptions from the tool — a broken tool must never crash the mech.
        """
        try:
            result = await asyncio.to_thread(
                self._run_fn, prompt=prompt, tool=self.metadata.id, **kwargs
            )
        except Exception as e:
            raise ToolExecutionError(self.metadata.id, str(e)) from e

        # run() returns a tuple — first element is the result string
        if isinstance(result, tuple) and len(result) >= 1:
            return result[0] or ""
        return str(result)

    async def execute_with_timeout(self, prompt: str, **kwargs: Any) -> str:
        """Execute with timeout enforcement from metadata."""
        try:
            return await asyncio.wait_for(
                self.execute(prompt, **kwargs),
                timeout=self.metadata.timeout,
            )
        except asyncio.TimeoutError:
            raise ToolExecutionError(self.metadata.id, f"Timed out after {self.metadata.timeout}s")
