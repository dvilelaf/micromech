"""Tool base classes and types.

Valory mech tool format:
- Each tool is a package with __init__.py, component.yaml, and <name>.py
- The .py file defines ALLOWED_TOOLS (list[str]) and run(**kwargs) -> MechResponse
- MechResponse = (result_json, prompt_used, transaction, counter_callback)

micromech wraps these as Tool instances for the async runtime.
"""

import asyncio
import concurrent.futures
import functools
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator

from micromech.core.errors import ToolExecutionError

TOOL_ID_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Single-worker process pool for serialized (GIL-heavy) tools like LLMs.
# A separate process has its own GIL, so the asyncio event loop stays
# responsive while LLM inference is running.
_LLM_EXECUTOR: concurrent.futures.ProcessPoolExecutor | None = None


def _get_llm_executor() -> concurrent.futures.ProcessPoolExecutor:
    """Lazy-initialize the global single-worker process pool."""
    global _LLM_EXECUTOR
    if _LLM_EXECUTOR is None:
        _LLM_EXECUTOR = concurrent.futures.ProcessPoolExecutor(max_workers=1)
    return _LLM_EXECUTOR


class ToolMetadata(BaseModel):
    """Tool metadata for registration and discovery."""

    id: str = Field(min_length=1)
    name: str = ""
    description: str = ""
    version: str = "0.1.0"
    timeout: int = Field(default=60, ge=1, le=3600)
    serialized: bool = False
    origin: str = "builtin"  # "builtin" or "custom"

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

    # Reserved kwargs that must not be overridden by user-supplied extra_params
    _RESERVED_KWARGS = frozenset({"prompt", "tool", "counter_callback"})

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        """Execute the tool asynchronously (offloaded to thread pool).

        Catches all exceptions from the tool — a broken tool must never crash the mech.
        """
        # Strip reserved keys to prevent parameter injection
        safe_kwargs = {k: v for k, v in kwargs.items() if k not in self._RESERVED_KWARGS}
        try:
            if self.metadata.serialized:
                loop = asyncio.get_running_loop()
                fn = functools.partial(
                    self._run_fn, prompt=prompt, tool=self.metadata.id, **safe_kwargs
                )
                result = await loop.run_in_executor(_get_llm_executor(), fn)
            else:
                result = await asyncio.to_thread(
                    self._run_fn, prompt=prompt, tool=self.metadata.id, **safe_kwargs
                )
        except concurrent.futures.process.BrokenProcessPool:
            # Worker process crashed (e.g. OOM). Reset the executor so the
            # next request spawns a fresh one.
            global _LLM_EXECUTOR
            _LLM_EXECUTOR = None
            raise ToolExecutionError(self.metadata.id, "LLM worker process crashed")
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
