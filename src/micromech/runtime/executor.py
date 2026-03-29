"""Tool executor — manages concurrent tool execution with timeout."""

import asyncio
import time

from loguru import logger

from micromech.core.errors import ToolExecutionError
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.tools.base import Tool
from micromech.tools.registry import ToolNotFoundError, ToolRegistry


class ToolExecutor:
    """Executes tools concurrently with timeout enforcement.

    Uses an asyncio semaphore for execution concurrency and an asyncio lock
    to serialize SQLite writes (Peewee is not safe for concurrent async writes).
    """

    def __init__(
        self,
        registry: ToolRegistry,
        queue: PersistentQueue,
        max_concurrent: int = 10,
    ):
        self.registry = registry
        self.queue = queue
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._db_lock = asyncio.Lock()
        self._active: set[str] = set()

    @property
    def active_count(self) -> int:
        return len(self._active)

    async def execute(self, request: MechRequest) -> ToolResult:
        """Execute tool for a request. Blocks if at concurrency limit."""
        async with self._semaphore:
            return await self._run(request)

    async def _db_write(self, fn, *args, **kwargs):
        """Serialize DB writes through asyncio lock."""
        async with self._db_lock:
            return fn(*args, **kwargs)

    async def _run(self, request: MechRequest) -> ToolResult:
        """Inner execution with persistence updates."""
        req_id = request.request_id
        self._active.add(req_id)
        start = time.monotonic()

        try:
            await self._db_write(self.queue.mark_executing, req_id)
        except Exception as e:
            logger.error("Failed to mark {} as executing: {}", req_id, e)
            self._active.discard(req_id)
            return ToolResult(error=str(e))

        try:
            tool = self._resolve_tool(request)
            result_str = await tool.execute_with_timeout(request.prompt, **request.extra_params)
            elapsed = time.monotonic() - start

            result = ToolResult(
                output=result_str,
                execution_time=elapsed,
                metadata={
                    "tool": tool.metadata.id,
                    "version": tool.metadata.version,
                },
            )
            await self._db_write(self.queue.mark_executed, req_id, result)
            logger.info(
                "Executed {} with tool {} in {:.2f}s",
                req_id,
                tool.metadata.id,
                elapsed,
            )
            return result

        except ToolExecutionError as e:
            elapsed = time.monotonic() - start
            result = ToolResult(error=str(e), execution_time=elapsed)
            await self._db_write(self.queue.mark_executed, req_id, result)
            logger.warning("Tool timeout for {}: {}", req_id, e)
            return result

        except ToolNotFoundError as e:
            result = ToolResult(error=str(e))
            await self._db_write(self.queue.mark_executed, req_id, result)
            logger.error("Tool not found for {}: {}", req_id, e)
            return result

        except Exception as e:
            elapsed = time.monotonic() - start
            result = ToolResult(error=str(e), execution_time=elapsed)
            try:
                await self._db_write(self.queue.mark_executed, req_id, result)
            except Exception:
                logger.error("Failed to persist error for {}", req_id)
            logger.error("Execution failed for {}: {}", req_id, e)
            return result

        finally:
            self._active.discard(req_id)

    def _resolve_tool(self, request: MechRequest) -> Tool:
        """Resolve tool from request. Falls back to default if unspecified."""
        tool_id = request.tool
        if not tool_id:
            tool_id = "echo"
        return self.registry.get(tool_id)
