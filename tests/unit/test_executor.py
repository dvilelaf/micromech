"""Tests for the tool executor."""

import asyncio
import json
from typing import Any

import pytest

from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.executor import ToolExecutor
from micromech.tools.base import Tool, ToolMetadata
from micromech.tools.builtin.echo import EchoTool
from micromech.tools.registry import ToolRegistry


class SlowTestTool(Tool):
    metadata = ToolMetadata(id="slow-test", timeout=1)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        await asyncio.sleep(10)
        return "never"


class FailTestTool(Tool):
    metadata = ToolMetadata(id="fail-test", timeout=5)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        msg = "boom"
        raise RuntimeError(msg)


@pytest.fixture
def registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(EchoTool())
    reg.register(SlowTestTool())
    reg.register(FailTestTool())
    return reg


@pytest.fixture
def executor(queue: PersistentQueue, registry: ToolRegistry) -> ToolExecutor:
    return ToolExecutor(registry=registry, queue=queue, max_concurrent=5)


class TestToolExecutor:
    @pytest.mark.asyncio
    async def test_execute_success(self, executor: ToolExecutor, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="hello", tool="echo")
        queue.add_request(req)
        result = await executor.execute(req)

        assert result.success
        data = json.loads(result.output)
        assert data["result"] == "hello"
        assert result.execution_time > 0

        # Check DB updated
        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTED

    @pytest.mark.asyncio
    async def test_execute_timeout(self, executor: ToolExecutor, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="wait", tool="slow-test")
        queue.add_request(req)
        result = await executor.execute(req)

        assert not result.success
        assert "Timed out" in result.error

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED

    @pytest.mark.asyncio
    async def test_execute_tool_error(self, executor: ToolExecutor, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="test", tool="fail-test")
        queue.add_request(req)
        result = await executor.execute(req)

        assert not result.success
        assert "boom" in result.error

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED

    @pytest.mark.asyncio
    async def test_execute_unknown_tool(self, executor: ToolExecutor, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="test", tool="nonexistent")
        queue.add_request(req)
        result = await executor.execute(req)

        assert not result.success
        assert "not found" in result.error

    @pytest.mark.asyncio
    async def test_execute_empty_tool_defaults_to_echo(
        self, executor: ToolExecutor, queue: PersistentQueue
    ):
        req = MechRequest(request_id="r1", prompt="hello", tool="")
        queue.add_request(req)
        result = await executor.execute(req)

        assert result.success
        data = json.loads(result.output)
        assert data["tool"] == "echo"

    @pytest.mark.asyncio
    async def test_concurrent_execution(self, executor: ToolExecutor, queue: PersistentQueue):
        """Multiple requests execute concurrently."""
        requests = []
        for i in range(5):
            req = MechRequest(request_id=f"r{i}", prompt=f"msg{i}", tool="echo")
            queue.add_request(req)
            requests.append(req)

        results = await asyncio.gather(*[executor.execute(r) for r in requests])

        assert all(r.success for r in results)
        assert executor.active_count == 0

    @pytest.mark.asyncio
    async def test_active_count(self, executor: ToolExecutor, queue: PersistentQueue):
        assert executor.active_count == 0
        req = MechRequest(request_id="r1", prompt="hello", tool="echo")
        queue.add_request(req)
        await executor.execute(req)
        assert executor.active_count == 0  # done
