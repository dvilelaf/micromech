"""Tests for the tool executor."""

import asyncio
import json
import time
from unittest.mock import MagicMock

import pytest

from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.executor import ToolExecutor
from micromech.tools.base import Tool, ToolMetadata
from micromech.tools.registry import ToolRegistry


def _echo_run(**kwargs):
    prompt = kwargs.get("prompt", "")
    return json.dumps({"p_yes": 0.5, "p_no": 0.5}), prompt, None, None


def _slow_run(**kwargs):
    time.sleep(10)
    return "never", None, None, None


def _fail_run(**kwargs):
    msg = "boom"
    raise RuntimeError(msg)


@pytest.fixture
def registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(Tool(ToolMetadata(id="echo", timeout=5), run_fn=_echo_run))
    reg.register(Tool(ToolMetadata(id="slow-test", timeout=1), run_fn=_slow_run))
    reg.register(Tool(ToolMetadata(id="fail-test", timeout=5), run_fn=_fail_run))
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
        assert "p_yes" in data
        assert result.execution_time > 0

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
        assert "p_yes" in data

    @pytest.mark.asyncio
    async def test_concurrent_execution(self, executor: ToolExecutor, queue: PersistentQueue):
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
        assert executor.active_count == 0

    @pytest.mark.asyncio
    async def test_mark_executing_failure(self, executor: ToolExecutor, queue: PersistentQueue):
        """When mark_executing fails, returns error result without crashing."""
        req = MechRequest(request_id="r1", prompt="hello", tool="echo")
        queue.add_request(req)

        # Make mark_executing raise
        original = queue.mark_executing
        queue.mark_executing = MagicMock(side_effect=RuntimeError("db locked"))

        result = await executor.execute(req)
        assert not result.success
        assert "db locked" in result.error
        assert executor.active_count == 0

        queue.mark_executing = original

    @pytest.mark.asyncio
    async def test_generic_exception_handler(self, queue: PersistentQueue, registry: ToolRegistry):
        """Generic exception (not ToolExecutionError) is caught and persisted."""
        executor = ToolExecutor(registry=registry, queue=queue, max_concurrent=5)

        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)

        # Patch the tool's execute_with_timeout to raise a raw Exception
        tool = registry.get("echo")
        original_exec = tool.execute_with_timeout

        async def raise_generic(*args, **kwargs):
            raise RuntimeError("unexpected crash")

        tool.execute_with_timeout = raise_generic
        result = await executor.execute(req)
        assert not result.success
        assert "unexpected crash" in result.error
        assert executor.active_count == 0

        tool.execute_with_timeout = original_exec

    @pytest.mark.asyncio
    async def test_generic_exception_with_persist_failure(
        self, queue: PersistentQueue, registry: ToolRegistry
    ):
        """When generic exception occurs AND mark_executed also fails, still returns result."""
        executor = ToolExecutor(registry=registry, queue=queue, max_concurrent=5)

        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)

        # Make tool raise a generic exception
        tool = registry.get("echo")
        original_exec = tool.execute_with_timeout

        async def raise_generic(*args, **kwargs):
            raise RuntimeError("tool crash")

        tool.execute_with_timeout = raise_generic

        # Also make mark_executed fail
        original_mark = queue.mark_executed
        queue.mark_executed = MagicMock(side_effect=RuntimeError("db locked"))

        result = await executor.execute(req)
        assert not result.success
        assert "tool crash" in result.error
        assert executor.active_count == 0

        tool.execute_with_timeout = original_exec
        queue.mark_executed = original_mark
