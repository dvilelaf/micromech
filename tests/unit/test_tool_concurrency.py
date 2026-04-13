"""Tests for GIL-safe concurrent tool execution.

Design note on subprocess tests
--------------------------------
Python's ProcessPoolExecutor uses the `fork` start method on Linux by default.
Forking inside a pytest session that already has an asyncio event loop running
can deadlock (the child inherits the loop's internal selector lock mid-hold).

To keep the test suite fast and reliable we therefore use mock-based tests that
verify the *routing* decision (serialized → ProcessPoolExecutor, not thread pool)
without actually spawning OS processes.  The real-world guarantee — that LLM
inference runs in a separate GIL — is a property of ProcessPoolExecutor itself,
which is well-tested by the Python standard library.

What we test here
-----------------
- Serialized tools call loop.run_in_executor with a ProcessPoolExecutor
- Non-serialized tools call asyncio.to_thread (thread pool)
- ProcessPoolExecutor has max_workers=1 → only one serialized task at a time
- BrokenProcessPool (worker crash / OOM) raises ToolExecutionError
- BrokenProcessPool resets the executor so next call spawns a fresh one
"""

import asyncio
import concurrent.futures
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from micromech.core.errors import ToolExecutionError
from micromech.tools import base as _base
from micromech.tools.base import Tool, ToolMetadata, _get_llm_executor

# ---------------------------------------------------------------------------
# Module-level run functions (picklable if real subprocesses ever needed)
# ---------------------------------------------------------------------------

def _fast_run(prompt, tool, **kwargs):
    return ("fast-result",)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolate_executor():
    """Reset the global ProcessPoolExecutor reference before each test."""
    original = _base._LLM_EXECUTOR
    _base._LLM_EXECUTOR = None
    yield
    current = _base._LLM_EXECUTOR
    if current is not None and current is not original:
        try:
            current.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    _base._LLM_EXECUTOR = original


def _make_mock_executor(result=("fast-result",)):
    """Return a MagicMock that mimics ProcessPoolExecutor.submit()."""
    f: Future = Future()
    f.set_result(result)
    mock_exec = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
    mock_exec._max_workers = 1
    mock_exec.submit.return_value = f
    return mock_exec


# ---------------------------------------------------------------------------
# Executor shape
# ---------------------------------------------------------------------------

class TestExecutorShape:
    def test_get_llm_executor_returns_process_pool(self):
        """Serialized tools must route through a ProcessPoolExecutor."""
        executor = _get_llm_executor()
        assert isinstance(executor, concurrent.futures.ProcessPoolExecutor)

    def test_executor_has_single_worker(self):
        """max_workers=1 serialises execution at the OS-process level."""
        executor = _get_llm_executor()
        assert executor._max_workers == 1

    def test_executor_singleton(self):
        """Same instance reused — model stays warm in the worker process."""
        assert _get_llm_executor() is _get_llm_executor()

    def test_executor_lazy_initialized(self):
        """No subprocess is spawned at import — only on first serialized call."""
        # isolate_executor fixture already reset to None
        assert _base._LLM_EXECUTOR is None
        _get_llm_executor()
        assert _base._LLM_EXECUTOR is not None


# ---------------------------------------------------------------------------
# Routing: serialized → ProcessPoolExecutor, non-serialized → to_thread
# ---------------------------------------------------------------------------

class TestExecutorRouting:
    @pytest.mark.asyncio
    async def test_serialized_tool_uses_process_executor(self):
        """When serialized=True, execute() must call loop.run_in_executor
        with the singleton ProcessPoolExecutor — not asyncio.to_thread.
        """
        tool = Tool(
            metadata=ToolMetadata(id="llm-tool", serialized=True, timeout=5),
            run_fn=_fast_run,
        )

        executor_used = []

        async def fake_run_in_executor(executor, fn):
            executor_used.append(executor)
            # Call fn synchronously to return the expected tuple
            return fn()

        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", new=fake_run_in_executor):
            result = await tool.execute("test")

        assert result == "fast-result"
        assert len(executor_used) == 1
        assert isinstance(executor_used[0], concurrent.futures.ProcessPoolExecutor)

    @pytest.mark.asyncio
    async def test_non_serialized_tool_uses_thread_pool(self):
        """When serialized=False, execute() calls run_in_executor(None, fn) —
        the default ThreadPoolExecutor — NOT the ProcessPoolExecutor.

        asyncio.to_thread() is implemented as run_in_executor(None, fn), so
        spying on run_in_executor lets us verify the executor argument.
        """
        tool = Tool(
            metadata=ToolMetadata(id="echo", serialized=False, timeout=5),
            run_fn=_fast_run,
        )

        executor_args = []

        async def spy_run_in_executor(executor, fn):
            executor_args.append(executor)
            return fn()

        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", new=spy_run_in_executor):
            result = await tool.execute("test")

        assert result == "fast-result"
        assert len(executor_args) == 1
        assert executor_args[0] is None, (
            "Non-serialized tool must use the default thread pool (None), "
            "not the ProcessPoolExecutor"
        )

    @pytest.mark.asyncio
    async def test_serialized_uses_singleton_executor(self):
        """Two consecutive serialized calls use the same executor instance,
        keeping the model warm between requests.
        """
        tool = Tool(
            metadata=ToolMetadata(id="llm", serialized=True, timeout=5),
            run_fn=_fast_run,
        )

        executors_seen = []

        async def capture_executor(executor, fn):
            executors_seen.append(executor)
            return fn()

        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", new=capture_executor):
            await tool.execute("first call")
            await tool.execute("second call")

        assert len(executors_seen) == 2
        assert executors_seen[0] is executors_seen[1], "Must reuse the same executor"


# ---------------------------------------------------------------------------
# BrokenProcessPool crash recovery
# ---------------------------------------------------------------------------

class TestBrokenProcessPoolRecovery:
    @pytest.mark.asyncio
    async def test_broken_pool_raises_tool_execution_error(self):
        """A crashed worker must surface as ToolExecutionError, never as the
        raw concurrent.futures internal exception.
        """
        tool = Tool(
            metadata=ToolMetadata(id="crasher", serialized=True, timeout=5),
            run_fn=_fast_run,
        )

        mock_exec = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        mock_exec.submit.side_effect = concurrent.futures.process.BrokenProcessPool(
            "simulated OOM"
        )
        _base._LLM_EXECUTOR = mock_exec

        with pytest.raises(ToolExecutionError, match="crashed"):
            await tool.execute("test")

    @pytest.mark.asyncio
    async def test_broken_pool_resets_executor_to_none(self):
        """After a crash the reference is cleared so the next call spawns a
        fresh process pool (new worker with no leaked state).
        """
        tool = Tool(
            metadata=ToolMetadata(id="crasher", serialized=True, timeout=5),
            run_fn=_fast_run,
        )

        mock_exec = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        mock_exec.submit.side_effect = concurrent.futures.process.BrokenProcessPool(
            "simulated OOM"
        )
        _base._LLM_EXECUTOR = mock_exec

        with pytest.raises(ToolExecutionError):
            await tool.execute("test")

        assert _base._LLM_EXECUTOR is None, (
            "_LLM_EXECUTOR must be reset to None after BrokenProcessPool"
        )

    @pytest.mark.asyncio
    async def test_executor_recreated_after_crash(self):
        """Next call after a crash must create a new ProcessPoolExecutor."""
        tool = Tool(
            metadata=ToolMetadata(id="crasher", serialized=True, timeout=5),
            run_fn=_fast_run,
        )

        mock_exec = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        mock_exec.submit.side_effect = concurrent.futures.process.BrokenProcessPool(
            "simulated OOM"
        )
        _base._LLM_EXECUTOR = mock_exec

        with pytest.raises(ToolExecutionError):
            await tool.execute("test")

        new_executor = _get_llm_executor()
        assert new_executor is not mock_exec
        assert isinstance(new_executor, concurrent.futures.ProcessPoolExecutor)

    @pytest.mark.asyncio
    async def test_non_serialized_unaffected_by_broken_pool(self):
        """A crashed pool must not affect non-serialized tools, which never
        touch the ProcessPoolExecutor.
        """
        non_serialized = Tool(
            metadata=ToolMetadata(id="echo", serialized=False, timeout=5),
            run_fn=_fast_run,
        )

        mock_exec = MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        mock_exec.submit.side_effect = concurrent.futures.process.BrokenProcessPool(
            "simulated OOM"
        )
        _base._LLM_EXECUTOR = mock_exec

        # Must succeed without touching the broken pool
        result = await non_serialized.execute("test")
        assert result == "fast-result"
        mock_exec.submit.assert_not_called()
