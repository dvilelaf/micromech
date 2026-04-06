"""Tests for the MechServer orchestrator."""

import asyncio

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS, DB_PATH
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.runtime.server import MechServer


@pytest.fixture
def server_config(tmp_path) -> MicromechConfig:
    from tests.conftest import make_test_config
    return make_test_config()


@pytest.fixture(autouse=True)
def _use_tmp_db(tmp_path, monkeypatch):
    """Redirect DB_PATH to tmp for all server tests."""
    monkeypatch.setattr(
        "micromech.core.constants.DB_PATH",
        tmp_path / "test.db",
    )
    monkeypatch.setattr(
        "micromech.runtime.server.DB_PATH",
        tmp_path / "test.db",
    )


class TestMechServerInit:
    def test_creates_components(self, server_config):
        server = MechServer(server_config)
        assert server.queue is not None
        assert server.registry is not None
        assert server.executor is not None
        assert server.listeners["gnosis"] is not None
        assert server.deliveries["gnosis"] is not None
        server.shutdown()

    def test_load_tools(self, server_config):
        server = MechServer(server_config)
        server._load_tools()
        assert server.registry.has("echo")
        server.shutdown()

    def test_get_status_stopped(self, server_config):
        server = MechServer(server_config)
        server._load_tools()
        status = server.get_status()
        assert status["status"] == "stopped"
        assert "echo" in status["tools"]
        assert isinstance(status["queue"], dict)
        server.shutdown()

    def test_get_status_running(self, server_config):
        server = MechServer(server_config)
        server._running = True
        status = server.get_status()
        assert status["status"] == "running"
        server.shutdown()


class TestMechServerRecovery:
    @pytest.mark.asyncio
    async def test_recover_pending(self, server_config):
        server = MechServer(server_config)
        server.queue.add_request(
            MechRequest(request_id="r1", prompt="test1", tool="echo"),
        )
        server.queue.add_request(
            MechRequest(request_id="r2", prompt="test2", tool="echo"),
        )
        await server._recover()
        assert server._request_queue.qsize() == 2
        assert "r1" in server._queued_ids
        assert "r2" in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_recover_executing(self, server_config):
        server = MechServer(server_config)
        server.queue.add_request(
            MechRequest(request_id="r1", prompt="test", tool="echo"),
        )
        server.queue.mark_executing("r1")
        await server._recover()
        assert server._request_queue.qsize() == 1
        assert "r1" in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_recover_dedup(self, server_config):
        """Already-queued IDs should not be re-queued."""
        server = MechServer(server_config)
        server.queue.add_request(
            MechRequest(request_id="r1", prompt="test", tool="echo"),
        )
        server._queued_ids.add("r1")
        await server._recover()
        assert server._request_queue.qsize() == 0
        server.shutdown()


class TestMechServerDedup:
    @pytest.mark.asyncio
    async def test_duplicate_request_skipped(self, server_config):
        server = MechServer(server_config)
        req = MechRequest(
            request_id="r1", prompt="hello", tool="echo",
        )
        await server._on_new_request(req)
        await server._on_new_request(req)  # duplicate
        assert server._request_queue.qsize() == 1
        server.shutdown()

    @pytest.mark.asyncio
    async def test_processed_request_skipped(self, server_config):
        """Request already in EXECUTED state should be skipped."""
        server = MechServer(server_config)
        req = MechRequest(
            request_id="r1", prompt="hello", tool="echo",
        )
        server.queue.add_request(req)
        server.queue.mark_executing("r1")
        server.queue.mark_executed("r1", ToolResult(output="done"))
        await server._on_new_request(
            MechRequest(request_id="r1", prompt="dup"),
        )
        assert server._request_queue.qsize() == 0
        server.shutdown()


class TestMechServerProcessing:
    @pytest.mark.asyncio
    async def test_on_new_request(self, server_config):
        server = MechServer(server_config)
        req = MechRequest(
            request_id="r1", prompt="hello", tool="echo",
        )
        await server._on_new_request(req)
        record = server.queue.get_by_id("r1")
        assert record is not None
        assert server._request_queue.qsize() == 1
        assert "r1" in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_full_request_cycle(self, server_config):
        """Test: request in -> tool execution -> result stored."""
        server = MechServer(server_config)
        server._load_tools()
        req = MechRequest(
            request_id="r1", prompt="hello world", tool="echo",
        )
        await server._on_new_request(req)
        queued_req = await server._request_queue.get()
        result = await server.executor.execute(queued_req)
        assert result.success
        assert "p_yes" in result.output
        record = server.queue.get_by_id("r1")
        assert record.result is not None
        assert record.result.success
        server.shutdown()

    @pytest.mark.asyncio
    async def test_execute_and_cleanup_removes_from_dedup(
        self, server_config,
    ):
        server = MechServer(server_config)
        server._load_tools()
        req = MechRequest(
            request_id="r1", prompt="hello", tool="echo",
        )
        await server._on_new_request(req)
        assert "r1" in server._queued_ids
        queued = await server._request_queue.get()
        await server._execute_and_cleanup(queued)
        assert "r1" not in server._queued_ids
        server.shutdown()

    def test_stop(self, server_config):
        server = MechServer(server_config)
        server._running = True
        server.stop()
        assert server._running is False
        server.shutdown()

    @pytest.mark.asyncio
    async def test_stop_cancels_executor_tasks(self, server_config):
        server = MechServer(server_config)
        server._running = True
        mock_task = asyncio.get_running_loop().create_future()
        server._executor_tasks.add(mock_task)
        server.stop()
        assert mock_task.cancelled()
        server.shutdown()


class TestMechServerRun:
    @pytest.mark.asyncio
    async def test_run_and_stop(self, server_config):
        """Server starts, processes a request, and stops."""
        server = MechServer(server_config)

        async def inject_and_stop():
            await asyncio.sleep(0.2)
            req = MechRequest(
                request_id="r1", prompt="hello", tool="echo",
            )
            await server._on_new_request(req)
            await asyncio.sleep(0.5)
            server.stop()

        asyncio.create_task(inject_and_stop())
        await asyncio.wait_for(
            server.run(with_http=False), timeout=5.0,
        )
        record = server.queue.get_by_id("r1")
        assert record is not None
        assert record.request.status in ("executed", "failed")
        server.shutdown()

    @pytest.mark.asyncio
    async def test_run_loads_tools(self, server_config):
        server = MechServer(server_config)

        async def stop_soon():
            await asyncio.sleep(0.1)
            server.stop()

        asyncio.create_task(stop_soon())
        await asyncio.wait_for(
            server.run(with_http=False), timeout=3.0,
        )
        assert server.registry.has("echo")
        server.shutdown()


class TestMultiChainServer:
    """Test multi-chain listener/delivery creation."""

    def test_multi_chain_creates_per_chain_components(
        self, tmp_path,
    ):
        gnosis = CHAIN_DEFAULTS["gnosis"]
        base = CHAIN_DEFAULTS["base"]
        config = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    marketplace_address=gnosis["marketplace"],
                    factory_address=gnosis["factory"],
                    staking_address=gnosis["staking"],
                ),
                "base": ChainConfig(
                    chain="base",
                    marketplace_address=base["marketplace"],
                    factory_address=base["factory"],
                    staking_address=base["staking"],
                ),
            },
        )
        server = MechServer(config)
        assert len(server.listeners) == 2
        assert len(server.deliveries) == 2
        assert "gnosis" in server.listeners
        assert "base" in server.listeners
        server.shutdown()

    def test_disabled_chain_excluded(self, tmp_path):
        gnosis = CHAIN_DEFAULTS["gnosis"]
        base = CHAIN_DEFAULTS["base"]
        config = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    marketplace_address=gnosis["marketplace"],
                    factory_address=gnosis["factory"],
                    staking_address=gnosis["staking"],
                ),
                "base": ChainConfig(
                    chain="base",
                    enabled=False,
                    marketplace_address=base["marketplace"],
                    factory_address=base["factory"],
                    staking_address=base["staking"],
                ),
            },
        )
        server = MechServer(config)
        assert len(server.listeners) == 1
        assert "gnosis" in server.listeners
        assert "base" not in server.listeners
        server.shutdown()

    def test_get_status_includes_chains(self, tmp_path):
        from tests.conftest import make_test_config
        config = make_test_config()
        server = MechServer(config)
        status = server.get_status()
        assert "chains" in status
        assert "gnosis" in status["chains"]
        assert "queue_by_chain" in status
        server.shutdown()
