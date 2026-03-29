"""Tests for the MechServer orchestrator."""

import pytest

from micromech.core.config import MicromechConfig, PersistenceConfig
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.server import MechServer


@pytest.fixture
def server_config(tmp_path) -> MicromechConfig:
    return MicromechConfig(
        persistence=PersistenceConfig(db_path=tmp_path / "test.db"),
    )


class TestMechServerInit:
    def test_creates_components(self, server_config: MicromechConfig):
        server = MechServer(server_config)
        assert server.queue is not None
        assert server.registry is not None
        assert server.executor is not None
        assert server.listener is not None
        assert server.delivery is not None
        server.shutdown()

    def test_load_tools(self, server_config: MicromechConfig):
        server = MechServer(server_config)
        server._load_tools()
        assert server.registry.has("echo")
        server.shutdown()

    def test_get_status(self, server_config: MicromechConfig):
        server = MechServer(server_config)
        server._load_tools()
        status = server.get_status()
        assert status["status"] == "stopped"
        assert "echo" in status["tools"]
        assert isinstance(status["queue"], dict)
        server.shutdown()


class TestMechServerRecovery:
    @pytest.mark.asyncio
    async def test_recover_pending(self, server_config: MicromechConfig):
        # Pre-seed the DB with pending requests
        queue = PersistentQueue(server_config.persistence.db_path)
        queue.add_request(MechRequest(request_id="r1", prompt="test1", tool="echo"))
        queue.add_request(MechRequest(request_id="r2", prompt="test2", tool="echo"))
        queue.close()

        server = MechServer(server_config)
        await server._recover()

        assert server._request_queue.qsize() == 2
        server.shutdown()

    @pytest.mark.asyncio
    async def test_recover_executing(self, server_config: MicromechConfig):
        queue = PersistentQueue(server_config.persistence.db_path)
        queue.add_request(MechRequest(request_id="r1", prompt="test", tool="echo"))
        queue.mark_executing("r1")
        queue.close()

        server = MechServer(server_config)
        await server._recover()

        assert server._request_queue.qsize() == 1
        server.shutdown()


class TestMechServerProcessing:
    @pytest.mark.asyncio
    async def test_on_new_request(self, server_config: MicromechConfig):
        server = MechServer(server_config)
        req = MechRequest(request_id="r1", prompt="hello", tool="echo")

        await server._on_new_request(req)

        # Check it's in the DB and queue
        record = server.queue.get_by_id("r1")
        assert record is not None
        assert server._request_queue.qsize() == 1
        server.shutdown()

    @pytest.mark.asyncio
    async def test_full_request_cycle(self, server_config: MicromechConfig):
        """Test: request in → tool execution → result stored."""
        server = MechServer(server_config)
        server._load_tools()

        req = MechRequest(request_id="r1", prompt="hello world", tool="echo")
        await server._on_new_request(req)

        # Manually process the request
        queued_req = await server._request_queue.get()
        result = await server.executor.execute(queued_req)

        assert result.success
        assert "hello world" in result.output

        # Verify in DB
        record = server.queue.get_by_id("r1")
        assert record.result is not None
        assert record.result.success
        server.shutdown()

    def test_stop(self, server_config: MicromechConfig):
        server = MechServer(server_config)
        server._running = True
        server.stop()
        assert server._running is False
        server.shutdown()
