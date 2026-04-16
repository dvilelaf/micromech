"""Tests for the MechServer orchestrator."""

import asyncio
from unittest.mock import patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS
from micromech.core.models import MechRequest, ToolResult
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

    @pytest.mark.asyncio
    async def test_reload_tools_honors_disabled(self, server_config, monkeypatch):
        """reload_tools() re-reads disabled_tools from disk and rebuilds registry."""
        server = MechServer(server_config)
        server._load_tools()
        assert server.registry.has("echo")

        # Simulate user saving disabled_tools via the web UI — MicromechConfig.load()
        # returns a fresh config with echo_tool disabled.
        fresh_cfg = MicromechConfig(
            chains=server_config.chains,
            disabled_tools=["echo_tool"],
        )
        monkeypatch.setattr(
            "micromech.runtime.server.MicromechConfig.load",
            classmethod(lambda cls: fresh_cfg),
        )

        tool_ids = await server.reload_tools()
        assert "echo" not in tool_ids
        assert not server.registry.has("echo")
        # Server state updated
        assert server.config.disabled_tools == ["echo_tool"]
        server.shutdown()

    @pytest.mark.asyncio
    async def test_reload_tools_restores_after_re_enable(self, server_config, monkeypatch):
        """Toggling a tool off and on again re-registers it."""
        server = MechServer(server_config)
        server._load_tools()

        # Disable
        disabled_cfg = MicromechConfig(
            chains=server_config.chains,
            disabled_tools=["echo_tool"],
        )
        monkeypatch.setattr(
            "micromech.runtime.server.MicromechConfig.load",
            classmethod(lambda cls: disabled_cfg),
        )
        await server.reload_tools()
        assert not server.registry.has("echo")

        # Re-enable
        enabled_cfg = MicromechConfig(
            chains=server_config.chains,
            disabled_tools=[],
        )
        monkeypatch.setattr(
            "micromech.runtime.server.MicromechConfig.load",
            classmethod(lambda cls: enabled_cfg),
        )
        await server.reload_tools()
        assert server.registry.has("echo")
        # And the re-enabled tool is actually usable via registry.get()
        assert server.registry.get("echo") is not None
        server.shutdown()

    @pytest.mark.asyncio
    async def test_reload_tools_atomic_swap_preserves_registry_identity(
        self, server_config, monkeypatch
    ):
        """Executor holds registry ref; the object identity must not change,
        but the internal dict MUST be a different object after swap."""
        server = MechServer(server_config)
        server._load_tools()
        registry_before = server.registry
        tools_dict_before = server.registry._tools
        assert server.executor.registry is registry_before

        fresh_cfg = MicromechConfig(chains=server_config.chains, disabled_tools=[])
        monkeypatch.setattr(
            "micromech.runtime.server.MicromechConfig.load",
            classmethod(lambda cls: fresh_cfg),
        )
        await server.reload_tools()
        # Outer object identity preserved (executor ref stays valid).
        assert server.registry is registry_before
        assert server.executor.registry is registry_before
        # But the internal dict was actually swapped for a new one.
        assert server.registry._tools is not tools_dict_before
        server.shutdown()

    @pytest.mark.asyncio
    async def test_reload_tools_leaves_state_unchanged_on_failure(self, server_config, monkeypatch):
        """If the rebuild raises, config.disabled_tools must NOT mutate and
        the registry must keep the previous contents."""
        server = MechServer(server_config)
        server._load_tools()
        original_disabled = list(server.config.disabled_tools)
        tools_before = dict(server.registry._tools)

        def explode(self):  # noqa: ARG001
            raise RuntimeError("simulated rebuild failure")

        monkeypatch.setattr(
            "micromech.runtime.server.MechServer._build_reloaded_registry",
            explode,
        )

        with pytest.raises(RuntimeError, match="simulated rebuild failure"):
            await server.reload_tools()

        assert server.config.disabled_tools == original_disabled
        assert server.registry._tools == tools_before
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
            request_id="r1",
            prompt="hello",
            tool="echo",
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
            request_id="r1",
            prompt="hello",
            tool="echo",
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
            request_id="r1",
            prompt="hello",
            tool="echo",
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
            request_id="r1",
            prompt="hello world",
            tool="echo",
        )
        await server._on_new_request(req)
        queued_req = await server._request_queue.get()
        result = await server.executor.execute(queued_req)
        assert result.success
        assert "hello world" in result.output
        record = server.queue.get_by_id("r1")
        assert record.result is not None
        assert record.result.success
        server.shutdown()

    @pytest.mark.asyncio
    async def test_execute_and_cleanup_removes_from_dedup(
        self,
        server_config,
    ):
        server = MechServer(server_config)
        server._load_tools()
        req = MechRequest(
            request_id="r1",
            prompt="hello",
            tool="echo",
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
                request_id="r1",
                prompt="hello",
                tool="echo",
            )
            await server._on_new_request(req)
            await asyncio.sleep(0.5)
            server.stop()

        asyncio.create_task(inject_and_stop())
        await asyncio.wait_for(
            server.run(with_http=False),
            timeout=5.0,
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
            server.run(with_http=False),
            timeout=3.0,
        )
        assert server.registry.has("echo")
        server.shutdown()


class TestMultiChainServer:
    """Test multi-chain listener/delivery creation."""

    def test_multi_chain_creates_per_chain_components(
        self,
        tmp_path,
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


class TestLLMPrefetch:
    """Tests for _prefetch_llm_model background download."""

    @pytest.mark.asyncio
    async def test_skips_if_model_already_exists(self, server_config, tmp_path, monkeypatch):
        """If model file already exists, _get_llm is never called."""
        monkeypatch.chdir(tmp_path)
        model_dir = tmp_path / "data" / "models"
        model_dir.mkdir(parents=True)

        from micromech.core.constants import DEFAULT_LLM_FILE

        (model_dir / DEFAULT_LLM_FILE).touch()

        server = MechServer(server_config)
        with patch("micromech.tools.local_llm.local_llm._get_llm") as mock_get:
            await server._prefetch_llm_model()
            mock_get.assert_not_called()
        server.shutdown()

    @pytest.mark.asyncio
    async def test_downloads_if_model_missing(self, server_config, tmp_path, monkeypatch):
        """If model is absent, _get_llm is called (which handles the download)."""
        monkeypatch.chdir(tmp_path)

        server = MechServer(server_config)
        with patch("micromech.tools.local_llm.local_llm._get_llm") as mock_get:
            await server._prefetch_llm_model()
            mock_get.assert_called_once()
        server.shutdown()

    @pytest.mark.asyncio
    async def test_silently_skips_if_llm_not_installed(self, server_config, tmp_path, monkeypatch):
        """ImportError (llm extra not installed) is swallowed silently."""
        monkeypatch.chdir(tmp_path)

        server = MechServer(server_config)
        with patch(
            "micromech.runtime.server.MechServer._prefetch_llm_model",
            wraps=server._prefetch_llm_model,
        ):
            with patch(
                "micromech.tools.local_llm.local_llm._get_llm",
                side_effect=ImportError("llama_cpp not installed"),
            ):
                # Should not raise
                await server._prefetch_llm_model()
        server.shutdown()

    @pytest.mark.asyncio
    async def test_logs_warning_on_download_failure(self, server_config, tmp_path, monkeypatch):
        """Download errors are caught and logged as warnings, not raised."""
        monkeypatch.chdir(tmp_path)

        server = MechServer(server_config)
        with patch(
            "micromech.tools.local_llm.local_llm._get_llm",
            side_effect=RuntimeError("network error"),
        ):
            # Should not raise
            await server._prefetch_llm_model()
        server.shutdown()
