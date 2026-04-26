"""Tests for the MechServer orchestrator."""

import asyncio
from unittest.mock import MagicMock, patch

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


# ===========================================================================
# Fallback mode
# ===========================================================================

MECH_ADDR = "0x" + "ab" * 20
OTHER_MECH = "0x" + "cd" * 20


def _make_fallback_config(tmp_path=None, **kwargs) -> MicromechConfig:
    gnosis = CHAIN_DEFAULTS["gnosis"]
    cfg = MicromechConfig(
        chains={
            "gnosis": ChainConfig(
                chain="gnosis",
                marketplace_address=gnosis["marketplace"],
                factory_address=gnosis["factory"],
                staking_address=gnosis["staking"],
                mech_address=MECH_ADDR,
            )
        },
        fallback_mode_enabled=True,
        **kwargs,
    )
    return cfg


class TestFallbackMode:
    @pytest.mark.asyncio
    async def test_fallback_request_goes_to_pending(self, tmp_path, monkeypatch):
        """A request from another priority mech is tracked in _fallback_pending."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server._load_tools()
        # Ensure registry reports the tool as available
        server.registry._tools = {"superforcaster": object()}

        req = MechRequest(
            request_id="aa" * 32,
            chain="gnosis",
            tool="superforcaster",
            prompt="Will ETH hit 10k?",
            priority_mech=OTHER_MECH,
        )

        await server._on_new_request(req)

        assert "aa" * 32 in server._fallback_pending
        assert "aa" * 32 not in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_own_request_processed_normally(self, tmp_path, monkeypatch):
        """Our own requests (priorityMech == us) are queued normally in fallback mode."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)

        req = MechRequest(
            request_id="bb" * 32,
            chain="gnosis",
            tool="echo",
            prompt="hello",
            priority_mech=MECH_ADDR,
        )

        await server._on_new_request(req)

        assert "bb" * 32 in server._queued_ids
        assert "bb" * 32 not in server._fallback_pending
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_skipped_if_tool_not_available(self, tmp_path, monkeypatch):
        """Fallback request is silently dropped when we don't have the tool."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        # Don't load tools — registry is empty

        req = MechRequest(
            request_id="cc" * 32,
            chain="gnosis",
            tool="unknown_tool",
            prompt="question?",
            priority_mech=OTHER_MECH,
        )

        await server._on_new_request(req)

        assert "cc" * 32 not in server._fallback_pending
        assert "cc" * 32 not in server._queued_ids
        server.shutdown()

    def test_get_request_status_returns_0_without_bridge(self, tmp_path, monkeypatch):
        """_get_request_status returns 0 (DoesNotExist) when no bridge configured."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)  # No bridges injected

        result = server._get_request_status("gnosis", "aa" * 32)

        assert result == 0
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_checker_queues_when_status_any(self, tmp_path, monkeypatch):
        """Checker moves request to queue when status == 2 (RequestedAny)."""
        from datetime import datetime, timezone, timedelta

        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server._running = True
        req_id = "dd" * 32
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
            created_at=datetime.now(timezone.utc) - timedelta(seconds=260),
        )
        server._fallback_pending[req_id] = req

        # Simulate: status check returns 2 (RequestedAny) then stop
        call_count = 0

        async def mock_sleep(_t):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                server._running = False

        with (
            patch("asyncio.sleep", mock_sleep),
            patch.object(
                server,
                "_get_request_status",
                return_value=MechServer._REQUEST_STATUS_ANY,
            ),
        ):
            await server._fallback_checker_loop()

        assert req_id not in server._fallback_pending
        assert req_id in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_checker_discards_when_delivered(self, tmp_path, monkeypatch):
        """Checker discards request when status == 3 (already delivered)."""
        from datetime import datetime, timezone, timedelta

        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server._running = True
        req_id = "ee" * 32
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
            created_at=datetime.now(timezone.utc) - timedelta(seconds=260),
        )
        server._fallback_pending[req_id] = req

        call_count = 0

        async def mock_sleep(_t):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                server._running = False

        with (
            patch("asyncio.sleep", mock_sleep),
            patch.object(server, "_get_request_status", return_value=3),
        ):
            await server._fallback_checker_loop()

        assert req_id not in server._fallback_pending
        assert req_id not in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_get_request_status_handles_0x_prefix(self, tmp_path, monkeypatch):
        """_get_request_status accepts request_id with '0x' prefix without crashing."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        mock_bridge = MagicMock()
        mock_bridge.with_retry.side_effect = lambda fn: fn()
        mock_contract = MagicMock()
        mock_contract.functions.getRequestStatus.return_value.call.return_value = 2
        mock_bridge.web3.eth.contract.return_value = mock_contract
        mock_bridge.web3.to_checksum_address.return_value = "0xDEAD"

        server = MechServer(cfg, bridges={"gnosis": mock_bridge})

        # Should not raise ValueError — removeprefix("0x") handles the prefix
        result = server._get_request_status("gnosis", "0x" + "aa" * 32)
        assert result == 2
        # Contract is cached after first call
        assert "gnosis" in server._fallback_contracts
        server.shutdown()

    def test_get_request_status_caches_contract(self, tmp_path, monkeypatch):
        """Contract is created once and reused on subsequent calls."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        mock_bridge = MagicMock()
        mock_bridge.with_retry.side_effect = lambda fn: fn()
        mock_contract = MagicMock()
        mock_contract.functions.getRequestStatus.return_value.call.return_value = 1
        mock_bridge.web3.eth.contract.return_value = mock_contract
        mock_bridge.web3.to_checksum_address.return_value = "0xDEAD"

        server = MechServer(cfg, bridges={"gnosis": mock_bridge})
        server._get_request_status("gnosis", "aa" * 32)
        server._get_request_status("gnosis", "bb" * 32)

        # eth.contract() called exactly once (cached after first call)
        assert mock_bridge.web3.eth.contract.call_count == 1
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_checker_evicts_expired_entries(self, tmp_path, monkeypatch):
        """Checker drops entries older than fallback_ttl_seconds."""
        from datetime import datetime, timezone, timedelta

        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config(fallback_ttl_seconds=60)
        server = MechServer(cfg)
        server._running = True
        req_id = "ff" * 32
        # Request created 2 hours ago — well past TTL
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
            created_at=old_time,
        )
        server._fallback_pending[req_id] = req

        call_count = 0

        async def mock_sleep(_t):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                server._running = False

        with (
            patch("asyncio.sleep", mock_sleep),
            patch.object(server, "_get_request_status", return_value=1),
        ):
            await server._fallback_checker_loop()

        assert req_id not in server._fallback_pending
        assert req_id not in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_db_dedup_before_fallback_tracking(self, tmp_path, monkeypatch):
        """Already-executed requests are skipped before entering _fallback_pending."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server.registry._tools = {"echo": object()}

        req_id = "ab" * 32
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
        )
        # Persist as already executed (non-pending) in the DB
        server.queue.add_request(req)
        server.queue.mark_executing(req_id)
        server.queue.mark_executed(req_id, ToolResult(output="done"))

        await server._on_new_request(req)

        assert req_id not in server._fallback_pending
        assert req_id not in server._queued_ids
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_checker_skips_before_poll_delay(self, tmp_path, monkeypatch):
        """Checker does not call getRequestStatus until _FALLBACK_POLL_DELAY seconds pass."""
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server._running = True
        req_id = "a1" * 32
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
            # created just now — well within the 250s poll delay
        )
        server._fallback_pending[req_id] = req

        call_count = 0

        async def mock_sleep(_t):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                server._running = False

        mock_status = MagicMock(return_value=1)
        with (
            patch("asyncio.sleep", mock_sleep),
            patch.object(server, "_get_request_status", mock_status),
        ):
            await server._fallback_checker_loop()

        # getRequestStatus should NOT have been called — request is too fresh
        mock_status.assert_not_called()
        assert req_id in server._fallback_pending
        server.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_checker_polls_after_delay(self, tmp_path, monkeypatch):
        """Checker calls getRequestStatus once _FALLBACK_POLL_DELAY seconds have passed."""
        from datetime import datetime, timezone, timedelta

        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        monkeypatch.setattr("micromech.core.constants.DB_PATH", tmp_path / "test.db")

        cfg = _make_fallback_config()
        server = MechServer(cfg)
        server._running = True
        req_id = "a2" * 32
        old_enough = datetime.now(timezone.utc) - timedelta(seconds=260)
        req = MechRequest(
            request_id=req_id,
            chain="gnosis",
            tool="echo",
            priority_mech=OTHER_MECH,
            created_at=old_enough,
        )
        server._fallback_pending[req_id] = req

        call_count = 0

        async def mock_sleep(_t):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                server._running = False

        with (
            patch("asyncio.sleep", mock_sleep),
            patch.object(server, "_get_request_status", return_value=1),
        ):
            await server._fallback_checker_loop()

        # Request old enough — getRequestStatus should have been called
        assert req_id in server._fallback_pending  # status=1, stays pending
        server.shutdown()
