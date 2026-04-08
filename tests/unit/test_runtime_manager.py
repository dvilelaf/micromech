"""Tests for RuntimeManager — dynamic start/stop/restart of MechServer."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.runtime.manager import RuntimeManager


@pytest.fixture
def config():
    return MicromechConfig()


@pytest.fixture
def manager(config):
    return RuntimeManager(config)


class TestRuntimeManagerState:
    def test_initial_state_is_stopped(self, manager):
        assert manager.state == "stopped"
        assert manager.error is None

    def test_get_status_when_stopped(self, manager):
        status = manager.get_status()
        assert status["state"] == "stopped"
        assert "error" not in status


class TestRuntimeManagerStart:
    @pytest.mark.asyncio
    async def test_start_already_running(self, manager):
        manager._state = "running"
        ok = await manager.start()
        assert ok is True

    @pytest.mark.asyncio
    async def test_start_failure_sets_error(self, manager):
        with patch.object(manager, "_create_bridges", side_effect=RuntimeError("boom")):
            ok = await manager.start()
        assert ok is False
        assert manager.state == "error"
        assert "boom" in manager.error


class TestRuntimeManagerStop:
    @pytest.mark.asyncio
    async def test_stop_when_stopped(self, manager):
        ok = await manager.stop()
        assert ok is True
        assert manager.state == "stopped"

    @pytest.mark.asyncio
    async def test_stop_cleans_up(self, manager):
        manager._state = "running"
        mock_server = MagicMock()
        manager._server = mock_server

        # Create a real asyncio task that we can cancel
        async def fake_run():
            await asyncio.sleep(100)

        manager._task = asyncio.create_task(fake_run())

        ok = await manager.stop()
        assert ok is True
        assert manager.state == "stopped"
        assert manager._server is None
        assert manager._task is None
        mock_server.stop.assert_called_once()
        mock_server.shutdown.assert_called_once()


class TestRuntimeManagerRestart:
    @pytest.mark.asyncio
    async def test_restart_reloads_config(self, manager):
        with patch.object(RuntimeManager, "start", new_callable=AsyncMock, return_value=True):
            with patch.object(RuntimeManager, "stop", new_callable=AsyncMock, return_value=True):
                with patch("micromech.runtime.manager.MicromechConfig") as mock_cfg:
                    mock_cfg.load.return_value = MicromechConfig()
                    ok = await manager.restart()

        assert ok is True


class TestRuntimeManagerGetStatus:
    def test_status_includes_error(self, manager):
        manager._state = "error"
        manager._error = "something broke"
        status = manager.get_status()
        assert status["state"] == "error"
        assert status["error"] == "something broke"

    def test_status_includes_server_status_when_running(self, manager):
        manager._state = "running"
        mock_server = MagicMock()
        mock_server.get_status.return_value = {
            "status": "running",
            "chains": ["gnosis"],
            "tools": ["echo"],
        }
        manager._server = mock_server

        status = manager.get_status()
        assert status["chains"] == ["gnosis"]
        assert status["tools"] == ["echo"]
