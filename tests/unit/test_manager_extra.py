"""Extra tests for runtime/manager.py covering missed lines.

Covers:
- metrics property when server is running (lines 38-40)
- _create_bridges method (lines 44-46)
- start() success path (lines 60-70)
- _run_and_monitor exception path (lines 83-86)
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.runtime.manager import RuntimeManager

_real_sleep = asyncio.sleep  # save before any patching


async def _instant_sleep(*_a, **_kw):
    await _real_sleep(0)


@pytest.fixture
def manager():
    return RuntimeManager(MicromechConfig())


# ---------------------------------------------------------------------------
# metrics property
# ---------------------------------------------------------------------------

class TestMetricsProperty:
    def test_metrics_returns_none_when_stopped(self, manager):
        assert manager.metrics is None

    def test_metrics_returns_none_when_no_server(self, manager):
        manager._state = "running"
        manager._server = None
        assert manager.metrics is None

    def test_metrics_returns_server_metrics_when_running(self, manager):
        mock_server = MagicMock()
        mock_server.metrics = MagicMock(name="metrics_collector")
        manager._server = mock_server
        manager._state = "running"
        assert manager.metrics is mock_server.metrics


# ---------------------------------------------------------------------------
# _create_bridges
# ---------------------------------------------------------------------------

class TestCreateBridges:
    def test_create_bridges_delegates_to_core(self, manager):
        with patch("micromech.core.bridge.create_bridges", return_value={"gnosis": MagicMock()}) as mock_cb:
            result = manager._create_bridges()
        mock_cb.assert_called_once_with(manager.config)
        assert "gnosis" in result


# ---------------------------------------------------------------------------
# start() success path
# ---------------------------------------------------------------------------

class TestStartSuccess:
    @pytest.mark.asyncio
    async def test_start_succeeds_and_sets_running(self, manager):
        mock_server = MagicMock()
        mock_server.run = AsyncMock()

        with patch.object(manager, "_create_bridges", return_value={}), \
             patch("micromech.runtime.server.MechServer", return_value=mock_server), \
             patch("micromech.runtime.manager.asyncio.sleep", _instant_sleep):
            ok = await manager.start()

        assert ok is True
        assert manager.state == "running"
        assert manager._server is mock_server

    @pytest.mark.asyncio
    async def test_start_returns_true_when_already_running(self, manager):
        manager._state = "running"
        ok = await manager.start()
        assert ok is True


# ---------------------------------------------------------------------------
# _run_and_monitor exception path
# ---------------------------------------------------------------------------

class TestRunAndMonitor:
    @pytest.mark.asyncio
    async def test_exception_sets_error_state(self, manager):
        mock_server = MagicMock()
        mock_server.run = AsyncMock(side_effect=RuntimeError("crash"))
        manager._server = mock_server
        manager._state = "starting"

        await manager._run_and_monitor()

        assert manager._state == "error"
        assert "crash" in manager._error

    @pytest.mark.asyncio
    async def test_cancelled_error_is_swallowed(self, manager):
        mock_server = MagicMock()
        mock_server.run = AsyncMock(side_effect=asyncio.CancelledError())
        manager._server = mock_server
        manager._state = "starting"

        # Should NOT raise
        await manager._run_and_monitor()
        # State not changed on CancelledError
        assert manager._state == "starting"
