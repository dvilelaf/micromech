"""Tests for failed_deliveries_alert_task."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from micromech.core.config import MicromechConfig
from micromech.tasks.failed_deliveries_alert import failed_deliveries_alert_task
from micromech.tasks.notifications import NotificationService


def _make_queue(failed: int = 0) -> MagicMock:
    queue = MagicMock()
    queue.count_by_status.return_value = {
        "pending": 0,
        "executing": 0,
        "executed": 0,
        "delivered": 5,
        "failed": failed,
    }
    return queue


def _make_notification() -> NotificationService:
    svc = MagicMock(spec=NotificationService)
    svc.send = AsyncMock()
    return svc


class TestFailedDeliveriesAlertTask:
    @pytest.mark.asyncio
    async def test_sends_alert_when_at_threshold(self):
        """Alert fires when failed count equals threshold."""
        queue = _make_queue(failed=10)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=True,
            failed_deliveries_alert_threshold=10,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()
        call_args = notif.send.call_args
        assert "10" in call_args.args[1]
        assert call_args.kwargs.get("level") == "warning" or call_args.args[2] == "warning"

    @pytest.mark.asyncio
    async def test_sends_alert_when_above_threshold(self):
        """Alert fires when failed count exceeds threshold."""
        queue = _make_queue(failed=25)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=True,
            failed_deliveries_alert_threshold=10,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_alert_below_threshold(self):
        """No alert when failed count is below threshold."""
        queue = _make_queue(failed=3)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=True,
            failed_deliveries_alert_threshold=10,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_alert_when_disabled(self):
        """Task is a no-op when disabled in config."""
        queue = _make_queue(failed=100)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=False,
            failed_deliveries_alert_threshold=10,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()
        queue.count_by_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_zero_failed(self):
        """No alert when there are no failed deliveries."""
        queue = _make_queue(failed=0)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=True,
            failed_deliveries_alert_threshold=10,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_custom_threshold(self):
        """Respects a custom threshold from config."""
        queue = _make_queue(failed=3)
        notif = _make_notification()
        cfg = MicromechConfig(
            failed_deliveries_alert_enabled=True,
            failed_deliveries_alert_threshold=3,
        )

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()

    def test_config_defaults(self):
        """Default config values are correct."""
        cfg = MicromechConfig()
        assert cfg.failed_deliveries_alert_enabled is True
        assert cfg.failed_deliveries_alert_threshold == 10
        assert cfg.failed_deliveries_alert_interval_hours == 1
