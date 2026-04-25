"""Tests for failed_deliveries_alert_task."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.tasks.failed_deliveries_alert import failed_deliveries_alert_task
from micromech.tasks.notifications import NotificationService


def _make_queue(failed: int = 0, include_failed_key: bool = True) -> MagicMock:
    queue = MagicMock()
    counts = {"pending": 0, "executing": 0, "executed": 0, "delivered": 5}
    if include_failed_key:
        counts["failed"] = failed
    queue.count_by_status.return_value = counts
    return queue


def _make_notification() -> NotificationService:
    svc = MagicMock(spec=NotificationService)
    svc.send = AsyncMock()
    return svc


def _make_cfg(**kwargs) -> MicromechConfig:
    defaults = {
        "failed_deliveries_alert_enabled": True,
        "failed_deliveries_alert_threshold": 10,
        "failed_deliveries_alert_interval_hours": 1,
    }
    defaults.update(kwargs)
    return MicromechConfig(**defaults)


class TestFailedDeliveriesAlertTask:
    @pytest.mark.asyncio
    async def test_sends_alert_when_at_threshold(self):
        """Alert fires when failed count equals threshold."""
        queue = _make_queue(failed=10)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()
        call_args = notif.send.call_args
        assert "10" in call_args.args[1]
        assert call_args.kwargs["level"] == "warning"

    @pytest.mark.asyncio
    async def test_sends_alert_when_above_threshold(self):
        """Alert fires when failed count exceeds threshold; message contains count."""
        queue = _make_queue(failed=25)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()
        call_args = notif.send.call_args
        assert "25" in call_args.args[1]

    @pytest.mark.asyncio
    async def test_no_alert_below_threshold(self):
        """No alert when failed count is below threshold."""
        queue = _make_queue(failed=3)
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_alert_at_threshold_minus_one(self):
        """No alert when failed count is exactly threshold - 1."""
        queue = _make_queue(failed=9)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_alert_when_disabled(self):
        """Task is a no-op when disabled in config."""
        queue = _make_queue(failed=100)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_enabled=False)

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()
        queue.count_by_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_zero_failed(self):
        """No alert when there are no failed deliveries."""
        queue = _make_queue(failed=0)
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_custom_threshold(self):
        """Respects a custom threshold from config."""
        queue = _make_queue(failed=3)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=3)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_queue_none_does_not_crash(self):
        """Task exits gracefully when queue is None (scheduler may pass None)."""
        notif = _make_notification()
        cfg = _make_cfg()

        await failed_deliveries_alert_task(None, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_failed_key_defaults_to_zero(self):
        """No alert when count_by_status returns dict without 'failed' key."""
        queue = _make_queue(include_failed_key=False)
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_count_by_status_exception_does_not_propagate(self):
        """DB errors are caught and logged; task does not crash the scheduler."""
        queue = MagicMock()
        queue.count_by_status.side_effect = RuntimeError("DB locked")
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_uses_interval_hours_as_time_window(self):
        """count_by_status is called with hours=interval_hours (recent window, not all-time)."""
        queue = _make_queue(failed=0)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_interval_hours=3)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        queue.count_by_status.assert_called_once_with(hours=3)

    @pytest.mark.asyncio
    async def test_message_contains_no_html_tags(self):
        """Alert message body contains no raw HTML tags (NotificationService escapes them)."""
        queue = _make_queue(failed=10)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        body = notif.send.call_args.args[1]
        assert "<b>" not in body
        assert "</b>" not in body

    def test_config_defaults(self):
        """Default config values are correct."""
        cfg = MicromechConfig()
        assert cfg.failed_deliveries_alert_enabled is True
        assert cfg.failed_deliveries_alert_threshold == 10
        assert cfg.failed_deliveries_alert_interval_hours == 1
