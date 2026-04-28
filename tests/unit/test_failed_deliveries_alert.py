"""Tests for failed_deliveries_alert_task."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.tasks.failed_deliveries_alert import failed_deliveries_alert_task
from micromech.tasks.notifications import NotificationService


def _make_queue(
    actionable: int = 0,
    *,
    timed_out: int = 0,
    other: int | None = None,
    already_final: int = 0,
) -> MagicMock:
    queue = MagicMock()
    other = actionable - timed_out if other is None else other
    queue.failure_summary.return_value = {
        "failed": actionable + already_final,
        "actionable": actionable,
        "timed_out": timed_out,
        "other": other,
        "already_final": already_final,
    }
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
        """Alert fires when actionable issue count equals threshold."""
        queue = _make_queue(actionable=10, timed_out=2, other=8)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()
        call_args = notif.send.call_args
        assert call_args.args[0] == "Delivery Issues Alert"
        assert "10" in call_args.args[1]
        assert "2 on-chain timeout" in call_args.args[1]
        assert "8 other failure" in call_args.args[1]
        assert call_args.kwargs["level"] == "warning"

    @pytest.mark.asyncio
    async def test_sends_alert_when_above_threshold(self):
        """Alert fires when actionable issue count exceeds threshold."""
        queue = _make_queue(actionable=25)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_awaited_once()
        call_args = notif.send.call_args
        assert "25" in call_args.args[1]

    @pytest.mark.asyncio
    async def test_no_alert_below_threshold(self):
        """No alert when actionable issue count is below threshold."""
        queue = _make_queue(actionable=3)
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_alert_at_threshold_minus_one(self):
        """No alert when actionable issue count is exactly threshold - 1."""
        queue = _make_queue(actionable=9)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_alert_when_disabled(self):
        """Task is a no-op when disabled in config."""
        queue = _make_queue(actionable=100)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_enabled=False)

        await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()
        queue.failure_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_zero_failed(self):
        """No alert when there are no actionable delivery issues."""
        queue = _make_queue(actionable=0)
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_custom_threshold(self):
        """Respects a custom threshold from config."""
        queue = _make_queue(actionable=3)
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
    async def test_missing_actionable_key_defaults_to_zero(self):
        """No alert when failure_summary returns dict without 'actionable' key."""
        queue = MagicMock()
        queue.failure_summary.return_value = {}
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_count_by_status_exception_does_not_propagate(self):
        """DB errors are caught and logged; task does not crash the scheduler."""
        queue = MagicMock()
        queue.failure_summary.side_effect = RuntimeError("DB locked")
        notif = _make_notification()
        cfg = _make_cfg()

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_uses_interval_hours_as_time_window(self):
        """failure_summary is called with hours=interval_hours (recent window, not all-time)."""
        queue = _make_queue(actionable=0)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_interval_hours=3)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        queue.failure_summary.assert_called_once_with(hours=3)

    @pytest.mark.asyncio
    async def test_message_contains_no_html_tags(self):
        """Alert message body contains no raw HTML tags (NotificationService escapes them)."""
        queue = _make_queue(actionable=10)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        body = notif.send.call_args.args[1]
        assert "<b>" not in body
        assert "</b>" not in body

    @pytest.mark.asyncio
    async def test_already_final_requests_are_reported_but_do_not_trigger_alert(self):
        """Requests already final on-chain are ignored for the alert threshold."""
        queue = _make_queue(actionable=2, already_final=50)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        notif.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_alert_mentions_ignored_already_final_requests(self):
        """When alerting, the message explains ignored already-final requests."""
        queue = _make_queue(actionable=10, timed_out=4, other=6, already_final=3)
        notif = _make_notification()
        cfg = _make_cfg(failed_deliveries_alert_threshold=10)

        with patch("asyncio.to_thread", side_effect=lambda fn, **kw: fn(**kw)):
            await failed_deliveries_alert_task(queue, notif, cfg)

        body = notif.send.call_args.args[1]
        assert "Ignored 3 request(s) already final on-chain" in body

    def test_config_defaults(self):
        """Default config values are correct."""
        cfg = MicromechConfig()
        assert cfg.failed_deliveries_alert_enabled is True
        assert cfg.failed_deliveries_alert_threshold == 10
        assert cfg.failed_deliveries_alert_interval_hours == 1
