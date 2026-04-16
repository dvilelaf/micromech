"""Tests for task modules with low coverage.

Covers:
- tasks/health.py           — HTTP heartbeat
- tasks/watchdog.py         — background task health monitor
- tasks/update_check.py     — DockerHub version check + auto-update logic
- tasks/low_balance_alert.py — low balance and eviction alerting
- tasks/metadata_check.py   — metadata staleness notification
- tools/prediction_request  — pure helper functions
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.tasks.notifications import NotificationService
from tests.conftest import make_test_config

# ===========================================================================
# Health task
# ===========================================================================


class TestHealthTask:
    @pytest.mark.asyncio
    async def test_no_health_url_skips(self):
        """When health_url is not configured, task returns without HTTP call."""
        from micromech.tasks.health import health_task

        with patch("micromech.tasks.health.secrets") as mock_secrets:
            mock_secrets.health_url = None
            with patch("httpx.AsyncClient") as mock_client:
                await health_task()
        mock_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_successful_heartbeat(self):
        """200 response from monitor is logged as success."""
        from micromech.tasks.health import health_task

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get = AsyncMock(return_value=mock_resp)

        with (
            patch("micromech.tasks.health.secrets") as mock_secrets,
            patch("httpx.AsyncClient") as MockClient,
        ):
            mock_secrets.health_url = "https://health.example.com/ping"
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = mock_get
            await health_task()

        mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_non_200_logs_warning(self):
        """Non-200 response is handled without raising."""
        from micromech.tasks.health import health_task

        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_get = AsyncMock(return_value=mock_resp)

        with (
            patch("micromech.tasks.health.secrets") as mock_secrets,
            patch("httpx.AsyncClient") as MockClient,
        ):
            mock_secrets.health_url = "https://health.example.com/ping"
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = mock_get
            await health_task()  # must not raise

    @pytest.mark.asyncio
    async def test_network_exception_handled(self):
        """Network errors are caught and logged — task must not crash."""
        from micromech.tasks.health import health_task

        with (
            patch("micromech.tasks.health.secrets") as mock_secrets,
            patch("httpx.AsyncClient") as MockClient,
        ):
            mock_secrets.health_url = "https://health.example.com/ping"
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = AsyncMock(side_effect=Exception("connection refused"))
            await health_task()  # must not raise


# ===========================================================================
# Watchdog task
# ===========================================================================


class TestWatchdogTask:
    def test_record_task_success_updates_timestamp(self):
        """record_task_success() must set _last_task_success to current time."""
        import micromech.tasks.watchdog as wd

        before = time.monotonic()
        wd.record_task_success()
        after = time.monotonic()

        assert before <= wd._last_task_success <= after

    def test_record_task_success_clears_alert_flag(self):
        """record_task_success() resets _alert_sent so next stale triggers alert."""
        import micromech.tasks.watchdog as wd

        wd._alert_sent = True
        wd.record_task_success()
        assert wd._alert_sent is False

    @pytest.mark.asyncio
    async def test_watchdog_sends_alert_when_stale(self):
        """When tasks are stale beyond threshold, a notification is sent."""
        import micromech.tasks.watchdog as wd

        notification = NotificationService()
        notification.send = AsyncMock()

        sleep_call_count = 0

        async def fake_sleep(interval):
            nonlocal sleep_call_count
            sleep_call_count += 1
            if sleep_call_count >= 2:
                raise StopAsyncIteration  # break the infinite loop

        # Set last success far in the past
        wd._last_task_success = time.monotonic() - wd.STALE_THRESHOLD_SECONDS - 60
        wd._alert_sent = False

        with (
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch("micromech.tasks.watchdog.CHECK_INTERVAL_SECONDS", 0),
        ):
            try:
                await wd.watchdog_loop(notification)
            except StopAsyncIteration:
                pass

        notification.send.assert_called_once()
        assert wd._alert_sent is True

    @pytest.mark.asyncio
    async def test_watchdog_no_alert_when_healthy(self):
        """When tasks complete within threshold, no alert is sent."""
        import micromech.tasks.watchdog as wd

        notification = NotificationService()
        notification.send = AsyncMock()

        sleep_call_count = 0

        async def fake_sleep(interval):
            nonlocal sleep_call_count
            sleep_call_count += 1
            if sleep_call_count >= 2:
                raise StopAsyncIteration

        wd._last_task_success = time.monotonic()  # just ran
        wd._alert_sent = False

        with (
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch("micromech.tasks.watchdog.CHECK_INTERVAL_SECONDS", 0),
        ):
            try:
                await wd.watchdog_loop(notification)
            except StopAsyncIteration:
                pass

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_watchdog_no_duplicate_alert(self):
        """If alert already sent, do not send again."""
        import micromech.tasks.watchdog as wd

        notification = NotificationService()
        notification.send = AsyncMock()

        sleep_call_count = 0

        async def fake_sleep(interval):
            nonlocal sleep_call_count
            sleep_call_count += 1
            if sleep_call_count >= 2:
                raise StopAsyncIteration

        wd._last_task_success = time.monotonic() - wd.STALE_THRESHOLD_SECONDS - 60
        wd._alert_sent = True  # already sent

        with (
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch("micromech.tasks.watchdog.CHECK_INTERVAL_SECONDS", 0),
        ):
            try:
                await wd.watchdog_loop(notification)
            except StopAsyncIteration:
                pass

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_watchdog_alert_send_failure_handled(self):
        """If notification.send() raises, the exception is caught and loop continues."""
        import micromech.tasks.watchdog as wd

        notification = NotificationService()
        notification.send = AsyncMock(side_effect=Exception("telegram down"))

        sleep_call_count = 0

        async def fake_sleep(interval):
            nonlocal sleep_call_count
            sleep_call_count += 1
            if sleep_call_count >= 2:
                raise StopAsyncIteration

        wd._last_task_success = time.monotonic() - wd.STALE_THRESHOLD_SECONDS - 60
        wd._alert_sent = False

        with (
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch("micromech.tasks.watchdog.CHECK_INTERVAL_SECONDS", 0),
        ):
            try:
                await wd.watchdog_loop(notification)
            except StopAsyncIteration:
                pass
        # _alert_sent should remain False since send raised
        assert wd._alert_sent is False


# ===========================================================================
# Update check task
# ===========================================================================


class TestParseVersion:
    def test_simple_version(self):
        from micromech.tasks.update_check import parse_version

        assert parse_version("1.2.3") == (1, 2, 3)

    def test_with_v_prefix(self):
        from micromech.tasks.update_check import parse_version

        assert parse_version("v0.7.7") == (0, 7, 7)

    def test_with_prerelease_suffix(self):
        from micromech.tasks.update_check import parse_version

        assert parse_version("1.2.3-beta") == (1, 2, 3)

    def test_with_build_metadata(self):
        from micromech.tasks.update_check import parse_version

        assert parse_version("1.2.3+build.1") == (1, 2, 3)

    def test_non_numeric_part(self):
        from micromech.tasks.update_check import parse_version

        result = parse_version("1.x.3")
        assert result == (1, 0, 3)

    def test_version_comparison(self):
        from micromech.tasks.update_check import parse_version

        assert parse_version("1.2.3") < parse_version("1.2.4")
        assert parse_version("0.7.7") > parse_version("0.7.6")


class TestGetCurrentVersion:
    def test_returns_installed_version(self):
        from micromech.tasks.update_check import get_current_version

        with patch("micromech.tasks.update_check.version", return_value="0.0.16"):
            result = get_current_version()

        assert result == "0.0.16"

    def test_returns_fallback_on_error(self):
        from micromech.tasks.update_check import get_current_version

        with patch("micromech.tasks.update_check.version", side_effect=Exception("not installed")):
            result = get_current_version()

        assert result == "0.0.0"


class TestCheckDockerhubLatest:
    @pytest.mark.asyncio
    async def test_returns_latest_tag(self):
        from micromech.tasks.update_check import check_dockerhub_latest

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "results": [
                {"name": "latest"},
                {"name": "0.0.17"},
                {"name": "0.0.16"},
            ]
        }

        with patch("httpx.AsyncClient") as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = AsyncMock(return_value=mock_resp)
            result = await check_dockerhub_latest()

        assert result == "0.0.17"

    @pytest.mark.asyncio
    async def test_skips_latest_tag(self):
        from micromech.tasks.update_check import check_dockerhub_latest

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"results": [{"name": "latest"}]}

        with patch("httpx.AsyncClient") as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = AsyncMock(return_value=mock_resp)
            result = await check_dockerhub_latest()

        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_returns_none(self):
        from micromech.tasks.update_check import check_dockerhub_latest

        with patch("httpx.AsyncClient") as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get = AsyncMock(side_effect=Exception("timeout"))
            result = await check_dockerhub_latest()

        assert result is None


class TestUpdateCheckTask:
    @pytest.mark.asyncio
    async def test_skips_when_disabled(self):
        from micromech.tasks.update_check import update_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        config = make_test_config()
        config.update_check_enabled = False

        with patch("micromech.tasks.update_check.check_dockerhub_latest") as mock_check:
            await update_check_task(notification, config)

        mock_check.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_latest_version_available(self):
        from micromech.tasks.update_check import update_check_task

        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch("micromech.tasks.update_check.check_dockerhub_latest", return_value=None),
            patch("micromech.tasks.update_check.get_current_version", return_value="0.0.16"),
        ):
            await update_check_task(notification)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_up_to_date_no_notification(self):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import update_check_task

        uc._notified_version = None
        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch("micromech.tasks.update_check.check_dockerhub_latest", return_value="0.0.16"),
            patch("micromech.tasks.update_check.get_current_version", return_value="0.0.16"),
        ):
            await update_check_task(notification)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_new_version_sends_notification(self):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import update_check_task

        uc._notified_version = None
        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch("micromech.tasks.update_check.check_dockerhub_latest", return_value="0.0.17"),
            patch("micromech.tasks.update_check.get_current_version", return_value="0.0.16"),
        ):
            await update_check_task(notification)

        notification.send.assert_called_once()
        assert "0.0.17" in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_already_notified_skips(self):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import update_check_task

        uc._notified_version = "0.0.17"  # already notified
        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch("micromech.tasks.update_check.check_dockerhub_latest", return_value="0.0.17"),
            patch("micromech.tasks.update_check.get_current_version", return_value="0.0.16"),
        ):
            await update_check_task(notification)

        notification.send.assert_not_called()
        uc._notified_version = None  # cleanup

    @pytest.mark.asyncio
    async def test_auto_update_schedules_and_triggers(self, tmp_path):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import update_check_task

        uc._notified_version = None
        uc._pending_version = None
        notification = NotificationService()
        notification.send = AsyncMock()
        config = make_test_config()
        config.auto_update_enabled = True

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"

        with (
            patch("micromech.tasks.update_check.check_dockerhub_latest", return_value="0.0.17"),
            patch("micromech.tasks.update_check.get_current_version", return_value="0.0.16"),
            patch("micromech.tasks.update_check.TRIGGER_PATH", trigger),
            patch("micromech.tasks.update_check.RESULT_PATH", result_path),
        ):
            await update_check_task(notification, config)

        # Should have sent "Auto-Update Scheduled" and "Auto-Update Triggered"
        assert notification.send.call_count >= 1
        uc._notified_version = None  # cleanup


class TestAutoUpdatePollTask:
    @pytest.mark.asyncio
    async def test_does_nothing_when_no_pending(self):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import auto_update_poll_task

        uc._pending_version = None
        uc._auto_update_started_at = None
        notification = NotificationService()
        notification.send = AsyncMock()

        await auto_update_poll_task(notification)
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_triggers_update_normally(self, tmp_path):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import auto_update_poll_task

        uc._pending_version = "0.0.17"
        uc._auto_update_started_at = time.time()
        notification = NotificationService()
        notification.send = AsyncMock()

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"

        with (
            patch("micromech.tasks.update_check.TRIGGER_PATH", trigger),
            patch("micromech.tasks.update_check.RESULT_PATH", result_path),
        ):
            await auto_update_poll_task(notification)

        notification.send.assert_called_once()
        assert "Auto-Update Triggered" in notification.send.call_args[0][0]
        uc._pending_version = None
        uc._auto_update_started_at = None

    @pytest.mark.asyncio
    async def test_forces_update_after_max_wait(self, tmp_path):
        import micromech.tasks.update_check as uc
        from micromech.tasks.update_check import AUTO_UPDATE_MAX_WAIT_HOURS, auto_update_poll_task

        uc._pending_version = "0.0.17"
        # Set start time far in the past — beyond max wait
        uc._auto_update_started_at = time.time() - (AUTO_UPDATE_MAX_WAIT_HOURS + 1) * 3600
        notification = NotificationService()
        notification.send = AsyncMock()

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"

        with (
            patch("micromech.tasks.update_check.TRIGGER_PATH", trigger),
            patch("micromech.tasks.update_check.RESULT_PATH", result_path),
        ):
            await auto_update_poll_task(notification)

        notification.send.assert_called_once()
        # Should say "forced"
        assert "forced" in notification.send.call_args[0][1]
        uc._pending_version = None
        uc._auto_update_started_at = None


# ===========================================================================
# Low balance alert task
# ===========================================================================


class TestLowBalanceAlertTask:
    @pytest.mark.asyncio
    async def test_disabled_skips_all_checks(self):
        from micromech.tasks.low_balance_alert import low_balance_alert_task

        config = make_test_config()
        config.low_balance_alert_enabled = False
        notification = NotificationService()
        notification.send = AsyncMock()
        lifecycle = MagicMock()

        # check_balances is a local import inside the task; patch the source
        with patch("micromech.core.bridge.check_balances") as mock_cb:
            await low_balance_alert_task({"gnosis": lifecycle}, {}, notification, config)

        mock_cb.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_low_native_balance_triggers_alert(self):
        from micromech.tasks.low_balance_alert import low_balance_alert_task

        config = make_test_config()
        config.low_balance_alert_enabled = True
        config.fund_threshold_native = 0.5
        notification = NotificationService()
        notification.send = AsyncMock()
        lifecycle = MagicMock()

        with (
            patch("micromech.core.bridge.check_balances", return_value=(0.1, 10.0)),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await low_balance_alert_task({"gnosis": lifecycle}, {}, notification, config)

        notification.send.assert_called()
        assert "Low Balance" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_healthy_balance_no_alert(self):
        from micromech.tasks.low_balance_alert import low_balance_alert_task

        config = make_test_config()
        config.low_balance_alert_enabled = True
        config.fund_threshold_native = 0.5
        notification = NotificationService()
        notification.send = AsyncMock()
        lifecycle = MagicMock()

        with (
            patch("micromech.core.bridge.check_balances", return_value=(5.0, 10.0)),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await low_balance_alert_task({"gnosis": lifecycle}, {}, notification, config)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_eviction_alert(self):
        from micromech.tasks.low_balance_alert import low_balance_alert_task

        config = make_test_config()
        config.low_balance_alert_enabled = True
        config.fund_threshold_native = 0.01
        notification = NotificationService()
        notification.send = AsyncMock()
        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "EVICTED",
            "is_staked": False,
        }

        with (
            patch("micromech.core.bridge.check_balances", return_value=(1.0, 10.0)),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey", "service_id": 1},
            ),
        ):
            await low_balance_alert_task({"gnosis": lifecycle}, {}, notification, config)

        notification.send.assert_called()
        assert "Eviction" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_exception_per_chain_is_caught(self):
        from micromech.tasks.low_balance_alert import low_balance_alert_task

        config = make_test_config()
        config.low_balance_alert_enabled = True
        config.fund_threshold_native = 0.5
        notification = NotificationService()
        notification.send = AsyncMock()
        lifecycle = MagicMock()

        with patch("micromech.core.bridge.check_balances", side_effect=Exception("rpc error")):
            # Must not raise — error is per-chain and caught
            await low_balance_alert_task({"gnosis": lifecycle}, {}, notification, config)


# ===========================================================================
# Metadata check task
# ===========================================================================


class TestMetadataCheckTask:
    @pytest.mark.asyncio
    async def test_up_to_date_no_notification(self):
        from micromech.tasks.metadata_check import metadata_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        status = MagicMock()
        status.needs_update = False

        metadata_manager = MagicMock()
        metadata_manager.get_status.return_value = status

        await metadata_check_task(metadata_manager, notification)
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_with_no_prior_publish(self):
        from micromech.tasks.metadata_check import metadata_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        status = MagicMock()
        status.needs_update = True
        status.ipfs_cid = None  # never published

        metadata_manager = MagicMock()
        metadata_manager.get_status.return_value = status

        await metadata_check_task(metadata_manager, notification)
        notification.send.assert_called_once()
        assert "never been published" in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_stale_with_changed_packages(self):
        from micromech.tasks.metadata_check import metadata_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        status = MagicMock()
        status.needs_update = True
        status.ipfs_cid = "Qmabc123"
        status.changed_packages = ["echo_tool", "local_llm"]

        metadata_manager = MagicMock()
        metadata_manager.get_status.return_value = status

        await metadata_check_task(metadata_manager, notification)
        notification.send.assert_called_once()
        assert "echo_tool" in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_stale_with_unknown_changes(self):
        from micromech.tasks.metadata_check import metadata_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        status = MagicMock()
        status.needs_update = True
        status.ipfs_cid = "Qmabc123"
        status.changed_packages = []  # empty → "unknown"

        metadata_manager = MagicMock()
        metadata_manager.get_status.return_value = status

        await metadata_check_task(metadata_manager, notification)
        notification.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_caught(self):
        from micromech.tasks.metadata_check import metadata_check_task

        notification = NotificationService()
        notification.send = AsyncMock()
        metadata_manager = MagicMock()
        metadata_manager.get_status.side_effect = Exception("db error")

        # Must not raise
        await metadata_check_task(metadata_manager, notification)
        notification.send.assert_not_called()


# ===========================================================================
# prediction_request helpers (pure functions, no external deps)
# ===========================================================================


class TestPredictionRequestHelpers:
    """_extract_json and _validate_prediction both work on strings (JSON in/out)."""

    def test_extract_json_plain(self):
        import json

        from micromech.tools.prediction_request.prediction_request import _extract_json

        text = '{"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.5}'
        result = _extract_json(text)
        assert result is not None
        assert json.loads(result)["p_yes"] == pytest.approx(0.7)

    def test_extract_json_wrapped_in_markdown(self):
        import json

        from micromech.tools.prediction_request.prediction_request import _extract_json

        inner = '{"p_yes": 0.6, "p_no": 0.4, "confidence": 0.9, "info_utility": 0.7}'
        text = f"```json\n{inner}\n```"
        result = _extract_json(text)
        assert result is not None
        assert json.loads(result)["p_yes"] == pytest.approx(0.6)

    def test_extract_json_passthrough_on_no_match(self):
        from micromech.tools.prediction_request.prediction_request import _extract_json

        # No JSON object found — returns input stripped
        result = _extract_json("  plain text  ")
        assert result == "plain text"

    def test_validate_prediction_valid(self):
        import json

        from micromech.tools.prediction_request.prediction_request import _validate_prediction

        raw = '{"p_yes": 0.7, "p_no": 0.3, "confidence": 0.8, "info_utility": 0.5}'
        result = _validate_prediction(raw)
        data = json.loads(result)
        assert data["p_yes"] == pytest.approx(0.7)

    def test_validate_prediction_normalizes_probabilities(self):
        import json

        from micromech.tools.prediction_request.prediction_request import _validate_prediction

        # p_yes + p_no = 2.0 → normalize to 0.5 each
        raw = '{"p_yes": 1.0, "p_no": 1.0, "confidence": 0.5, "info_utility": 0.5}'
        result = _validate_prediction(raw)
        data = json.loads(result)
        assert data["p_yes"] == pytest.approx(0.5)
        assert data["p_no"] == pytest.approx(0.5)

    def test_validate_prediction_adds_missing_keys(self):
        import json

        from micromech.tools.prediction_request.prediction_request import _validate_prediction

        raw = '{"p_yes": 0.6}'
        result = _validate_prediction(raw)
        data = json.loads(result)
        assert "p_no" in data
        assert "confidence" in data
        assert "info_utility" in data

    def test_validate_prediction_returns_default_on_invalid_json(self):
        from micromech.tools.prediction_request.prediction_request import (
            DEFAULT_PREDICTION,
            _validate_prediction,
        )

        result = _validate_prediction("not json at all")
        assert result == DEFAULT_PREDICTION

    def test_validate_prediction_returns_default_on_type_error(self):
        from micromech.tools.prediction_request.prediction_request import (
            DEFAULT_PREDICTION,
            _validate_prediction,
        )

        # Passing None causes json.loads to raise TypeError
        result = _validate_prediction(None)
        assert result == DEFAULT_PREDICTION
