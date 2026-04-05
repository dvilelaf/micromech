"""Tests for watchdog module."""

import time

import micromech.tasks.watchdog as watchdog


class TestRecordTaskSuccess:
    def setup_method(self):
        watchdog._last_task_success = 0.0
        watchdog._alert_sent = False

    def test_updates_timestamp(self):
        before = time.monotonic()
        watchdog.record_task_success()
        after = time.monotonic()
        assert before <= watchdog._last_task_success <= after

    def test_resets_alert_flag(self):
        watchdog._alert_sent = True
        watchdog.record_task_success()
        assert watchdog._alert_sent is False

    def test_multiple_calls_advance_timestamp(self):
        watchdog.record_task_success()
        t1 = watchdog._last_task_success
        # monotonic always advances (or stays same)
        watchdog.record_task_success()
        t2 = watchdog._last_task_success
        assert t2 >= t1


class TestWatchdogConstants:
    def test_check_interval(self):
        assert watchdog.CHECK_INTERVAL_SECONDS == 300

    def test_stale_threshold(self):
        assert watchdog.STALE_THRESHOLD_SECONDS == 1800
