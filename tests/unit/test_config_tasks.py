"""Tests for TasksConfig and TelegramConfig models."""

import pytest
from pydantic import ValidationError

from micromech.core.config import TasksConfig, TelegramConfig


class TestTelegramConfig:
    def test_defaults(self):
        cfg = TelegramConfig()
        assert cfg.enabled is False
        assert cfg.rate_limit_seconds == 2

    def test_custom_values(self):
        cfg = TelegramConfig(enabled=True, rate_limit_seconds=10)
        assert cfg.enabled is True
        assert cfg.rate_limit_seconds == 10

    def test_rate_limit_bounds(self):
        with pytest.raises(ValidationError):
            TelegramConfig(rate_limit_seconds=0)
        with pytest.raises(ValidationError):
            TelegramConfig(rate_limit_seconds=31)


class TestTasksConfig:
    def test_defaults(self):
        cfg = TasksConfig()
        assert cfg.enabled is True
        assert cfg.checkpoint_interval_minutes == 10
        assert cfg.checkpoint_grace_period_seconds == 120
        assert cfg.checkpoint_alert_enabled is True
        assert cfg.claim_interval_minutes == 240
        assert cfg.claim_threshold_olas == 1.0
        assert cfg.fund_enabled is True
        assert cfg.fund_interval_minutes == 360
        assert cfg.fund_threshold_native == 0.05
        assert cfg.fund_target_native == 0.5
        assert cfg.auto_sell_enabled is False
        assert cfg.low_balance_alert_enabled is True
        assert cfg.update_check_enabled is True
        assert cfg.auto_update_enabled is False
        assert cfg.update_channel == "release"
        assert cfg.health_interval_seconds == 55

    def test_checkpoint_interval_min(self):
        with pytest.raises(ValidationError):
            TasksConfig(checkpoint_interval_minutes=0)

    def test_checkpoint_interval_max(self):
        with pytest.raises(ValidationError):
            TasksConfig(checkpoint_interval_minutes=121)

    def test_claim_interval_min(self):
        with pytest.raises(ValidationError):
            TasksConfig(claim_interval_minutes=5)

    def test_claim_interval_max(self):
        with pytest.raises(ValidationError):
            TasksConfig(claim_interval_minutes=1441)

    def test_fund_threshold_non_negative(self):
        cfg = TasksConfig(fund_threshold_native=0)
        assert cfg.fund_threshold_native == 0

    def test_health_interval_bounds(self):
        with pytest.raises(ValidationError):
            TasksConfig(health_interval_seconds=5)
        with pytest.raises(ValidationError):
            TasksConfig(health_interval_seconds=301)

    def test_custom_values(self):
        cfg = TasksConfig(
            checkpoint_interval_minutes=30,
            claim_threshold_olas=5.0,
            fund_enabled=False,
            auto_update_enabled=True,
        )
        assert cfg.checkpoint_interval_minutes == 30
        assert cfg.claim_threshold_olas == 5.0
        assert cfg.fund_enabled is False
        assert cfg.auto_update_enabled is True
