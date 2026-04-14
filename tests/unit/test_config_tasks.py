"""Tests for flattened task fields on MicromechConfig."""

import pytest
from pydantic import ValidationError

from micromech.core.config import MicromechConfig


class TestTaskFields:
    def test_defaults(self):
        cfg = MicromechConfig()
        assert cfg.checkpoint_interval_minutes == 10
        assert cfg.checkpoint_alert_enabled is True
        assert cfg.claim_interval_minutes == 240
        assert cfg.claim_threshold_olas == 1.0
        assert cfg.fund_enabled is True
        assert cfg.fund_interval_minutes == 360
        assert cfg.fund_threshold_native == 0.1
        assert cfg.fund_target_native == 1.0
        assert cfg.low_balance_alert_enabled is True
        assert cfg.update_check_enabled is True
        assert cfg.auto_update_enabled is False

    def test_checkpoint_interval_min(self):
        with pytest.raises(ValidationError):
            MicromechConfig(checkpoint_interval_minutes=0)

    def test_checkpoint_interval_max(self):
        with pytest.raises(ValidationError):
            MicromechConfig(checkpoint_interval_minutes=121)

    def test_claim_interval_min(self):
        with pytest.raises(ValidationError):
            MicromechConfig(claim_interval_minutes=5)

    def test_claim_interval_max(self):
        with pytest.raises(ValidationError):
            MicromechConfig(claim_interval_minutes=1441)

    def test_fund_threshold_non_negative(self):
        cfg = MicromechConfig(fund_threshold_native=0, fund_target_native=0)
        assert cfg.fund_threshold_native == 0

    def test_fund_target_must_be_above_threshold(self):
        with pytest.raises(ValidationError):
            MicromechConfig(fund_threshold_native=1.0, fund_target_native=0.5)

    def test_fund_target_upper_bound(self):
        with pytest.raises(ValidationError):
            MicromechConfig(fund_target_native=51)

    def test_custom_values(self):
        cfg = MicromechConfig(
            checkpoint_interval_minutes=30,
            claim_threshold_olas=5.0,
            fund_enabled=False,
            auto_update_enabled=True,
        )
        assert cfg.checkpoint_interval_minutes == 30
        assert cfg.claim_threshold_olas == 5.0
        assert cfg.fund_enabled is False
        assert cfg.auto_update_enabled is True
