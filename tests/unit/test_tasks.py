"""Tests for periodic tasks: checkpoint, rewards, fund."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig, TasksConfig
from micromech.tasks.notifications import NotificationService


def _make_config(**task_overrides) -> MicromechConfig:
    """Create a MicromechConfig with custom task settings."""
    tasks = TasksConfig(**task_overrides)
    cfg = MicromechConfig()
    cfg.tasks = tasks
    return cfg


def _make_lifecycle(service_key="0xkey", is_staked=True, rewards=0.0):
    """Create a mock MechLifecycle.

    service_key controls what get_service_info will return (mock it
    with _svc_info_patch).
    """
    lc = MagicMock()
    lc.chain_config = MagicMock(spec=[
        "chain", "staking_address", "mech_address", "account_tag",
    ])
    lc.chain_config.chain = "gnosis"
    lc.chain_config.staking_address = "0x" + "a" * 40
    # Stash for callers that need to build get_service_info mock
    lc._test_service_key = service_key

    status = {"is_staked": is_staked, "rewards": rewards, "staking_state": "STAKED"}
    lc.get_status.return_value = status
    lc.claim_rewards.return_value = True
    lc.checkpoint.return_value = True
    return lc


def _svc_info_for(service_key):
    """Build a get_service_info return value."""
    if service_key:
        return {"service_key": service_key, "service_id": 1}
    return {}


# ── Rewards Task ──────────────────────────────────────────────────────────


class TestRewardsTask:
    @pytest.mark.asyncio
    async def test_claims_when_above_threshold(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(rewards=5.0)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_olas=1.0)
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_called_once()
        notification.send.assert_called_once()
        assert "5.0000 OLAS" in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_skips_when_below_threshold(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(rewards=0.5)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_olas=1.0)
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_service_key(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(service_key=None)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for(None),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.get_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_not_staked(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(is_staked=False)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle()
        lifecycle.get_status.side_effect = Exception("rpc fail")
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            # Should not raise
            await rewards_task({"gnosis": lifecycle}, notification, config)


# ── Fund Task ─────────────────────────────────────────────────────────────


class TestFundTask:
    @pytest.mark.asyncio
    async def test_alerts_on_low_balance(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(fund_threshold_native=0.1)

        with patch(
            "micromech.core.bridge.check_balances", return_value=(0.01, 10.0)
        ):
            await fund_task({}, notification, config)

        notification.send.assert_called_once()
        assert "Fund Required" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_alert_when_balance_ok(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(fund_threshold_native=0.01)

        with patch(
            "micromech.core.bridge.check_balances", return_value=(1.0, 10.0)
        ):
            await fund_task({}, notification, config)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_fund_disabled(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(fund_enabled=False)
        await fund_task({}, notification, config)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()

        with patch(
            "micromech.core.bridge.check_balances",
            side_effect=Exception("rpc fail"),
        ):
            # Should not raise
            await fund_task({}, notification, config)


# ── Checkpoint Task ───────────────────────────────────────────────────────


class TestCheckpointTask:
    @pytest.mark.asyncio
    async def test_skips_when_no_service_key(self):
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle(service_key=None)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for(None),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        lifecycle.get_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_not_staked(self):
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle(is_staked=False)
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle()
        lifecycle.get_status.side_effect = Exception("rpc fail")
        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config()
        # Should not raise
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)
