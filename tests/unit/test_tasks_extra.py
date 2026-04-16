"""Extra tests for tasks/rewards.py and tasks/checkpoint.py covering missed lines.

Covers:
- rewards_task: claim returns False
- checkpoint_task: epoch still active
- checkpoint_task: within grace period
- checkpoint_task: past grace, checkpoint called + alert
- checkpoint_task: checkpoint returns False
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.constants import CHECKPOINT_GRACE_PERIOD_SECONDS
from micromech.tasks.notifications import NotificationService
from tests.conftest import make_test_config


def _make_config(**kw):
    return make_test_config(**kw)


def _make_lifecycle(is_staked=True, rewards=5.0, claim_ok=True, checkpoint_ok=True):
    lc = MagicMock()
    lc.chain_config = MagicMock()
    lc.chain_config.chain = "gnosis"
    lc.chain_config.staking_address = "0x" + "a" * 40
    lc.get_status.return_value = {"is_staked": is_staked, "rewards": rewards}
    lc.claim_rewards.return_value = claim_ok
    lc.checkpoint.return_value = checkpoint_ok
    return lc


def _svc_info(key="0xkey"):
    return {"service_key": key, "service_id": 1} if key else {}


def _make_staking_contract_cls(epoch_end: datetime):
    """Return a mock StakingContract class whose instances have a fixed epoch end."""
    mock_contract = MagicMock()
    mock_contract.get_next_epoch_start.return_value = epoch_end
    mock_cls = MagicMock(return_value=mock_contract)
    return mock_cls


# ---------------------------------------------------------------------------
# rewards_task
# ---------------------------------------------------------------------------


class TestRewardsTask:
    @pytest.mark.asyncio
    async def test_claim_returns_false_no_notification(self):
        """When lifecycle.claim_rewards returns False, warning logged, no notification."""
        from micromech.tasks.rewards import rewards_task

        config = _make_config(claim_threshold_olas=1.0)
        lifecycle = _make_lifecycle(rewards=5.0, claim_ok=False)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_service_info", return_value=_svc_info()):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        notification.send.assert_not_called()


# ---------------------------------------------------------------------------
# checkpoint_task — epoch/grace logic
# ---------------------------------------------------------------------------


class TestCheckpointEpochLogic:
    @pytest.mark.asyncio
    async def test_epoch_still_active_skips_checkpoint(self):
        """If epoch_end is in the future, skip checkpoint."""
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle()
        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config()

        future_end = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_cls = _make_staking_contract_cls(future_end)

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_svc_info()),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", mock_cls),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        lifecycle.checkpoint.assert_not_called()

    @pytest.mark.asyncio
    async def test_within_grace_period_skips_checkpoint(self):
        """If epoch ended but grace period not yet elapsed, skip checkpoint."""
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle()
        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config()

        # Ended 5 seconds ago — well within the 120s grace period
        recent_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        mock_cls = _make_staking_contract_cls(recent_end)

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_svc_info()),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", mock_cls),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        lifecycle.checkpoint.assert_not_called()

    @pytest.mark.asyncio
    async def test_past_grace_period_calls_checkpoint(self):
        """Past grace period → checkpoint is called."""
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle()
        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config()
        config.checkpoint_alert_enabled = False

        old_end = datetime.now(timezone.utc) - timedelta(
            seconds=CHECKPOINT_GRACE_PERIOD_SECONDS + 3600
        )
        mock_cls = _make_staking_contract_cls(old_end)

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_svc_info()),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", mock_cls),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        lifecycle.checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_checkpoint_success_with_alert_enabled(self):
        """Successful checkpoint sends notification when alert enabled."""
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle()
        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config()
        config.checkpoint_alert_enabled = True

        old_end = datetime.now(timezone.utc) - timedelta(
            seconds=CHECKPOINT_GRACE_PERIOD_SECONDS + 3600
        )
        mock_cls = _make_staking_contract_cls(old_end)

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_svc_info()),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", mock_cls),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        notification.send.assert_called_once()
        assert "Checkpoint" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_checkpoint_returns_false_no_notification(self):
        """When checkpoint() returns False, no notification is sent."""
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle(checkpoint_ok=False)
        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config()
        config.checkpoint_alert_enabled = True

        old_end = datetime.now(timezone.utc) - timedelta(
            seconds=CHECKPOINT_GRACE_PERIOD_SECONDS + 3600
        )
        mock_cls = _make_staking_contract_cls(old_end)

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_svc_info()),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", mock_cls),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

        notification.send.assert_not_called()
