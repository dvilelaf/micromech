"""Tests for periodic tasks: checkpoint, rewards, fund."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from tests.conftest import make_test_config
from micromech.tasks.notifications import NotificationService


def _make_config(**overrides) -> MicromechConfig:
    """Create a MicromechConfig with custom task settings."""
    cfg = make_test_config(**overrides)
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
    async def test_alerts_when_no_bridge(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(fund_threshold_native=0.1)

        with patch(
            "micromech.core.bridge.check_safe_balance", return_value=0.01,
        ), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": "0x" + "a" * 40},
        ):
            await fund_task({}, notification, config)

        notification.send.assert_called_once()
        assert "Auto-Fund" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_transfers_when_bridge_available(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.send.return_value = "0xtxhash"

        config = _make_config(
            fund_threshold_native=0.1, fund_target_native=0.5,
        )

        with patch(
            "micromech.core.bridge.check_safe_balance", return_value=0.01,
        ), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": "0x" + "a" * 40},
        ), patch(
            "micromech.core.bridge.check_balances", return_value=(10.0, 0.0),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        bridge.wallet.send.assert_called_once()
        call_kwargs = bridge.wallet.send.call_args[1]
        assert call_kwargs["from_address_or_tag"] == "master"
        assert call_kwargs["chain_name"] == "gnosis"
        notification.send.assert_called_once()
        assert "Auto-Fund Safe" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_action_when_balance_ok(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(fund_threshold_native=0.01)

        with patch(
            "micromech.core.bridge.check_safe_balance", return_value=1.0,
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
            "micromech.core.bridge.check_safe_balance",
            side_effect=Exception("rpc fail"),
        ):
            # Should not raise
            await fund_task({}, notification, config)

    @pytest.mark.asyncio
    async def test_alerts_on_transfer_failure(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.send.side_effect = Exception("insufficient funds")

        config = _make_config(
            fund_threshold_native=0.1, fund_target_native=0.5,
        )

        with patch(
            "micromech.core.bridge.check_safe_balance", return_value=0.01,
        ), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": "0x" + "a" * 40},
        ), patch(
            "micromech.core.bridge.check_balances", return_value=(10.0, 0.0),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        notification.send.assert_called_once()
        assert "Auto-Fund Failed" in notification.send.call_args[0][0]


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


# ── Auto-Sell Task ───────────────────────────────────────────────────────


class TestAutoSellTask:
    @pytest.mark.asyncio
    async def test_sells_olas_above_floor(self):
        from micromech.tasks.auto_sell import auto_sell_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.swap = AsyncMock(return_value=True)

        config = _make_config(auto_sell_enabled=True, auto_sell_min_olas=1.0)

        with patch(
            "micromech.core.bridge.check_balances",
            return_value=(1.0, 10.0),  # 10 OLAS available
        ):
            await auto_sell_task(
                {"gnosis": bridge}, notification, config,
                olas_floor_wei={"gnosis": 0},
            )

        bridge.wallet.swap.assert_called_once()
        call_kwargs = bridge.wallet.swap.call_args[1]
        assert call_kwargs["sell_token_name"] == "olas"
        assert call_kwargs["buy_token_name"] == "wxdai"
        assert call_kwargs["chain_name"] == "gnosis"
        notification.send.assert_called_once()
        assert "Auto-Sell" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_respects_olas_floor(self):
        from micromech.tasks.auto_sell import auto_sell_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.swap = AsyncMock(return_value=True)

        config = _make_config(auto_sell_enabled=True, auto_sell_min_olas=1.0)

        # 10 OLAS total, floor at 9.5 OLAS → only 0.5 sellable (below min)
        floor_wei = {"gnosis": int(9.5 * 10**18)}
        with patch(
            "micromech.core.bridge.check_balances",
            return_value=(1.0, 10.0),
        ):
            await auto_sell_task(
                {"gnosis": bridge}, notification, config,
                olas_floor_wei=floor_wei,
            )

        bridge.wallet.swap.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_disabled(self):
        from micromech.tasks.auto_sell import auto_sell_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        config = _make_config(auto_sell_enabled=False)
        await auto_sell_task({}, notification, config)
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_unsupported_chain(self):
        from micromech.tasks.auto_sell import auto_sell_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.swap = AsyncMock(return_value=True)

        # "base" is not in COW_SUPPORTED_CHAINS
        config = _make_config(auto_sell_enabled=True)
        # Override enabled_chains to return "base"
        config.chains = {"base": MagicMock(enabled=True)}

        with patch(
            "micromech.core.bridge.check_balances",
            return_value=(1.0, 10.0),
        ):
            await auto_sell_task({"base": bridge}, notification, config)

        bridge.wallet.swap.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_swap_failure(self):
        from micromech.tasks.auto_sell import auto_sell_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        bridge = MagicMock()
        bridge.wallet.swap = AsyncMock(side_effect=Exception("cow api error"))

        config = _make_config(auto_sell_enabled=True, auto_sell_min_olas=1.0)

        with patch(
            "micromech.core.bridge.check_balances",
            return_value=(1.0, 10.0),
        ):
            await auto_sell_task(
                {"gnosis": bridge}, notification, config,
                olas_floor_wei={"gnosis": 0},
            )

        notification.send.assert_called_once()
        assert "Auto-Sell Error" in notification.send.call_args[0][0]


# ── Profitability Check Task ─────────────────────────────────────────────


class TestProfitabilityCheckTask:
    @pytest.mark.asyncio
    async def test_alerts_when_unprofitable(self):
        from micromech.tasks.profitability_check import profitability_check_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        queue = MagicMock()
        queue.count_delivered_since.return_value = 0  # no deliveries = no revenue

        lifecycle = _make_lifecycle(is_staked=False, rewards=0.0)
        config = _make_config()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await profitability_check_task(
                queue, {"gnosis": lifecycle}, {}, notification, config,
            )

        notification.send.assert_called_once()
        assert "Unprofitable" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_alert_when_profitable(self):
        from micromech.tasks.profitability_check import profitability_check_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        queue = MagicMock()
        queue.count_delivered_since.return_value = 100  # lots of deliveries

        lifecycle = _make_lifecycle(rewards=5.0)
        config = _make_config()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await profitability_check_task(
                queue, {"gnosis": lifecycle}, {}, notification, config,
            )

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.profitability_check import profitability_check_task

        notification = NotificationService()
        notification._skip_resolve()
        notification.send = AsyncMock()

        queue = MagicMock()
        queue.count_delivered_since.side_effect = Exception("db error")

        config = _make_config()

        # Should not raise
        await profitability_check_task(queue, {}, {}, notification, config)
