"""Tests for periodic tasks: checkpoint, rewards, fund."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.tasks.notifications import NotificationService
from tests.conftest import make_test_config


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
    lc.chain_config = MagicMock(
        spec=[
            "chain",
            "staking_address",
            "mech_address",
            "account_tag",
        ]
    )
    lc.chain_config.chain = "gnosis"
    lc.chain_config.staking_address = "0x" + "a" * 40
    # Stash for callers that need to build get_service_info mock
    lc._test_service_key = service_key

    status = {"is_staked": is_staked, "rewards": rewards, "staking_state": "STAKED"}
    lc.get_status.return_value = status
    lc.claim_rewards.return_value = True
    lc.withdraw_rewards.return_value = (True, rewards)
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

        # 5 OLAS * 4.0 €/OLAS = 20€ >= 10€ threshold
        lifecycle = _make_lifecycle(rewards=5.0)
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_eur=10.0)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch(
                "micromech.core.bridge.get_olas_price_eur",
                return_value=4.0,
            ),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_called_once()
        notification.send.assert_called_once()
        assert "5.0000 OLAS" in notification.send.call_args[0][1]
        assert "20.00€" in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_skips_when_below_threshold(self):
        from micromech.tasks.rewards import rewards_task

        # 0.5 OLAS * 4.0 €/OLAS = 2€ < 10€ threshold
        lifecycle = _make_lifecycle(rewards=0.5)
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_eur=10.0)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch(
                "micromech.core.bridge.get_olas_price_eur",
                return_value=4.0,
            ),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_price_unavailable(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(rewards=100.0)
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_eur=10.0)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch(
                "micromech.core.bridge.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_service_key(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle(service_key=None)
        notification = NotificationService()
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
        notification.send = AsyncMock()

        config = _make_config()
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch("micromech.core.bridge.get_olas_price_eur", return_value=4.0),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_not_called()

    @pytest.mark.asyncio
    async def test_claim_returns_false_no_notification(self):
        from micromech.tasks.rewards import rewards_task

        # 5 OLAS * 4.0 €/OLAS = 20€ >= 10€ threshold → proceeds to claim
        lifecycle = _make_lifecycle(rewards=5.0)
        lifecycle.claim_rewards.return_value = False
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_eur=10.0)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch("micromech.core.bridge.get_olas_price_eur", return_value=4.0),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_called_once()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_withdraw_fails_no_transfer_line(self):
        from micromech.tasks.rewards import rewards_task

        # 5 OLAS * 4.0 €/OLAS = 20€ >= 10€ threshold → claims OK, withdraw fails
        lifecycle = _make_lifecycle(rewards=5.0)
        lifecycle.withdraw_rewards.return_value = (False, 0.0)
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(claim_threshold_eur=10.0)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch("micromech.core.bridge.get_olas_price_eur", return_value=4.0),
        ):
            await rewards_task({"gnosis": lifecycle}, notification, config)

        lifecycle.claim_rewards.assert_called_once()
        notification.send.assert_called_once()
        # No "Transferred" line when withdraw fails
        assert "Transferred" not in notification.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.rewards import rewards_task

        lifecycle = _make_lifecycle()
        lifecycle.get_status.side_effect = Exception("rpc fail")
        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config()
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_svc_info_for("0xkey"),
            ),
            patch("micromech.core.bridge.get_olas_price_eur", return_value=4.0),
        ):
            # Should not raise
            await rewards_task({"gnosis": lifecycle}, notification, config)


# ── Fund Task ─────────────────────────────────────────────────────────────


AGENT_ADDR = "0x" + "e" * 40
MASTER_ADDR = "0x" + "f" * 40

_SVC_INFO_WITH_AGENT = {"agent_address": AGENT_ADDR, "multisig_address": "0x" + "a" * 40}


def _make_wallet_mock(agent_balance=0.01, master_balance=10.0):
    """Mock iwa wallet for fund task tests."""
    wallet = MagicMock()
    wallet.master_account.address = MASTER_ADDR
    wallet.get_native_balance_eth.side_effect = lambda addr, chain: (
        agent_balance if addr == AGENT_ADDR else master_balance
    )
    return wallet


class TestFundTask:
    @pytest.mark.asyncio
    async def test_skips_when_fund_disabled(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()

        config = _make_config(fund_enabled=False)
        await fund_task({}, notification, config)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_action_when_balance_ok(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config(fund_threshold_native=0.01)
        wallet = _make_wallet_mock(agent_balance=1.0)

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_SVC_INFO_WITH_AGENT,
            ),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        notification.send.assert_not_called()
        bridge.wallet.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_bridge(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        config = _make_config(fund_threshold_native=0.1)

        await fund_task({}, notification, config)

        # No bridge → debug log only, no alert
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_agent_address(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config(fund_threshold_native=0.1)

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={},  # no agent_address
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        notification.send.assert_not_called()
        bridge.wallet.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_transfers_when_bridge_available(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        bridge.wallet.send.return_value = "0xtxhash"
        config = _make_config(
            fund_threshold_native=0.1,
            fund_target_native=0.5,
        )
        wallet = _make_wallet_mock(agent_balance=0.01, master_balance=10.0)

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_SVC_INFO_WITH_AGENT,
            ),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        bridge.wallet.send.assert_called_once()
        call_kwargs = bridge.wallet.send.call_args[1]
        assert call_kwargs["from_address_or_tag"] == "master"
        assert call_kwargs["to_address_or_tag"] == AGENT_ADDR
        assert call_kwargs["chain_name"] == "gnosis"
        notification.send.assert_called_once()
        assert "Auto-Fund Agent" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_alerts_when_master_too_low(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config(
            fund_threshold_native=0.1,
            fund_target_native=0.5,
        )
        wallet = _make_wallet_mock(agent_balance=0.01, master_balance=0.001)

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_SVC_INFO_WITH_AGENT,
            ),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        bridge.wallet.send.assert_not_called()
        notification.send.assert_called_once()
        assert "Insufficient Master" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_alerts_on_transfer_failure(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        bridge.wallet.send.side_effect = Exception("insufficient funds")
        config = _make_config(
            fund_threshold_native=0.1,
            fund_target_native=0.5,
        )
        wallet = _make_wallet_mock(agent_balance=0.01, master_balance=10.0)

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=_SVC_INFO_WITH_AGENT,
            ),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        notification.send.assert_called_once()
        assert "Auto-Fund Failed" in notification.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config()

        with patch(
            "micromech.core.bridge.get_service_info",
            side_effect=Exception("rpc fail"),
        ):
            # Should not raise
            await fund_task({"gnosis": bridge}, notification, config)

    @pytest.mark.asyncio
    async def test_decimal_balance_no_type_error_when_ok(self):
        """iwa 0.7.5 returns Decimal from get_native_balance_eth — must not crash when balance OK."""
        from decimal import Decimal

        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config(fund_threshold_native=0.1)
        wallet = MagicMock()
        wallet.master_account.address = MASTER_ADDR
        wallet.get_native_balance_eth.side_effect = lambda addr, chain: (
            Decimal("1.0") if addr == AGENT_ADDR else Decimal("10.0")
        )

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_SVC_INFO_WITH_AGENT),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            # Should not raise TypeError: unsupported operand type(s) for >=: 'decimal.Decimal' and 'float'
            await fund_task({"gnosis": bridge}, notification, config)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_decimal_balance_no_type_error_when_low(self):
        """iwa 0.7.5: Decimal balance triggers fund transfer without TypeError."""
        from decimal import Decimal

        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        bridge.wallet.send.return_value = "0xtxhash"
        config = _make_config(fund_threshold_native=0.1, fund_target_native=0.5)
        wallet = MagicMock()
        wallet.master_account.address = MASTER_ADDR
        wallet.get_native_balance_eth.side_effect = lambda addr, chain: (
            Decimal("0.01") if addr == AGENT_ADDR else Decimal("10.0")
        )

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_SVC_INFO_WITH_AGENT),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            # Should not raise TypeError: unsupported operand type(s) for -: 'float' and 'decimal.Decimal'
            await fund_task({"gnosis": bridge}, notification, config)

        bridge.wallet.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_decimal_balance_master_too_low(self):
        """iwa 0.7.5: Decimal balance comparison for master insufficient funds — no TypeError."""
        from decimal import Decimal

        from micromech.tasks.fund import fund_task

        notification = NotificationService()
        notification.send = AsyncMock()
        bridge = MagicMock()
        config = _make_config(fund_threshold_native=0.1, fund_target_native=0.5)
        wallet = MagicMock()
        wallet.master_account.address = MASTER_ADDR
        wallet.get_native_balance_eth.side_effect = lambda addr, chain: (
            Decimal("0.01") if addr == AGENT_ADDR else Decimal("0.001")
        )

        with (
            patch("micromech.core.bridge.get_service_info", return_value=_SVC_INFO_WITH_AGENT),
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
        ):
            await fund_task({"gnosis": bridge}, notification, config)

        bridge.wallet.send.assert_not_called()
        notification.send.assert_called_once()
        assert "Insufficient Master" in notification.send.call_args[0][0]


# ── Checkpoint Task ───────────────────────────────────────────────────────


class TestCheckpointTask:
    @pytest.mark.asyncio
    async def test_skips_when_no_service_key(self):
        from micromech.tasks.checkpoint import checkpoint_task

        lifecycle = _make_lifecycle(service_key=None)
        notification = NotificationService()
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
        notification.send = AsyncMock()

        config = _make_config()
        # Should not raise
        with patch(
            "micromech.core.bridge.get_service_info",
            return_value=_svc_info_for("0xkey"),
        ):
            await checkpoint_task({"gnosis": lifecycle}, notification, config)

