"""Tests for tasks/payment_withdraw.py."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.marketplace import get_balance_tracker_address, get_pending_balance
from micromech.tasks.notifications import NotificationService
from micromech.tasks.payment_withdraw import payment_withdraw_task
from tests.conftest import make_test_config

MECH = "0x" + "a" * 40
MULTISIG = "0x" + "b" * 40
MARKETPLACE = "0x" + "c" * 40
BT_ADDR = "0x" + "d" * 40
ZERO = "0x" + "0" * 40
PAYMENT_TYPE = b"\xba" + b"\x00" * 31


def _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18)):
    """Create a mock IwaBridge with web3, wallet, and safe_service."""
    web3 = MagicMock()
    web3.to_checksum_address.side_effect = lambda x: x

    # Mech contract: paymentType()
    mech_contract = MagicMock()
    mech_contract.functions.paymentType.return_value.call.return_value = PAYMENT_TYPE

    # Marketplace contract: mapPaymentTypeBalanceTrackers()
    marketplace_contract = MagicMock()
    marketplace_contract.functions.mapPaymentTypeBalanceTrackers.return_value.call.return_value = (
        bt_address
    )

    # Balance tracker contract: mapMechBalances()
    bt_contract = MagicMock()
    bt_contract.functions.mapMechBalances.return_value.call.return_value = mech_balance_raw
    bt_fn_call = MagicMock()
    bt_fn_call.build_transaction.return_value = {"data": b"0xCAFE"}
    bt_contract.functions.processPaymentByMultisig.return_value = bt_fn_call

    def contract_factory(address=None, abi=None):
        if address == MECH:
            return mech_contract
        if address == MARKETPLACE:
            return marketplace_contract
        if address == bt_address:
            return bt_contract
        return MagicMock()

    web3.eth.contract.side_effect = contract_factory

    receipt = MagicMock()
    receipt.__getitem__ = lambda self, key: 1 if key == "status" else None
    web3.eth.wait_for_transaction_receipt.return_value = receipt

    safe_service = MagicMock()
    safe_service.execute_safe_transaction.return_value = "0xtxhash"

    master_account = MagicMock()
    master_account.address = "0x" + "f" * 40

    wallet = MagicMock()
    wallet.safe_service = safe_service
    # Bug B fix: _transfer_to_master must use wallet.send, not execute_safe_transaction
    wallet.send.return_value = "0xtxhash_transfer"
    wallet.master_account = master_account

    bridge = MagicMock()
    bridge.web3 = web3
    bridge.wallet = wallet
    bridge.with_retry.side_effect = lambda fn, **kw: fn()
    return bridge


def _make_config(**kw):
    return make_test_config(**kw)


def _make_chain_config(mech=MECH, marketplace=MARKETPLACE):
    cc = MagicMock()
    cc.mech_address = mech
    cc.marketplace_address = marketplace
    cc.enabled = True
    return cc


# ---------------------------------------------------------------------------
# get_balance_tracker_address
# ---------------------------------------------------------------------------


class TestGetBalanceTrackerAddress:
    def test_returns_tracker_address(self):
        bridge = _make_bridge(bt_address=BT_ADDR)
        result = get_balance_tracker_address(bridge, "gnosis", MECH, MARKETPLACE)
        assert result == BT_ADDR

    def test_returns_none_for_zero_address(self):
        bridge = _make_bridge(bt_address=ZERO)
        result = get_balance_tracker_address(bridge, "gnosis", MECH, MARKETPLACE)
        assert result is None

    def test_returns_none_when_rpc_fails(self):
        """RPC failure inside with_retry is caught — returns None instead of raising."""
        bridge = _make_bridge()
        bridge.with_retry.side_effect = Exception("RPC timeout")
        result = get_balance_tracker_address(bridge, "gnosis", MECH, MARKETPLACE)
        assert result is None


# ---------------------------------------------------------------------------
# get_pending_balance
# ---------------------------------------------------------------------------


class TestGetPendingBalance:
    def test_converts_wei_to_ether(self):
        bridge = _make_bridge(mech_balance_raw=int(0.42e18))
        result = get_pending_balance(bridge, BT_ADDR, MECH)
        assert abs(result - 0.42) < 1e-9

    def test_zero_balance(self):
        bridge = _make_bridge(mech_balance_raw=0)
        result = get_pending_balance(bridge, BT_ADDR, MECH)
        assert result == 0.0

    def test_returns_zero_when_rpc_fails(self):
        """RPC failure inside with_retry is caught — returns 0.0 instead of raising."""
        bridge = _make_bridge()
        bridge.with_retry.side_effect = Exception("connection refused")
        result = get_pending_balance(bridge, BT_ADDR, MECH)
        assert result == 0.0


# ---------------------------------------------------------------------------
# payment_withdraw_task
# ---------------------------------------------------------------------------


class TestPaymentWithdrawTask:
    @pytest.mark.asyncio
    async def test_skips_chain_without_mech_address(self):
        """Chains without mech_address are skipped silently."""
        cfg = _make_config()
        cfg.payment_withdraw_enabled = True
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config(mech=None)
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge()
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_chain_without_bridge(self):
        """Chains without a matching bridge are skipped."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        notification = NotificationService()
        notification.send = AsyncMock()

        await payment_withdraw_task({}, notification, cfg)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_balance_below_threshold(self):
        """Does not withdraw when pending balance < threshold."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 1.0  # high threshold

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.01e18))  # only 0.01
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info", return_value={"multisig_address": MULTISIG}
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_withdraws_when_balance_above_threshold(self):
        """Calls processPaymentByMultisig and notifies when balance >= threshold."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info", return_value={"multisig_address": MULTISIG}
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # Bug B: processPaymentByMultisig goes through execute_safe_transaction (contract call),
        # but _transfer_to_master must use wallet.send (native transfer via iwa pipeline).
        bridge.wallet.safe_service.execute_safe_transaction.assert_called_once()
        bridge.wallet.send.assert_called_once()
        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "0.500000" in msg

    @pytest.mark.asyncio
    async def test_transfer_to_master_uses_wallet_send_not_execute_safe_tx(self):
        """Bug B: _transfer_to_master must use wallet.send, not execute_safe_transaction.

        Direct execute_safe_transaction calls bypass iwa's signing pipeline and cause
        GS013 (Invalid signatures) on-chain. wallet.send handles Safe account lookup
        and signing correctly.
        """
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info", return_value={"multisig_address": MULTISIG}
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # wallet.send must be called for the master transfer
        bridge.wallet.send.assert_called_once()
        send_kwargs = bridge.wallet.send.call_args[1]
        assert send_kwargs["from_address_or_tag"] == MULTISIG
        assert send_kwargs["chain_name"] == "gnosis"
        # Amount must be > 0 (non-trivial xDAI balance)
        assert send_kwargs["amount_wei"] > 0

        # execute_safe_transaction should be called only once: processPaymentByMultisig
        # (not for transfer_to_master, which is now handled by wallet.send)
        bridge.wallet.safe_service.execute_safe_transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_zero_address_balance_tracker(self):
        """No withdrawal when balance tracker returns zero address."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=ZERO)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info", return_value={"multisig_address": MULTISIG}
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_multisig(self):
        """No withdrawal when multisig_address is not configured."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_bridge_without_safe_service(self):
        """Bridge without safe_service attribute is skipped."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = MagicMock()
        bridge.web3 = MagicMock()
        # Remove safe_service so hasattr returns False
        del bridge.wallet.safe_service
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_in_task_does_not_raise(self):
        """Exceptions inside the task loop are caught and logged, not raised."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = MagicMock()
        bridge.web3.to_checksum_address.side_effect = lambda x: x
        bridge.with_retry.side_effect = RuntimeError("network error")
        bridge.wallet.safe_service = MagicMock()
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        # Should not raise
        await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_wallet_send_none_raises_and_is_caught(self):
        """Bug B: wallet.send returning None raises RuntimeError caught by outer handler.

        The xDAI stays in the Safe (withdrawal still completed) but the sweep to
        master fails with a warning notification.
        """
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridge.wallet.send.return_value = None  # Simulate wallet.send failure
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info", return_value={"multisig_address": MULTISIG}
        ):
            # Should not raise — outer except catches RuntimeError from _transfer_to_master
            await payment_withdraw_task(bridges, notification, cfg)

        # The xDAI transfer to master failed → warning notification sent
        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "WARNING" in msg or "failed" in msg.lower()
