"""Tests for tasks/payment_withdraw.py."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.marketplace import get_balance_tracker_address, get_pending_balance
from micromech.tasks.notifications import NotificationService
from micromech.tasks.payment_withdraw import (
    _drain_mech_to_safe,
    payment_withdraw_task,
)
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

    # Mech contract: paymentType() + exec()
    mech_contract = MagicMock()
    mech_contract.functions.paymentType.return_value.call.return_value = PAYMENT_TYPE
    mech_exec_fn = MagicMock()
    mech_exec_fn.build_transaction.return_value = {"data": b"0xEXEC"}
    mech_contract.functions.exec.return_value = mech_exec_fn

    # Marketplace contract: mapPaymentTypeBalanceTrackers()
    marketplace_contract = MagicMock()
    marketplace_contract.functions.mapPaymentTypeBalanceTrackers.return_value.call.return_value = (
        bt_address
    )

    # Balance tracker contract: mapMechBalances() + processPaymentByMultisig()
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
    web3.eth.get_balance.return_value = mech_balance_raw

    safe_service = MagicMock()
    safe_service.execute_safe_transaction.return_value = "0xtxhash"

    master_account = MagicMock()
    master_account.address = "0x" + "f" * 40

    wallet = MagicMock()
    wallet.safe_service = safe_service
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
# _drain_mech_to_safe
# ---------------------------------------------------------------------------


class TestDrainMechToSafe:
    def test_calls_exec_via_safe(self):
        """mech.exec() is called via execute_safe_transaction with correct args."""
        bridge = _make_bridge()
        amount_wei = int(Decimal("41.79") * Decimal("1000000000000000000"))

        _drain_mech_to_safe(bridge, "gnosis", MECH, MULTISIG, amount_wei)

        # Safe must call the mech contract (to=MECH, value=0)
        bridge.wallet.safe_service.execute_safe_transaction.assert_called_once_with(
            safe_address_or_tag=MULTISIG,
            to=MECH,
            value=0,
            chain_name="gnosis",
            data=b"0xEXEC",
        )

    def test_builds_exec_calldata_with_correct_params(self):
        """mech.functions.exec is called with multisig as destination and amount_wei."""
        bridge = _make_bridge()
        amount_wei = int(5e18)

        _drain_mech_to_safe(bridge, "gnosis", MECH, MULTISIG, amount_wei)

        mech_contract = bridge.web3.eth.contract(address=MECH)
        mech_contract.functions.exec.assert_called_once_with(
            MULTISIG,   # to
            amount_wei, # value (native xDAI)
            b"",        # data
            0,          # operation = Call
            100_000,    # txGas
        )

    def test_waits_for_receipt_via_with_retry(self):
        """Receipt wait goes through bridge.with_retry."""
        bridge = _make_bridge()
        _drain_mech_to_safe(bridge, "gnosis", MECH, MULTISIG, int(1e18))
        bridge.with_retry.assert_called()

    def test_raises_on_revert(self):
        """RuntimeError raised if receipt status != 1."""
        bridge = _make_bridge()
        bad_receipt = MagicMock()
        bad_receipt.__getitem__ = lambda self, key: 0 if key == "status" else None
        bridge.with_retry.side_effect = lambda fn, **kw: bad_receipt

        with pytest.raises(RuntimeError, match="mech.exec drain reverted"):
            _drain_mech_to_safe(bridge, "gnosis", MECH, MULTISIG, int(1e18))

    def test_zero_amount_still_calls_exec(self):
        """Even with zero amount, exec is called (contract decides to no-op)."""
        bridge = _make_bridge()
        _drain_mech_to_safe(bridge, "gnosis", MECH, MULTISIG, 0)
        bridge.wallet.safe_service.execute_safe_transaction.assert_called_once()


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

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.01e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_full_flow_calls_three_steps(self):
        """Full flow: processPaymentByMultisig → mech.exec → wallet.send (master)."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # execute_safe_transaction called TWICE:
        # 1. processPaymentByMultisig (BT → mech)
        # 2. mech.exec (mech → Safe)
        assert bridge.wallet.safe_service.execute_safe_transaction.call_count == 2

        # wallet.send called ONCE for master transfer (Safe → master)
        bridge.wallet.send.assert_called_once()

        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "0.500000" in msg

    @pytest.mark.asyncio
    async def test_drain_step_targets_mech_not_bt(self):
        """Second execute_safe_transaction targets the mech, first targets the BT."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(1e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        calls = bridge.wallet.safe_service.execute_safe_transaction.call_args_list
        assert len(calls) == 2
        # First call: to=BT_ADDR (processPaymentByMultisig)
        assert calls[0].kwargs["to"] == BT_ADDR
        # Second call: to=MECH (mech.exec drain)
        assert calls[1].kwargs["to"] == MECH

    @pytest.mark.asyncio
    async def test_drain_amount_uses_actual_mech_balance(self):
        """mech.exec uses the real on-chain mech balance, not float reconstruction.

        This avoids float round-trip precision loss: balance tracker returns wei,
        get_pending_balance() converts to float, then float→Decimal→wei can drift.
        Instead, after processPaymentByMultisig we read w3.eth.get_balance(mech)
        and pass that exact wei to mech.exec.
        """
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        # mech_balance_raw is what both the BT and get_balance return in the mock
        balance_raw = int(41.79e18)
        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=balance_raw)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # exec must be called with the exact wei from get_balance, not float conversion
        mech_contract = bridge.web3.eth.contract(address=MECH)
        exec_call = mech_contract.functions.exec.call_args
        amount_wei_passed = exec_call.args[1]
        assert amount_wei_passed == balance_raw, (
            f"Expected exact wei {balance_raw}, got {amount_wei_passed}"
        )

    @pytest.mark.asyncio
    async def test_transfer_to_master_uses_wallet_send(self):
        """_transfer_to_master uses wallet.send, not execute_safe_transaction."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.send.assert_called_once()
        send_kwargs = bridge.wallet.send.call_args[1]
        assert send_kwargs["from_address_or_tag"] == MULTISIG
        assert send_kwargs["chain_name"] == "gnosis"
        assert send_kwargs["amount_wei"] > 0

        # execute_safe_transaction: only processPaymentByMultisig + mech.exec (not master)
        assert bridge.wallet.safe_service.execute_safe_transaction.call_count == 2

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
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
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
    async def test_drain_failure_propagates_to_outer_except(self):
        """If _drain_mech_to_safe raises, outer except catches it (no notification)."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        # First execute_safe_tx call (processPaymentByMultisig) succeeds.
        # Second call (mech.exec drain) raises — simulates exec revert.
        bridge.wallet.safe_service.execute_safe_transaction.side_effect = [
            "0xtxhash",
            RuntimeError("mech.exec reverted"),
        ]
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # Outer except caught the error: no notification, no master transfer
        notification.send.assert_not_called()
        bridge.wallet.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_wallet_send_none_raises_and_is_caught(self):
        """wallet.send returning None → RuntimeError caught as inner transfer failure."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridge.wallet.send.return_value = None
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "WARNING" in msg or "failed" in msg.lower()
