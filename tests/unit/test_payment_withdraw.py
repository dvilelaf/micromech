"""Tests for tasks/payment_withdraw.py."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.marketplace import get_balance_tracker_address, get_pending_balance
from micromech.tasks.notifications import NotificationService
from micromech.tasks.payment_withdraw import (
    _drain_mech_to_safe,
    _transfer_to_master,
    _transfer_to_master_with_retry,
    _withdraw,
    execute_payment_withdraw,
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
        bridge = _make_bridge(bt_address=ZERO, mech_balance_raw=0)
        result = get_balance_tracker_address(bridge, "gnosis", MECH, MARKETPLACE)
        assert result is None

    def test_returns_none_when_rpc_fails(self):
        """RPC failure inside with_retry is caught — returns None instead of raising."""
        bridge = _make_bridge()
        bridge.with_retry.side_effect = Exception("RPC timeout")
        result = get_balance_tracker_address(bridge, "gnosis", MECH, MARKETPLACE)
        assert result is None

    def test_raises_when_requested_on_rpc_failure(self):
        """Strict callers can distinguish tracker lookup failure from no tracker."""
        bridge = _make_bridge()
        bridge.with_retry.side_effect = Exception("RPC timeout")
        with pytest.raises(Exception, match="RPC timeout"):
            get_balance_tracker_address(
                bridge,
                "gnosis",
                MECH,
                MARKETPLACE,
                raise_on_error=True,
            )


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

    def test_raises_when_requested_on_rpc_failure(self):
        """Strict callers can distinguish unknown pending from a real zero."""
        bridge = _make_bridge()
        bridge.with_retry.side_effect = Exception("connection refused")
        with pytest.raises(Exception, match="connection refused"):
            get_pending_balance(bridge, BT_ADDR, MECH, raise_on_error=True)


# ---------------------------------------------------------------------------
# _withdraw
# ---------------------------------------------------------------------------


class TestWithdraw:
    def test_calls_processPaymentByMultisig_via_safe(self):
        """processPaymentByMultisig is called via execute_safe_transaction."""
        bridge = _make_bridge()
        _withdraw(bridge, "gnosis", BT_ADDR, MECH, MULTISIG)
        bridge.wallet.safe_service.execute_safe_transaction.assert_called_once_with(
            safe_address_or_tag=MULTISIG,
            to=BT_ADDR,
            value=0,
            chain_name="gnosis",
            data=b"0xCAFE",
        )

    def test_raises_on_revert(self):
        """RuntimeError raised if processPaymentByMultisig receipt status != 1."""
        bridge = _make_bridge()
        bad_receipt = MagicMock()
        bad_receipt.__getitem__ = lambda self, key: 0 if key == "status" else None
        bridge.with_retry.side_effect = lambda fn, **kw: bad_receipt

        with pytest.raises(RuntimeError, match="processPaymentByMultisig reverted"):
            _withdraw(bridge, "gnosis", BT_ADDR, MECH, MULTISIG)


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
            MULTISIG,  # to
            amount_wei,  # value (native xDAI)
            b"",  # data
            0,  # operation = Call
            100_000,  # txGas
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
# _transfer_to_master
# ---------------------------------------------------------------------------


class TestTransferToMaster:
    def test_raises_on_revert(self):
        """RuntimeError raised if Safe→master receipt status != 1."""
        bridge = _make_bridge()
        bad_receipt = MagicMock()
        bad_receipt.__getitem__ = lambda self, key: 0 if key == "status" else None
        bridge.with_retry.side_effect = lambda fn, **kw: bad_receipt

        with pytest.raises(RuntimeError, match="transfer to master reverted"):
            _transfer_to_master(bridge, "gnosis", MULTISIG, int(1e18))

    def test_retry_wrapper_retries_gs026(self):
        """GS026 can be a transient Safe nonce-state race and is retried."""
        bridge = _make_bridge()
        bridge.wallet.send.side_effect = [
            RuntimeError("Safe transaction failed: GS026"),
            "0xtxhash_transfer",
        ]

        with patch("micromech.tasks.payment_withdraw.time.sleep") as sleep_mock:
            _transfer_to_master_with_retry(
                bridge,
                "gnosis",
                MULTISIG,
                int(1e18),
                retry_delay_seconds=0,
            )

        assert bridge.wallet.send.call_count == 2
        sleep_mock.assert_called_once_with(0)

    def test_retry_wrapper_does_not_retry_non_gs026(self):
        """Non-GS026 Safe transfer errors still fail immediately."""
        bridge = _make_bridge()
        bridge.wallet.send.side_effect = RuntimeError("insufficient funds")

        with pytest.raises(RuntimeError, match="insufficient funds"):
            _transfer_to_master_with_retry(
                bridge,
                "gnosis",
                MULTISIG,
                int(1e18),
                retry_delay_seconds=0,
            )

        bridge.wallet.send.assert_called_once()

    def test_transfers_safe_excess_above_reserve(self):
        """Stranded xDAI already in the Safe can be swept to master."""
        from micromech.tasks.payment_withdraw import _transfer_safe_excess_to_master

        bridge = _make_bridge()
        bridge.web3.eth.get_balance.return_value = int(33.92e18)

        swept = _transfer_safe_excess_to_master(bridge, "gnosis", MULTISIG, reserve_xdai=0.5)

        assert swept == int(33.42e18)
        bridge.wallet.send.assert_called_once_with(
            from_address_or_tag=MULTISIG,
            to_address_or_tag=str(bridge.wallet.master_account.address),
            amount_wei=int(33.42e18),
            chain_name="gnosis",
        )

    def test_safe_excess_below_reserve_is_noop(self):
        """Safe reserve is not swept."""
        from micromech.tasks.payment_withdraw import _transfer_safe_excess_to_master

        bridge = _make_bridge()
        bridge.web3.eth.get_balance.return_value = int(0.5e18)

        swept = _transfer_safe_excess_to_master(bridge, "gnosis", MULTISIG, reserve_xdai=0.5)

        assert swept == 0
        bridge.wallet.send.assert_not_called()


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
        # Mech has dust below threshold; Safe has no stranded balance.
        bridge.web3.eth.get_balance.side_effect = [int(0.01e18), 0]
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
    async def test_sweeps_stranded_safe_balance_when_pending_below_threshold(self):
        """The next scheduled payment task retries Safe→master leftovers."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 30.0

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        # First balance read is the mech contract, second is the Safe.
        bridge.web3.eth.get_balance.side_effect = [0, int(33.92e18)]
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()
        bridge.wallet.send.assert_called_once()
        assert bridge.wallet.send.call_args.kwargs["amount_wei"] == int(33.92e18)
        notification.send.assert_awaited_once()
        assert "Safe Payment Swept" in notification.send.call_args.args[0]

    @pytest.mark.asyncio
    async def test_safe_only_sweep_failure_reports_safe_amount(self):
        """Safe-only transfer failures report the stranded Safe amount, not 0 drained."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 30.0

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        bridge.web3.eth.get_balance.side_effect = [0, int(33.42e18)]
        bridge.wallet.send.side_effect = RuntimeError("Safe tx failed")
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args.args[:2]
        assert title == "Safe Payment Sweep Failed"
        assert "33.420000 xDAI" in msg
        assert "Amount: 0.000000" not in msg

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
    async def test_drains_stranded_mech_balance_when_pending_below_threshold(self):
        """If payment is already on the mech, drain it even with low tracker pending."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 1.0

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridge.web3.eth.get_balance.return_value = int(31.44e18)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        calls = bridge.wallet.safe_service.execute_safe_transaction.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs["to"] == MECH
        bridge.wallet.send.assert_called_once()
        assert bridge.wallet.send.call_args[1]["amount_wei"] == int(31.44e18)

    @pytest.mark.asyncio
    async def test_drains_stranded_mech_balance_without_balance_tracker(self):
        """Already-stranded xDAI can be recovered even if tracker resolution fails."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 1.0

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=ZERO, mech_balance_raw=0)
        bridge.web3.eth.get_balance.return_value = int(31.44e18)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        calls = bridge.wallet.safe_service.execute_safe_transaction.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs["to"] == MECH
        bridge.wallet.send.assert_called_once()
        assert bridge.wallet.send.call_args[1]["amount_wei"] == int(31.44e18)

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
        """mech.exec uses the real on-chain balance, not a float reconstruction.

        The balance tracker reports bt_balance_raw. After marketplace fees the
        mech actually receives less (actual_mech_wei). The code must pass the
        get_balance() result to exec(), not a re-derived value from the tracker.
        """
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bt_balance_raw = int(41.79e18)  # tracker amount (above threshold)
        actual_mech_wei = int(41.78e18)  # mech received less (fees)
        assert bt_balance_raw != actual_mech_wei, "values must differ to prove divergence"

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=bt_balance_raw)
        # Override get_balance to return the actual mech amount (after fees)
        bridge.web3.eth.get_balance.return_value = actual_mech_wei
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # exec must use the actual on-chain balance, not bt_balance_raw
        mech_contract = bridge.web3.eth.contract(address=MECH)
        exec_call = mech_contract.functions.exec.call_args
        amount_wei_passed = exec_call.args[1]
        assert amount_wei_passed == actual_mech_wei, (
            f"Expected actual wei {actual_mech_wei}, got {amount_wei_passed} "
            f"(bt had {bt_balance_raw})"
        )

    @pytest.mark.asyncio
    async def test_drains_existing_plus_newly_processed_mech_balance(self):
        """When pending and stranded balances coexist, drain the combined mech balance."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        existing_wei = int(0.2e18)
        combined_wei = int(0.7e18)
        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        bridge.web3.eth.get_balance.side_effect = [
            existing_wei,
            combined_wei,
            combined_wei,
        ]
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        mech_contract = bridge.web3.eth.contract(address=MECH)
        exec_call = mech_contract.functions.exec.call_args
        assert exec_call.args[1] == combined_wei
        assert bridge.wallet.send.call_args[1]["amount_wei"] == combined_wei

    @pytest.mark.asyncio
    async def test_pending_balance_read_failure_notifies_operator(self):
        """Pending read failures are not silently treated as zero."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch(
                "micromech.tasks.payment_withdraw.get_pending_balance",
                side_effect=RuntimeError("mapMechBalances timeout"),
            ),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": MULTISIG},
            ),
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: read pending balance" in msg
        assert "mapMechBalances timeout" in msg
        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_balance_tracker_resolution_failure_notifies_operator(self):
        """Tracker lookup failures are not silently treated as no tracker."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.tasks.payment_withdraw.get_balance_tracker_address",
            side_effect=RuntimeError("tracker RPC timeout"),
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: resolve balance tracker" in msg
        assert "tracker RPC timeout" in msg
        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_tracker_failure_still_sweeps_stranded_safe_balance(self):
        """Safe-only recovery is not blocked by tracker RPC failures."""
        chain_cfg = _make_chain_config()

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        # First read: mech has no xDAI. Second read: Safe has stranded xDAI.
        bridge.web3.eth.get_balance.side_effect = [0, int(33.42e18)]

        with (
            patch(
                "micromech.tasks.payment_withdraw.get_balance_tracker_address",
                side_effect=RuntimeError("tracker RPC timeout"),
            ),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": MULTISIG},
            ),
            patch("micromech.tasks.payment_withdraw.time.sleep"),
        ):
            result = await execute_payment_withdraw(
                bridge,
                "gnosis",
                chain_cfg,
                threshold_xdai=1.0,
                safe_reserve_xdai=0.0,
            )

        assert result.status == "swept_safe"
        assert result.transferred_to_master_wei == int(33.42e18)
        bridge.wallet.safe_service.execute_safe_transaction.assert_not_called()
        bridge.wallet.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_pending_read_failure_still_drains_existing_mech_balance(self):
        """If xDAI is already on the mech, recover it even when pending read fails."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 1.0

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=0)
        bridge.web3.eth.get_balance.return_value = int(31.44e18)
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with (
            patch(
                "micromech.tasks.payment_withdraw.get_pending_balance",
                side_effect=RuntimeError("mapMechBalances timeout"),
            ),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": MULTISIG},
            ),
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        calls = bridge.wallet.safe_service.execute_safe_transaction.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs["to"] == MECH
        assert bridge.wallet.send.call_args[1]["amount_wei"] == int(31.44e18)
        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "WARNING: pending balance read failed" in msg
        assert "mapMechBalances timeout" in msg

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

        bridge = _make_bridge(bt_address=ZERO, mech_balance_raw=0)
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

        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: read existing mech balance" in msg
        assert "network error" in msg

    @pytest.mark.asyncio
    async def test_drain_failure_propagates_to_outer_except(self):
        """If _drain_mech_to_safe raises, operator is notified."""
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

        # Outer except caught the error: notify operator, no master transfer.
        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: mech.exec drain" in msg
        assert "Mech contract balance: 0.500000 xDAI" in msg
        bridge.wallet.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_withdraw_step_failure_propagates_to_outer_except(self):
        """If _withdraw (step 1) raises, operator is notified."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        # processPaymentByMultisig call raises — simulates on-chain revert
        bridge.wallet.safe_service.execute_safe_transaction.side_effect = RuntimeError(
            "processPayment failed"
        )
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # Outer except caught the error: notify operator, no drain, no master transfer.
        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: processPaymentByMultisig" in msg
        assert "processPayment failed" in msg
        bridge.wallet.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_balance_failure_propagates_to_outer_except(self):
        """If get_balance raises between step 1 and 2, outer except catches it."""
        cfg = _make_config()
        cfg.payment_withdraw_threshold_xdai = 0.01

        chain_cfg = _make_chain_config()
        cfg.chains = {"gnosis": chain_cfg}

        bridge = _make_bridge(bt_address=BT_ADDR, mech_balance_raw=int(0.5e18))
        # First get_balance sees no stranded funds; second read after
        # processPaymentByMultisig fails before mech.exec.
        bridge.web3.eth.get_balance.side_effect = [
            int(0.0),
            RuntimeError("RPC timeout"),
        ]
        bridges = {"gnosis": bridge}
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            await payment_withdraw_task(bridges, notification, cfg)

        # Outer except caught the error: notify operator, drain and master transfer never ran.
        notification.send.assert_awaited_once()
        title, msg = notification.send.call_args[0][:2]
        assert title == "Mech Payment Withdraw Failed"
        assert "Stage: read mech balance" in msg
        assert "RPC timeout" in msg
        bridge.wallet.send.assert_not_called()
        # _withdraw succeeded (1 call), drain never ran (only 1 total)
        assert bridge.wallet.safe_service.execute_safe_transaction.call_count == 1

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
