"""Tests for runtime/delivery.py — covering missed lines.

Covers:
- _wait_and_check_receipt: revert path (43-44)
- _get_mech_contract: no mech_address (98-99)
- deliver_batch: no wallet (119-126), delivery exception (158-168)
- _deliver_one: no result (186-190), IPFS unavailable (221-228), offchain (231)
- _submit_delivery: no multisig (258), non-hex request_id (265-269)
- _submit_offchain_delivery: body (284-319)
- _submit_tx: safe path (329)
- _via_safe: body (334-346)
- _via_impersonation: gas fallback (357)
- run loop: stop + delivery count (379-380)

SAFETY RULES applied here:
- Never modify type() of MagicMock globally — use inner classes or spec= instead
- Never use asyncio.gather with a mock sleep — use wait_for + real asyncio.sleep(0)
- Never patch asyncio.sleep with AsyncMock — use a real async wrapper around sleep(0)
"""

import asyncio as _real_asyncio  # save real reference before any patching

_real_sleep = _real_asyncio.sleep  # save the function itself — patch replaces module attr, not this ref
from unittest.mock import MagicMock, patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.models import MechRequest, RequestRecord, ToolResult
from micromech.runtime.delivery import DeliveryManager, _wait_and_check_receipt


def _make_config():
    return MicromechConfig()


def _make_chain_config(**kw):
    defaults = dict(
        chain="gnosis",
        marketplace_address="0x" + "a" * 40,
        factory_address="0x" + "b" * 40,
        staking_address="0x" + "c" * 40,
        mech_address="0x" + "d" * 40,
    )
    defaults.update(kw)
    return ChainConfig(**defaults)


def _make_record(request_id="0x" + "1" * 64, is_offchain=False, has_result=True):
    req = MagicMock(spec=MechRequest)
    req.request_id = request_id
    req.chain = "gnosis"
    req.tool = "echo"
    req.prompt = "test prompt"
    req.is_offchain = is_offchain
    req.data = None
    req.sender = None
    req.signature = None
    result = ToolResult(output='{"result": "ok"}', execution_time=0.1) if has_result else None
    return RequestRecord(request=req, result=result)


def _make_queue():
    q = MagicMock()
    q.get_undelivered.return_value = []
    q.mark_delivered = MagicMock()
    q.mark_failed = MagicMock()
    return q


def _make_bridge(has_safe=False):
    bridge = MagicMock()
    bridge.web3 = MagicMock()
    bridge.web3.to_checksum_address = lambda x: x
    bridge.wallet = MagicMock()
    bridge.wallet.key_storage = MagicMock()  # present by default
    if not has_safe:
        # Remove safe_service so _has_safe returns False
        del bridge.wallet.safe_service
    return bridge


# Safe sleep replacement: yields to event loop but returns instantly.
# Uses _real_sleep (the function reference saved before any patching).
# IMPORTANT: do NOT use _real_asyncio.sleep here — patching replaces the
# module attribute, so _real_asyncio.sleep would point to _instant_sleep
# itself after the patch runs, causing infinite recursion.
async def _instant_sleep(*_args, **_kwargs):
    await _real_sleep(0)


# ---------------------------------------------------------------------------
# _wait_and_check_receipt — revert path
# ---------------------------------------------------------------------------

class TestWaitAndCheckReceipt:
    def test_reverted_tx_raises(self):
        mock_web3 = MagicMock()
        mock_tx = MagicMock()
        mock_tx.hex.return_value = "0xabc"
        mock_web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
        with pytest.raises(RuntimeError, match="reverted"):
            _wait_and_check_receipt(mock_web3, mock_tx, "Delivery")


# ---------------------------------------------------------------------------
# _get_mech_contract — no mech_address
# ---------------------------------------------------------------------------

class TestGetMechContract:
    def test_raises_when_no_mech_address(self):
        chain_cfg = _make_chain_config(mech_address=None)
        dm = DeliveryManager(_make_config(), chain_cfg, _make_queue(), _make_bridge())
        with pytest.raises(ValueError, match="mech_address"):
            dm._get_mech_contract()


# ---------------------------------------------------------------------------
# deliver_batch — no wallet (uses inner class, not type() hack)
# ---------------------------------------------------------------------------

class TestDeliverBatchNoWallet:
    @pytest.mark.asyncio
    async def test_no_wallet_skips_delivery_and_warns(self):
        """deliver_batch returns 0 and logs a warning when wallet.key_storage raises."""

        class _LockedWallet:
            """Wallet stub whose key_storage raises — simulates locked wallet."""
            @property
            def key_storage(self):
                raise Exception("wallet locked — no password")

        bridge = MagicMock()
        bridge.web3 = MagicMock()
        bridge.wallet = _LockedWallet()

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge)
        count = await dm.deliver_batch()
        assert count == 0
        # Calling again should NOT log again (warning_logged flag)
        count2 = await dm.deliver_batch()
        assert count2 == 0

    @pytest.mark.asyncio
    async def test_delivery_exception_marks_failed(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()
        q.get_undelivered.return_value = [record]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        with patch.object(dm, "_deliver_one", side_effect=Exception("tx reverted")):
            count = await dm.deliver_batch()

        assert count == 0
        q.mark_failed.assert_called_once()


# ---------------------------------------------------------------------------
# _deliver_one — no result
# ---------------------------------------------------------------------------

class TestDeliverOneNoResult:
    @pytest.mark.asyncio
    async def test_no_result_returns_none_none(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        tx, ipfs = await dm._deliver_one(_make_record(has_result=False))
        assert tx is None
        assert ipfs is None


# ---------------------------------------------------------------------------
# _deliver_one — IPFS unavailable → raw delivery_data
# ---------------------------------------------------------------------------

class TestDeliverOneIpfsUnavailable:
    @pytest.mark.asyncio
    async def test_ipfs_failure_falls_back_to_raw(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        with patch("micromech.ipfs.client.push_to_ipfs", side_effect=Exception("ipfs down")), \
             patch.object(dm, "_submit_delivery", return_value="0xtx") as mock_submit:
            tx, ipfs = await dm._deliver_one(_make_record())

        assert tx == "0xtx"
        assert ipfs is None
        mock_submit.assert_called_once()


# ---------------------------------------------------------------------------
# _deliver_one — offchain delivery path
# ---------------------------------------------------------------------------

class TestDeliverOneOffchain:
    @pytest.mark.asyncio
    async def test_offchain_request_uses_offchain_delivery(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        record = _make_record(request_id="http-abc123", is_offchain=True)
        with patch("micromech.ipfs.client.push_to_ipfs", side_effect=Exception("offline")), \
             patch.object(dm, "_submit_offchain_delivery", return_value="0xtx") as mock_off:
            tx, ipfs = await dm._deliver_one(record)

        assert tx == "0xtx"
        mock_off.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_delivery — no multisig / non-hex request_id
# ---------------------------------------------------------------------------

class TestSubmitDelivery:
    def test_no_multisig_raises(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        with patch("micromech.core.bridge.get_service_info", return_value={}), \
             patch.object(dm, "_get_mech_contract", return_value=MagicMock()):
            with pytest.raises(ValueError, match="multisig"):
                dm._submit_delivery("0x" + "1" * 64, b"data")

    def test_non_hex_request_id_uses_sha256(self):
        mock_contract = MagicMock()
        mock_contract.functions.deliverToMarketplace.return_value = MagicMock()

        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        with patch("micromech.core.bridge.get_service_info",
                   return_value={"multisig_address": "0xmultisig"}), \
             patch.object(dm, "_get_mech_contract", return_value=mock_contract), \
             patch.object(dm, "_submit_tx", return_value="0xtx") as mock_tx:
            result = dm._submit_delivery("http-not-hex-id", b"data")

        assert result == "0xtx"
        mock_tx.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_offchain_delivery
# ---------------------------------------------------------------------------

class TestSubmitOffchainDelivery:
    def test_no_multisig_raises(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        with patch("micromech.core.bridge.get_service_info", return_value={}), \
             patch.object(dm, "_get_mech_contract", return_value=MagicMock()):
            with pytest.raises(ValueError, match="multisig"):
                dm._submit_offchain_delivery(_make_record(is_offchain=True), b"data")

    def test_offchain_delivery_calls_submit_tx(self):
        mock_contract = MagicMock()
        mock_contract.functions.deliverMarketplaceWithSignatures.return_value = MagicMock()

        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        with patch("micromech.core.bridge.get_service_info",
                   return_value={"multisig_address": "0xmulti"}), \
             patch.object(dm, "_get_mech_contract", return_value=mock_contract), \
             patch.object(dm, "_submit_tx", return_value="0xtx") as mock_tx:
            result = dm._submit_offchain_delivery(_make_record(is_offchain=True), b"data")

        assert result == "0xtx"
        mock_tx.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_tx — routes to safe or impersonation
# ---------------------------------------------------------------------------

class TestSubmitTx:
    def test_uses_safe_when_bridge_has_safe_service(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge(has_safe=True)
        )
        with patch.object(dm, "_via_safe", return_value="0xtx") as mock_safe:
            result = dm._submit_tx(MagicMock(), "0xfrom")
        assert result == "0xtx"
        mock_safe.assert_called_once()

    def test_uses_impersonation_without_safe_service(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge(has_safe=False)
        )
        with patch.object(dm, "_via_impersonation", return_value="0xtx") as mock_imp:
            result = dm._submit_tx(MagicMock(), "0xfrom")
        assert result == "0xtx"
        mock_imp.assert_called_once()


# ---------------------------------------------------------------------------
# _via_safe
# ---------------------------------------------------------------------------

class TestViaSafe:
    def test_via_safe_submits_and_returns_hash(self):
        bridge = _make_bridge(has_safe=True)
        bridge.wallet.safe_service = MagicMock()
        bridge.wallet.safe_service.execute_safe_transaction.return_value = "0xsafetx"
        bridge.web3.to_checksum_address = lambda x: x

        fn_call = MagicMock()
        fn_call.build_transaction.return_value = {"data": b"calldata"}

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge)
        result = dm._via_safe(fn_call, "0xmultisig")
        assert result == "0xsafetx"


# ---------------------------------------------------------------------------
# _via_impersonation — gas fallback
# ---------------------------------------------------------------------------

class TestViaImpersonation:
    def test_gas_estimation_failure_uses_fallback(self):
        bridge = _make_bridge(has_safe=False)
        ci = MagicMock()
        ci.estimate_gas.side_effect = Exception("no gas")
        bridge.wallet.chain_interfaces.get.return_value = ci

        fn_call = MagicMock()
        mock_tx = MagicMock()
        mock_tx.hex.return_value = "0ximptx"
        fn_call.transact.return_value = mock_tx

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge)
        with patch("micromech.runtime.delivery._wait_and_check_receipt", return_value="0ximptx"):
            result = dm._via_impersonation(fn_call, "0xfrom")

        assert result == "0ximptx"


# ---------------------------------------------------------------------------
# run loop — safe asyncio patterns
#
# RULES:
# - Never use asyncio.gather with a mocked sleep (doesn't yield → spin freeze)
# - Always use asyncio.wait_for with a real timeout as safety net
# - Patch delivery.asyncio.sleep with _instant_sleep (real sleep(0) wrapper)
#   so the loop actually suspends and the while condition is re-evaluated
# ---------------------------------------------------------------------------

class TestRunLoop:
    @pytest.mark.asyncio
    async def test_run_exits_after_stop_called(self):
        """run() exits cleanly when stop() is called inside deliver_batch."""
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), bridge=None
        )

        async def _one_shot():
            dm.stop()
            return 0

        with patch.object(dm, "deliver_batch", side_effect=_one_shot), \
             patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep):
            # wait_for is a safety net; the loop should exit on its own
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

    @pytest.mark.asyncio
    async def test_run_logs_delivery_count(self):
        """run() debug-logs when deliver_batch returns a non-zero count."""
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        call_count = [0]

        async def _one_delivery():
            call_count[0] += 1
            dm.stop()
            return 3  # Non-zero → triggers logger.debug inside run()

        with patch.object(dm, "deliver_batch", side_effect=_one_delivery), \
             patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep):
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_run_handles_deliver_batch_exception(self):
        """run() catches exceptions from deliver_batch and keeps looping."""
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), _make_bridge()
        )
        call_count = [0]

        async def _raise_then_stop():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("batch error")
            dm.stop()
            return 0

        with patch.object(dm, "deliver_batch", side_effect=_raise_then_stop), \
             patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep):
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

        assert call_count[0] == 2  # ran twice: first raised, then stopped
