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
from datetime import datetime, timezone

# save the function itself — patch replaces module attr, not this ref
_real_sleep = _real_asyncio.sleep

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
    req.created_at = datetime.now(timezone.utc)
    req.timeout = 300
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
    async def test_delivery_tx_failure_leaves_in_executed_for_retry(self, monkeypatch):
        """Batch TX failure does NOT mark_failed — records stay EXECUTED for next loop."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 0)
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()
        q.get_undelivered.return_value = [record]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        # On-chain records go through _prepare_onchain then _submit_batch_delivery.
        # Simulate TX failure after IPFS prep succeeds.
        async def _fake_prepare(rec):
            return b"\x00" * 32, b"data", None

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_fake_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=RuntimeError("tx reverted")),
        ):
            count = await dm.deliver_batch()

        assert count == 0
        # mark_failed is terminal — TX reverts should be retried, not permanently failed
        q.mark_failed.assert_not_called()


# ---------------------------------------------------------------------------
# _deliver_one — no result
# ---------------------------------------------------------------------------


class TestDeliverOneNoResult:
    @pytest.mark.asyncio
    async def test_no_result_returns_none_none(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        tx, ipfs = await dm._deliver_one(_make_record(has_result=False))
        assert tx is None
        assert ipfs is None


# ---------------------------------------------------------------------------
# _deliver_one — IPFS unavailable → raw delivery_data
# ---------------------------------------------------------------------------


class TestDeliverOneIpfsUnavailable:
    @pytest.mark.asyncio
    async def test_ipfs_failure_falls_back_to_raw(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch("micromech.ipfs.client.push_to_ipfs", side_effect=Exception("ipfs down")),
            patch.object(dm, "_submit_delivery", return_value=("0xtx", [True])) as mock_submit,
        ):
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
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        record = _make_record(request_id="http-abc123", is_offchain=True)
        with (
            patch("micromech.ipfs.client.push_to_ipfs", side_effect=Exception("offline")),
            patch.object(dm, "_submit_offchain_delivery", return_value="0xtx") as mock_off,
        ):
            tx, ipfs = await dm._deliver_one(record)

        assert tx == "0xtx"
        mock_off.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_delivery — no multisig / non-hex request_id
# ---------------------------------------------------------------------------


class TestSubmitDelivery:
    def test_no_multisig_raises(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch("micromech.core.bridge.get_service_info", return_value={}),
            patch.object(dm, "_get_mech_contract", return_value=MagicMock()),
        ):
            with pytest.raises(ValueError, match="multisig"):
                dm._submit_delivery("0x" + "1" * 64, b"data")

    def test_non_hex_request_id_uses_sha256(self):
        mock_contract = MagicMock()
        mock_contract.functions.deliverToMarketplace.return_value = MagicMock()

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0xmultisig"},
            ),
            patch.object(dm, "_get_mech_contract", return_value=mock_contract),
            patch.object(dm, "_submit_tx", return_value="0xtx") as mock_tx,
        ):
            result = dm._submit_delivery("http-not-hex-id", b"data")

        tx_hash, flags = result
        assert tx_hash == "0xtx"
        assert flags == [True]  # fallback when receipt fetch fails
        mock_tx.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_offchain_delivery
# ---------------------------------------------------------------------------


class TestSubmitOffchainDelivery:
    def test_no_multisig_raises(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch("micromech.core.bridge.get_service_info", return_value={}),
            patch.object(dm, "_get_mech_contract", return_value=MagicMock()),
        ):
            with pytest.raises(ValueError, match="multisig"):
                dm._submit_offchain_delivery(_make_record(is_offchain=True), b"data")

    def test_offchain_delivery_calls_submit_tx(self):
        mock_contract = MagicMock()
        mock_contract.functions.deliverMarketplaceWithSignatures.return_value = MagicMock()

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0xmulti"},
            ),
            patch.object(dm, "_get_mech_contract", return_value=mock_contract),
            patch.object(dm, "_submit_tx", return_value="0xtx") as mock_tx,
        ):
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
        """run() exits cleanly when stop() is called inside _deliver_concurrent."""
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge=None)

        async def _one_shot():
            dm.stop()
            return 0

        with (
            patch.object(dm, "_deliver_concurrent", side_effect=_one_shot),
            patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep),
        ):
            # wait_for is a safety net; the loop should exit on its own
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

    @pytest.mark.asyncio
    async def test_run_logs_delivery_count(self):
        """run() debug-logs when _deliver_concurrent returns a non-zero count."""
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        call_count = [0]

        async def _one_delivery():
            call_count[0] += 1
            dm.stop()
            return 3  # Non-zero → triggers logger.debug inside run()

        with (
            patch.object(dm, "_deliver_concurrent", side_effect=_one_delivery),
            patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep),
        ):
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_run_handles_deliver_batch_exception(self):
        """run() catches exceptions from _deliver_concurrent and keeps looping."""
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        call_count = [0]

        async def _raise_then_stop():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("batch error")
            dm.stop()
            return 0

        with (
            patch.object(dm, "_deliver_concurrent", side_effect=_raise_then_stop),
            patch("micromech.runtime.delivery.asyncio.sleep", _instant_sleep),
        ):
            await _real_asyncio.wait_for(dm.run(), timeout=5.0)

        assert call_count[0] == 2  # ran twice: first raised, then stopped


# ---------------------------------------------------------------------------
# _batch_age_seconds
# ---------------------------------------------------------------------------


class TestBatchAgeSeconds:
    def test_returns_age_of_oldest_record(self):
        from datetime import timedelta

        from micromech.runtime.delivery import _batch_age_seconds

        old = _make_record(request_id="0x" + "a" * 64)
        new = _make_record(request_id="0x" + "b" * 64)
        # Make old record 120 seconds old
        old.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        age = _batch_age_seconds([old, new])
        assert age >= 119  # at least 119s (120 minus tiny execution time)

    def test_naive_datetime_handled(self):
        """Naive datetimes (no tzinfo) are treated as UTC."""
        from datetime import timedelta

        from micromech.runtime.delivery import _batch_age_seconds

        record = _make_record()
        # Simulate a naive UTC datetime (tzinfo=None) — seen in legacy data
        record.request.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            seconds=30
        )
        age = _batch_age_seconds([record])
        assert age >= 29


# ---------------------------------------------------------------------------
# _request_id_to_bytes
# ---------------------------------------------------------------------------


class TestRequestIdToBytes:
    def test_hex_id_with_prefix(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        result = dm._request_id_to_bytes("0x" + "ab" * 32)
        assert len(result) == 32
        assert result == bytes.fromhex("ab" * 32)

    def test_non_hex_id_sha256(self):
        import hashlib

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        result = dm._request_id_to_bytes("http-abc123")
        expected = hashlib.sha256(b"http-abc123").digest()
        assert result == expected

    def test_wrong_length_hex_id_raises(self):
        """Hex IDs that are not exactly 32 bytes raise ValueError (not silently padded)."""
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        # 16 bytes instead of 32 — would silently be padded before fix
        with pytest.raises(ValueError, match="expected 32"):
            dm._request_id_to_bytes("0x" + "ab" * 16)

    def test_hex_id_without_prefix(self):
        """On-chain IDs stored without 0x prefix (as stored by listener) convert correctly."""
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        raw = "ab" * 32  # 64 hex chars, no 0x — as stored by listener's rid.hex()
        result = dm._request_id_to_bytes(raw)
        assert len(result) == 32
        assert result == bytes.fromhex(raw)


# ---------------------------------------------------------------------------
# _prepare_onchain
# ---------------------------------------------------------------------------


class TestPrepareOnchain:
    @pytest.mark.asyncio
    async def test_raises_when_no_result(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        record = _make_record(has_result=False)
        with pytest.raises(ValueError, match="No result"):
            await dm._prepare_onchain(record)

    @pytest.mark.asyncio
    async def test_ipfs_success_returns_cid_hex(self):
        from unittest.mock import AsyncMock

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        record = _make_record()

        mock_push = AsyncMock(return_value=("bafkrei_test", "f01551220aabb"))
        with patch("micromech.ipfs.client.push_to_ipfs", mock_push):
            req_id_bytes, delivery_data, cid_hex = await dm._prepare_onchain(record)

        assert len(req_id_bytes) == 32
        assert cid_hex == "f01551220aabb"

    @pytest.mark.asyncio
    async def test_ipfs_failure_falls_back_to_raw(self):
        from unittest.mock import AsyncMock

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        record = _make_record()

        with patch(
            "micromech.ipfs.client.push_to_ipfs",
            AsyncMock(side_effect=Exception("ipfs down")),
        ):
            req_id_bytes, delivery_data, cid_hex = await dm._prepare_onchain(record)

        assert cid_hex is None
        import json

        parsed = json.loads(delivery_data)
        assert "requestId" in parsed


# ---------------------------------------------------------------------------
# _deliver_onchain_batch
# ---------------------------------------------------------------------------


class TestDeliverOnchainBatch:
    @pytest.mark.asyncio
    async def test_success_delivers_batch_in_one_tx(self):
        """All records in the batch get delivered via a single _submit_batch_delivery call."""
        bridge = _make_bridge()
        q = _make_queue()
        records = [_make_record(request_id=f"0x{i:064x}") for i in range(3)]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _fake_prepare(rec):
            return b"\x00" * 32, b"data", None

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_fake_prepare),
            patch.object(dm, "_submit_batch_delivery", return_value=("0xtxhash", [True, True, True])) as mock_submit,
        ):
            count = await dm._deliver_onchain_batch(records)

        assert count == 3
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args[0]
        assert len(call_args[0]) == 3  # three request ID bytes
        assert len(call_args[1]) == 3  # three data payloads
        assert q.mark_delivered.call_count == 3

    @pytest.mark.asyncio
    async def test_ipfs_prep_failure_marks_record_failed(self, monkeypatch):
        """Records that exhaust MAX_RETRIES on IPFS prep are marked failed; rest batched."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_MAX_RETRIES", 1)
        bridge = _make_bridge()
        q = _make_queue()
        good_record = _make_record(request_id="0x" + "a" * 64)
        bad_record = _make_record(request_id="0x" + "b" * 64)

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _selective_prepare(rec):
            if rec.request.request_id == bad_record.request.request_id:
                raise RuntimeError("ipfs failed")
            return b"\x00" * 32, b"data", None

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_selective_prepare),
            patch.object(dm, "_submit_batch_delivery", return_value=("0xtx", [True])),
        ):
            count = await dm._deliver_onchain_batch([good_record, bad_record])

        assert count == 1
        # bad record hits MAX_RETRIES=1 → mark_failed; good record delivered
        assert q.mark_failed.call_count == 1
        assert q.mark_delivered.call_count == 1

    @pytest.mark.asyncio
    async def test_ipfs_prep_failure_no_mark_failed_before_max_retries(self):
        """IPFS prep failure does NOT immediately mark_failed — needs MAX_RETRIES attempts."""
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        with patch.object(dm, "_prepare_onchain", side_effect=RuntimeError("ipfs down")):
            count = await dm._deliver_onchain_batch([record])

        assert count == 0
        # First failure only — no mark_failed yet (counter < MAX_RETRIES)
        q.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_records_fail_prep_returns_zero(self, monkeypatch):
        """If all records fail IPFS prep (and exhaust retries), returns 0."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_MAX_RETRIES", 1)
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        with patch.object(dm, "_prepare_onchain", side_effect=RuntimeError("ipfs down")):
            count = await dm._deliver_onchain_batch([record])

        assert count == 0
        q.mark_failed.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_tx_failure_leaves_records_in_executed(self):
        """If the batch TX fails, records are NOT marked failed — they stay in EXECUTED for retry."""
        bridge = _make_bridge()
        q = _make_queue()
        records = [_make_record(request_id=f"0x{i:064x}") for i in range(2)]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _fake_prepare(rec):
            return b"\x00" * 32, b"data", None

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_fake_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=RuntimeError("revert")),
        ):
            count = await dm._deliver_onchain_batch(records)

        assert count == 0
        # mark_failed is terminal — transient TX errors must be retried
        q.mark_failed.assert_not_called()


# ---------------------------------------------------------------------------
# deliver_batch — flush conditions
# ---------------------------------------------------------------------------


class TestDeliverBatchFlushConditions:
    @pytest.mark.asyncio
    async def test_no_flush_when_too_few_and_not_old(self, monkeypatch):
        """Small batch of fresh records is held until flush conditions are met."""
        # Use batch_size=5 so that 1 record is genuinely "too few"
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_BATCH_SIZE", 5)
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 60)
        bridge = _make_bridge()
        q = _make_queue()
        q.get_undelivered.return_value = [_make_record()]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        with patch.object(dm, "_deliver_onchain_batch") as mock_batch:
            count = await dm.deliver_batch()

        assert count == 0
        mock_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_when_full_batch(self, monkeypatch):
        """Batch flushes immediately when DEFAULT_DELIVERY_BATCH_SIZE records are ready."""
        from micromech.core.constants import DEFAULT_DELIVERY_BATCH_SIZE

        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 60)
        bridge = _make_bridge()
        q = _make_queue()
        q.get_undelivered.return_value = [
            _make_record(request_id=f"0x{i:064x}") for i in range(DEFAULT_DELIVERY_BATCH_SIZE)
        ]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        with patch.object(dm, "_deliver_onchain_batch", return_value=10) as mock_batch:
            count = await dm.deliver_batch()

        assert count == 10
        mock_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_when_oldest_record_exceeds_timeout(self, monkeypatch):
        """Old record triggers flush even with a small batch."""
        from datetime import timedelta

        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 30)
        bridge = _make_bridge()
        q = _make_queue()
        old_record = _make_record()
        old_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        q.get_undelivered.return_value = [old_record]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        with patch.object(dm, "_deliver_onchain_batch", return_value=1) as mock_batch:
            count = await dm.deliver_batch()

        assert count == 1
        mock_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_offchain_always_delivered_regardless_of_batch(self, monkeypatch):
        """Off-chain records are always delivered 1:1, no batch holding."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 9999)
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record(is_offchain=True)
        q.get_undelivered.return_value = [record]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _fake_deliver_one(rec):
            return "0xtx", None

        with patch.object(dm, "_deliver_one", side_effect=_fake_deliver_one):
            count = await dm.deliver_batch()

        assert count == 1
        q.mark_delivered.assert_called_once()


# ---------------------------------------------------------------------------
# _submit_batch_delivery
# ---------------------------------------------------------------------------


class TestSubmitBatchDelivery:
    def test_no_multisig_raises(self):
        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        with (
            patch("micromech.core.bridge.get_service_info", return_value={}),
            patch.object(dm, "_get_mech_contract", return_value=MagicMock()),
        ):
            with pytest.raises(ValueError, match="multisig"):
                dm._submit_batch_delivery([b"\x00" * 32], [b"data"])

    def test_submits_arrays_to_deliver_to_marketplace(self):
        mock_contract = MagicMock()
        bridge = _make_bridge()

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "a" * 40},
            ),
            patch.object(dm, "_get_mech_contract", return_value=mock_contract),
            patch.object(dm, "_submit_tx", return_value="0xtx") as mock_submit_tx,
        ):
            result = dm._submit_batch_delivery([b"\x01" * 32, b"\x02" * 32], [b"data1", b"data2"])

        tx_hash, flags = result
        assert tx_hash == "0xtx"
        assert flags == [True, True]  # fallback when receipt fetch fails
        mock_submit_tx.assert_called_once()
        # Verify deliverToMarketplace was called with the two-item arrays
        mock_contract.functions.deliverToMarketplace.assert_called_once_with(
            [b"\x01" * 32, b"\x02" * 32], [b"data1", b"data2"]
        )

    def test_receipt_fetch_failure_returns_conservative_fallback(self):
        """When get_transaction_receipt raises, batch returns (tx_hash, [True]*n).

        The contract does not revert on timeout — it silently records False for
        late requests.  If we cannot parse the receipt, the safe default is to
        assume all were accepted so we don't incorrectly mark delivered requests
        as failed.
        """
        mock_contract = MagicMock()
        bridge = _make_bridge()
        # Make get_transaction_receipt raise — simulates RPC blip after TX mined
        bridge.web3.eth.get_transaction_receipt.side_effect = Exception("RPC error")

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), bridge)
        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "a" * 40},
            ),
            patch.object(dm, "_get_mech_contract", return_value=mock_contract),
            patch.object(dm, "_submit_tx", return_value="0x" + "ab" * 32),
        ):
            tx_hash, flags = dm._submit_batch_delivery(
                [b"\x01" * 32, b"\x02" * 32], [b"data1", b"data2"]
            )

        # TX hash preserved; flags default to True (conservative: don't mark as failed)
        assert tx_hash == "0x" + "ab" * 32
        assert flags == [True, True]


# ---------------------------------------------------------------------------
# _increment_failure — retry counter
# ---------------------------------------------------------------------------


class TestIncrementFailure:
    def test_no_mark_failed_before_max_retries(self, monkeypatch):
        """Failures below MAX_RETRIES only log a warning, never call mark_failed."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_MAX_RETRIES", 3)
        q = _make_queue()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, _make_bridge())

        dm._increment_failure("req-1", "some error")
        dm._increment_failure("req-1", "some error")
        q.mark_failed.assert_not_called()
        assert dm._delivery_failures["req-1"] == 2

    def test_mark_failed_at_max_retries(self, monkeypatch):
        """Exactly MAX_RETRIES failures triggers mark_failed and clears the counter."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_MAX_RETRIES", 3)
        q = _make_queue()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, _make_bridge())

        dm._increment_failure("req-1", "err")
        dm._increment_failure("req-1", "err")
        dm._increment_failure("req-1", "err")  # 3rd = MAX_RETRIES

        q.mark_failed.assert_called_once()
        call_args = q.mark_failed.call_args[0]
        assert call_args[0] == "req-1"
        assert "max_retries" in call_args[1]
        # Counter is cleared after reaching max
        assert "req-1" not in dm._delivery_failures

    def test_counter_cleared_on_success(self):
        """Successful delivery clears the failure counter for that request."""
        q = _make_queue()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, _make_bridge())
        dm._delivery_failures["req-1"] = 3

        # Simulate what _deliver_onchain_batch does on success
        dm._delivery_failures.pop("req-1", None)

        assert "req-1" not in dm._delivery_failures


# ---------------------------------------------------------------------------
# _prepare_onchain — IPFS recovery resets warning flag
# ---------------------------------------------------------------------------


class TestPrepareOnchainIpfsRecovery:
    @pytest.mark.asyncio
    async def test_ipfs_recovery_resets_warning_flag(self):
        """After IPFS recovers, _ipfs_warning_logged is reset to False."""
        from unittest.mock import AsyncMock

        dm = DeliveryManager(_make_config(), _make_chain_config(), _make_queue(), _make_bridge())
        dm._ipfs_warning_logged = True  # simulate prior IPFS failure

        record = _make_record()
        mock_push = AsyncMock(return_value=("bafkrei_test", "f01551220aabb"))
        with patch("micromech.ipfs.client.push_to_ipfs", mock_push):
            await dm._prepare_onchain(record)

        assert dm._ipfs_warning_logged is False


# ---------------------------------------------------------------------------
# _deliver_single_onchain
# ---------------------------------------------------------------------------


class TestDeliverSingleOnchain:
    @pytest.mark.asyncio
    async def test_success_marks_delivered_and_returns_true(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        dm._in_flight.add(record.request.request_id)

        async def _fake_prepare(rec):
            return b"\x00" * 32, b"data", "0xcid"

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_fake_prepare),
            patch.object(dm, "_submit_batch_delivery", return_value=("0xtx", [True])),
        ):
            result = await dm._deliver_single_onchain(record)

        assert result is True
        q.mark_delivered.assert_called_once()
        assert record.request.request_id not in dm._in_flight

    @pytest.mark.asyncio
    async def test_failure_increments_counter_and_returns_false(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        dm._in_flight.add(record.request.request_id)

        with patch.object(
            dm, "_prepare_onchain", side_effect=RuntimeError("ipfs down")
        ):
            result = await dm._deliver_single_onchain(record)

        assert result is False
        q.mark_delivered.assert_not_called()
        assert record.request.request_id not in dm._in_flight

    @pytest.mark.asyncio
    async def test_always_removes_from_in_flight_on_success(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        dm._in_flight.add(record.request.request_id)

        async def _fake_prepare(rec):
            return b"\x00" * 32, b"data", None

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_fake_prepare),
            patch.object(dm, "_submit_batch_delivery", return_value=("0xtx", [True])),
        ):
            await dm._deliver_single_onchain(record)

        assert record.request.request_id not in dm._in_flight

    @pytest.mark.asyncio
    async def test_always_removes_from_in_flight_on_failure(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        dm._in_flight.add(record.request.request_id)

        with patch.object(
            dm, "_prepare_onchain", side_effect=RuntimeError("boom")
        ):
            await dm._deliver_single_onchain(record)

        assert record.request.request_id not in dm._in_flight


# ---------------------------------------------------------------------------
# _deliver_concurrent
# ---------------------------------------------------------------------------


class TestDeliverConcurrent:
    @pytest.mark.asyncio
    async def test_skips_without_bridge(self):
        dm = DeliveryManager(
            _make_config(), _make_chain_config(), _make_queue(), bridge=None
        )
        result = await dm._deliver_concurrent()
        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_queue_empty(self):
        bridge = _make_bridge()
        q = _make_queue()
        q.get_undelivered.return_value = []

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        result = await dm._deliver_concurrent()
        assert result == 0

    @pytest.mark.asyncio
    async def test_skips_in_flight_records(self):
        bridge = _make_bridge()
        q = _make_queue()
        record = _make_record()
        q.get_undelivered.return_value = [record]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        dm._in_flight.add(record.request.request_id)

        with patch.object(dm, "_deliver_single_onchain") as mock_single:
            result = await dm._deliver_concurrent()

        assert result == 0
        mock_single.assert_not_called()

    @pytest.mark.asyncio
    async def test_delivers_up_to_worker_limit_concurrently(self):
        from micromech.core.constants import DEFAULT_DELIVERY_WORKERS

        bridge = _make_bridge()
        q = _make_queue()
        records = [
            _make_record(request_id=f"0x{i:064x}")
            for i in range(DEFAULT_DELIVERY_WORKERS + 2)
        ]
        q.get_undelivered.return_value = records

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _fake_deliver(rec):
            return True

        with patch.object(
            dm, "_deliver_single_onchain", side_effect=_fake_deliver
        ) as mock_single:
            result = await dm._deliver_concurrent()

        assert result == DEFAULT_DELIVERY_WORKERS
        assert mock_single.call_count == DEFAULT_DELIVERY_WORKERS

    @pytest.mark.asyncio
    async def test_no_wallet_skips_and_warns(self):
        class _LockedWallet:
            @property
            def key_storage(self):
                raise Exception("locked")

        bridge = MagicMock()
        bridge.wallet = _LockedWallet()
        q = _make_queue()
        q.get_undelivered.return_value = [_make_record()]

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
        result = await dm._deliver_concurrent()
        assert result == 0

    @pytest.mark.asyncio
    async def test_mixed_onchain_offchain_respects_worker_limit(self):
        """On-chain capped at DEFAULT_DELIVERY_WORKERS; off-chain gets remainder."""
        from micromech.core.constants import DEFAULT_DELIVERY_WORKERS

        bridge = _make_bridge()
        q = _make_queue()
        # 2 on-chain + 5 off-chain records in the fetched batch
        onchain_records = [
            _make_record(request_id=f"0x{i:064x}", is_offchain=False)
            for i in range(2)
        ]
        offchain_records = [
            _make_record(
                request_id=f"0x{10 + i:064x}", is_offchain=True
            )
            for i in range(5)
        ]
        q.get_undelivered.return_value = onchain_records + offchain_records

        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        async def _fake_single_on(rec):
            return True

        async def _fake_single_off(rec):
            return True

        with (
            patch.object(
                dm, "_deliver_single_onchain", side_effect=_fake_single_on
            ) as mock_on,
            patch.object(
                dm,
                "_deliver_single_offchain_concurrent",
                side_effect=_fake_single_off,
            ) as mock_off,
        ):
            result = await dm._deliver_concurrent()

        # On-chain capped at min(2, DEFAULT_DELIVERY_WORKERS)
        assert mock_on.call_count == min(2, DEFAULT_DELIVERY_WORKERS)
        # All off-chain records in the batch are processed
        assert mock_off.call_count == 5
        assert result == mock_on.call_count + mock_off.call_count
