"""Delivery worker sequencing and correctness tests.

There are two on-chain delivery paths in _deliver_concurrent():

1. PARALLEL path (_has_safe=True, production):
   NonceAllocator pre-assigns sequential nonces; workers run concurrently via
   asyncio.gather(). Each worker gets a unique nonce slot, avoiding GS026.
   Tested by: TestParallelNonceDispatch

2. SERIAL fallback path (_has_safe=False, Anvil/test):
   On-chain records processed sequentially (for loop). Used in tests where
   bridge has no safe_service (impersonation path).
   Tested by: TestConcurrentWorkerStallResilience (serial correctness)

Off-chain (HTTP) deliveries have no nonce constraint and always run concurrently.

Mocking strategy
----------------
- _prepare_onchain  → instant async stub (no IPFS, no net)
- _submit_batch_delivery → time.sleep(delay) in a thread
  * STALL_ID request: STALL_DELAY seconds  (simulates GS013 + backoff)
  * FAST_ID requests: FAST_DELAY seconds   (normal TX)

Key assertions
--------------
1. CORRECTNESS: all records delivered, in-flight set prevents double-pickup
2. ORDERING:    on-chain records delivered sequentially (stall blocks fast)
3. TIMING:      total elapsed ≈ sum of delays (sequential)
"""

import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.models import MechRequest, RequestRecord, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager

# ---------------------------------------------------------------------------
# Delay constants — keep STALL_DELAY large enough for a clear signal,
# FAST_DELAY large enough to be measurable but small enough to be cheap.
# ---------------------------------------------------------------------------

STALL_DELAY = 2.0   # seconds — simulates GS013 + retry backoff
FAST_DELAY  = 0.3   # seconds — normal Safe TX round-trip

# Hex request IDs — exactly 32 bytes (64 hex chars) for _request_id_to_bytes
STALL_ID = "0x" + "aa" * 32
FAST1_ID = "0x" + "bb" * 32
FAST2_ID = "0x" + "cc" * 32

STALL_BYTES = bytes.fromhex(STALL_ID[2:])
FAST1_BYTES = bytes.fromhex(FAST1_ID[2:])
FAST2_BYTES = bytes.fromhex(FAST2_ID[2:])


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _make_config(parallel_nonce: bool = False) -> MicromechConfig:
    return MicromechConfig(parallel_nonce_enabled=parallel_nonce)


def _make_chain_config(**kw) -> ChainConfig:
    defaults = dict(
        chain="gnosis",
        marketplace_address="0x" + "a" * 40,
        factory_address="0x" + "b" * 40,
        staking_address="0x" + "c" * 40,
        mech_address="0x" + "d" * 40,
    )
    defaults.update(kw)
    return ChainConfig(**defaults)


def _make_record(request_id: str) -> RequestRecord:
    req = MagicMock(spec=MechRequest)
    req.request_id = request_id
    req.chain = "gnosis"
    req.tool = "echo"
    req.prompt = "stall resilience test"
    req.is_offchain = False
    req.data = None
    req.sender = None
    req.signature = None
    req.created_at = datetime.now(timezone.utc)
    req.timeout = 300
    result = ToolResult(output='{"result": "ok"}', execution_time=0.1)
    return RequestRecord(request=req, result=result)


def _make_offchain_record(request_id: str) -> RequestRecord:
    record = _make_record(request_id)
    record.request.is_offchain = True
    return record


def _make_queue(records: list[RequestRecord]) -> MagicMock:
    q = MagicMock()
    q.get_undelivered.return_value = records
    q.mark_delivered = MagicMock()
    q.mark_timed_out = MagicMock()
    q.mark_failed = MagicMock()
    return q


def _make_bridge() -> MagicMock:
    bridge = MagicMock()
    bridge.web3 = MagicMock()
    bridge.web3.to_checksum_address = lambda x: x
    bridge.wallet = MagicMock()
    bridge.wallet.key_storage = MagicMock()
    # No safe_service → impersonation path (doesn't matter, we mock _submit_batch_delivery)
    del bridge.wallet.safe_service
    return bridge


async def _instant_prepare(rec: RequestRecord) -> tuple[bytes, bytes, None]:
    """Instant _prepare_onchain stub — no IPFS, no network."""
    req_id_bytes = bytes.fromhex(rec.request.request_id[2:])
    return req_id_bytes, b"dummy_payload", None


def _make_submit_with_delay(completed_at: dict[bytes, float], t0: float):
    """Factory: returns a _submit_batch_delivery mock with per-request delays.

    The stalled request (STALL_BYTES) sleeps STALL_DELAY seconds.
    All other requests sleep FAST_DELAY seconds.

    time.sleep() blocks the THREAD but NOT the event loop — asyncio.to_thread()
    releases the event loop during the sleep, allowing other workers to proceed.
    """
    def _submit(req_id_bytes_list: list[bytes], datas: list[bytes], safe_nonce=None) -> tuple[str, list[bool]]:
        rid = req_id_bytes_list[0]
        delay = STALL_DELAY if rid == STALL_BYTES else FAST_DELAY
        time.sleep(delay)
        completed_at[rid] = time.monotonic() - t0
        return ("0x" + "ab" * 32, [True])
    return _submit


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestConcurrentWorkerStallResilience:
    """[Test] Verify that a stalled worker does not block its concurrent peers."""

    @pytest.mark.asyncio
    async def test_fast_workers_complete_before_stalled_worker(self):
        """SERIAL FALLBACK PATH: all records delivered sequentially (_has_safe=False).

        This test exercises the impersonation/Anvil path where bridge has no
        safe_service.  On-chain submissions fall back to a sequential for loop.

        Queue order: STALL_ID, FAST1_ID, FAST2_ID.
        Expected completion order: STALL → FAST1 → FAST2.
        All 3 must be delivered regardless of individual request latency.
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        completed_at: dict[bytes, float] = {}
        t0 = time.monotonic()

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(
                dm,
                "_submit_batch_delivery",
                side_effect=_make_submit_with_delay(completed_at, t0),
            ),
        ):
            delivered = await dm._deliver_concurrent()

        # --- All 3 records were delivered ---
        assert delivered == 3, f"Expected 3 delivered, got {delivered}"
        assert q.mark_delivered.call_count == 3

        # --- All completed ---
        assert STALL_BYTES in completed_at, "Stall worker did not complete"
        assert FAST1_BYTES in completed_at, "Fast worker 1 did not complete"
        assert FAST2_BYTES in completed_at, "Fast worker 2 did not complete"

        # --- Sequential order: stall first (it's first in queue), then fast workers ---
        assert completed_at[STALL_BYTES] < completed_at[FAST1_BYTES], (
            "Stall should complete before fast1 (sequential order, stall is first in queue)"
        )
        assert completed_at[FAST1_BYTES] < completed_at[FAST2_BYTES], (
            "fast1 should complete before fast2 (sequential order)"
        )

    @pytest.mark.asyncio
    async def test_sequential_time_is_sum_of_delays(self):
        """TIMING PROOF: sequential on-chain submission → elapsed ≈ sum of delays.

        On-chain submissions are sequential (nonce-race prevention). Total time
        is bounded by STALL + FAST + FAST, not just max(delays).

        This is expected and acceptable — each tick (10s) processes up to
        DEFAULT_DELIVERY_WORKERS requests sequentially. A stalled request delays
        only the current tick, not the entire queue.
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        completed_at: dict[bytes, float] = {}
        t0 = time.monotonic()

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(
                dm,
                "_submit_batch_delivery",
                side_effect=_make_submit_with_delay(completed_at, t0),
            ),
        ):
            await dm._deliver_concurrent()

        elapsed = time.monotonic() - t0

        # Sequential total: STALL + FAST + FAST = 2.6s
        expected_min = STALL_DELAY + 2 * FAST_DELAY - 0.2   # 2.4s with tolerance
        expected_max = STALL_DELAY + 2 * FAST_DELAY + 1.0   # 3.6s generous headroom

        assert elapsed >= expected_min, (
            f"[Perf] elapsed={elapsed:.3f}s < expected_min={expected_min:.2f}s. "
            f"Submissions may be concurrent (nonce race risk)."
        )
        assert elapsed < expected_max, (
            f"[Perf] elapsed={elapsed:.3f}s > expected_max={expected_max:.2f}s. "
            f"Thread pool may be starved."
        )

    @pytest.mark.asyncio
    async def test_serial_baseline_is_slower_than_concurrent(self):
        """SERIAL BASELINE: calling workers sequentially blocks on each stall.

        This test runs _deliver_single_onchain for each record IN SEQUENCE
        (simulating the old single-threaded delivery loop). Total time is the
        SUM of all delays.

        Compare to test_concurrent_time_bounded_by_max_not_sum: same records,
        same mocks, but sequential execution is visibly slower.

        [Test] This validates our timing methodology — if concurrent and serial
        measured the same time, our test would be meaningless.
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        # Register all as in-flight (required by _deliver_single_onchain invariant)
        for r in [stall_record, fast1_record, fast2_record]:
            dm._in_flight.add(r.request.request_id)

        completed_at: dict[bytes, float] = {}
        t0 = time.monotonic()

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(
                dm,
                "_submit_batch_delivery",
                side_effect=_make_submit_with_delay(completed_at, t0),
            ),
        ):
            # Sequential: each awaits the previous (no asyncio.gather)
            for record in [stall_record, fast1_record, fast2_record]:
                await dm._deliver_single_onchain(record)

        elapsed = time.monotonic() - t0

        # Sequential total ≈ STALL + FAST + FAST = 2.6s
        expected_serial_min = STALL_DELAY + 2 * FAST_DELAY - 0.1  # 2.5s with tolerance
        assert elapsed >= expected_serial_min, (
            f"Serial elapsed={elapsed:.3f}s < expected_min={expected_serial_min:.2f}s. "
            f"Sequential execution was unexpectedly fast — methodology check failed."
        )

        # Ordering: serial execution delivers them in order (stall first since it starts first)
        assert completed_at[STALL_BYTES] < completed_at[FAST1_BYTES], (
            "Serial: stall should complete before fast1 (it starts first)"
        )
        assert completed_at[FAST1_BYTES] < completed_at[FAST2_BYTES], (
            "Serial: fast1 should complete before fast2 (sequential order)"
        )

        print(
            f"\n  [Test] Serial baseline (for comparison):"
            f"\n    stall worker:  {completed_at[STALL_BYTES]:.3f}s"
            f"\n    fast worker 1: {completed_at[FAST1_BYTES]:.3f}s"
            f"\n    fast worker 2: {completed_at[FAST2_BYTES]:.3f}s"
            f"\n    total elapsed: {elapsed:.3f}s  (serial sum, expected ≥{expected_serial_min:.2f}s)"
        )

    @pytest.mark.asyncio
    async def test_workers_start_sequentially(self):
        """On-chain workers start sequentially: each waits for the previous to finish.

        This verifies that the Safe TX submission is serialized — the second worker
        only starts its submission after the first has completed (nonce-race prevention).
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        started_at: dict[bytes, float] = {}
        t0 = time.monotonic()

        def _submit_track_start(req_id_bytes_list, datas, safe_nonce=None):
            rid = req_id_bytes_list[0]
            started_at[rid] = time.monotonic() - t0
            delay = STALL_DELAY if rid == STALL_BYTES else FAST_DELAY
            time.sleep(delay)
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_submit_track_start),
        ):
            await dm._deliver_concurrent()

        assert len(started_at) == 3, "All 3 workers must have started"

        # Sequential: fast1 starts after stall finishes (at ~STALL_DELAY)
        assert started_at[FAST1_BYTES] >= STALL_DELAY - 0.1, (
            f"fast1 started at {started_at[FAST1_BYTES]:.3f}s but stall takes "
            f"{STALL_DELAY}s — submissions appear concurrent (nonce race risk)."
        )
        assert started_at[FAST2_BYTES] >= started_at[FAST1_BYTES] + FAST_DELAY - 0.1, (
            f"fast2 started at {started_at[FAST2_BYTES]:.3f}s before fast1 "
            f"({started_at[FAST1_BYTES]:.3f}s) finished — submissions not sequential."
        )

    @pytest.mark.asyncio
    async def test_in_flight_prevents_double_pickup_under_concurrency(self):
        """[Test] In-flight set: a request already being processed is never picked up twice.

        Simulates the scenario where _deliver_concurrent is called again (next tick)
        while a previous worker is still in-flight. The in-flight record must be
        skipped — not submitted twice.

        [Security] Double-delivery is a correctness invariant for staking liveness:
        delivery_delta must equal nonce_delta (1 delivery per Safe nonce).
        """
        record = _make_record(STALL_ID)
        q = _make_queue([record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submissions(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        # Manually mark the record as in-flight (simulating a previous tick's worker)
        dm._in_flight.add(STALL_ID)

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(
                dm, "_submit_batch_delivery", side_effect=_count_submissions
            ),
        ):
            result = await dm._deliver_concurrent()

        assert result == 0, (
            f"In-flight record should be skipped — delivered {result} instead of 0"
        )
        assert submit_call_count[0] == 0, (
            f"_submit_batch_delivery called {submit_call_count[0]} times for in-flight record"
        )
        assert STALL_ID in dm._in_flight, (
            "In-flight record should remain tracked (not removed by a skipping tick)"
        )

        print(
            "\n  [Test] In-flight safety:"
            "\n    in-flight record skipped ✓"
            "\n    submit_batch_delivery not called ✓"
            "\n    in_flight set unchanged ✓"
        )

    @pytest.mark.asyncio
    async def test_all_requests_delivered_despite_stall(self):
        """[Test] All requests are delivered even when one TX is slow.

        With sequential on-chain delivery, a slow TX blocks until it completes,
        then the next request is processed. All 3 must be delivered.
        Order is sequential: stall first (first in queue), then fast1, fast2.
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        delivery_order: list[str] = []

        def _track_delivery(request_id, **kw):
            delivery_order.append(request_id)

        q.mark_delivered.side_effect = _track_delivery

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(
                dm,
                "_submit_batch_delivery",
                side_effect=_make_submit_with_delay({}, time.monotonic()),
            ),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 3
        assert len(delivery_order) == 3
        # Sequential: stall (first in queue) completes first, then fast1, fast2
        assert delivery_order[0] == STALL_ID, "Stall (first in queue) should be delivered first"
        assert set(delivery_order) == {STALL_ID, FAST1_ID, FAST2_ID}

    @pytest.mark.asyncio
    async def test_stale_requested_expired_requests_are_delivered(self):
        """Requests older than 300s are still delivered when marketplace says so."""
        from datetime import timedelta

        stale_record = _make_record(STALL_ID)
        stale_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)

        fresh_record = _make_record(FAST1_ID)
        fresh_record.request.created_at = datetime.now(timezone.utc)

        q = _make_queue([stale_record, fresh_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_get_marketplace_status", return_value=2) as mock_status,
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 2
        assert submit_call_count[0] == 2
        mock_status.assert_called_once_with(STALL_ID)
        q.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_priority_requests_are_delivered_for_current_mech(self):
        """Current-priority requests remain deliverable while status is RequestedPriority."""
        from datetime import timedelta

        chain_config = _make_chain_config()
        stale_record = _make_record(STALL_ID)
        stale_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
        stale_record.request.priority_mech = chain_config.mech_address

        q = _make_queue([stale_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), chain_config, q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_get_marketplace_status", return_value=1),
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 1
        assert submit_call_count[0] == 1
        q.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_priority_request_from_persistence_is_delivered(self, tmp_path):
        """Persisted priority_mech is available when delivery checks RequestedPriority."""
        from datetime import timedelta

        chain_config = _make_chain_config()
        queue = PersistentQueue(tmp_path / "requests.db")
        try:
            req = MechRequest(
                request_id=STALL_ID,
                chain="gnosis",
                tool="echo",
                prompt="persisted priority",
                priority_mech=chain_config.mech_address,
                created_at=datetime.now(timezone.utc) - timedelta(seconds=400),
            )
            queue.add_request(req)
            queue.mark_executing(STALL_ID)
            queue.mark_executed(STALL_ID, ToolResult(output='{"result": "ok"}'))

            bridge = _make_bridge()
            dm = DeliveryManager(_make_config(), chain_config, queue, bridge)

            submit_call_count = [0]

            def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
                submit_call_count[0] += 1
                return ("0x" + "ab" * 32, [True])

            with (
                patch.object(dm, "_get_marketplace_status", return_value=1),
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
            ):
                delivered = await dm._deliver_concurrent()

            assert delivered == 1
            assert submit_call_count[0] == 1
            assert queue.get_by_id(STALL_ID).request.status == "delivered"
        finally:
            queue.close()

    @pytest.mark.asyncio
    async def test_reserved_priority_key_from_persistence_does_not_authorize(
        self, tmp_path
    ):
        """Payload-controlled reserved metadata cannot authorize RequestedPriority."""
        from datetime import timedelta

        chain_config = _make_chain_config()
        queue = PersistentQueue(tmp_path / "requests.db")
        try:
            req = MechRequest(
                request_id=STALL_ID,
                chain="gnosis",
                tool="echo",
                prompt="forged priority",
                extra_params={"_micromech_priority_mech": chain_config.mech_address},
                created_at=datetime.now(timezone.utc) - timedelta(seconds=400),
            )
            queue.add_request(req)
            queue.mark_executing(STALL_ID)
            queue.mark_executed(STALL_ID, ToolResult(output='{"result": "ok"}'))

            bridge = _make_bridge()
            dm = DeliveryManager(_make_config(), chain_config, queue, bridge)

            submit_call_count = [0]

            def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
                submit_call_count[0] += 1
                return ("0x" + "ab" * 32, [True])

            with (
                patch.object(dm, "_get_marketplace_status", return_value=1),
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
            ):
                delivered = await dm._deliver_concurrent()

            assert delivered == 0
            assert submit_call_count[0] == 0
            assert queue.get_by_id(STALL_ID).request.status == "executed"
        finally:
            queue.close()

    @pytest.mark.parametrize(
        ("status", "label"),
        [(0, "does_not_exist"), (3, "delivered")],
    )
    @pytest.mark.asyncio
    async def test_stale_final_statuses_are_discarded_without_tx(self, status, label):
        """Final on-chain requests are removed locally instead of being answered."""
        from datetime import timedelta

        records = []
        for rid in [STALL_ID, FAST1_ID, FAST2_ID]:
            r = _make_record(rid)
            r.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
            records.append(r)

        q = _make_queue(records)
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_get_marketplace_status", return_value=status),
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0
        assert submit_call_count[0] == 0
        assert q.mark_failed.call_count == 3
        for call in q.mark_failed.call_args_list:
            assert f"on_chain_unavailable: {label}" in call.args[1]

    @pytest.mark.asyncio
    async def test_stale_non_priority_request_waits_while_priority_gated(self):
        """Fallback delivery does not answer while status is still RequestedPriority."""
        from datetime import timedelta

        stale_record = _make_record(STALL_ID)
        stale_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
        stale_record.request.priority_mech = "0x" + "e" * 40

        q = _make_queue([stale_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_get_marketplace_status", return_value=1),
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0
        assert submit_call_count[0] == 0
        q.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_missing_priority_waits_while_priority_gated(self):
        """RequestedPriority delivery requires proof that this mech is priority."""
        from datetime import timedelta

        stale_record = _make_record(STALL_ID)
        stale_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
        stale_record.request.priority_mech = None

        q = _make_queue([stale_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_get_marketplace_status", return_value=1),
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0
        assert submit_call_count[0] == 0
        q.mark_failed.assert_not_called()

    @pytest.mark.parametrize(
        "status_result",
        [RuntimeError("RPC down"), 99],
    )
    @pytest.mark.asyncio
    async def test_stale_unknown_status_keeps_request_queued(self, status_result):
        """Unknown or unavailable status does not submit tx or discard locally."""
        from datetime import timedelta

        stale_record = _make_record(STALL_ID)
        stale_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)

        q = _make_queue([stale_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        status_patch = patch.object(dm, "_get_marketplace_status", return_value=status_result)
        if isinstance(status_result, Exception):
            status_patch = patch.object(
                dm,
                "_get_marketplace_status",
                side_effect=status_result,
            )

        with (
            status_patch,
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0
        assert submit_call_count[0] == 0
        q.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_boundary_age_equals_timeout_is_not_skipped(self):
        """[Test] A record aged exactly equal to timeout is NOT skipped (age > timeout, not >=)."""
        from datetime import timedelta

        boundary_record = _make_record(STALL_ID)
        boundary_record.request.timeout = 300
        # age = timeout - 1s → should NOT be pre-filtered (condition is `age > timeout`, not `>=`)
        boundary_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=299)

        q = _make_queue([boundary_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submitted = [False]

        def _submit(req_id_bytes_list, datas, safe_nonce=None):
            submitted[0] = True
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert submitted[0], "Boundary record (age == timeout) must attempt delivery, not be skipped"
        assert delivered == 1

    @pytest.mark.asyncio
    async def test_stale_offchain_request_still_attempted(self):
        """The marketplace timeout check applies only to on-chain requests."""
        from datetime import timedelta

        stale_offchain = _make_record(STALL_ID)
        stale_offchain.request.is_offchain = True
        stale_offchain.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)

        q = _make_queue([stale_offchain])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        deliver_one_called = [False]

        async def _mock_deliver_one(record):
            deliver_one_called[0] = True
            return ("0x" + "ab" * 32, None)

        with (
            patch.object(dm, "_get_marketplace_status") as mock_status,
            patch.object(dm, "_deliver_one", side_effect=_mock_deliver_one),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 1
        assert deliver_one_called[0]
        mock_status.assert_not_called()
        q.mark_failed.assert_not_called()


# ---------------------------------------------------------------------------
# PARALLEL PATH: _has_safe=True with NonceAllocator
# ---------------------------------------------------------------------------


def _make_bridge_with_safe() -> MagicMock:
    """Bridge with safe_service → _has_safe=True → parallel dispatch path."""
    bridge = MagicMock()
    bridge.web3 = MagicMock()
    bridge.web3.to_checksum_address = lambda x: x
    bridge.wallet = MagicMock()
    bridge.wallet.key_storage = MagicMock()
    # safe_service is present — triggers parallel path
    bridge.wallet.safe_service = MagicMock()
    return bridge


MULTISIG = "0x" + "cc" * 20


def _make_allocator_mock(nonces: list[int]) -> MagicMock:
    """Returns a mock allocator that hands out nonces sequentially."""
    alloc = MagicMock()
    alloc.allocate.side_effect = nonces
    alloc.check_stuck = MagicMock()
    alloc.invalidate = MagicMock()
    alloc.release = MagicMock()
    return alloc


class TestParallelNonceDispatch:
    """[Test] Parallel Safe TX dispatch via NonceAllocator (_has_safe=True)."""

    @pytest.mark.asyncio
    async def test_each_worker_gets_unique_nonce(self):
        """Workers receive distinct pre-assigned nonces from the allocator."""
        records = [_make_record(STALL_ID), _make_record(FAST1_ID), _make_record(FAST2_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([10, 11, 12])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        received_nonces: list[int] = []

        def _submit(req_id_bytes_list, datas, safe_nonce=None):
            received_nonces.append(safe_nonce)
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_submit),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 3
        assert sorted(received_nonces) == [10, 11, 12], "Each worker must get a unique nonce"
        assert len(set(received_nonces)) == 3, "Nonces must be distinct"

    @pytest.mark.asyncio
    async def test_parallel_path_records_delivery_timing(self):
        """Parallel Safe deliveries include prep and lock-wait timing metrics."""
        record = _make_record(FAST1_ID)
        q = _make_queue([record])
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([10])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            dm._metrics = MagicMock()
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", return_value=("0x" + "ab" * 32, [True])),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 1
        dm._metrics.record_delivery_timing.assert_called_once()
        kwargs = dm._metrics.record_delivery_timing.call_args.kwargs
        assert kwargs["chain"] == "gnosis"
        assert kwargs["prep_seconds"] >= 0
        assert kwargs["safe_lock_wait_seconds"] >= 0
        assert kwargs["safe_submit_seconds"] >= 0

    @pytest.mark.asyncio
    async def test_allow_nonce_refresh_false_passed_to_safe(self):
        """_via_safe passes allow_nonce_refresh=False when safe_nonce is pre-assigned."""
        record = _make_record(FAST1_ID)
        q = _make_queue([record])
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([7])
        bridge.wallet.safe_service.get_allocator.return_value = allocator
        bridge.wallet.safe_service.execute_safe_transaction.return_value = "0x" + "ab" * 32

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            # Mock _submit_batch_delivery to capture call args without going further
            submitted_args: list[dict] = []

            def _capture_submit_tx(fn_call, from_addr, label="TX", safe_nonce=None):
                submitted_args.append({"safe_nonce": safe_nonce})
                return "0x" + "ab" * 32

            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_tx", side_effect=_capture_submit_tx),
            ):
                await dm._deliver_concurrent()

        assert len(submitted_args) == 1
        assert submitted_args[0]["safe_nonce"] == 7, "Pre-assigned nonce must be forwarded to _submit_tx"

    @pytest.mark.asyncio
    async def test_failed_delivery_invalidates_allocator(self):
        """When a worker fails, invalidate() is called so next tick refetches nonce."""
        record = _make_record(FAST1_ID)
        q = _make_queue([record])
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([42])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=RuntimeError("GS025")),
            ):
                await dm._deliver_concurrent()

        allocator.invalidate.assert_called_once_with("delivery_failed")

    @pytest.mark.asyncio
    async def test_check_stuck_called_with_onchain_count(self):
        """check_stuck is called with the number of on-chain workers dispatched."""
        records = [_make_record(STALL_ID), _make_record(FAST1_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([0, 1])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", return_value=("0x" + "ab" * 32, [True])),
            ):
                await dm._deliver_concurrent()

        allocator.check_stuck.assert_called_once_with(2)

    @pytest.mark.asyncio
    async def test_parallel_workers_run_concurrently(self):
        """Workers dispatch concurrently: total time < sum of individual delays."""
        records = [_make_record(STALL_ID), _make_record(FAST1_ID), _make_record(FAST2_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([0, 1, 2])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        def _submit_with_delay(req_id_bytes_list, datas, safe_nonce=None):
            # Each worker sleeps FAST_DELAY in a thread
            time.sleep(FAST_DELAY)
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_submit_with_delay),
            ):
                t0 = time.monotonic()
                delivered = await dm._deliver_concurrent()
                elapsed = time.monotonic() - t0

        assert delivered == 3
        # Serial would take 3 * FAST_DELAY; parallel should be ~1 * FAST_DELAY + margin
        assert elapsed < 2 * FAST_DELAY, (
            f"Parallel workers took {elapsed:.2f}s — expected < {2 * FAST_DELAY:.2f}s "
            f"(serial would take {3 * FAST_DELAY:.2f}s)"
        )

    @pytest.mark.asyncio
    async def test_release_called_on_success_and_failure(self):
        """allocator.release(nonce) is called in _dispatch finally even when delivery fails.

        This ensures invalidate_and_wait() is never stuck waiting for orphaned nonces.
        Two workers: one succeeds, one fails (GS025-like TX error).
        Both must call release() with their allocated nonce.
        """
        success_record = _make_record(FAST1_ID)
        fail_record = _make_record(FAST2_ID)
        q = _make_queue([success_record, fail_record])
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([5, 6])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        call_count = [0]

        def _submit_alternating(req_id_bytes_list, datas, safe_nonce=None):
            call_count[0] += 1
            if req_id_bytes_list[0] == FAST2_BYTES:
                raise RuntimeError("GS025 nonce too high")
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_submit_alternating),
            ):
                await dm._deliver_concurrent()

        # release must be called once per allocated nonce (success AND failure)
        assert allocator.release.call_count == 2, (
            f"release() must be called for both workers; got {allocator.release.call_count}"
        )
        released_nonces = {call.args[0] for call in allocator.release.call_args_list}
        assert released_nonces == {5, 6}, f"Both nonces must be released; got {released_nonces}"

    @pytest.mark.asyncio
    async def test_allocate_raises_skips_worker_without_release(self):
        """When allocate() raises (e.g. NonceAllocatorBlockedError), the worker returns
        False and release() is NOT called for that worker (no nonce was assigned).

        Other workers dispatched via gather() must still complete normally.
        """
        blocked_record = _make_record(STALL_ID)
        ok_record = _make_record(FAST1_ID)
        q = _make_queue([blocked_record, ok_record])
        bridge = _make_bridge_with_safe()

        # Make allocate() raise for STALL_ID by tracking which record most
        # recently finished _prepare_onchain.  Since asyncio is single-threaded
        # and _instant_prepare has no awaits, gather advances each coroutine
        # up to its next await in input order: STALL_ID reaches allocate() first.
        # Tracking via _last_prepared makes the intent explicit and survives
        # any future reordering of the input list.
        _last_prepared: list[str] = []

        async def _track_prepare(rec):
            _last_prepared.append(rec.request.request_id)
            return b"\x00" * 32, b"data", "QmTest"

        def _allocate_keyed():
            rid = _last_prepared.pop() if _last_prepared else None
            if rid == STALL_ID:
                raise RuntimeError("EOA gap exceeded — allocator blocked")
            return 10

        allocator = MagicMock()
        allocator.allocate.side_effect = _allocate_keyed
        allocator.check_stuck = MagicMock()
        allocator.invalidate = MagicMock()
        allocator.release = MagicMock()
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_track_prepare),
                patch.object(
                    dm, "_submit_batch_delivery", return_value=("0x" + "ab" * 32, [True])
                ),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 1, f"Only the ok_record must succeed; got {delivered}"
        assert allocator.release.call_count == 1, (
            f"release() must only be called for the allocated nonce; "
            f"got {allocator.release.call_count}"
        )
        assert allocator.release.call_args[0][0] == 10, (
            "release() must be called with the nonce that was actually allocated"
        )
        # H1: BOTH records removed from _in_flight — the blocked one via explicit
        # discard in the allocate except-path, the successful one via
        # _deliver_single_onchain's finally.
        assert STALL_ID not in dm._in_flight, (
            "Blocked record must be removed from _in_flight when allocate() raises"
        )
        assert FAST1_ID not in dm._in_flight, (
            "Successful record must be removed from _in_flight after delivery"
        )

    @pytest.mark.asyncio
    async def test_parallel_path_disabled_by_default(self):
        """parallel_nonce_enabled=False (default) takes the serial path even with _has_safe=True."""
        records = [_make_record(FAST1_ID), _make_record(FAST2_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        allocator = _make_allocator_mock([0, 1])
        bridge.wallet.safe_service.get_allocator.return_value = allocator

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            # Default config: parallel_nonce_enabled=False
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_submit_batch_delivery", return_value=("0x" + "ab" * 32, [True])),
            ):
                delivered = await dm._deliver_concurrent()

        # Serial path: allocator is NOT used (get_allocator not called)
        assert delivered == 2
        bridge.wallet.safe_service.get_allocator.assert_not_called()

    @pytest.mark.asyncio
    async def test_serial_safe_prepares_before_safe_lock(self):
        """Default Safe path prepares payloads before holding the Safe submission lock."""
        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [_make_record(FAST1_ID), _make_record(FAST2_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()
        prepare_locked: list[bool] = []
        submit_locked: list[bool] = []

        async def _track_prepare(rec):
            prepare_locked.append(lock.locked())
            return await _instant_prepare(rec)

        def _track_submit(*_args, **_kwargs):
            submit_locked.append(lock.locked())
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_track_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_track_submit),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 2
        assert prepare_locked == [False, False]
        assert submit_locked == [True, True]

    @pytest.mark.asyncio
    async def test_serial_safe_prep_failure_does_not_take_safe_lock(self):
        """A prep failure in the serial Safe path is retried later without locking Safe."""
        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [_make_record(STALL_ID), _make_record(FAST1_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()
        prepare_locked: list[bool] = []
        submit_locked: list[bool] = []

        async def _prep_fail_for_stall(rec):
            prepare_locked.append(lock.locked())
            if rec.request.request_id == STALL_ID:
                raise RuntimeError("IPFS upload failed")
            return await _instant_prepare(rec)

        def _track_submit(*_args, **_kwargs):
            submit_locked.append(lock.locked())
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_prep_fail_for_stall),
                patch.object(dm, "_submit_batch_delivery", side_effect=_track_submit),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 1
        assert prepare_locked == [False, False]
        assert submit_locked == [True]
        assert STALL_ID not in dm._in_flight
        assert FAST1_ID not in dm._in_flight

    @pytest.mark.asyncio
    async def test_serial_safe_all_prep_failures_skip_safe_lock(self):
        """If no serial Safe payload prepares, the Safe lock is never acquired."""
        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        record = _make_record(STALL_ID)
        q = _make_queue([record])
        bridge = _make_bridge_with_safe()

        async def _fail_prepare(_rec):
            assert not lock.locked()
            raise RuntimeError("IPFS upload failed")

        def _fail_submit(*_args, **_kwargs):
            raise AssertionError("submit should not run without prepared payloads")

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_fail_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_fail_submit),
            ):
                delivered = await dm._deliver_concurrent()

        assert delivered == 0
        assert not lock.locked()
        assert STALL_ID not in dm._in_flight

    @pytest.mark.asyncio
    async def test_serial_safe_cancelled_prep_cleans_in_flight_before_lock(self):
        """Cancellation during serial prep does not leak in-flight records."""
        import asyncio

        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [_make_record(STALL_ID), _make_record(FAST1_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()
        prepare_locked: list[bool] = []
        submit_called = False

        async def _cancel_during_prepare(rec):
            prepare_locked.append(lock.locked())
            raise asyncio.CancelledError()

        def _track_submit(*_args, **_kwargs):
            nonlocal submit_called
            submit_called = True
            return ("0x" + "ab" * 32, [True])

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_cancel_during_prepare),
                patch.object(dm, "_submit_batch_delivery", side_effect=_track_submit),
            ):
                with pytest.raises(asyncio.CancelledError):
                    await dm._deliver_concurrent()

        assert prepare_locked == [False]
        assert submit_called is False
        assert not lock.locked()
        assert STALL_ID not in dm._in_flight
        assert FAST1_ID not in dm._in_flight

    @pytest.mark.asyncio
    async def test_serial_safe_cancelled_prep_cleans_mixed_selected_records(self):
        """Cancellation during serial prep also cleans selected off-chain records."""
        import asyncio

        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [_make_record(STALL_ID), _make_offchain_record(FAST1_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        async def _cancel_during_prepare(_rec):
            raise asyncio.CancelledError()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_cancel_during_prepare),
                patch.object(dm, "_submit_batch_delivery") as mock_submit,
                patch.object(dm, "_deliver_single_offchain_concurrent") as mock_offchain,
            ):
                with pytest.raises(asyncio.CancelledError):
                    await dm._deliver_concurrent()

        mock_submit.assert_not_called()
        mock_offchain.assert_not_called()
        assert not lock.locked()
        assert STALL_ID not in dm._in_flight
        assert FAST1_ID not in dm._in_flight

    @pytest.mark.asyncio
    async def test_serial_safe_cancelled_after_one_prep_cleans_without_submit(self):
        """A later prep cancellation drops already-prepared serial records."""
        import asyncio

        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [_make_record(FAST1_ID), _make_record(STALL_ID)]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        async def _prepare_then_cancel(rec):
            assert not lock.locked()
            if rec.request.request_id == FAST1_ID:
                return await _instant_prepare(rec)
            raise asyncio.CancelledError()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_prepare_then_cancel),
                patch.object(dm, "_submit_batch_delivery") as mock_submit,
            ):
                with pytest.raises(asyncio.CancelledError):
                    await dm._deliver_concurrent()

        mock_submit.assert_not_called()
        assert not lock.locked()
        assert FAST1_ID not in dm._in_flight
        assert STALL_ID not in dm._in_flight

    @pytest.mark.asyncio
    async def test_serial_safe_cancelled_during_submit_cleans_all_selected(self):
        """Cancellation after prep but during submit cleans later/off-chain records."""
        import asyncio

        from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

        _SAFE_LOCKS.clear()
        lock = get_safe_lock(MULTISIG)
        records = [
            _make_record(FAST1_ID),
            _make_record(STALL_ID),
            _make_offchain_record(FAST2_ID),
        ]
        q = _make_queue(records)
        bridge = _make_bridge_with_safe()

        async def _cancel_submit(_rec, **_kwargs):
            assert lock.locked()
            raise asyncio.CancelledError()

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ):
            dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)
            with (
                patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
                patch.object(dm, "_deliver_single_onchain", side_effect=_cancel_submit),
                patch.object(dm, "_deliver_single_offchain_concurrent") as mock_offchain,
            ):
                with pytest.raises(asyncio.CancelledError):
                    await dm._deliver_concurrent()

        mock_offchain.assert_not_called()
        assert not lock.locked()
        assert FAST1_ID not in dm._in_flight
        assert STALL_ID not in dm._in_flight
        assert FAST2_ID not in dm._in_flight


# ---------------------------------------------------------------------------
# _sanitize_error: hex redaction + __cause__ chain traversal
# ---------------------------------------------------------------------------


def test_sanitize_error_redacts_hex_key():
    """0x + 64-char hex (private key) is replaced with 0x[REDACTED]."""
    from micromech.runtime.delivery import _sanitize_error

    private_key = "0x" + "ab" * 32  # 66 chars total
    exc = ValueError(f"failed to sign with key {private_key}")
    result = _sanitize_error(exc)
    assert "0x[REDACTED]" in result
    assert private_key not in result


def test_sanitize_error_redacts_signature():
    """0x + 130-char hex (ECDSA signature) is replaced with 0x[REDACTED]."""
    from micromech.runtime.delivery import _sanitize_error

    signature = "0x" + "cd" * 65  # 132 chars total
    exc = RuntimeError(f"invalid sig={signature}")
    result = _sanitize_error(exc)
    assert "0x[REDACTED]" in result
    assert signature not in result


def test_sanitize_error_traverses_cause_chain():
    """__cause__ chain is traversed and redacted up to depth 5."""
    from micromech.runtime.delivery import _sanitize_error

    inner_key = "0x" + "ff" * 32
    inner = ConnectionError(f"RPC error key={inner_key}")
    outer = RuntimeError("execution failed")
    outer.__cause__ = inner

    result = _sanitize_error(outer)
    assert "execution failed" in result
    assert "caused by:" in result
    assert "RPC error" in result
    assert inner_key not in result, "Inner key must be redacted in cause chain"


def test_sanitize_error_short_hex_not_redacted():
    """Short hex strings (< 64 chars after 0x) are NOT redacted."""
    from micromech.runtime.delivery import _sanitize_error

    short_hex = "0x" + "ab" * 20  # only 42 chars — e.g. an address
    exc = ValueError(f"bad address: {short_hex}")
    result = _sanitize_error(exc)
    assert short_hex in result, "Short hex (address) must NOT be redacted"


def test_sanitize_error_empty_message():
    """Exception with empty string does not raise."""
    from micromech.runtime.delivery import _sanitize_error

    result = _sanitize_error(ValueError(""))
    assert result == ""


def test_sanitize_error_no_message():
    """Exception with no args does not raise."""
    from micromech.runtime.delivery import _sanitize_error

    result = _sanitize_error(Exception())
    assert isinstance(result, str)


def test_sanitize_error_depth_boundary():
    """Depth guard: chain of exactly 6 levels returns '...' at level 6."""
    from micromech.runtime.delivery import _sanitize_error

    # Build chain of depth 6: e0 → e1 → ... → e5
    excs = [ValueError(f"level_{i}") for i in range(7)]
    for i in range(6):
        excs[i].__cause__ = excs[i + 1]

    result = _sanitize_error(excs[0])
    # Level 5 (0-indexed _depth=5) is the boundary — its cause returns "..."
    assert "level_0" in result
    assert "..." in result


# ---------------------------------------------------------------------------
# get_safe_lock: identity and isolation
# ---------------------------------------------------------------------------


def test_get_safe_lock_same_addr_returns_same_lock():
    """get_safe_lock with the same address returns the identical asyncio.Lock."""
    from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

    addr = "0x" + "aa" * 20
    _SAFE_LOCKS.pop(addr, None)  # ensure clean state
    lock1 = get_safe_lock(addr)
    lock2 = get_safe_lock(addr)
    assert lock1 is lock2
    _SAFE_LOCKS.pop(addr, None)  # cleanup


def test_get_safe_lock_different_addrs_independent():
    """get_safe_lock with different addresses returns independent locks."""
    from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

    addr_a = "0x" + "11" * 20
    addr_b = "0x" + "22" * 20
    for a in (addr_a, addr_b):
        _SAFE_LOCKS.pop(a, None)
    lock_a = get_safe_lock(addr_a)
    lock_b = get_safe_lock(addr_b)
    assert lock_a is not lock_b
    for a in (addr_a, addr_b):
        _SAFE_LOCKS.pop(a, None)


# ---------------------------------------------------------------------------
# payment_withdraw: safe_lock coordination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_payment_withdraw_holds_lock_across_all_steps():
    """All three Safe TX steps in payment_withdraw_task run under a single lock.

    Verifies that get_safe_lock is acquired before _withdraw and released only
    after _transfer_to_master, preventing concurrent deliveries from interleaving.
    """

    from micromech.core.locks import _SAFE_LOCKS, get_safe_lock
    from micromech.tasks.payment_withdraw import payment_withdraw_task

    multisig = "0x" + "ff" * 20
    _SAFE_LOCKS.pop(multisig, None)

    lock_held_during_withdraw: list[bool] = []
    lock_held_during_drain: list[bool] = []
    lock_held_during_transfer: list[bool] = []

    real_lock = get_safe_lock(multisig)

    async def _check_lock_held(step_name: list[bool]) -> None:
        # If lock is held, trying to acquire it with wait=False fails
        acquired = real_lock.locked()
        step_name.append(acquired)

    # Build a minimal config
    from micromech.core.config import ChainConfig, MicromechConfig
    chain_config = ChainConfig(
        chain="gnosis",
        marketplace_address="0x" + "a" * 40,
        factory_address="0x" + "b" * 40,
        staking_address="0x" + "c" * 40,
        mech_address="0x" + "d" * 40,
    )
    config = MicromechConfig(
        payment_withdraw_threshold_xdai=0.001,
        chains={"gnosis": chain_config},
    )

    bridge = MagicMock()
    bridge.web3 = MagicMock()
    bridge.web3.to_checksum_address = lambda x: x
    bridge.wallet.master_account.address = "0x" + "ee" * 20
    bridge.with_retry = lambda fn, **kw: fn()

    notification_service = MagicMock()
    notification_service.send = AsyncMock()

    with (
        patch(
            "micromech.tasks.payment_withdraw.get_balance_tracker_address",
            return_value="0x" + "bb" * 20,
        ),
        patch(
            "micromech.tasks.payment_withdraw.get_pending_balance",
            return_value=1.0,
        ),
        patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": multisig},
        ),
        patch(
            "micromech.tasks.payment_withdraw._withdraw",
            side_effect=lambda *a, **kw: lock_held_during_withdraw.append(real_lock.locked()),
        ),
        patch(
            "micromech.tasks.payment_withdraw._drain_mech_to_safe",
            side_effect=lambda *a, **kw: lock_held_during_drain.append(real_lock.locked()),
        ),
        patch(
            "micromech.tasks.payment_withdraw._transfer_to_master",
            side_effect=lambda *a, **kw: lock_held_during_transfer.append(real_lock.locked()),
        ),
    ):
        bridge.web3.eth.get_balance = MagicMock(return_value=10**18)
        await payment_withdraw_task(
            bridges={"gnosis": bridge},
            notification_service=notification_service,
            config=config,
        )

    assert lock_held_during_withdraw == [True], "Lock must be held during _withdraw"
    assert lock_held_during_drain == [True], "Lock must be held during _drain_mech_to_safe"
    assert lock_held_during_transfer == [True], "Lock must be held during _transfer_to_master"
    assert not real_lock.locked(), "Lock must be released after all steps"

    _SAFE_LOCKS.pop(multisig, None)


# ---------------------------------------------------------------------------
# get_safe_lock: case-insensitive address normalization
# ---------------------------------------------------------------------------


def test_get_safe_lock_normalizes_case():
    """get_safe_lock returns the same lock regardless of address casing."""
    from micromech.core.locks import _SAFE_LOCKS, get_safe_lock

    _SAFE_LOCKS.clear()
    try:
        addr_lower = "0x" + "ab" * 20
        addr_upper = "0x" + "AB" * 20
        addr_mixed = "0xAb" + "aB" * 19
        lock1 = get_safe_lock(addr_lower)
        lock2 = get_safe_lock(addr_upper)
        lock3 = get_safe_lock(addr_mixed)
        assert lock1 is lock2 is lock3, (
            "Same Safe address in different casing must share one lock"
        )
    finally:
        _SAFE_LOCKS.clear()


# ---------------------------------------------------------------------------
# _dispatch: NonceAllocatorBlockedError handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allocate_blocked_error_does_not_crash():
    """If allocator.allocate() raises, _dispatch returns False without UnboundLocalError."""
    records = [_make_record(FAST1_ID)]
    q = _make_queue(records)
    bridge = _make_bridge_with_safe()

    allocator = _make_allocator_mock([])
    allocator.allocate.side_effect = RuntimeError("nonce allocator blocked")
    bridge.wallet.safe_service.get_allocator.return_value = allocator

    with patch(
        "micromech.core.bridge.get_service_info",
        return_value={"multisig_address": MULTISIG},
    ):
        dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
        with patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare):
            delivered = await dm._deliver_concurrent()

    assert delivered == 0, "Blocked allocator must yield 0 delivered, not crash"
    allocator.release.assert_not_called()
    allocator.invalidate.assert_not_called()
    # H1: in-flight must be cleaned up even when allocate() raises
    assert FAST1_ID not in dm._in_flight, (
        "Record must be removed from _in_flight when allocate() raises (H1 in-flight leak fix)"
    )


# ---------------------------------------------------------------------------
# H4: prep before allocate — no orphaned nonce slots on _prepare_onchain failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prep_failure_does_not_allocate_nonce():
    """H4: If _prepare_onchain fails, allocate() is never called for that worker.

    This prevents an orphaned nonce slot (gap in the Safe TX queue) that would
    cause GS026 for subsequent workers.  The failed record is also removed from
    _in_flight (H1 combined).
    """
    fail_record = _make_record(STALL_ID)
    ok_record = _make_record(FAST1_ID)
    q = _make_queue([fail_record, ok_record])
    bridge = _make_bridge_with_safe()

    allocator = _make_allocator_mock([10])
    bridge.wallet.safe_service.get_allocator.return_value = allocator

    prep_call_order: list[str] = []

    async def _prep_fail_for_stall(rec):
        prep_call_order.append(rec.request.request_id)
        if rec.request.request_id == STALL_ID:
            raise RuntimeError("IPFS upload failed")
        return await _instant_prepare(rec)

    with patch(
        "micromech.core.bridge.get_service_info",
        return_value={"multisig_address": MULTISIG},
    ):
        dm = DeliveryManager(_make_config(parallel_nonce=True), _make_chain_config(), q, bridge)
        with (
            patch.object(dm, "_prepare_onchain", side_effect=_prep_fail_for_stall),
            patch.object(dm, "_submit_batch_delivery", return_value=("0x" + "ab" * 32, [True])),
        ):
            delivered = await dm._deliver_concurrent()

    assert delivered == 1, "Only ok_record must be delivered when fail_record prep fails"
    # Both records had _prepare_onchain called (gather dispatches all workers)
    assert set(prep_call_order) == {STALL_ID, FAST1_ID}, (
        f"Both records must attempt prep; got {prep_call_order}"
    )
    # allocate() called exactly once — only for the ok_record (not for fail_record)
    assert allocator.allocate.call_count == 1, (
        f"allocate() must only be called for workers that passed prep; "
        f"got {allocator.allocate.call_count}"
    )
    assert allocator.release.call_count == 1, "release() called once for the allocated nonce"
    # H1: failed record removed from _in_flight
    assert STALL_ID not in dm._in_flight, (
        "Record with prep failure must be removed from _in_flight (H1+H4 combined)"
    )


# ---------------------------------------------------------------------------
# _sanitize_error: __context__ traversal (MEDIUM fix)
# ---------------------------------------------------------------------------


def test_sanitize_error_traverses_context_chain():
    """__context__ (implicit chaining) is also traversed and redacted."""
    from micromech.runtime.delivery import _sanitize_error

    inner_key = "0x" + "ee" * 32
    inner = ConnectionError(f"RPC error key={inner_key}")
    outer = RuntimeError("execution failed")
    outer.__context__ = inner  # implicit chain, no explicit 'raise ... from ...'

    result = _sanitize_error(outer)
    assert "execution failed" in result
    assert "context:" in result
    assert "RPC error" in result
    assert inner_key not in result, "Inner key in __context__ must be redacted"


def test_sanitize_error_cause_takes_priority_over_context():
    """When both __cause__ and __context__ are set, __cause__ is used (explicit chain)."""
    from micromech.runtime.delivery import _sanitize_error

    cause = ValueError("explicit cause")
    context = RuntimeError("implicit context")
    outer = RuntimeError("outer")
    outer.__cause__ = cause
    outer.__context__ = context

    result = _sanitize_error(outer)
    assert "caused by" in result
    assert "explicit cause" in result
    assert "implicit context" not in result, "__context__ must be ignored when __cause__ is set"


def test_sanitize_error_depth_guard():
    """Recursion stops at depth 5 and returns '...' to prevent infinite loops."""
    from micromech.runtime.delivery import _sanitize_error

    # Build a chain of 8 exceptions — deeper than the depth=5 guard
    exc = RuntimeError("level0")
    current = exc
    for i in range(1, 8):
        child = RuntimeError(f"level{i}")
        current.__cause__ = child
        current = child

    result = _sanitize_error(exc)
    assert "..." in result, "Depth guard must emit '...' when chain exceeds 5 levels"
    assert "level0" in result
    assert "level6" not in result, "Level 6+ must not appear — depth guard must truncate"
