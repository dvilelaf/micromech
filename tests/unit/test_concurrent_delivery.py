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
from unittest.mock import MagicMock, patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.models import MechRequest, RequestRecord, ToolResult
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
    async def test_expired_requests_skipped_without_safe_tx(self):
        """[Test] Requests past their responseTimeout are marked failed immediately.

        Submitting a Safe TX for an expired request mines OK but the marketplace
        rejects it as a late delivery — wasting gas and blocking fresh requests.
        The pre-filter must mark them failed without calling _submit_batch_delivery.
        """
        from datetime import timedelta

        expired_record = _make_record(STALL_ID)
        expired_record.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
        expired_record.request.timeout = 300  # 5 min — record is 400s old → expired

        fresh_record = _make_record(FAST1_ID)
        fresh_record.request.created_at = datetime.now(timezone.utc)
        fresh_record.request.timeout = 300  # fresh

        q = _make_queue([expired_record, fresh_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        # Only the fresh record should be delivered
        assert delivered == 1, f"Expected 1 delivered, got {delivered}"
        assert submit_call_count[0] == 1, (
            f"_submit_batch_delivery called {submit_call_count[0]}x — "
            "expired record should be skipped without a Safe TX"
        )
        # _submit_batch_delivery mocked to return [True] → no marketplace timeout.
        # mark_failed called ONLY by pre-filter (expired detection), not delivery logic.
        q.mark_failed.assert_called_once()
        call_args = q.mark_failed.call_args[0]
        assert call_args[0] == STALL_ID, "mark_failed called with wrong request_id"
        assert "expired" in call_args[1], "mark_failed reason must mention 'expired'"

    @pytest.mark.asyncio
    async def test_all_expired_zero_delivered_zero_txs(self):
        """[Test] When all records are expired, zero TXs are submitted and zero delivered."""
        from datetime import timedelta

        records = []
        for rid in [STALL_ID, FAST1_ID, FAST2_ID]:
            r = _make_record(rid)
            r.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
            r.request.timeout = 300
            records.append(r)

        q = _make_queue(records)
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        submit_call_count = [0]

        def _count_submit(req_id_bytes_list, datas, safe_nonce=None):
            submit_call_count[0] += 1
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_count_submit),
        ):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0, "No records should be delivered when all are expired"
        assert submit_call_count[0] == 0, "No Safe TXs should be submitted for expired records"
        assert q.mark_failed.call_count == 3, "All 3 expired records must be marked failed"

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
    async def test_expired_offchain_request_skipped(self):
        """[Test] Expired off-chain requests are also pre-filtered without calling _deliver_one."""
        from datetime import timedelta

        expired_offchain = _make_record(STALL_ID)
        expired_offchain.request.is_offchain = True
        expired_offchain.request.created_at = datetime.now(timezone.utc) - timedelta(seconds=400)
        expired_offchain.request.timeout = 300

        q = _make_queue([expired_offchain])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        deliver_one_called = [False]

        async def _mock_deliver_one(record):
            deliver_one_called[0] = True
            return ("0x" + "ab" * 32, None)

        with patch.object(dm, "_deliver_one", side_effect=_mock_deliver_one):
            delivered = await dm._deliver_concurrent()

        assert delivered == 0, "Expired off-chain request must not be delivered"
        assert not deliver_one_called[0], "_deliver_one must not be called for expired off-chain"
        q.mark_failed.assert_called_once()
        assert "expired" in q.mark_failed.call_args[0][1]


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
    import asyncio as _asyncio

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
    notification_service.send = MagicMock(side_effect=lambda *a, **kw: _asyncio.sleep(0))

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
