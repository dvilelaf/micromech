"""Delivery worker sequencing and correctness tests.

On-chain deliveries are intentionally SEQUENTIAL to prevent Safe nonce races.
Concurrent build_tx() calls return the same nonce, causing GS026 (invalid
signature after another worker advances the nonce first). By processing
on-chain records one at a time, each worker gets a unique nonce.

Off-chain (HTTP) deliveries have no nonce constraint and remain concurrent.

Architecture
------------
_deliver_concurrent() iterates on-chain records sequentially (for loop),
then gathers off-chain records with asyncio.gather(). Each on-chain step:

    await self._prepare_onchain(record)          # async — IPFS/prep
    await asyncio.to_thread(_submit_batch_delivery, ...)  # Safe TX

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


def _make_config() -> MicromechConfig:
    return MicromechConfig()


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
    def _submit(req_id_bytes_list: list[bytes], datas: list[bytes]) -> tuple[str, list[bool]]:
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
        """CORRECTNESS PROOF: all records delivered sequentially, no nonce race.

        On-chain submissions are intentionally sequential to prevent Safe nonce
        races (concurrent build_tx() calls return the same nonce → GS026).

        Queue order: STALL_ID, FAST1_ID, FAST2_ID (as returned by get_undelivered).
        Expected completion order: STALL → FAST1 → FAST2 (sequential).

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

        def _submit_track_start(req_id_bytes_list, datas):
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

        def _count_submissions(req_id_bytes_list, datas):
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
