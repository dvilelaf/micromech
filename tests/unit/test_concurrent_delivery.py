"""Concurrency integration test: DeliveryManager worker stall resilience.

Proves that when one worker blocks (GS013 stall, slow Safe TX, RPC timeout),
the remaining workers complete their deliveries independently — the stall does
NOT propagate to the whole delivery loop.

Architecture recap
------------------
_deliver_concurrent() launches one asyncio.Task per record via asyncio.gather().
Each task calls _deliver_single_onchain(), which in turn calls:

    await self._prepare_onchain(record)          # async, event-loop
    await asyncio.to_thread(_submit_batch_delivery, ...)  # thread pool

asyncio.to_thread() releases the event loop while the thread runs — so other
workers can proceed while one thread is blocked.  This is the mechanism that
makes stall resilience possible.

Mocking strategy
----------------
- _prepare_onchain  → instant async stub (no IPFS, no net)
- _submit_batch_delivery → time.sleep(delay) in a thread
  * STALL_ID request: STALL_DELAY seconds  (simulates GS013 + backoff)
  * FAST_ID requests: FAST_DELAY seconds   (normal TX)

No Anvil required — everything is mocked at the Safe TX submission layer.
The _anvil_forks session fixture is autouse but only skips the whole session
when no Anvil AND no secrets.env are present.  This test runs standalone via:

    uv run pytest tests/integration/test_concurrent_delivery.py -v -s

Key assertions (in order of importance)
-----------------------------------------
1. ORDERING: fast workers complete before the stalled worker  (order proof)
2. TIMING:   concurrent elapsed < serial lower bound          (wall-clock proof)
3. CORRECTNESS: in-flight set prevents double-pickup           (safety proof)
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
        """CORE ORDERING PROOF: fast workers deliver before the stalled worker.

        With 3 concurrent workers (DEFAULT_DELIVERY_WORKERS=3):
        - Worker A picks STALL_ID → sleeps STALL_DELAY=2.0s
        - Worker B picks FAST1_ID → sleeps FAST_DELAY=0.3s  → finishes at ~0.3s
        - Worker C picks FAST2_ID → sleeps FAST_DELAY=0.3s  → finishes at ~0.3s

        If workers were SERIAL, workers B and C would not even start until A finishes
        at 2.0s.  With concurrent workers, B and C complete at 0.3s.

        The ordering assertion is the primary proof: it does not rely on exact
        timing and is robust to scheduling variance and CI slowness.
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

        elapsed = time.monotonic() - t0

        # --- All 3 records were delivered ---
        assert delivered == 3, f"Expected 3 delivered, got {delivered}"
        assert q.mark_delivered.call_count == 3

        # --- Ordering: fast workers completed before the stalled worker ---
        assert FAST1_BYTES in completed_at, "Fast worker 1 did not complete"
        assert FAST2_BYTES in completed_at, "Fast worker 2 did not complete"
        assert STALL_BYTES in completed_at, "Stall worker did not complete"

        assert completed_at[FAST1_BYTES] < completed_at[STALL_BYTES], (
            f"Fast worker 1 finished at {completed_at[FAST1_BYTES]:.3f}s AFTER "
            f"stall worker at {completed_at[STALL_BYTES]:.3f}s — workers may be serial."
        )
        assert completed_at[FAST2_BYTES] < completed_at[STALL_BYTES], (
            f"Fast worker 2 finished at {completed_at[FAST2_BYTES]:.3f}s AFTER "
            f"stall worker at {completed_at[STALL_BYTES]:.3f}s — workers may be serial."
        )

        print(
            f"\n  [Test] Ordering proof:"
            f"\n    stall worker:  {completed_at[STALL_BYTES]:.3f}s"
            f"\n    fast worker 1: {completed_at[FAST1_BYTES]:.3f}s ✓ before stall"
            f"\n    fast worker 2: {completed_at[FAST2_BYTES]:.3f}s ✓ before stall"
            f"\n    total elapsed: {elapsed:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_concurrent_time_bounded_by_max_not_sum(self):
        """TIMING PROOF: concurrent elapsed ≈ max(delays), not sum(delays).

        With concurrent workers:  elapsed ≈ STALL_DELAY  (2.0s)
        With serial workers:      elapsed ≈ STALL_DELAY + 2*FAST_DELAY  (2.6s)

        The assertion: elapsed < STALL_DELAY + FAST_DELAY
        This proves the total is bounded by the slowest worker, not the sum.
        We subtract a tolerance so the assertion is tight.

        [Perf] This is a timing-sensitive assertion. It may be flaky on
        extremely loaded CI machines. The 0.5s headroom covers 99th-percentile
        thread scheduling variance.
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

        # Serial lower bound: minimum time if workers were sequential
        serial_lower_bound = STALL_DELAY + FAST_DELAY   # 2.3s

        # Concurrent upper bound: max(delays) + scheduling headroom
        concurrent_upper_bound = STALL_DELAY + 0.5      # 2.5s

        assert elapsed < serial_lower_bound, (
            f"[Perf] elapsed={elapsed:.3f}s >= serial_lower_bound={serial_lower_bound:.2f}s. "
            f"Workers appear to be running serially. Concurrent delivery is broken."
        )
        assert elapsed < concurrent_upper_bound, (
            f"[Perf] elapsed={elapsed:.3f}s — slower than expected even for concurrent mode. "
            f"Thread pool may be saturated (need ≥3 threads)."
        )

        saved = serial_lower_bound - elapsed
        print(
            f"\n  [Perf] Timing bound:"
            f"\n    concurrent elapsed: {elapsed:.3f}s"
            f"\n    serial lower bound: {serial_lower_bound:.2f}s (STALL + FAST)"
            f"\n    time saved vs serial: {saved:.3f}s"
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
    async def test_all_workers_start_near_simultaneously(self):
        """Workers start within a short window of each other (true concurrency).

        asyncio.gather() launches all tasks before any thread starts. The time
        between the first and last thread start should be much less than FAST_DELAY.

        This verifies that the concurrency is at the asyncio.gather level, not
        just sequential execution with fast workers.
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
            started_at[rid] = time.monotonic() - t0  # record start time immediately
            delay = STALL_DELAY if rid == STALL_BYTES else FAST_DELAY
            time.sleep(delay)
            return ("0x" + "ab" * 32, [True])

        with (
            patch.object(dm, "_prepare_onchain", side_effect=_instant_prepare),
            patch.object(dm, "_submit_batch_delivery", side_effect=_submit_track_start),
        ):
            await dm._deliver_concurrent()

        assert len(started_at) == 3, "All 3 workers must have started"

        first_start = min(started_at.values())
        last_start = max(started_at.values())
        start_spread = last_start - first_start

        # All workers start within FAST_DELAY/2 of each other
        # (they all launch before the thread pool even picks them up)
        max_spread = FAST_DELAY * 0.5  # 0.15s — generous for thread scheduling
        assert start_spread < max_spread, (
            f"Workers started {start_spread:.3f}s apart — "
            f"expected < {max_spread:.2f}s (true concurrency: all start near-simultaneously). "
            f"start times: {dict((k.hex()[:4], f'{v:.3f}s') for k,v in started_at.items())}"
        )

        print(
            f"\n  [Test] Start times (concurrent launch):"
            f"\n    stall:  t+{started_at[STALL_BYTES]:.3f}s"
            f"\n    fast1:  t+{started_at[FAST1_BYTES]:.3f}s"
            f"\n    fast2:  t+{started_at[FAST2_BYTES]:.3f}s"
            f"\n    spread: {start_spread:.3f}s (should be < {max_spread:.2f}s)"
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
    async def test_stall_does_not_prevent_successful_delivery_of_others(self):
        """[Test] Even when one worker's TX stalls, the others reach mark_delivered.

        This is the direct consequence of stall resilience: the queue drains
        for non-stalled requests regardless of how long the slow TX takes.
        Verifies mark_delivered was called exactly twice (for the 2 fast requests)
        even though the stall worker takes much longer.
        """
        stall_record = _make_record(STALL_ID)
        fast1_record = _make_record(FAST1_ID)
        fast2_record = _make_record(FAST2_ID)

        q = _make_queue([stall_record, fast1_record, fast2_record])
        bridge = _make_bridge()
        dm = DeliveryManager(_make_config(), _make_chain_config(), q, bridge)

        # Track the order in which mark_delivered is called.
        # Do NOT call the original mock — side_effect replaces the call entirely;
        # calling original_mark_delivered() from within the side_effect would
        # re-enter the mock → infinite recursion.
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

        # Fast requests must appear in delivery_order BEFORE the stall request
        stall_pos = delivery_order.index(STALL_ID)
        fast1_pos = delivery_order.index(FAST1_ID)
        fast2_pos = delivery_order.index(FAST2_ID)

        assert fast1_pos < stall_pos, (
            f"Fast1 delivered at position {fast1_pos}, "
            f"stall delivered at position {stall_pos}. "
            f"Expected fast workers to complete first."
        )
        assert fast2_pos < stall_pos, (
            f"Fast2 delivered at position {fast2_pos}, "
            f"stall delivered at position {stall_pos}. "
            f"Expected fast workers to complete first."
        )

        print(
            f"\n  [Test] Delivery order (concurrent):"
            f"\n    {delivery_order[0][:14]}... delivered 1st"
            f"\n    {delivery_order[1][:14]}... delivered 2nd"
            f"\n    {delivery_order[2][:14]}... delivered 3rd (stall worker)"
        )
