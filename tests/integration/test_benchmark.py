"""Throughput benchmark for micromech.

Measures the maximum request processing capacity in two scenarios:

  TestBenchmarkExecution  — pure tool execution (off-chain, no delivery tx)
                            bottleneck: executor semaphore + SQLite writes
  TestBenchmarkDelivery   — full on-chain cycle (execute + Safe TX delivery)
                            bottleneck: Safe TX mining time × delivery workers

Run:
  uv run pytest tests/integration/test_benchmark.py -v -s

Results are printed to stdout as a table.  No assertions fail on throughput
numbers — the tests always pass and just report what they measured.
"""

import asyncio
import time
from unittest.mock import patch

import pytest

from tests.integration.test_anvil_e2e import (
    MARKETPLACE_ADDR,
    MECH_ADDR,
    MECH_DELIVERY_RATE,
    MECH_FACTORY,
    MECH_MULTISIG,
    MECH_SERVICE_ID,
    PAYMENT_TYPE_NATIVE,
    RICH_ACCOUNT,
    SUPPLY_STAKING_ADDR,
    AnvilBridge,
    _load_abi,
)

# ---------------------------------------------------------------------------
# w3 fixture (mirrors test_anvil_e2e.py)
# ---------------------------------------------------------------------------


@pytest.fixture()
def w3(_anvil_forks):
    """Connect to Anvil fork of Gnosis with snapshot isolation."""
    from web3 import Web3

    _w3 = Web3(Web3.HTTPProvider("http://localhost:18545", request_kwargs={"timeout": 30}))
    if not _w3.is_connected():
        pytest.skip("Anvil not running on port 18545")

    try:
        from web3.middleware import ExtraDataToPOAMiddleware

        _w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except ImportError:
        from web3.middleware import ExtraDataLengthMiddleware

        _w3.middleware_onion.inject(ExtraDataLengthMiddleware, layer=0)

    snapshot_id = _w3.provider.make_request("evm_snapshot", [])["result"]
    _w3.provider.make_request(
        "anvil_setBalance",
        ["0xe1CB04A0fA36DdD16a06ea828007E35e1a3cBC37", hex(100 * 10**18)],
    )

    yield _w3

    _w3.provider.make_request("evm_revert", [snapshot_id])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _percentile(data: list[float], p: int) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def _print_table(title: str, rows: list[tuple]) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")
    for label, value in rows:
        print(f"  {label:<35} {value}")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# Benchmark 1: Pure execution throughput (off-chain, no delivery)
# ---------------------------------------------------------------------------


class TestBenchmarkExecution:
    """Measure max tool execution rate with no on-chain delivery overhead.

    Sends N requests as fast as possible via _on_new_request() and waits
    for all of them to reach STATUS_EXECUTED.  Uses the 'echo' tool so
    execution is CPU/IO-bound by the framework itself, not the tool.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("n_requests", [50, 200])
    async def test_execution_throughput(self, tmp_path, n_requests):
        import micromech.runtime.listener as _listener_mod
        import micromech.runtime.server as _server_mod
        from micromech.core.config import MicromechConfig
        from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
        from micromech.core.models import MechRequest
        from micromech.runtime.server import MechServer

        _server_mod.DB_PATH = tmp_path / f"bench_exec_{n_requests}.db"
        _listener_mod.DEFAULT_EVENT_POLL_INTERVAL = 60  # disable polling

        config = MicromechConfig()
        server = MechServer(config)
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            await asyncio.sleep(0.3)  # let server warm up

            # --- Submit all requests as fast as possible ---
            t_submit_start = time.perf_counter()
            for i in range(n_requests):
                req = MechRequest(
                    request_id=f"bench-exec-{i}",
                    prompt=f"benchmark request {i}",
                    tool="echo",
                    is_offchain=True,
                )
                await server._on_new_request(req)
            submit_time = time.perf_counter() - t_submit_start

            # --- Wait for all to finish ---
            t_exec_start = time.perf_counter()
            deadline = time.perf_counter() + 120.0
            while time.perf_counter() < deadline:
                counts = server.queue.count_by_status()
                done = (
                    counts.get(STATUS_EXECUTED, 0)
                    + counts.get(STATUS_FAILED, 0)
                )
                if done >= n_requests:
                    break
                await asyncio.sleep(0.05)
            total_time = time.perf_counter() - t_exec_start

            # --- Collect latencies ---
            records = server.queue.get_recent(limit=n_requests + 10)
            latencies = [
                r.result.execution_time
                for r in records
                if r.result and r.result.execution_time
            ]

            counts = server.queue.count_by_status()
            executed = counts.get(STATUS_EXECUTED, 0)
            failed = counts.get(STATUS_FAILED, 0)
            throughput = executed / total_time if total_time > 0 else 0

            _print_table(
                f"Execution — {n_requests} requests (echo, off-chain)",
                [
                    ("Requests submitted", n_requests),
                    ("Executed OK", executed),
                    ("Failed", failed),
                    ("Submit time", f"{submit_time * 1000:.1f} ms"),
                    ("Total execution time", f"{total_time:.2f} s"),
                    ("Throughput", f"{throughput:.1f} req/s"),
                    (
                        "Tool latency P50",
                        f"{_percentile(latencies, 50) * 1000:.1f} ms",
                    ),
                    (
                        "Tool latency P95",
                        f"{_percentile(latencies, 95) * 1000:.1f} ms",
                    ),
                    (
                        "Tool latency P99",
                        f"{_percentile(latencies, 99) * 1000:.1f} ms",
                    ),
                    (
                        "Tool latency max",
                        f"{max(latencies) * 1000:.1f} ms"
                        if latencies
                        else "n/a",
                    ),
                ],
            )

        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()


# ---------------------------------------------------------------------------
# Benchmark 2: Full on-chain delivery throughput
# ---------------------------------------------------------------------------


class TestBenchmarkDelivery:
    """Measure end-to-end throughput including on-chain Safe TX delivery.

    Sends N on-chain marketplace requests via Anvil impersonation, starts
    the full MechServer (listener + executor + delivery), and measures:
      - Time from server start to last delivery confirmed
      - Time to first delivery
      - Effective deliveries/minute
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("n_requests", [5, 15])
    async def test_delivery_throughput(self, w3, tmp_path, n_requests):
        import micromech.runtime.listener as _listener_mod
        import micromech.runtime.server as _server_mod
        from micromech.core.config import ChainConfig, MicromechConfig
        from micromech.core.constants import STATUS_DELIVERED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        _server_mod.DB_PATH = tmp_path / f"bench_delivery_{n_requests}.db"
        _listener_mod.DEFAULT_EVENT_POLL_INTERVAL = 1
        _listener_mod.DEFAULT_EVENT_LOOKBACK_BLOCKS = 50

        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )

        # Fund the multisig for gas
        w3.provider.make_request(
            "anvil_setBalance", [MECH_MULTISIG, hex(50 * 10**18)]
        )

        # --- Submit all requests on-chain ---
        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee
        request_data = '{"prompt":"benchmark","tool":"echo"}'.encode()

        block_before = w3.eth.block_number
        w3.provider.make_request("anvil_impersonateAccount", [RICH_ACCOUNT])

        t_submit_start = time.perf_counter()
        for _ in range(n_requests):
            tx = marketplace.functions.request(
                request_data,
                MECH_DELIVERY_RATE,
                PAYMENT_TYPE_NATIVE,
                w3.to_checksum_address(MECH_ADDR),
                300,
                b"",
            ).transact(
                {"from": RICH_ACCOUNT, "value": value, "gas": 500_000}
            )
            w3.eth.wait_for_transaction_receipt(tx)

        w3.provider.make_request(
            "anvil_stopImpersonatingAccount", [RICH_ACCOUNT]
        )
        submit_time = time.perf_counter() - t_submit_start
        print(
            f"\n  Submitted {n_requests} on-chain requests"
            f" in {submit_time:.1f}s"
        )

        # --- Start full server ---
        config = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    mech_address=MECH_ADDR,
                    multisig_address=MECH_MULTISIG,
                    marketplace_address=MARKETPLACE_ADDR,
                    factory_address=MECH_FACTORY,
                    staking_address=SUPPLY_STAKING_ADDR,
                )
            },
        )

        bridge = AnvilBridge(w3)
        svc_info = {
            "service_id": MECH_SERVICE_ID,
            "service_key": f"gnosis:{MECH_SERVICE_ID}",
            "multisig_address": MECH_MULTISIG,
        }

        t_first_delivery: list[float] = []

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value=svc_info,
            ),
            patch(
                "micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT",
                0,
            ),
        ):
            server = MechServer(config, bridges={"gnosis": bridge})
            server.listeners["gnosis"]._last_block = block_before

            t_server_start = time.perf_counter()

            async def stop_when_done():
                deadline = time.perf_counter() + 180.0
                prev_delivered = 0
                while time.perf_counter() < deadline:
                    await asyncio.sleep(0.5)
                    counts = server.queue.count_by_status()
                    delivered = counts.get(STATUS_DELIVERED, 0)
                    failed = counts.get(STATUS_FAILED, 0)

                    if delivered > prev_delivered:
                        now = time.perf_counter()
                        if not t_first_delivery:
                            t_first_delivery.append(now - t_server_start)
                        prev_delivered = delivered
                        elapsed = now - t_server_start
                        print(
                            f"  [{elapsed:.1f}s]"
                            f" delivered={delivered} failed={failed}"
                        )

                    if delivered + failed >= n_requests:
                        break

                server.stop()

            asyncio.create_task(stop_when_done())

            try:
                await asyncio.wait_for(
                    server.run(with_http=False), timeout=200.0
                )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        t_total = time.perf_counter() - t_server_start

        counts = server.queue.count_by_status()
        delivered = counts.get(STATUS_DELIVERED, 0)
        failed = counts.get(STATUS_FAILED, 0)

        # Query delivered_at directly from RequestRow (not mapped in _row_to_record)
        from micromech.core.persistence import RequestRow

        e2e_times = []
        for row in RequestRow.select().where(
            RequestRow.status == STATUS_DELIVERED
        ):
            if row.delivered_at and row.created_at:
                delta = (row.delivered_at - row.created_at).total_seconds()
                e2e_times.append(delta)

        throughput = delivered / t_total if t_total > 0 else 0

        _print_table(
            f"Delivery — {n_requests} requests (echo, on-chain)",
            [
                ("Requests submitted on-chain", n_requests),
                ("Delivered", delivered),
                ("Failed", failed),
                ("Submit time (all requests)", f"{submit_time:.1f} s"),
                (
                    "Total time (server start → done)",
                    f"{t_total:.1f} s",
                ),
                (
                    "Time to first delivery",
                    f"{t_first_delivery[0]:.1f} s"
                    if t_first_delivery
                    else "n/a",
                ),
                (
                    "Throughput",
                    f"{throughput:.2f} req/s"
                    f"  ({throughput * 60:.1f} req/min)",
                ),
                (
                    "E2E latency P50 (created→delivered)",
                    f"{_percentile(e2e_times, 50):.1f} s"
                    if e2e_times
                    else "n/a",
                ),
                (
                    "E2E latency P95",
                    f"{_percentile(e2e_times, 95):.1f} s"
                    if e2e_times
                    else "n/a",
                ),
                (
                    "E2E latency max",
                    f"{max(e2e_times):.1f} s" if e2e_times else "n/a",
                ),
            ],
        )

        server.shutdown()
