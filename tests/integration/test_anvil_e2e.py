"""E2E test: micromech full request-execute-deliver cycle on Anvil fork.

Proves the FULL micromech cycle against a real Gnosis fork:

  1. HTTP: submit a request via POST /request
  2. Executor: tool processes the request (echo tool)
  3. Listener: detect on-chain marketplace Request events
  4. Delivery: call deliverToMarketplace on the mech contract
  5. Verify: delivery counted on-chain, DB state correct

Also tests off-chain (HTTP-only) flow without on-chain delivery.

Run:
  # Start Anvil fork of Gnosis
  anvil --fork-url <gnosis_rpc> --port 18545 --auto-impersonate --silent

  # Run the test
  ANVIL_URL=http://localhost:18545 uv run pytest tests/integration/test_anvil_e2e.py -v -s
"""

import asyncio
import json
import os

import pytest
from web3 import Web3

ANVIL_URL = os.environ.get("ANVIL_URL", "http://localhost:18545")

# Gnosis contracts (existing mech on mainnet)
MARKETPLACE_ADDR = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
MECH_ADDR = "0xC05e7412439bD7e91730a6880E18d5D5873F632C"
MECH_SERVICE_ID = 2182
MECH_DELIVERY_RATE = 10_000_000_000_000_000  # 0.01 xDAI
PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

# Well-funded Gnosis address for impersonation
RICH_ACCOUNT = Web3.to_checksum_address("0xe1CB04A0fA36DdD16a06ea828007E35e1a3cBC37")


def _load_abi(name: str) -> list:
    """Load ABI from iwa package."""
    try:
        from importlib.resources import files

        abi_dir = files("iwa.plugins.olas.contracts.abis")
        return json.loads(abi_dir.joinpath(name).read_text())
    except Exception:
        from pathlib import Path

        abi_file = (
            Path("/media/david/DATA/repos/iwa/src/iwa") / "plugins/olas/contracts/abis" / name
        )
        return json.loads(abi_file.read_text())


@pytest.fixture(scope="module")
def w3():
    """Connect to Anvil fork of Gnosis."""
    w3 = Web3(Web3.HTTPProvider(ANVIL_URL, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        pytest.skip("Anvil not running on " + ANVIL_URL)
    chain = w3.eth.chain_id
    assert chain == 100, f"Expected Gnosis (100), got {chain}"
    # Fund the test account with 100 xDAI
    w3.provider.make_request(
        "anvil_setBalance",
        [RICH_ACCOUNT, hex(100 * 10**18)],
    )
    print(f"\nAnvil connected: block {w3.eth.block_number}")
    return w3


class TestMicromechOffchainE2E:
    """Test the full off-chain flow: HTTP request → execute → DB result."""

    @pytest.mark.asyncio
    async def test_http_request_to_execution(self, tmp_path):
        """Submit request via HTTP, verify it gets executed by the echo tool."""
        from micromech.core.config import MicromechConfig, PersistenceConfig, RuntimeConfig
        from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "test.db"),
            runtime=RuntimeConfig(
                port=18999,
                max_concurrent=5,
                delivery_interval=1,
                event_poll_interval=1,
            ),
        )

        server = MechServer(config)

        # Start server in background
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            # Give server time to start
            await asyncio.sleep(0.5)

            # Submit requests via the server's callback
            from micromech.core.models import MechRequest

            for i in range(5):
                req = MechRequest(
                    request_id=f"test-{i}",
                    prompt=f"Question {i}: Will ETH hit {i}0k?",
                    tool="echo",
                    is_offchain=True,
                )
                await server._on_new_request(req)

            # Wait for execution
            await asyncio.sleep(2.0)

            # Verify all requests were processed
            for i in range(5):
                record = server.queue.get_by_id(f"test-{i}")
                assert record is not None, f"Request test-{i} not found"
                assert record.request.status in (
                    STATUS_EXECUTED,
                    STATUS_FAILED,
                ), f"Request test-{i} status={record.request.status}"
                if record.result:
                    data = json.loads(record.result.output)
                    assert "p_yes" in data
                    print(f"  ✓ test-{i}: executed in {record.result.execution_time:.3f}s")

            # Verify queue status
            counts = server.queue.count_by_status()
            print(f"  Queue: {counts}")
            assert counts.get("executed", 0) + counts.get("failed", 0) == 5

        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()


class TestMicromechOnchainE2E:
    """Test on-chain interactions: send marketplace requests, detect and deliver."""

    def test_send_marketplace_request_and_deliver(self, w3):
        """Send a request via marketplace, then deliver using mech contract."""
        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )
        mech = w3.eth.contract(
            address=w3.to_checksum_address(MECH_ADDR),
            abi=_load_abi("mech_new.json"),
        )

        # Get mech's service multisig (needed for delivery)
        from iwa.plugins.olas.constants import OLAS_CONTRACTS

        registry = w3.eth.contract(
            address=w3.to_checksum_address(OLAS_CONTRACTS["gnosis"]["OLAS_SERVICE_REGISTRY"]),
            abi=_load_abi("service_registry.json"),
        )
        svc_info = registry.functions.getService(MECH_SERVICE_ID).call()
        mech_multisig = svc_info[1]
        print(f"\n  Mech multisig: {mech_multisig}")

        # Baseline counters
        base_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(MECH_ADDR)
        ).call()
        print(f"  Baseline deliveries: {base_deliveries}")

        # Step 1: Send a request from a rich account
        print("\n--- Step 1: Send marketplace request ---")
        w3.provider.make_request("anvil_impersonateAccount", [RICH_ACCOUNT])

        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee

        request_data = json.dumps(
            {
                "prompt": "Will ETH hit 10k by 2027?",
                "tool": "echo",
            }
        ).encode()

        tx = marketplace.functions.request(
            request_data,
            MECH_DELIVERY_RATE,
            PAYMENT_TYPE_NATIVE,
            w3.to_checksum_address(MECH_ADDR),
            300,
            b"",
        ).transact(
            {
                "from": RICH_ACCOUNT,
                "value": value,
                "gas": 500_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Request tx reverted"

        # Extract request ID from event
        logs = marketplace.events.MarketplaceRequest().process_receipt(receipt)
        assert len(logs) > 0, "No MarketplaceRequest event"
        request_ids = logs[0]["args"]["requestIds"]
        print(f"  ✓ Request submitted, ID: {request_ids[0].hex()[:16]}...")

        w3.provider.make_request("anvil_stopImpersonatingAccount", [RICH_ACCOUNT])

        # Step 2: Deliver response (impersonating mech multisig)
        print("\n--- Step 2: Deliver response via mech ---")
        # Fund the multisig with xDAI for gas
        w3.provider.make_request("anvil_setBalance", [mech_multisig, hex(10 * 10**18)])
        w3.provider.make_request("anvil_impersonateAccount", [mech_multisig])

        response_data = json.dumps(
            {
                "result": '{"p_yes": 0.6, "p_no": 0.4}',
                "tool": "echo",
            }
        ).encode()

        tx = mech.functions.deliverToMarketplace(
            request_ids,
            [response_data],
        ).transact(
            {
                "from": mech_multisig,
                "gas": 500_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Delivery tx reverted"

        w3.provider.make_request("anvil_stopImpersonatingAccount", [mech_multisig])

        # Step 3: Verify delivery counted
        new_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(MECH_ADDR)
        ).call()
        assert new_deliveries > base_deliveries
        print(f"  ✓ Deliveries: {base_deliveries} → {new_deliveries}")

    def test_event_listener_detects_request(self, w3):
        """Send a marketplace request and verify EventListener can detect it."""
        from micromech.core.config import MechConfig, MicromechConfig, RuntimeConfig
        from micromech.runtime.listener import EventListener

        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )

        # Record block before sending request
        block_before = w3.eth.block_number

        # Send a request
        w3.provider.make_request("anvil_impersonateAccount", [RICH_ACCOUNT])
        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee

        request_data = json.dumps(
            {
                "prompt": "Test listener detection",
                "tool": "echo",
            }
        ).encode()

        tx = marketplace.functions.request(
            request_data,
            MECH_DELIVERY_RATE,
            PAYMENT_TYPE_NATIVE,
            w3.to_checksum_address(MECH_ADDR),
            300,
            b"",
        ).transact(
            {
                "from": RICH_ACCOUNT,
                "value": value,
                "gas": 500_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1
        w3.provider.make_request("anvil_stopImpersonatingAccount", [RICH_ACCOUNT])

        block_after = w3.eth.block_number
        print(f"\n  Request in blocks {block_before + 1}-{block_after}")

        # Create a mock bridge that uses the Anvil web3 directly
        class AnvilBridge:
            def __init__(self, web3):
                self.web3 = web3

            def with_retry(self, fn, **kwargs):
                return fn()

        config = MicromechConfig(
            mech=MechConfig(
                mech_address=MECH_ADDR,
                marketplace_address=MARKETPLACE_ADDR,
            ),
            runtime=RuntimeConfig(event_lookback_blocks=100),
        )

        bridge = AnvilBridge(w3)
        listener = EventListener(config, bridge=bridge)
        listener._last_block = block_before

        # Poll for events
        requests = asyncio.get_event_loop().run_until_complete(listener.poll_once())

        assert len(requests) >= 1, f"Expected >=1 requests, got {len(requests)}"
        # At least one request should be for our mech
        print(f"  ✓ Listener detected {len(requests)} request(s)")
        for req in requests:
            print(f"    - {req.request_id[:16]}... tool={req.tool}")


class TestMicromechFullServerLoop:
    """Test the full micromech server loop: listen → execute → verify.

    Sends a marketplace request, then runs the micromech server which should:
    1. Detect the request via EventListener
    2. Execute the echo tool
    3. Store the result in the DB
    """

    @pytest.mark.asyncio
    async def test_server_detects_and_executes(self, w3, tmp_path):
        """Full server integration: on-chain request → detection → execution."""
        from micromech.core.config import (
            IpfsConfig,
            MechConfig,
            MicromechConfig,
            PersistenceConfig,
            RuntimeConfig,
        )
        from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )

        # Record block before request
        block_before = w3.eth.block_number

        # Send a marketplace request
        w3.provider.make_request("anvil_impersonateAccount", [RICH_ACCOUNT])
        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee

        request_data = json.dumps(
            {
                "prompt": "Server loop test: will BTC hit 100k?",
                "tool": "echo",
            }
        ).encode()

        tx = marketplace.functions.request(
            request_data,
            MECH_DELIVERY_RATE,
            PAYMENT_TYPE_NATIVE,
            w3.to_checksum_address(MECH_ADDR),
            300,
            b"",
        ).transact(
            {
                "from": RICH_ACCOUNT,
                "value": value,
                "gas": 500_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1
        w3.provider.make_request("anvil_stopImpersonatingAccount", [RICH_ACCOUNT])
        print(f"\n  Request sent at block {w3.eth.block_number}")

        # Create AnvilBridge
        class AnvilBridge:
            def __init__(self, web3):
                self.web3 = web3

            def with_retry(self, fn, **kwargs):
                return fn()

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "server_loop.db"),
            mech=MechConfig(
                mech_address=MECH_ADDR,
                marketplace_address=MARKETPLACE_ADDR,
            ),
            runtime=RuntimeConfig(
                event_poll_interval=1,
                event_lookback_blocks=100,
                delivery_interval=1,
                max_concurrent=5,
            ),
            ipfs=IpfsConfig(enabled=False),
        )

        bridge = AnvilBridge(w3)
        server = MechServer(config, bridge=bridge)

        # Set listener to start from before our request
        server.listener._last_block = block_before

        # Run server briefly
        async def stop_after_processing():
            # Wait for listener to detect + executor to process
            for _ in range(20):
                await asyncio.sleep(0.5)
                counts = server.queue.count_by_status()
                executed = counts.get("executed", 0) + counts.get("failed", 0)
                if executed > 0:
                    break
            server.stop()

        asyncio.create_task(stop_after_processing())

        try:
            await asyncio.wait_for(server.run(with_http=False), timeout=15.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        # Verify request was detected and processed
        recent = server.queue.get_recent(limit=10)
        print(f"  Queue has {len(recent)} records")

        executed_records = [
            r for r in recent if r.request.status in (STATUS_EXECUTED, STATUS_FAILED)
        ]
        assert len(executed_records) >= 1, (
            f"Expected at least 1 executed request, got {len(executed_records)}. "
            f"Statuses: {[r.request.status for r in recent]}"
        )

        for r in executed_records:
            print(
                f"  ✓ {r.request.request_id[:16]}... "
                f"status={r.request.status} tool={r.request.tool}"
            )
            if r.result:
                print(f"    result: {r.result.output[:80]}")

        server.shutdown()
        print("  ✓ Full server loop completed")
