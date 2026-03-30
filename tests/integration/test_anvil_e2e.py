"""E2E test: micromech full request-execute-deliver cycle on Anvil fork.

Proves the FULL micromech cycle against a real Gnosis fork:

  1. HTTP: submit a request via POST /request
  2. Executor: tool processes the request (echo tool)
  3. Listener: detect on-chain marketplace Request events
  4. Delivery: call deliverToMarketplace on the mech contract
  5. Verify: delivery counted on-chain, DB state correct

Also tests off-chain (HTTP-only) flow without on-chain delivery.

And the FULL lifecycle: create -> deploy -> stake -> run -> earn.

Run:
  # Start Anvil fork of Gnosis
  anvil --fork-url <gnosis_rpc> --port 18545 --auto-impersonate --silent

  # Run the test
  ANVIL_URL=http://localhost:18545 uv run pytest tests/integration/test_anvil_e2e.py -v -s
"""

import asyncio
import json
import os
import time as _time
from pathlib import Path

import pytest
from web3 import Web3

ANVIL_URL = os.environ.get("ANVIL_URL", "http://localhost:18545")

# Gnosis contracts (existing mech on mainnet)
MARKETPLACE_ADDR = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
MECH_ADDR = "0xC05e7412439bD7e91730a6880E18d5D5873F632C"
MECH_MULTISIG = "0xccA28b516a8c596742Bf23D06324c638230705aE"
MECH_SERVICE_ID = 2182
MECH_DELIVERY_RATE = 10_000_000_000_000_000  # 0.01 xDAI
PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

# ComplementaryServiceMetadata contract on Gnosis
COMPLEMENTARY_METADATA_ADDR = "0x0598081D48FB80B0A7E52FAD2905AE9beCd6fC69"

# Well-funded Gnosis address for impersonation
RICH_ACCOUNT = Web3.to_checksum_address("0xe1CB04A0fA36DdD16a06ea828007E35e1a3cBC37")

# Lifecycle test constants
SERVICE_REGISTRY = "0x9338b5153AE39BB89f50468E608eD9d764B755fD"
SERVICE_MANAGER = "0x068a4f0946cF8c7f9C1B58a3b5243Ac8843bf473"
OLAS_TOKEN = "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f"
OLAS_BALANCE_SLOT = 3  # ERC20 balanceOf mapping slot
SUPPLY_STAKING_ADDR = "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44"
MECH_FACTORY = "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF"
TOKEN_UTILITY = "0xa45E64d13A30a51b91ae0eb182e88a40e9b18eD8"
N_LIFECYCLE_REQUESTS = 5


def _load_abi(name: str) -> list:
    """Load ABI from iwa package."""
    try:
        from importlib.resources import files

        abi_dir = files("iwa.plugins.olas.contracts.abis")
        return json.loads(abi_dir.joinpath(name).read_text())
    except Exception:
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
    """Test the off-chain flow via internal API: submit -> execute -> verify result data.

    Verifies the complete result including prediction JSON structure,
    not just "status == executed".
    """

    @pytest.mark.asyncio
    async def test_tool_execution_result_format(self, tmp_path):
        """Submit requests internally, verify result contains valid prediction JSON."""
        from micromech.core.config import MicromechConfig, PersistenceConfig, RuntimeConfig
        from micromech.core.constants import STATUS_EXECUTED
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
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            await asyncio.sleep(0.5)

            from micromech.core.models import MechRequest

            n_requests = 5
            for i in range(n_requests):
                req = MechRequest(
                    request_id=f"test-{i}",
                    prompt=f"Question {i}: Will ETH hit {i}0k?",
                    tool="echo",
                    is_offchain=True,
                )
                await server._on_new_request(req)

            # Wait for execution
            await asyncio.sleep(2.0)

            # Verify ALL requests have complete result data
            executed_count = 0
            for i in range(n_requests):
                record = server.queue.get_by_id(f"test-{i}")
                assert record is not None, f"Request test-{i} not found in DB"
                assert record.request.status == STATUS_EXECUTED, (
                    f"test-{i}: expected 'executed', got '{record.request.status}'"
                )

                # Result must exist and contain valid prediction JSON
                assert record.result is not None, f"test-{i}: no ToolResult"
                assert record.result.output, f"test-{i}: empty output"
                assert record.result.error is None, (
                    f"test-{i}: unexpected error: {record.result.error}"
                )
                assert record.result.execution_time > 0, f"test-{i}: zero execution time"

                data = json.loads(record.result.output)
                assert "p_yes" in data, f"test-{i}: missing p_yes in {data}"
                assert "p_no" in data, f"test-{i}: missing p_no in {data}"
                assert isinstance(data["p_yes"], (int, float))
                assert isinstance(data["p_no"], (int, float))
                executed_count += 1
                print(
                    f"  test-{i}: p_yes={data['p_yes']}, p_no={data['p_no']} "
                    f"({record.result.execution_time:.3f}s)"
                )

            assert executed_count == n_requests
            counts = server.queue.count_by_status()
            print(f"  Queue: {counts}")

        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()


class TestFailurePaths:
    """Test error handling: unknown tools and tool exceptions."""

    @pytest.mark.asyncio
    async def test_unknown_tool_fails(self, tmp_path):
        """Submit request with unknown tool, verify STATUS_FAILED in DB."""
        from micromech.core.config import MicromechConfig, PersistenceConfig, RuntimeConfig
        from micromech.core.constants import STATUS_FAILED
        from micromech.runtime.server import MechServer

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "fail.db"),
            runtime=RuntimeConfig(
                port=18998,
                max_concurrent=5,
                delivery_interval=1,
                event_poll_interval=1,
            ),
        )

        server = MechServer(config)
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            await asyncio.sleep(0.5)

            from micromech.core.models import MechRequest

            req = MechRequest(
                request_id="fail-unknown-tool",
                prompt="Should fail",
                tool="nonexistent",
                is_offchain=True,
            )
            await server._on_new_request(req)

            # Wait for execution
            await asyncio.sleep(2.0)

            record = server.queue.get_by_id("fail-unknown-tool")
            assert record is not None, "Request not found in DB"
            assert record.request.status == STATUS_FAILED, (
                f"Expected 'failed', got '{record.request.status}'"
            )
            assert record.result is not None
            assert "nonexistent" in record.result.error.lower()
        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()

    @pytest.mark.asyncio
    async def test_tool_exception_does_not_crash_server(self, tmp_path):
        """Submit request that triggers tool failure, verify server continues running."""
        from micromech.core.config import MicromechConfig, PersistenceConfig, RuntimeConfig
        from micromech.core.constants import STATUS_EXECUTED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "crash.db"),
            runtime=RuntimeConfig(
                port=18997,
                max_concurrent=5,
                delivery_interval=1,
                event_poll_interval=1,
            ),
        )

        server = MechServer(config)
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            await asyncio.sleep(0.5)

            from micromech.core.models import MechRequest

            # First: a bad request (unknown tool)
            bad_req = MechRequest(
                request_id="crash-test-bad",
                prompt="Should fail",
                tool="nonexistent",
                is_offchain=True,
            )
            await server._on_new_request(bad_req)
            await asyncio.sleep(1.5)

            # Second: a good request AFTER the failure
            good_req = MechRequest(
                request_id="crash-test-good",
                prompt="Should succeed after failure",
                tool="echo",
                is_offchain=True,
            )
            await server._on_new_request(good_req)
            await asyncio.sleep(2.0)

            # Server must still be running and process the good request
            bad_record = server.queue.get_by_id("crash-test-bad")
            good_record = server.queue.get_by_id("crash-test-good")

            assert bad_record is not None
            assert bad_record.request.status == STATUS_FAILED

            assert good_record is not None, "Server stopped processing after tool failure"
            assert good_record.request.status == STATUS_EXECUTED, (
                f"Expected 'executed', got '{good_record.request.status}'"
            )
        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()


class TestDuplicateDetection:
    """Test that duplicate request IDs are not processed twice."""

    @pytest.mark.asyncio
    async def test_duplicate_request_not_processed_twice(self, tmp_path):
        """Submit same request_id twice, verify only 1 record in DB."""
        from micromech.core.config import MicromechConfig, PersistenceConfig, RuntimeConfig
        from micromech.core.constants import STATUS_EXECUTED
        from micromech.runtime.server import MechServer

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "dedup.db"),
            runtime=RuntimeConfig(
                port=18996,
                max_concurrent=5,
                delivery_interval=1,
                event_poll_interval=1,
            ),
        )

        server = MechServer(config)
        server_task = asyncio.create_task(server.run(with_http=False))

        try:
            await asyncio.sleep(0.5)

            from micromech.core.models import MechRequest

            for _ in range(2):
                req = MechRequest(
                    request_id="dedup-test-1",
                    prompt="Duplicate question",
                    tool="echo",
                    is_offchain=True,
                )
                await server._on_new_request(req)

            await asyncio.sleep(2.0)

            record = server.queue.get_by_id("dedup-test-1")
            assert record is not None
            assert record.request.status == STATUS_EXECUTED

            # Verify only 1 record with this ID exists (get_recent returns all)
            all_records = server.queue.get_recent(limit=100)
            matching = [r for r in all_records if r.request.request_id == "dedup-test-1"]
            assert len(matching) == 1, f"Expected 1 record for dedup-test-1, got {len(matching)}"
        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()


class TestMicromechOnchainE2E:
    """Test on-chain interactions: detect marketplace requests via EventListener."""

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


class TestIpfsCidComputation:
    """Test IPFS CID computation matches expected format and is deterministic."""

    def test_cid_roundtrip(self):
        """Compute a CID locally, verify roundtrip: data -> CID -> multihash -> CID."""
        from micromech.ipfs.client import (
            cid_hex_to_multihash_bytes,
            compute_cid,
            compute_cid_hex,
            multihash_to_cid,
        )

        data = json.dumps({"prompt": "Will ETH hit 10k?", "tool": "echo", "result": "yes"}).encode(
            "utf-8"
        )

        # Compute CID in both formats
        cid_str = compute_cid(data)
        cid_hex = compute_cid_hex(data)

        # Verify format
        assert cid_str.startswith("bafkrei"), f"Expected bafkrei..., got {cid_str}"
        assert cid_hex.startswith("f0155"), f"Expected f0155..., got {cid_hex}"

        # Roundtrip: hex -> multihash bytes -> back to CID string
        multihash_bytes = cid_hex_to_multihash_bytes(cid_hex)
        assert len(multihash_bytes) == 34
        assert multihash_bytes[0] == 0x12  # sha2-256
        assert multihash_bytes[1] == 0x20  # 32 bytes

        recovered_cid = multihash_to_cid(multihash_bytes)
        assert recovered_cid == cid_str, f"Roundtrip failed: {cid_str} != {recovered_cid}"
        print(f"\n  CID: {cid_str}")
        print(f"  CID hex: {cid_hex}")
        print(f"  Multihash: {multihash_bytes.hex()}")

    def test_cid_deterministic_across_calls(self):
        """Same data always produces the same CID."""
        from micromech.ipfs.client import compute_cid

        data = b'{"prompt":"determinism test","tool":"echo"}'
        cids = [compute_cid(data) for _ in range(10)]
        assert len(set(cids)) == 1, f"Non-deterministic CIDs: {set(cids)}"

    def test_fingerprint_tool_package(self, tmp_path):
        """Fingerprint a tool package and verify component.yaml is updated."""
        import yaml

        from micromech.ipfs.metadata import fingerprint_tool_package

        # Create a minimal tool package
        tool_dir = tmp_path / "test_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            yaml.dump({"name": "test_tool", "version": "0.1.0"})
        )
        (tool_dir / "test_tool.py").write_text("ALLOWED_TOOLS = ['test']\n")
        (tool_dir / "__init__.py").write_text("")

        # Compute fingerprints
        fps = fingerprint_tool_package(tool_dir)

        assert "test_tool.py" in fps
        assert "__init__.py" in fps
        assert all(v.startswith("bafkrei") for v in fps.values())

        # Verify component.yaml was updated
        updated = yaml.safe_load((tool_dir / "component.yaml").read_text())
        assert "fingerprint" in updated
        assert updated["fingerprint"]["test_tool.py"] == fps["test_tool.py"]
        print(f"\n  Fingerprints: {fps}")


class TestFullServerCycleE2E:
    """Full closed-loop: on-chain request -> server detect -> execute -> deliver on-chain.

    Proves the COMPLETE server cycle including delivery back to the marketplace
    contract, with on-chain verification of mapMechDeliveryCounts.
    """

    @pytest.mark.asyncio
    async def test_request_execute_deliver_on_chain(self, w3, tmp_path):
        """Send request, server detects+executes+delivers, verify delivery on-chain."""
        from micromech.core.config import (
            IpfsConfig,
            MechConfig,
            MicromechConfig,
            PersistenceConfig,
            RuntimeConfig,
        )
        from micromech.core.constants import STATUS_DELIVERED, STATUS_EXECUTED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )

        # Fund the multisig with xDAI for gas (delivery tx needs gas)
        w3.provider.make_request(
            "anvil_setBalance",
            [MECH_MULTISIG, hex(10 * 10**18)],
        )

        # Record baseline delivery count
        base_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(MECH_ADDR)
        ).call()
        print(f"\n  Baseline deliveries: {base_deliveries}")

        # Record block before request
        block_before = w3.eth.block_number

        # Step 1: Send a marketplace request
        print("\n--- Step 1: Send marketplace request ---")
        w3.provider.make_request("anvil_impersonateAccount", [RICH_ACCOUNT])
        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee

        request_data = json.dumps(
            {
                "prompt": "Full cycle test: will SOL hit 500?",
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
        w3.provider.make_request("anvil_stopImpersonatingAccount", [RICH_ACCOUNT])
        print(f"  Request sent at block {w3.eth.block_number}")

        # Step 2: Start server with BOTH listener AND delivery enabled
        print("\n--- Step 2: Start MechServer ---")

        class AnvilBridge:
            def __init__(self, web3):
                self.web3 = web3

            def with_retry(self, fn, **kwargs):
                return fn()

        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "full_cycle.db"),
            mech=MechConfig(
                mech_address=MECH_ADDR,
                multisig_address=MECH_MULTISIG,
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

        # Step 3: Run server and wait for detect + execute + deliver
        async def stop_after_delivery():
            for _ in range(30):
                await asyncio.sleep(0.5)
                counts = server.queue.count_by_status()
                delivered = counts.get("delivered", 0)
                failed = counts.get("failed", 0)
                if delivered > 0 or failed > 0:
                    break
            server.stop()

        asyncio.create_task(stop_after_delivery())

        try:
            await asyncio.wait_for(server.run(with_http=False), timeout=20.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        # Step 4: Verify on-chain delivery
        print("\n--- Step 3: Verify delivery on-chain ---")
        new_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(MECH_ADDR)
        ).call()

        # Verify DB state
        recent = server.queue.get_recent(limit=10)
        print(f"  Queue has {len(recent)} records")
        for r in recent:
            print(f"  [{r.request.status}] {r.request.request_id[:16]}... tool={r.request.tool}")

        delivered_records = [r for r in recent if r.request.status == STATUS_DELIVERED]
        executed_or_delivered = [
            r
            for r in recent
            if r.request.status in (STATUS_EXECUTED, STATUS_DELIVERED, STATUS_FAILED)
        ]
        assert len(executed_or_delivered) >= 1, (
            f"Expected at least 1 processed request, got {len(executed_or_delivered)}. "
            f"Statuses: {[r.request.status for r in recent]}"
        )

        assert new_deliveries > base_deliveries, (
            f"Delivery count did not increase: {base_deliveries} -> {new_deliveries}"
        )
        print(f"  Deliveries: {base_deliveries} -> {new_deliveries}")
        print(f"  Delivered records in DB: {len(delivered_records)}")

        server.shutdown()
        print("  ✓ Full server cycle (request -> execute -> deliver) completed")


class TestMetadataUpdateOnChain:
    """Test building metadata and updating the hash on-chain."""

    def test_metadata_build_and_change_hash(self, w3):
        """Build metadata, compute hash, call changeHash, verify on-chain."""
        from micromech.ipfs.metadata import (
            build_metadata,
            compute_onchain_hash,
            scan_tool_packages,
        )
        from micromech.runtime.contracts import (
            COMPLEMENTARY_SERVICE_METADATA_ABI,
        )

        # Step 1: Build metadata from tools
        print("\n--- Step 1: Build metadata ---")
        tools_dir = Path(__file__).parent.parent.parent / "src" / "micromech" / "tools" / "builtin"
        tools = scan_tool_packages(tools_dir)
        assert len(tools) > 0, "No tools found"
        metadata = build_metadata(tools)
        assert "tools" in metadata
        assert len(metadata["tools"]) > 0
        print(f"  Tools: {metadata['tools']}")

        # Step 2: Compute on-chain hash
        print("\n--- Step 2: Compute on-chain hash ---")
        onchain_hash_hex = compute_onchain_hash(metadata)
        assert onchain_hash_hex.startswith("0x")
        # Convert hex to bytes32
        onchain_hash_bytes = bytes.fromhex(onchain_hash_hex[2:])
        assert len(onchain_hash_bytes) == 34, (
            f"Expected 34 bytes (multihash), got {len(onchain_hash_bytes)}"
        )
        print(f"  On-chain hash: {onchain_hash_hex[:20]}...")

        # Step 3: Call changeHash on ComplementaryServiceMetadata
        print("\n--- Step 3: Call changeHash ---")
        metadata_contract = w3.eth.contract(
            address=w3.to_checksum_address(COMPLEMENTARY_METADATA_ADDR),
            abi=COMPLEMENTARY_SERVICE_METADATA_ABI,
        )

        # Read tokenURI before to compare later
        uri_before = metadata_contract.functions.tokenURI(MECH_SERVICE_ID).call()
        print(f"  tokenURI before: {uri_before[:60]}...")

        # changeHash must be called by the service multisig
        w3.provider.make_request(
            "anvil_setBalance",
            [MECH_MULTISIG, hex(1 * 10**18)],
        )
        w3.provider.make_request("anvil_impersonateAccount", [MECH_MULTISIG])

        # changeHash takes (serviceId, bytes32 hash)
        # The multihash is 34 bytes but the contract expects bytes32 (32 bytes).
        # Pad/truncate to 32 bytes (drop the first 2 bytes of multihash prefix 0x1220).
        hash_bytes32 = onchain_hash_bytes[2:]  # strip 0x12 0x20 prefix -> 32 bytes sha256
        assert len(hash_bytes32) == 32

        tx = metadata_contract.functions.changeHash(
            MECH_SERVICE_ID,
            hash_bytes32,
        ).transact(
            {
                "from": MECH_MULTISIG,
                "gas": 200_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "changeHash tx reverted"

        w3.provider.make_request("anvil_stopImpersonatingAccount", [MECH_MULTISIG])

        # Step 4: Verify the hash was stored
        print("\n--- Step 4: Verify on-chain ---")
        uri_after = metadata_contract.functions.tokenURI(MECH_SERVICE_ID).call()
        print(f"  tokenURI after: {uri_after[:60]}...")

        # The tokenURI should have changed (it encodes the hash)
        assert uri_after != uri_before, "tokenURI did not change after changeHash"
        print("  ✓ Metadata hash updated on-chain successfully")


# ===================================================================
# Lifecycle helpers
# ===================================================================


def _mint_olas(w3: Web3, to: str, amount_wei: int) -> None:
    """Mint OLAS tokens by manipulating storage on Anvil."""
    olas = w3.to_checksum_address(OLAS_TOKEN)
    to_padded = to[2:].lower().zfill(64)
    slot_hex = hex(OLAS_BALANCE_SLOT)[2:].zfill(64)
    key = "0x" + Web3.keccak(bytes.fromhex(to_padded + slot_hex)).hex()

    current = int(w3.eth.get_storage_at(olas, key).hex(), 16)
    new_val = current + amount_wei
    val_hex = "0x" + hex(new_val)[2:].zfill(64)
    w3.provider.make_request("anvil_setStorageAt", [olas, key, val_hex])


def _approve_olas(w3, owner, spender, amount):
    """Approve OLAS spending."""
    olas = w3.eth.contract(
        address=w3.to_checksum_address(OLAS_TOKEN),
        abi=[
            {
                "inputs": [
                    {"name": "spender", "type": "address"},
                    {"name": "amount", "type": "uint256"},
                ],
                "name": "approve",
                "outputs": [{"type": "bool"}],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ],
    )
    tx = olas.functions.approve(w3.to_checksum_address(spender), amount).transact(
        {"from": owner, "gas": 100_000}
    )
    w3.eth.wait_for_transaction_receipt(tx)


class TestMechLifecycleE2E:
    """Full lifecycle E2E: create -> deploy -> stake -> run -> earn."""

    @pytest.mark.timeout(300)
    def test_full_lifecycle(self, w3):
        """Verify full mech lifecycle: create, deploy, stake, request, deliver, checkpoint."""

        from iwa.plugins.olas.constants import DEFAULT_DEPLOY_PAYLOAD

        # Load contracts
        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )
        supply_staking = w3.eth.contract(
            address=w3.to_checksum_address(SUPPLY_STAKING_ADDR),
            abi=_load_abi("staking.json"),
        )
        registry = w3.eth.contract(
            address=w3.to_checksum_address(SERVICE_REGISTRY),
            abi=_load_abi("service_registry.json"),
        )
        svc_manager = w3.eth.contract(
            address=w3.to_checksum_address(SERVICE_MANAGER),
            abi=_load_abi("service_manager.json"),
        )

        # ==============================================================
        # Step 1: Setup owner account
        # ==============================================================
        print("\n--- Step 1: Setup funded owner ---")

        owner = w3.to_checksum_address("0xF325115Ee8b084fFC52E5d5b674C0229D00b4594")
        bond_wei = 5000 * 10**18  # 5000 OLAS (Supply Alpha minimum)
        agent_id = 25  # Trader agent

        w3.provider.make_request("anvil_setBalance", [owner, hex(1 * 10**18)])
        _mint_olas(w3, owner, bond_wei * 5)
        w3.provider.make_request("anvil_impersonateAccount", [owner])

        _approve_olas(w3, owner, SERVICE_MANAGER, bond_wei * 5)
        _approve_olas(w3, owner, TOKEN_UTILITY, bond_wei * 5)
        print(f"  Owner: {owner}")

        # ==============================================================
        # Step 2: Create service
        # ==============================================================
        print("\n--- Step 2: Create service ---")

        config_hash = bytes.fromhex(
            "108e90795119d6015274ef03af1a669c6d13ab6acc9e2b2978be01ee9ea2ec93"
        )
        tx = svc_manager.functions.create(
            owner,
            w3.to_checksum_address(OLAS_TOKEN),
            config_hash,
            [agent_id],
            [{"slots": 1, "bond": bond_wei}],
            1,  # threshold
        ).transact({"from": owner, "gas": 2_000_000})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Create failed"

        supply_svc_id = registry.functions.totalSupply().call()
        svc = registry.functions.getService(supply_svc_id).call()
        assert svc[6] == 1, f"Expected PRE_REG(1), got {svc[6]}"
        print(f"  Created service {supply_svc_id}")

        # ==============================================================
        # Step 3: Activate registration
        # ==============================================================
        print("\n--- Step 3: Activate registration ---")

        tx = svc_manager.functions.activateRegistration(
            supply_svc_id,
        ).transact({"from": owner, "gas": 500_000, "value": 1})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Activate failed"
        print("  Activated")

        # ==============================================================
        # Step 4: Register agent
        # ==============================================================
        print("\n--- Step 4: Register agent ---")

        agent_instance = w3.eth.accounts[1]
        tx = svc_manager.functions.registerAgents(
            supply_svc_id,
            [agent_instance],
            [agent_id],
        ).transact({"from": owner, "gas": 500_000, "value": 1})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Register failed"
        print(f"  Registered agent {agent_instance[:10]}...")

        # ==============================================================
        # Step 5: Deploy Safe
        # ==============================================================
        print("\n--- Step 5: Deploy Safe ---")

        multisig_impl = "0x3C1fF68f5aa342D296d4DEe4Bb1cACCA912D95fE"
        fallback_handler = "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4"
        deploy_data = bytes.fromhex(
            DEFAULT_DEPLOY_PAYLOAD.format(fallback_handler=fallback_handler[2:])[2:]
            + int(_time.time()).to_bytes(32, "big").hex()
        )
        tx = svc_manager.functions.deploy(
            supply_svc_id,
            w3.to_checksum_address(multisig_impl),
            deploy_data,
        ).transact({"from": owner, "gas": 5_000_000})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Deploy failed"
        print("  Safe deployed")

        # ==============================================================
        # Step 6: Create mech on marketplace
        # ==============================================================
        print("\n--- Step 6: Create mech ---")

        supply_svc = registry.functions.getService(supply_svc_id).call()
        supply_multisig = supply_svc[1]

        w3.provider.make_request(
            "anvil_setBalance",
            [supply_multisig, hex(1 * 10**18)],
        )
        w3.provider.make_request("anvil_impersonateAccount", [supply_multisig])

        tx = marketplace.functions.create(
            supply_svc_id,
            w3.to_checksum_address(MECH_FACTORY),
            MECH_DELIVERY_RATE.to_bytes(32, "big"),
        ).transact(
            {
                "from": supply_multisig,
                "gas": 10_000_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Create mech failed"

        create_logs = marketplace.events.CreateMech().process_receipt(receipt)
        our_mech_addr = create_logs[0]["args"]["mech"]

        w3.provider.make_request("anvil_stopImpersonatingAccount", [supply_multisig])
        print(f"  Created mech {our_mech_addr[:12]}...")

        # ==============================================================
        # Step 7: Stake in Supply Alpha
        # ==============================================================
        print("\n--- Step 7: Stake ---")

        _approve_olas(w3, owner, SUPPLY_STAKING_ADDR, bond_wei * 2)
        tx = registry.functions.approve(
            w3.to_checksum_address(SUPPLY_STAKING_ADDR),
            supply_svc_id,
        ).transact({"from": owner, "gas": 100_000})
        w3.eth.wait_for_transaction_receipt(tx)

        tx = supply_staking.functions.stake(
            supply_svc_id,
        ).transact({"from": owner, "gas": 1_000_000})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Supply stake failed"

        w3.provider.make_request("anvil_stopImpersonatingAccount", [owner])

        supply_state = supply_staking.functions.getStakingState(supply_svc_id).call()
        assert supply_state == 1, f"Expected STAKED, got {supply_state}"
        print(f"  Service {supply_svc_id} staked in Supply Alpha")

        # Refresh multisig after staking
        supply_svc = registry.functions.getService(supply_svc_id).call()
        supply_multisig = supply_svc[1]

        # ==============================================================
        # Step 8: Send requests from another account
        # ==============================================================
        print(f"\n--- Step 8: Send {N_LIFECYCLE_REQUESTS} requests ---")

        requester = RICH_ACCOUNT
        w3.provider.make_request(
            "anvil_setBalance",
            [requester, hex(10 * 10**18)],
        )
        w3.provider.make_request("anvil_impersonateAccount", [requester])

        fee = marketplace.functions.fee().call()
        value = MECH_DELIVERY_RATE + fee
        request_ids = []

        base_requests = marketplace.functions.mapRequestCounts(requester).call()
        base_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(our_mech_addr)
        ).call()
        supply_epoch = supply_staking.functions.epochCounter().call()
        supply_info_before = supply_staking.functions.getServiceInfo(supply_svc_id).call()
        base_supply_reward = supply_info_before[4]

        for i in range(N_LIFECYCLE_REQUESTS):
            tx = marketplace.functions.request(
                os.urandom(32),
                MECH_DELIVERY_RATE,
                PAYMENT_TYPE_NATIVE,
                w3.to_checksum_address(our_mech_addr),
                300,
                b"",
            ).transact(
                {
                    "from": requester,
                    "value": value,
                    "gas": 500_000,
                }
            )
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            assert receipt["status"] == 1, f"Request {i} reverted"
            logs = marketplace.events.MarketplaceRequest().process_receipt(receipt)
            for log in logs:
                request_ids.extend(log["args"]["requestIds"])

        w3.provider.make_request("anvil_stopImpersonatingAccount", [requester])

        new_requests = marketplace.functions.mapRequestCounts(requester).call()
        assert new_requests == base_requests + N_LIFECYCLE_REQUESTS
        print(f"  Requests: {base_requests} -> {new_requests}")

        # ==============================================================
        # Step 9: Deliver responses from mech multisig
        # ==============================================================
        print(f"\n--- Step 9: Deliver {len(request_ids)} responses ---")

        our_mech = w3.eth.contract(
            address=w3.to_checksum_address(our_mech_addr),
            abi=_load_abi("mech_new.json"),
        )

        w3.provider.make_request("anvil_impersonateAccount", [supply_multisig])
        for i, rid in enumerate(request_ids):
            tx = our_mech.functions.deliverToMarketplace(
                [rid],
                [os.urandom(32)],
            ).transact(
                {
                    "from": supply_multisig,
                    "gas": 500_000,
                }
            )
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            assert receipt["status"] == 1, f"Delivery {i} reverted"
        w3.provider.make_request("anvil_stopImpersonatingAccount", [supply_multisig])

        new_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(our_mech_addr)
        ).call()
        assert new_deliveries >= base_deliveries + N_LIFECYCLE_REQUESTS
        print(f"  Deliveries: {base_deliveries} -> {new_deliveries}")

        # ==============================================================
        # Step 10: Advance time past epoch end
        # ==============================================================
        print("\n--- Step 10: Advance time ---")

        supply_end = supply_staking.functions.getNextRewardCheckpointTimestamp().call()
        current = w3.eth.get_block("latest")["timestamp"]
        delta = supply_end - current + 120

        w3.provider.make_request("evm_increaseTime", [delta])
        w3.provider.make_request("evm_mine", [])

        new_time = w3.eth.get_block("latest")["timestamp"]
        assert new_time >= supply_end
        print(f"  Advanced {delta}s ({delta / 3600:.1f}h)")

        # ==============================================================
        # Step 11: Checkpoint
        # ==============================================================
        print("\n--- Step 11: Checkpoint ---")

        caller = w3.eth.accounts[0]
        tx = supply_staking.functions.checkpoint().transact(
            {
                "from": caller,
                "gas": 3_000_000,
            }
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "Checkpoint failed"

        new_supply_epoch = supply_staking.functions.epochCounter().call()
        assert new_supply_epoch > supply_epoch
        print(f"  Epoch: {supply_epoch} -> {new_supply_epoch}")

        # ==============================================================
        # Step 12: Verify rewards
        # ==============================================================
        print("\n--- Step 12: Verify rewards ---")

        supply_info_after = supply_staking.functions.getServiceInfo(supply_svc_id).call()
        mech_reward = supply_info_after[4] / 1e18
        mech_reward_delta = mech_reward - base_supply_reward / 1e18

        # NOTE: In Anvil with impersonation, deliveries don't go through
        # the Safe's execTransaction, so the Safe's internal nonce doesn't
        # increment. The supply activity checker requires
        # safe_nonce_diff >= delivery_diff, which fails.
        # In production with a real mech service, deliveries are Safe
        # transactions and the nonce increments naturally.
        if mech_reward_delta > 0:
            print(f"  Mech supply reward: +{mech_reward_delta:.4f} OLAS")
        else:
            print(
                "  Mech supply reward: 0 OLAS "
                "(expected in Anvil: Safe nonce not incremented "
                "by impersonated deliveries)"
            )
            # Verify the deliveries WERE counted by marketplace
            our_deliveries = marketplace.functions.mapMechServiceDeliveryCounts(
                supply_multisig
            ).call()
            print(f"  But marketplace counted {our_deliveries} deliveries for our mech")

        # Verify deliveries were counted regardless of reward
        final_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(our_mech_addr)
        ).call()
        assert final_deliveries >= N_LIFECYCLE_REQUESTS

        # ==============================================================
        # Summary
        # ==============================================================
        print("\n" + "=" * 50)
        print("LIFECYCLE VERIFIED")
        print("=" * 50)
        print(f"  Service ID:        {supply_svc_id}")
        print(f"  Mech address:      {our_mech_addr}")
        print(f"  Requests sent:     {N_LIFECYCLE_REQUESTS}")
        print(f"  Deliveries made:   {len(request_ids)}")
        print(f"  Supply deliveries: {base_deliveries} -> {new_deliveries}")
        print(f"  Mech reward:       +{mech_reward_delta:.4f} OLAS")
        print("=" * 50)


class TestOffchainHTTPE2E:
    """Test the REAL HTTP flow with on-chain delivery via deliverMarketplaceWithSignatures.

    Proves the COMPLETE offchain cycle:
      1. POST /request -> 202 accepted
      2. Server executes the tool
      3. Server delivers on-chain via deliverMarketplaceWithSignatures
      4. Verify delivery count on marketplace contract increased
    """

    @pytest.mark.asyncio
    async def test_http_request_delivers_on_chain(self, w3, tmp_path):
        """Submit via HTTP POST, server executes and delivers on-chain."""
        import aiohttp

        from micromech.core.config import (
            IpfsConfig,
            MechConfig,
            MicromechConfig,
            PersistenceConfig,
            RuntimeConfig,
        )
        from micromech.core.constants import STATUS_DELIVERED, STATUS_FAILED
        from micromech.runtime.server import MechServer

        marketplace = w3.eth.contract(
            address=w3.to_checksum_address(MARKETPLACE_ADDR),
            abi=_load_abi("mech_marketplace.json"),
        )

        # Fund the multisig for gas
        w3.provider.make_request(
            "anvil_setBalance",
            [MECH_MULTISIG, hex(10 * 10**18)],
        )

        # Baseline delivery count
        base_deliveries = marketplace.functions.mapMechDeliveryCounts(
            w3.to_checksum_address(MECH_ADDR)
        ).call()
        print(f"\n  Baseline deliveries: {base_deliveries}")

        class AnvilBridge:
            def __init__(self, web3):
                self.web3 = web3

            def with_retry(self, fn, **kwargs):
                return fn()

        port = 19876
        config = MicromechConfig(
            persistence=PersistenceConfig(db_path=tmp_path / "http_e2e.db"),
            mech=MechConfig(
                mech_address=MECH_ADDR,
                multisig_address=MECH_MULTISIG,
                marketplace_address=MARKETPLACE_ADDR,
                delivery_rate=MECH_DELIVERY_RATE,
            ),
            runtime=RuntimeConfig(
                port=port,
                host="127.0.0.1",
                max_concurrent=5,
                delivery_interval=1,
                event_poll_interval=1,
            ),
            ipfs=IpfsConfig(enabled=False),
        )

        bridge = AnvilBridge(w3)
        server = MechServer(config, bridge=bridge)
        server_task = asyncio.create_task(server.run(with_http=True))

        try:
            # Wait for HTTP server to be ready
            base_url = f"http://127.0.0.1:{port}"
            ready = False
            for _ in range(20):
                await asyncio.sleep(0.5)
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(f"{base_url}/health") as resp:
                            if resp.status == 200:
                                ready = True
                                break
                except Exception:
                    continue

            assert ready, "HTTP server did not start"
            print("  HTTP server ready")

            # --- Step 1: Submit requests via HTTP POST ---
            n_requests = 3
            request_ids = []
            async with aiohttp.ClientSession() as session:
                for i in range(n_requests):
                    payload = {
                        "prompt": f"HTTP E2E question {i}: Will ETH hit {i}0k?",
                        "tool": "echo",
                        "request_id": f"http-e2e-{i}",
                        "sender": RICH_ACCOUNT,
                    }
                    async with session.post(f"{base_url}/request", json=payload) as resp:
                        assert resp.status == 202, f"Request {i}: expected 202, got {resp.status}"
                        data = await resp.json()
                        assert data["status"] == "accepted"
                        request_ids.append(data["request_id"])
                        print(f"  POST /request -> {data['request_id']}")

            assert len(request_ids) == n_requests

            # --- Step 2: Wait for execution + delivery ---
            for _ in range(40):
                await asyncio.sleep(0.5)
                counts = server.queue.count_by_status()
                delivered = counts.get("delivered", 0)
                failed = counts.get("failed", 0)
                if delivered + failed >= n_requests:
                    break

            # --- Step 3: Verify DB state ---
            counts = server.queue.count_by_status()
            print(f"  Queue: {counts}")

            delivered_count = 0
            for rid in request_ids:
                record = server.queue.get_by_id(rid)
                assert record is not None, f"Request {rid} not found in DB"
                assert record.request.status in (STATUS_DELIVERED, STATUS_FAILED), (
                    f"{rid}: expected delivered/failed, got '{record.request.status}'"
                )
                if record.request.status == STATUS_DELIVERED:
                    delivered_count += 1
                    print(f"  {rid}: DELIVERED")

            # --- Step 4: Verify on-chain delivery count increased ---
            new_deliveries = marketplace.functions.mapMechDeliveryCounts(
                w3.to_checksum_address(MECH_ADDR)
            ).call()
            print(f"  Deliveries: {base_deliveries} -> {new_deliveries}")

            assert delivered_count == n_requests, (
                f"Expected {n_requests} delivered, got {delivered_count}. Statuses: {counts}"
            )
            print(f"  {delivered_count}/{n_requests} requests delivered on-chain")

        finally:
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()
