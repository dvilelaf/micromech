"""E2E test: FULL mech cycle — request -> execute -> deliver -> verify on-chain.

Proves the complete MechServer lifecycle against Anvil forks:

  1. Connect to running Anvil forks (gnosis:18545, base:18546)
  2. Use an existing deployed mech from the multichain test infrastructure
  3. Send a request on-chain to the marketplace
  4. Start MechServer with listener + executor + delivery
  5. Wait for the request to be picked up, executed, and delivered
  6. Verify the Deliver event appears on-chain
  7. Verify the delivered data is valid (CID format or parseable JSON)

Run:
  # Start Anvil forks first
  just anvil-fork

  # Run the test
  uv run pytest tests/integration/test_full_cycle_e2e.py -v -s
"""

import asyncio
import json
import os
import time as _time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from web3 import Web3

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS, STATUS_DELIVERED, STATUS_EXECUTED
from micromech.core.models import MechRequest
from micromech.runtime.contracts import load_marketplace_abi, load_mech_abi
from micromech.runtime.server import MechServer

# ---------------------------------------------------------------------------
# Per-chain infrastructure (mirrors test_multichain_e2e.py)
# ---------------------------------------------------------------------------

CHAIN_INFRA: dict[str, dict[str, Any]] = {
    "gnosis": {
        "anvil_env": "ANVIL_GNOSIS",
        "default_port": 18545,
        "chain_id": 100,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f",
        "olas_balance_slot": 3,
        "service_registry": "0x9338b5153AE39BB89f50468E608eD9d764B755fD",
        "service_manager": "0x068a4f0946cF8c7f9C1B58a3b5243Ac8843bf473",
        "token_utility": "0xa45E64d13A30a51b91ae0eb182e88a40e9b18eD8",
        "safe_impl": "0x3C1fF68f5aa342D296d4DEe4Bb1cACCA912D95fE",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0xe1CB04A0fA36DdD16a06ea828007E35e1a3cBC37",
    },
    "base": {
        "anvil_env": "ANVIL_BASE",
        "default_port": 18546,
        "chain_id": 8453,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0x54330d28ca3357F294334BDC454a032e7f353416",
        "olas_balance_slot": 0,
        "service_registry": "0x3C1fF68f5aa342D296d4DEe4Bb1cACCA912D95fE",
        "service_manager": "0x1262136cac6a06A782DC94eb3a3dF0b4d09FF6A6",
        "token_utility": "0x34C895f302D0b5cf52ec0Edd3945321EB0f83dd5",
        "safe_impl": "0x22bE6fDcd3e29851B29b512F714C328A00A96B83",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x4200000000000000000000000000000000000006",
    },
}

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)


# ---------------------------------------------------------------------------
# Helpers (reused from multichain test)
# ---------------------------------------------------------------------------

def _load_abi(name: str) -> list:
    """Load ABI from iwa package."""
    try:
        from importlib.resources import files
        abi_dir = files("iwa.plugins.olas.contracts.abis")
        return json.loads(abi_dir.joinpath(name).read_text())
    except Exception:
        abi_file = (
            Path("/media/david/DATA/repos/iwa/src/iwa")
            / "plugins/olas/contracts/abis" / name
        )
        return json.loads(abi_file.read_text())


class _FakeKeyStorage:
    """Stub key_storage so DeliveryManager's wallet check passes."""

    def get_address_by_tag(self, tag: str) -> None:
        return None


class _FakeWallet:
    """Stub wallet so DeliveryManager doesn't skip delivery on Anvil."""

    def __init__(self) -> None:
        self.key_storage = _FakeKeyStorage()


class AnvilBridge:
    """Minimal bridge for Anvil testing — provides web3, wallet stub, with_retry.

    The wallet stub makes DeliveryManager's wallet check pass so it proceeds
    to the impersonation path (which works on Anvil with auto-impersonate).
    """

    def __init__(self, web3: Web3):
        self.web3 = web3
        self.wallet = _FakeWallet()

    def with_retry(self, fn: Any, **kwargs: Any) -> Any:
        return fn()


def _get_anvil_url(chain_name: str) -> str:
    info = CHAIN_INFRA[chain_name]
    return os.environ.get(info["anvil_env"], f"http://localhost:{info['default_port']}")


def _connect(chain_name: str) -> Web3 | None:
    url = _get_anvil_url(chain_name)
    try:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 15}))
        if not w3.is_connected():
            return None
        if w3.eth.chain_id != CHAIN_INFRA[chain_name]["chain_id"]:
            return None
        try:
            from web3.middleware import ExtraDataToPOAMiddleware
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        except ImportError:
            try:
                from web3.middleware import ExtraDataLengthMiddleware
                w3.middleware_onion.inject(ExtraDataLengthMiddleware, layer=0)
            except ImportError:
                pass
        return w3
    except Exception:
        return None


def _fund_native(w3: Web3, address: str, amount_eth: int = 100) -> None:
    w3.provider.make_request(
        "anvil_setBalance",
        [Web3.to_checksum_address(address), hex(amount_eth * 10**18)],
    )


def _mint_olas(w3: Web3, chain_name: str, to: str, amount_wei: int) -> None:
    """Mint OLAS by manipulating storage slot."""
    infra = CHAIN_INFRA[chain_name]
    olas = Web3.to_checksum_address(infra["olas_token"])
    slot = infra["olas_balance_slot"]
    to_padded = to.lower()[2:].zfill(64)
    slot_hex = hex(slot)[2:].zfill(64)
    key = "0x" + Web3.keccak(bytes.fromhex(to_padded + slot_hex)).hex()
    current = int(w3.eth.get_storage_at(olas, key).hex(), 16)
    new_val = current + amount_wei
    val_hex = "0x" + hex(new_val)[2:].zfill(64)
    w3.provider.make_request("anvil_setStorageAt", [olas, key, val_hex])


def _approve_olas(w3: Web3, chain_name: str, owner: str, spender: str, amount: int) -> None:
    infra = CHAIN_INFRA[chain_name]
    abi = [{"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}], "name": "approve", "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"}]  # noqa: E501
    olas = w3.eth.contract(address=Web3.to_checksum_address(infra["olas_token"]), abi=abi)
    tx = olas.functions.approve(Web3.to_checksum_address(spender), amount).transact(
        {"from": owner, "gas": 100_000}
    )
    w3.eth.wait_for_transaction_receipt(tx)


def _deploy_mech_for_chain(
    w3: Web3, chain_name: str,
) -> dict[str, Any]:
    """Deploy a full mech service on Anvil: create -> activate -> register -> deploy -> create mech.

    Returns dict with service_id, mech_address, multisig_address.
    """
    from iwa.plugins.olas.constants import DEFAULT_DEPLOY_PAYLOAD

    infra = CHAIN_INFRA[chain_name]
    addrs = CHAIN_DEFAULTS[chain_name]

    marketplace_abi = _load_abi("mech_marketplace.json")
    registry_abi = _load_abi("service_registry.json")
    svc_manager_abi = _load_abi("service_manager.json")

    marketplace = w3.eth.contract(
        address=Web3.to_checksum_address(addrs["marketplace"]),
        abi=marketplace_abi,
    )
    registry = w3.eth.contract(
        address=Web3.to_checksum_address(infra["service_registry"]),
        abi=registry_abi,
    )

    # Discover authorized ServiceManager
    mgr_abi = [{"inputs": [], "name": "manager", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"}]  # noqa: E501
    mgr_lookup = w3.eth.contract(
        address=Web3.to_checksum_address(infra["service_registry"]), abi=mgr_abi
    )
    actual_sm_addr = mgr_lookup.functions.manager().call()
    svc_manager = w3.eth.contract(
        address=Web3.to_checksum_address(actual_sm_addr),
        abi=svc_manager_abi,
    )

    owner = Web3.to_checksum_address("0xF325115Ee8b084fFC52E5d5b674C0229D00b4594")
    bond_wei = 5000 * 10**18
    agent_id = 1

    _fund_native(w3, owner, 10)
    _mint_olas(w3, chain_name, owner, bond_wei * 5)
    w3.provider.make_request("anvil_impersonateAccount", [owner])

    _approve_olas(w3, chain_name, owner, actual_sm_addr, bond_wei * 5)
    _approve_olas(w3, chain_name, owner, infra["token_utility"], bond_wei * 5)

    # Create service
    config_hash = bytes.fromhex(
        "108e90795119d6015274ef03af1a669c6d13ab6acc9e2b2978be01ee9ea2ec93"
    )
    tx = svc_manager.functions.create(
        owner,
        Web3.to_checksum_address(infra["olas_token"]),
        config_hash,
        [agent_id],
        [{"slots": 1, "bond": bond_wei}],
        1,
    ).transact({"from": owner, "gas": 2_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx)
    assert receipt["status"] == 1, f"create service failed on {chain_name}"

    svc_id = registry.functions.totalSupply().call()

    # Activate
    tx = svc_manager.functions.activateRegistration(svc_id).transact(
        {"from": owner, "gas": 500_000, "value": 1}
    )
    r = w3.eth.wait_for_transaction_receipt(tx)
    assert r["status"] == 1

    # Register agent
    agent_instance = w3.eth.accounts[1]
    tx = svc_manager.functions.registerAgents(
        svc_id, [agent_instance], [agent_id]
    ).transact({"from": owner, "gas": 500_000, "value": 1})
    r = w3.eth.wait_for_transaction_receipt(tx)
    assert r["status"] == 1

    # Deploy Safe
    deploy_data = bytes.fromhex(
        DEFAULT_DEPLOY_PAYLOAD.format(
            fallback_handler=infra["safe_fallback"][2:]
        )[2:] + int(_time.time()).to_bytes(32, "big").hex()
    )
    tx = svc_manager.functions.deploy(
        svc_id,
        Web3.to_checksum_address(infra["safe_impl"]),
        deploy_data,
    ).transact({"from": owner, "gas": 5_000_000})
    r = w3.eth.wait_for_transaction_receipt(tx)
    assert r["status"] == 1

    svc = registry.functions.getService(svc_id).call()
    multisig = svc[1]

    # Create mech
    _fund_native(w3, multisig, 5)
    w3.provider.make_request("anvil_impersonateAccount", [multisig])

    tx = marketplace.functions.create(
        svc_id,
        Web3.to_checksum_address(addrs["factory"]),
        infra["delivery_rate"].to_bytes(32, "big"),
    ).transact({"from": multisig, "gas": 10_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx)
    assert receipt["status"] == 1

    create_logs = marketplace.events.CreateMech().process_receipt(receipt)
    mech_addr = create_logs[0]["args"]["mech"]
    w3.provider.make_request("anvil_stopImpersonatingAccount", [multisig])
    w3.provider.make_request("anvil_stopImpersonatingAccount", [owner])

    print(f"  Deployed: svc_id={svc_id}, mech={mech_addr}, multisig={multisig}")
    return {
        "service_id": svc_id,
        "mech_address": mech_addr,
        "multisig_address": multisig,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def anvil_chain():
    """Connect to the first available Anvil fork — fresh snapshot per test."""
    for chain_name in ("gnosis", "base"):
        w3 = _connect(chain_name)
        if w3 is not None:
            snapshot_id = w3.provider.make_request("evm_snapshot", [])["result"]
            print(f"\n  Connected to {chain_name} Anvil: "
                  f"chain_id={w3.eth.chain_id}, block={w3.eth.block_number}")
            yield chain_name, w3
            w3.provider.make_request("evm_revert", [snapshot_id])
            return
    pytest.skip("No Anvil forks running on ports 18545 or 18546")


@pytest.fixture
def deployed_mech(anvil_chain: tuple[str, Web3]) -> dict[str, Any]:
    """Deploy a fresh mech service on the Anvil fork (per test)."""
    chain_name, w3 = anvil_chain
    result = _deploy_mech_for_chain(w3, chain_name)
    result["chain_name"] = chain_name
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFullMechCycle:
    """FULL cycle: request on-chain -> MechServer picks up -> executes -> delivers -> verify."""

    @pytest.mark.timeout(120)
    @pytest.mark.asyncio
    async def test_request_execute_deliver_verify(
        self,
        anvil_chain: tuple[str, Web3],
        deployed_mech: dict[str, Any],
        tmp_path: Path,
    ):
        """End-to-end: send on-chain request, MechServer delivers, verify Deliver event."""
        chain_name, w3 = anvil_chain
        mech_addr = deployed_mech["mech_address"]
        multisig = deployed_mech["multisig_address"]
        addrs = CHAIN_DEFAULTS[chain_name]
        infra = CHAIN_INFRA[chain_name]

        marketplace_abi = _load_abi("mech_marketplace.json")
        marketplace = w3.eth.contract(
            address=Web3.to_checksum_address(addrs["marketplace"]),
            abi=marketplace_abi,
        )

        # ── Step 1: Send request on-chain ──────────────────────────────
        requester = Web3.to_checksum_address(infra["rich_account"])
        _fund_native(w3, requester, 10)
        w3.provider.make_request("anvil_impersonateAccount", [requester])

        fee = marketplace.functions.fee().call()
        value = infra["delivery_rate"] + fee

        request_data = json.dumps({
            "prompt": "Will ETH hit 100k by 2030?",
            "tool": "echo",
        }).encode()

        block_before_request = w3.eth.block_number

        tx = marketplace.functions.request(
            request_data,
            infra["delivery_rate"],
            PAYMENT_TYPE_NATIVE,
            Web3.to_checksum_address(mech_addr),
            300,
            b"",
        ).transact({"from": requester, "value": value, "gas": 500_000})
        receipt = w3.eth.wait_for_transaction_receipt(tx)
        assert receipt["status"] == 1, "On-chain request transaction reverted"

        req_logs = marketplace.events.MarketplaceRequest().process_receipt(receipt)
        assert len(req_logs) >= 1, "No MarketplaceRequest events emitted"
        request_id_bytes = req_logs[0]["args"]["requestIds"][0]
        request_id_hex = request_id_bytes.hex()

        w3.provider.make_request("anvil_stopImpersonatingAccount", [requester])
        print(f"\n  Step 1: Request sent on-chain. ID: {request_id_hex[:24]}...")

        # ── Step 2: Configure and start MechServer ─────────────────────
        # Enable impersonation of the multisig for delivery
        w3.provider.make_request("anvil_impersonateAccount", [multisig])

        chain_cfg = ChainConfig(
            chain=chain_name,
            mech_address=mech_addr,
            multisig_address=multisig,
            marketplace_address=addrs["marketplace"],
            factory_address=addrs["factory"],
            staking_address=addrs["staking"],
            delivery_rate=infra["delivery_rate"],
        )

        config = MicromechConfig(
            chains={chain_name: chain_cfg},
        )

        bridge = AnvilBridge(w3)

        # Mock get_service_info to provide multisig_address for delivery
        svc_info = {
            "service_id": deployed_mech.get("service_id", 1),
            "service_key": f"{chain_name}:{deployed_mech.get('service_id', 1)}",
            "multisig_address": multisig,
        }
        svc_patch = patch("micromech.core.bridge.get_service_info", return_value=svc_info)
        svc_patch.start()

        server = MechServer(config, bridges={chain_name: bridge})

        # Set listener start block to just before our request
        server.listeners[chain_name]._last_block = block_before_request

        print("  Step 2: MechServer configured, starting...")

        server_task = asyncio.create_task(
            server.run(with_http=False, register_signals=False)
        )

        try:
            # ── Step 3: Wait for request pickup + execution ────────────
            max_wait_execution = 30  # seconds
            poll_interval = 1.0
            elapsed = 0.0

            while elapsed < max_wait_execution:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                record = server.queue.get_by_id(request_id_hex)
                if record and record.request.status in (STATUS_EXECUTED, STATUS_DELIVERED):
                    break

            record = server.queue.get_by_id(request_id_hex)
            assert record is not None, (
                f"Request {request_id_hex[:16]}... never appeared in queue "
                f"after {max_wait_execution}s"
            )
            assert record.request.status in (STATUS_EXECUTED, STATUS_DELIVERED), (
                f"Expected executed/delivered, got '{record.request.status}'"
            )
            assert record.result is not None, "No tool result produced"
            assert record.result.output, "Empty tool output"
            assert record.result.error is None, f"Tool error: {record.result.error}"

            # Verify result contains valid echo response
            result_data = json.loads(record.result.output)
            assert "result" in result_data, f"Missing 'result' in: {result_data}"
            print(f"  Step 3: Request executed in {elapsed:.1f}s. "
                  f"result={str(result_data['result'])[:40]}")

            # ── Step 4: Wait for on-chain delivery ─────────────────────
            max_wait_delivery = 30  # seconds
            elapsed = 0.0

            while elapsed < max_wait_delivery:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                record = server.queue.get_by_id(request_id_hex)
                if record and record.request.status == STATUS_DELIVERED:
                    break

            record = server.queue.get_by_id(request_id_hex)
            assert record is not None
            assert record.request.status == STATUS_DELIVERED, (
                f"Expected 'delivered', got '{record.request.status}' "
                f"after {max_wait_delivery}s"
            )
            print(f"  Step 4: Delivery confirmed in DB after {elapsed:.1f}s")

            # ── Step 5: Verify delivery transaction on-chain ─────────
            # Get the delivery tx hash from the DB record
            assert record.response is not None, "No response record after delivery"
            delivery_tx_hash = record.response.delivery_tx_hash
            assert delivery_tx_hash, "No delivery tx hash in DB"

            tx_receipt = w3.eth.get_transaction_receipt(delivery_tx_hash)
            assert tx_receipt["status"] == 1, (
                f"Delivery tx {delivery_tx_hash} reverted on-chain"
            )
            assert len(tx_receipt["logs"]) > 0, "Delivery tx emitted no events"
            print(f"  Step 5: Delivery tx confirmed on-chain: "
                  f"{delivery_tx_hash[:24]}... "
                  f"({len(tx_receipt['logs'])} event logs)")

            # Verify via MarketplaceDelivery event (emitted by deliverToMarketplace)
            from web3._utils.events import EventLogErrorFlags
            mp_delivery_events = marketplace.events.MarketplaceDelivery().process_receipt(
                tx_receipt, errors=EventLogErrorFlags.Discard,
            )
            assert len(mp_delivery_events) >= 1, (
                f"No MarketplaceDelivery events in delivery tx "
                f"(total logs: {len(tx_receipt['logs'])})"
            )

            # Verify our request ID is in the delivered set
            found_in_delivery = False
            for evt in mp_delivery_events:
                delivered_ids = evt["args"].get("requestIds", [])
                for rid in delivered_ids:
                    rid_hex = rid.hex() if isinstance(rid, bytes) else str(rid)
                    if rid_hex == request_id_hex:
                        found_in_delivery = True
                        break
                if found_in_delivery:
                    break

            assert found_in_delivery, (
                f"Request {request_id_hex[:16]}... not found in "
                f"MarketplaceDelivery events"
            )
            print(f"  Step 5b: MarketplaceDelivery event confirmed for our request")

            # ── Step 6: Verify marketplace delivery count on-chain ─────
            delivery_count = marketplace.functions.mapMechDeliveryCounts(
                Web3.to_checksum_address(mech_addr)
            ).call()
            assert delivery_count >= 1, (
                f"Expected delivery count >= 1, got {delivery_count}"
            )
            print(f"  Step 6: Marketplace delivery count = {delivery_count}")

            # ── Step 7: Verify IPFS hash if available ──────────────────
            if record.response.ipfs_hash:
                # Verify it looks like a valid CID hex
                ipfs_hash = record.response.ipfs_hash
                assert ipfs_hash.startswith("f0155"), (
                    f"Unexpected IPFS hash format: {ipfs_hash[:20]}"
                )
                print(f"  Step 7: IPFS CID hex stored: {ipfs_hash[:30]}...")
            else:
                print("  Step 7: No IPFS hash (IPFS push may be disabled/unreachable)")

            print(f"\n  FULL CYCLE TEST PASSED on {chain_name}")

        finally:
            svc_patch.stop()
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()
            w3.provider.make_request("anvil_stopImpersonatingAccount", [multisig])

    @pytest.mark.timeout(120)
    @pytest.mark.asyncio
    async def test_multiple_requests_all_delivered(
        self,
        anvil_chain: tuple[str, Web3],
        deployed_mech: dict[str, Any],
        tmp_path: Path,
    ):
        """Send 3 requests, verify ALL are picked up, executed, and delivered."""
        chain_name, w3 = anvil_chain
        mech_addr = deployed_mech["mech_address"]
        multisig = deployed_mech["multisig_address"]
        addrs = CHAIN_DEFAULTS[chain_name]
        infra = CHAIN_INFRA[chain_name]
        n_requests = 3

        marketplace_abi = _load_abi("mech_marketplace.json")
        marketplace = w3.eth.contract(
            address=Web3.to_checksum_address(addrs["marketplace"]),
            abi=marketplace_abi,
        )

        # Send N requests
        requester = Web3.to_checksum_address(infra["rich_account"])
        _fund_native(w3, requester, 10)
        w3.provider.make_request("anvil_impersonateAccount", [requester])

        fee = marketplace.functions.fee().call()
        block_before = w3.eth.block_number

        request_ids: list[str] = []
        for i in range(n_requests):
            value = infra["delivery_rate"] + fee
            request_data = json.dumps({
                "prompt": f"Multi-request test #{i}: Will BTC hit {i+1}00k?",
                "tool": "echo",
            }).encode()

            tx = marketplace.functions.request(
                request_data,
                infra["delivery_rate"],
                PAYMENT_TYPE_NATIVE,
                Web3.to_checksum_address(mech_addr),
                300,
                b"",
            ).transact({"from": requester, "value": value, "gas": 500_000})
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            assert receipt["status"] == 1, f"Request {i} reverted"

            logs = marketplace.events.MarketplaceRequest().process_receipt(receipt)
            for log in logs:
                for rid in log["args"]["requestIds"]:
                    rid_hex = rid.hex() if isinstance(rid, bytes) else str(rid)
                    request_ids.append(rid_hex)

        w3.provider.make_request("anvil_stopImpersonatingAccount", [requester])
        assert len(request_ids) == n_requests
        print(f"\n  Sent {n_requests} requests on-chain")

        # Start MechServer
        w3.provider.make_request("anvil_impersonateAccount", [multisig])

        chain_cfg = ChainConfig(
            chain=chain_name,
            mech_address=mech_addr,
            multisig_address=multisig,
            marketplace_address=addrs["marketplace"],
            factory_address=addrs["factory"],
            staking_address=addrs["staking"],
            delivery_rate=infra["delivery_rate"],
        )

        config = MicromechConfig(
            chains={chain_name: chain_cfg},
        )

        bridge = AnvilBridge(w3)

        svc_info2 = {
            "service_id": deployed_mech.get("service_id", 1),
            "service_key": f"{chain_name}:{deployed_mech.get('service_id', 1)}",
            "multisig_address": multisig,
        }
        svc_patch2 = patch("micromech.core.bridge.get_service_info", return_value=svc_info2)
        svc_patch2.start()

        server = MechServer(config, bridges={chain_name: bridge})
        server.listeners[chain_name]._last_block = block_before

        server_task = asyncio.create_task(
            server.run(with_http=False, register_signals=False)
        )

        try:
            # Wait for all requests to be delivered
            max_wait = 60
            poll_interval = 2.0
            elapsed = 0.0

            while elapsed < max_wait:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                all_delivered = True
                for rid in request_ids:
                    record = server.queue.get_by_id(rid)
                    if not record or record.request.status != STATUS_DELIVERED:
                        all_delivered = False
                        break

                if all_delivered:
                    break

            # Verify all delivered
            delivered_count = 0
            for rid in request_ids:
                record = server.queue.get_by_id(rid)
                assert record is not None, f"Request {rid[:16]}... not in queue"
                if record.request.status == STATUS_DELIVERED:
                    delivered_count += 1
                else:
                    print(f"  WARNING: {rid[:16]}... status={record.request.status}")

            assert delivered_count == n_requests, (
                f"Only {delivered_count}/{n_requests} delivered after {elapsed:.0f}s"
            )

            # Verify delivery on-chain via mapMechDeliveryCounts
            on_chain_count = marketplace.functions.mapMechDeliveryCounts(
                Web3.to_checksum_address(mech_addr)
            ).call()
            assert on_chain_count >= n_requests, (
                f"On-chain delivery count {on_chain_count} < {n_requests} expected"
            )

            # Also verify each delivery tx receipt is valid
            for rid in request_ids:
                record = server.queue.get_by_id(rid)
                assert record.response is not None, f"{rid[:16]}... has no response"
                tx_hash = record.response.delivery_tx_hash
                assert tx_hash, f"{rid[:16]}... has no tx hash"
                receipt = w3.eth.get_transaction_receipt(tx_hash)
                assert receipt["status"] == 1, f"Delivery tx for {rid[:16]}... reverted"

            print(f"  ALL {n_requests} requests delivered on-chain in {elapsed:.0f}s "
                  f"(mapMechDeliveryCounts={on_chain_count})")

        finally:
            svc_patch2.stop()
            server.stop()
            try:
                await asyncio.wait_for(server_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            server.shutdown()
            w3.provider.make_request("anvil_stopImpersonatingAccount", [multisig])
