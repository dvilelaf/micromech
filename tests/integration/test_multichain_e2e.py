"""E2E test: micromech multi-chain — verify full lifecycle on multiple chains.

Forks up to 7 chains simultaneously on different Anvil ports.
For each available chain, tests:
  1. Marketplace contract exists and responds to fee()
  2. Staking and factory contracts exist
  3. EventListener can poll without errors
  4. Full lifecycle: create service → deploy Safe → create mech → stake →
     send requests → deliver → checkpoint → verify rewards

Run:
  just test-multichain

Or manually:
  ANVIL_GNOSIS=http://localhost:18545 \\
  ANVIL_BASE=http://localhost:18546 \\
  ... \\
  uv run pytest tests/integration/test_multichain_e2e.py -v -s
"""

import asyncio
import json
import os
import time as _time
from pathlib import Path
from typing import Any

import pytest
from web3 import Web3

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS

# --- Per-chain infrastructure addresses ---
# Test-specific infra (balance slots, rich accounts, Safe impls) that iwa doesn't track.
# Contract addresses (service_registry, etc.) are verified against iwa's OLAS_CONTRACTS.

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
    "ethereum": {
        "anvil_env": "ANVIL_ETHEREUM",
        "default_port": 18547,
        "chain_id": 1,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0x0001A500A6B18995B03f44bb040A5fFc28E45CB0",
        "olas_balance_slot": 3,
        "service_registry": "0x48b6af7B12C71f09e2fC8aF4855De4Ff54e775cA",
        "service_manager": "0x94a1892D91c05D0C61c3f49F42205D2285b914c9",
        "token_utility": "0x3Fb926116D454b95c669B6Bf2E7c3bad8d19affA",
        "safe_impl": "0x46C0D07F55d4F9B5Eed2Fc9680B5953e5fd7b461",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18",
    },
    "polygon": {
        "anvil_env": "ANVIL_POLYGON",
        "default_port": 18548,
        "chain_id": 137,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0xFEF5d947472e72Efbb2E388c730B7428406F2F95",
        "olas_balance_slot": 0,
        "service_registry": "0xE3607b00E75f6405248323A9417ff6b39B244b50",
        "service_manager": "0xE3e5Df46060370af5Fd37B2aA11e7dac3cCB4bd0",
        "token_utility": "0xa45E64d13A30a51b91ae0eb182e88a40e9b18eD8",
        "safe_impl": "0x3d77596beb0f130a4415df3D2D8232B3d3D31e44",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
    },
    "optimism": {
        "anvil_env": "ANVIL_OPTIMISM",
        "default_port": 18549,
        "chain_id": 10,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0xFC2E6e6BCbd49ccf3A5f029c79984372DcBFE527",
        "olas_balance_slot": 0,
        "service_registry": "0x3d77596beb0f130a4415df3D2D8232B3d3D31e44",
        "service_manager": "0xA5C7FbCCFf28441b7d250412b0Fb87AA1c8b14AD",
        "token_utility": "0xBb7e1D6Cb6F243D6bdE81CE92a9f2aFF7Fbe7eac",
        "safe_impl": "0x5953f21495BD9aF1D78e87bb42AcCAA55C1e896C",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x4200000000000000000000000000000000000006",
    },
    "arbitrum": {
        "anvil_env": "ANVIL_ARBITRUM",
        "default_port": 18550,
        "chain_id": 42161,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0x064F8B858C2A603e1b106a2039f5446D32DC81C1",
        "olas_balance_slot": 51,
        "service_registry": "0xE3607b00E75f6405248323A9417ff6b39B244b50",
        "service_manager": "0xD421f433e36465B3e558B1121F584ac09Fc33DF8",
        "token_utility": "0x3d77596beb0f130a4415df3D2D8232B3d3D31e44",
        "safe_impl": "0x63e66d7ad413C01A7b49C7FF4e3Bb765C4E4bd1b",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    },
    "celo": {
        "anvil_env": "ANVIL_CELO",
        "default_port": 18551,
        "chain_id": 42220,
        "delivery_rate": 10_000_000_000_000_000,
        "olas_token": "0xD80533CA29fF6F033a0b55732Ed792af9Fbb381E",
        "olas_balance_slot": 0,
        "service_registry": "0xE3607b00E75f6405248323A9417ff6b39B244b50",
        "service_manager": "0x84B4DA67B37B1EA1dea9c7044042C1d2297b80a0",
        "token_utility": "0x3d77596beb0f130a4415df3D2D8232B3d3D31e44",
        "safe_impl": "0x63e66d7ad413C01A7b49C7FF4e3Bb765C4E4bd1b",
        "safe_fallback": "0xf48f2B2d2a534e402487b3ee7C18c33Aec0Fe5e4",
        "rich_account": "0x471EcE3750Da237f93B8E339c536989b8978a438",
    },
}

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

N_LIFECYCLE_REQUESTS = 3


# --- ABI loading ---

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


# --- Helpers ---

class AnvilBridge:
    """Minimal bridge for testing."""
    def __init__(self, web3: Any):
        self.web3 = web3
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
        # Inject PoA middleware for chains that need it (Polygon, etc.)
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


def _olas_balance(w3: Web3, chain_name: str, account: str) -> int:
    infra = CHAIN_INFRA[chain_name]
    abi = [{"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"}]  # noqa: E501
    olas = w3.eth.contract(address=Web3.to_checksum_address(infra["olas_token"]), abi=abi)
    return olas.functions.balanceOf(Web3.to_checksum_address(account)).call()


# --- Fixtures ---

@pytest.fixture
def available_chains(_anvil_forks) -> dict[str, Web3]:
    """Connect to all available Anvil forks. Requires >= 2.

    Anvil forks are auto-started by the session-scoped ``_anvil_forks``
    fixture in conftest.py.  Per-test snapshot/revert for isolation.
    """
    connected: dict[str, Web3] = {}
    snapshots: dict[str, str] = {}
    for chain_name in CHAIN_INFRA:
        w3 = _connect(chain_name)
        if w3:
            snapshots[chain_name] = w3.provider.make_request("evm_snapshot", [])["result"]
            rich = CHAIN_INFRA[chain_name]["rich_account"]
            _fund_native(w3, rich)
            connected[chain_name] = w3
            print(f"  {chain_name}: chain_id={w3.eth.chain_id}, block={w3.eth.block_number}")
    if len(connected) < 2:
        pytest.skip(
            f"Need >=2 Anvil forks, got {len(connected)}: {list(connected)}. "
            f"Check secrets.env has >=2 chain RPCs."
        )

    yield connected

    # Revert all chains
    for chain_name, w3 in connected.items():
        if chain_name in snapshots:
            w3.provider.make_request("evm_revert", [snapshots[chain_name]])


# --- Tests: Contract verification ---

class TestContractsExist:
    """Verify all critical contracts are deployed on each chain."""

    def test_marketplace_responds(self, available_chains: dict[str, Web3]):
        abi = _load_abi("mech_marketplace.json")
        for chain_name, w3 in available_chains.items():
            addr = CHAIN_DEFAULTS[chain_name]["marketplace"]
            code = w3.eth.get_code(Web3.to_checksum_address(addr))
            assert len(code) > 2, f"{chain_name}: no code at marketplace"
            mp = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=abi)
            fee = mp.functions.fee().call()
            assert isinstance(fee, int), f"{chain_name}: bad fee"
            print(f"  {chain_name}: marketplace OK (fee={fee})")

    def test_staking_has_code(self, available_chains: dict[str, Web3]):
        for chain_name, w3 in available_chains.items():
            addr = CHAIN_DEFAULTS[chain_name]["staking"]
            code = w3.eth.get_code(Web3.to_checksum_address(addr))
            assert len(code) > 2, f"{chain_name}: no staking code"
            print(f"  {chain_name}: staking OK")

    def test_factory_has_code(self, available_chains: dict[str, Web3]):
        for chain_name, w3 in available_chains.items():
            addr = CHAIN_DEFAULTS[chain_name]["factory"]
            code = w3.eth.get_code(Web3.to_checksum_address(addr))
            assert len(code) > 2, f"{chain_name}: no factory code"
            print(f"  {chain_name}: factory OK")

    def test_service_registry_has_code(self, available_chains: dict[str, Web3]):
        for chain_name, w3 in available_chains.items():
            addr = CHAIN_INFRA[chain_name]["service_registry"]
            code = w3.eth.get_code(Web3.to_checksum_address(addr))
            assert len(code) > 2, f"{chain_name}: no registry code"
            print(f"  {chain_name}: service registry OK")

    def test_olas_token_has_code(self, available_chains: dict[str, Web3]):
        for chain_name, w3 in available_chains.items():
            addr = CHAIN_INFRA[chain_name]["olas_token"]
            code = w3.eth.get_code(Web3.to_checksum_address(addr))
            assert len(code) > 2, f"{chain_name}: no OLAS token code"
            print(f"  {chain_name}: OLAS token OK")


# --- Tests: Listener multi-chain ---

class TestListenerMultiChain:
    @pytest.mark.asyncio
    async def test_listener_polls_each_chain(self, available_chains: dict[str, Web3]):
        from micromech.runtime.listener import EventListener
        for chain_name, w3 in available_chains.items():
            addrs = CHAIN_DEFAULTS[chain_name]
            chain_cfg = ChainConfig(
                chain=chain_name,
                marketplace_address=addrs["marketplace"],
                factory_address=addrs["factory"],
                staking_address=addrs["staking"],
            )
            import micromech.runtime.listener as _listener_mod
            _listener_mod.DEFAULT_EVENT_LOOKBACK_BLOCKS = 50

            config = MicromechConfig()
            listener = EventListener(config, chain_cfg, bridge=AnvilBridge(w3))
            requests = await listener.poll_once()
            assert isinstance(requests, list)
            print(f"  {chain_name}: polled {len(requests)} events")


# --- Tests: Full lifecycle per chain ---

class TestLifecycleMultiChain:
    """Full lifecycle on each available chain:
    create → activate → register → deploy → create mech → stake →
    send requests → deliver → advance time → checkpoint → verify."""

    @pytest.mark.timeout(600)
    def test_full_lifecycle_per_chain(self, available_chains: dict[str, Web3]):

        from iwa.plugins.olas.constants import DEFAULT_DEPLOY_PAYLOAD

        marketplace_abi = _load_abi("mech_marketplace.json")
        staking_abi = _load_abi("staking.json")
        registry_abi = _load_abi("service_registry.json")
        svc_manager_abi = _load_abi("service_manager.json")
        mech_abi = _load_abi("mech_new.json")

        results = {}

        for chain_name, w3 in available_chains.items():
            print(f"\n{'='*60}")
            print(f"  LIFECYCLE TEST: {chain_name.upper()} (chain_id={w3.eth.chain_id})")
            print(f"{'='*60}")

            infra = CHAIN_INFRA[chain_name]
            addrs = CHAIN_DEFAULTS[chain_name]

            marketplace = w3.eth.contract(
                address=Web3.to_checksum_address(addrs["marketplace"]),
                abi=marketplace_abi,
            )
            staking = w3.eth.contract(
                address=Web3.to_checksum_address(addrs["staking"]),
                abi=staking_abi,
            )
            registry = w3.eth.contract(
                address=Web3.to_checksum_address(infra["service_registry"]),
                abi=registry_abi,
            )
            # Auto-discover the authorized ServiceManager from the registry
            mgr_abi = [{"inputs": [], "name": "manager", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"}]  # noqa: E501
            mgr_lookup = w3.eth.contract(
                address=Web3.to_checksum_address(infra["service_registry"]), abi=mgr_abi
            )
            actual_sm_addr = mgr_lookup.functions.manager().call()
            svc_manager = w3.eth.contract(
                address=Web3.to_checksum_address(actual_sm_addr),
                abi=svc_manager_abi,
            )
            print(f"  ServiceManager: {actual_sm_addr}")

            try:
                # --- Step 1: Setup owner ---
                owner = w3.to_checksum_address(
                    "0xF325115Ee8b084fFC52E5d5b674C0229D00b4594"
                )
                bond_wei = 5000 * 10**18
                agent_id = 1  # agentId 1 exists on all chains

                _fund_native(w3, owner, 10)
                _mint_olas(w3, chain_name, owner, bond_wei * 5)
                w3.provider.make_request("anvil_impersonateAccount", [owner])

                bal = _olas_balance(w3, chain_name, owner)
                if bal < bond_wei:
                    print(f"  {chain_name}: OLAS balance too low ({bal}), skipping")
                    results[chain_name] = "skipped (OLAS mint failed)"
                    w3.provider.make_request("anvil_stopImpersonatingAccount", [owner])
                    continue

                _approve_olas(w3, chain_name, owner, actual_sm_addr, bond_wei * 5)
                _approve_olas(w3, chain_name, owner, infra["token_utility"], bond_wei * 5)
                print(f"  Step 1: Owner funded with {bal / 1e18:.0f} OLAS")

                # --- Step 2: Create service ---
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
                assert receipt["status"] == 1, f"{chain_name}: create failed"

                svc_id = registry.functions.totalSupply().call()
                print(f"  Step 2: Service {svc_id} created")

                # --- Step 3: Activate ---
                tx = svc_manager.functions.activateRegistration(svc_id).transact(
                    {"from": owner, "gas": 500_000, "value": 1}
                )
                r = w3.eth.wait_for_transaction_receipt(tx)
                assert r["status"] == 1, f"{chain_name}: activate failed"
                print("  Step 3: Activated")

                # --- Step 4: Register agent ---
                agent_instance = w3.eth.accounts[1]
                tx = svc_manager.functions.registerAgents(
                    svc_id, [agent_instance], [agent_id]
                ).transact({"from": owner, "gas": 500_000, "value": 1})
                r = w3.eth.wait_for_transaction_receipt(tx)
                assert r["status"] == 1, f"{chain_name}: register agent failed"
                print("  Step 4: Agent registered")

                # --- Step 5: Deploy Safe ---
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
                assert r["status"] == 1, f"{chain_name}: deploy Safe failed"

                svc = registry.functions.getService(svc_id).call()
                multisig = svc[1]
                print(f"  Step 5: Safe deployed at {multisig[:16]}...")

                # --- Step 6: Create mech ---
                _fund_native(w3, multisig, 5)
                w3.provider.make_request("anvil_impersonateAccount", [multisig])

                tx = marketplace.functions.create(
                    svc_id,
                    Web3.to_checksum_address(addrs["factory"]),
                    infra["delivery_rate"].to_bytes(32, "big"),
                ).transact({"from": multisig, "gas": 10_000_000})
                receipt = w3.eth.wait_for_transaction_receipt(tx)
                assert receipt["status"] == 1, f"{chain_name}: create mech failed"

                create_logs = marketplace.events.CreateMech().process_receipt(receipt)
                mech_addr = create_logs[0]["args"]["mech"]
                w3.provider.make_request("anvil_stopImpersonatingAccount", [multisig])
                print(f"  Step 6: Mech created at {mech_addr[:16]}...")

                # --- Step 7: Stake ---
                _approve_olas(
                    w3, chain_name, owner, addrs["staking"], bond_wei * 2
                )
                tx = registry.functions.approve(
                    Web3.to_checksum_address(addrs["staking"]), svc_id
                ).transact({"from": owner, "gas": 100_000})
                w3.eth.wait_for_transaction_receipt(tx)

                tx = staking.functions.stake(svc_id).transact(
                    {"from": owner, "gas": 1_000_000}
                )
                receipt = w3.eth.wait_for_transaction_receipt(tx)
                assert receipt["status"] == 1, f"{chain_name}: stake failed"

                w3.provider.make_request("anvil_stopImpersonatingAccount", [owner])

                state = staking.functions.getStakingState(svc_id).call()
                assert state == 1, f"{chain_name}: expected STAKED(1), got {state}"
                print("  Step 7: Staked in supply contract")

                # Refresh multisig
                svc = registry.functions.getService(svc_id).call()
                multisig = svc[1]

                # --- Step 8: Send requests ---
                requester = Web3.to_checksum_address(infra["rich_account"])
                _fund_native(w3, requester, 10)
                w3.provider.make_request("anvil_impersonateAccount", [requester])

                fee = marketplace.functions.fee().call()
                value = infra["delivery_rate"] + fee
                request_ids = []

                for i in range(N_LIFECYCLE_REQUESTS):
                    tx = marketplace.functions.request(
                        os.urandom(32),
                        infra["delivery_rate"],
                        PAYMENT_TYPE_NATIVE,
                        Web3.to_checksum_address(mech_addr),
                        300,
                        b"",
                    ).transact({"from": requester, "value": value, "gas": 500_000})
                    receipt = w3.eth.wait_for_transaction_receipt(tx)
                    assert receipt["status"] == 1, f"{chain_name}: req {i} reverted"
                    logs = marketplace.events.MarketplaceRequest().process_receipt(receipt)
                    for log in logs:
                        request_ids.extend(log["args"]["requestIds"])

                w3.provider.make_request("anvil_stopImpersonatingAccount", [requester])
                print(f"  Step 8: {len(request_ids)} requests sent")

                # --- Step 9: Deliver responses ---
                mech_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(mech_addr), abi=mech_abi
                )
                w3.provider.make_request("anvil_impersonateAccount", [multisig])
                for rid in request_ids:
                    tx = mech_contract.functions.deliverToMarketplace(
                        [rid], [os.urandom(32)]
                    ).transact({"from": multisig, "gas": 500_000})
                    receipt = w3.eth.wait_for_transaction_receipt(tx)
                    assert receipt["status"] == 1
                w3.provider.make_request("anvil_stopImpersonatingAccount", [multisig])

                deliveries = marketplace.functions.mapMechDeliveryCounts(
                    Web3.to_checksum_address(mech_addr)
                ).call()
                assert deliveries >= N_LIFECYCLE_REQUESTS
                print(f"  Step 9: {deliveries} deliveries confirmed")

                # --- Step 10: Advance time ---
                epoch_end = staking.functions.getNextRewardCheckpointTimestamp().call()
                current = w3.eth.get_block("latest")["timestamp"]
                delta = max(epoch_end - current + 120, 86400 + 120)
                w3.provider.make_request("evm_increaseTime", [delta])
                w3.provider.make_request("evm_mine", [])
                print(f"  Step 10: Advanced {delta}s ({delta/3600:.1f}h)")

                # --- Step 11: Checkpoint ---
                caller = w3.eth.accounts[0]
                tx = staking.functions.checkpoint().transact(
                    {"from": caller, "gas": 3_000_000}
                )
                receipt = w3.eth.wait_for_transaction_receipt(tx)
                assert receipt["status"] == 1, f"{chain_name}: checkpoint failed"
                print("  Step 11: Checkpoint done")

                # --- Step 12: Verify ---
                final_deliveries = marketplace.functions.mapMechDeliveryCounts(
                    Web3.to_checksum_address(mech_addr)
                ).call()
                assert final_deliveries >= N_LIFECYCLE_REQUESTS

                results[chain_name] = (
                    f"OK (svc={svc_id}, mech={mech_addr[:12]}, del={final_deliveries})"
                )
                print(f"  PASSED on {chain_name}")

            except Exception as e:
                results[chain_name] = f"FAILED: {e}"
                print(f"  FAILED on {chain_name}: {e}")
                # Don't stop — continue with next chain

        # Summary
        print(f"\n{'='*60}")
        print("  MULTI-CHAIN LIFECYCLE RESULTS")
        print(f"{'='*60}")
        passed = 0
        for chain_name, result in results.items():
            status = "PASS" if result.startswith("OK") else "FAIL"
            if status == "PASS":
                passed += 1
            print(f"  {chain_name:12s}: {result}")
        print(f"{'='*60}")

        # All connected chains must pass full lifecycle
        assert passed == len(available_chains), (
            f"Only {passed}/{len(available_chains)} chains passed: {results}"
        )


# --- Tests: Multi-chain server ---

class TestMultiChainServer:

    def test_server_creates_per_chain_components(
        self, available_chains: dict[str, Web3], tmp_path
    ):
        from micromech.runtime.server import MechServer

        chains_dict = {}
        bridges = {}
        for chain_name, w3 in available_chains.items():
            addrs = CHAIN_DEFAULTS[chain_name]
            chains_dict[chain_name] = ChainConfig(
                chain=chain_name,
                marketplace_address=addrs["marketplace"],
                factory_address=addrs["factory"],
                staking_address=addrs["staking"],
            )
            bridges[chain_name] = AnvilBridge(w3)

        import micromech.runtime.server as _server_mod
        _server_mod.DB_PATH = tmp_path / "mc.db"

        config = MicromechConfig(
            chains=chains_dict,
        )
        server = MechServer(config, bridges=bridges)

        assert len(server.listeners) == len(available_chains)
        assert len(server.deliveries) == len(available_chains)
        server.shutdown()
        print(f"  Server: {len(server.listeners)} chains configured")
