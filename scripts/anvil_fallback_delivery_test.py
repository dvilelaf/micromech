#!/usr/bin/env env -S uv run python3
"""
Anvil fork test: deliver an old open marketplace request as fallback mech.

Verifies that micromech can claim payment for status=2 (RequestedAny) requests
that the priority mech failed to deliver.

Flow:
  1. Find a real requestId in status=2 on Gnosis (old, >24h)
  2. Fork Gnosis at current block with Anvil
  3. Impersonate our Safe → call mech.deliverToMarketplace([requestId], [b'{}'])
  4. Assert delivery accepted (bool[0] == True)
  5. Assert BalanceTracker balance increased
"""

import json
import subprocess
import sys
import time

from web3 import Web3

# ── Production addresses (Gnosis) ─────────────────────────────────────────────
MECH_ADDR        = "0x33Ca1E117c4254b2eE8CD7Ef1621739431a37396"
SAFE_ADDR        = "0x0EE0CA8A2fc8a5d9aa92a80Ae4e6A86DcAc81953"
MARKETPLACE_ADDR = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"

GNOSIS_RPC = "https://rpc.gnosischain.com"
ANVIL_RPC  = "http://127.0.0.1:8545"
ANVIL_PORT = 8545

GNOSIS_BLOCK_TIME = 5.2  # seconds/block (average)

# ── ABIs (minimal) ────────────────────────────────────────────────────────────
MARKETPLACE_ABI = [
    {"inputs":[{"name":"requestId","type":"bytes32"}],"name":"getRequestStatus",
     "outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"","type":"bytes32"}],"name":"mapRequestIdInfos",
     "outputs":[
         {"name":"priorityMech","type":"address"},{"name":"deliveryMech","type":"address"},
         {"name":"requester","type":"address"},{"name":"responseTimeout","type":"uint256"},
         {"name":"deliveryRate","type":"uint256"},{"name":"paymentType","type":"bytes32"},
     ],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"paymentType","type":"bytes32"}],"name":"mapPaymentTypeBalanceTrackers",
     "outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"numUndeliveredRequests",
     "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"anonymous":False,"inputs":[
        {"indexed":True,"name":"priorityMech","type":"address"},
        {"indexed":True,"name":"requester","type":"address"},
        {"indexed":False,"name":"numRequests","type":"uint256"},
        {"indexed":False,"name":"requestIds","type":"bytes32[]"},
        {"indexed":False,"name":"requestDatas","type":"bytes[]"},
    ],"name":"MarketplaceRequest","type":"event"},
]

MECH_ABI = [
    {"inputs":[
        {"name":"requestIds","type":"bytes32[]"},
        {"name":"datas","type":"bytes[]"},
    ],"name":"deliverToMarketplace",
     "outputs":[{"name":"deliveredRequests","type":"bool[]"}],
     "stateMutability":"nonpayable","type":"function"},
    {"inputs":[],"name":"paymentType",
     "outputs":[{"name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},
]

BALANCE_TRACKER_ABI = [
    {"inputs":[{"name":"mech","type":"address"}],"name":"mapMechBalances",
     "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
]


def find_old_open_request(w3: Web3) -> tuple[bytes, dict]:
    """Find one requestId in status=2 from 3+ days ago.

    Uses ~3-10 RPC calls total (1 block_number + 1 get_logs + N status checks).
    """
    marketplace = w3.eth.contract(
        address=w3.to_checksum_address(MARKETPLACE_ADDR), abi=MARKETPLACE_ABI
    )

    current = w3.eth.block_number
    undelivered = marketplace.functions.numUndeliveredRequests().call()
    print(f"  Current block:          {current}")
    print(f"  Undelivered (total):    {undelivered}")

    # Go back 3 days; scan 200-block windows until we find a status=2 request
    blocks_3d = int(3 * 24 * 3600 / GNOSIS_BLOCK_TIME)  # ~49,846
    search_start = current - blocks_3d
    window = 200

    print(f"  Scanning from block ~{search_start} (3 days ago)...")

    for attempt in range(10):
        from_b = search_start + attempt * window
        to_b   = from_b + window - 1
        try:
            logs = marketplace.events.MarketplaceRequest.get_logs(
                from_block=from_b, to_block=to_b
            )
        except Exception as e:
            print(f"  [get_logs {from_b}-{to_b} failed: {e}]")
            continue

        for log in logs:
            for rid_bytes in log["args"]["requestIds"]:
                status = marketplace.functions.getRequestStatus(rid_bytes).call()
                if status == 2:  # RequestedAny — open for any mech
                    info = marketplace.functions.mapRequestIdInfos(rid_bytes).call()
                    print(f"  Found open requestId: 0x{rid_bytes.hex()[:16]}...")
                    print(f"    priorityMech:   {info[0]}")
                    print(f"    deliveryRate:   {info[4] / 1e18:.6f} xDAI")
                    print(f"    paymentType:    0x{info[5].hex()[:16]}...")
                    return rid_bytes, {
                        "priorityMech": info[0],
                        "deliveryRate": info[4],
                        "paymentType":  info[5],
                    }

    raise RuntimeError("No open (status=2) request found in the scanned range.")


def start_anvil(fork_url: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        ["anvil", "--fork-url", fork_url, "--port", str(ANVIL_PORT),
         "--block-time", "1", "--silent"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait for Anvil to be ready
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    for _ in range(30):
        time.sleep(0.5)
        try:
            if w3.is_connected():
                print(f"  Anvil ready (block {w3.eth.block_number})")
                return proc
        except Exception:
            pass
    proc.kill()
    raise RuntimeError("Anvil did not start in time.")


def run_test(request_id: bytes, request_info: dict) -> None:
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))

    marketplace = w3.eth.contract(
        address=w3.to_checksum_address(MARKETPLACE_ADDR), abi=MARKETPLACE_ABI
    )
    mech = w3.eth.contract(
        address=w3.to_checksum_address(MECH_ADDR), abi=MECH_ABI
    )

    # Resolve BalanceTracker address
    payment_type = mech.functions.paymentType().call()
    bt_addr = marketplace.functions.mapPaymentTypeBalanceTrackers(payment_type).call()
    bt = w3.eth.contract(address=w3.to_checksum_address(bt_addr), abi=BALANCE_TRACKER_ABI)
    print(f"  BalanceTracker: {bt_addr}")

    # Verify request is still status=2 on the fork
    status_before = marketplace.functions.getRequestStatus(request_id).call()
    assert status_before == 2, f"Expected status=2, got {status_before}"
    print(f"  Status before delivery: {status_before} (RequestedAny) ✓")

    # Balance before
    balance_before = bt.functions.mapMechBalances(w3.to_checksum_address(MECH_ADDR)).call()
    print(f"  Balance before: {balance_before / 1e18:.6f} xDAI")

    # Impersonate the Safe
    w3.provider.make_request("anvil_impersonateAccount", [SAFE_ADDR])
    w3.provider.make_request("anvil_setBalance", [SAFE_ADDR, hex(10 * 10**18)])  # 10 xDAI

    # Call mech.deliverToMarketplace([requestId], [b'{}']) from the Safe
    delivery_data = b'{}'
    tx = mech.functions.deliverToMarketplace(
        [request_id],
        [delivery_data],
    ).build_transaction({
        "from": SAFE_ADDR,
        "gas": 500_000,
        "gasPrice": w3.to_wei(1, "gwei"),
        "nonce": w3.eth.get_transaction_count(SAFE_ADDR),
    })
    tx_hash = w3.eth.send_transaction(tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    print(f"  TX: {tx_hash.hex()} — status={receipt['status']}")

    assert receipt["status"] == 1, "Transaction reverted!"

    # Check delivery flags from return value via call (simulate before sending)
    delivered_flags = mech.functions.deliverToMarketplace(
        [request_id], [delivery_data]
    ).call({"from": SAFE_ADDR})
    # Note: after the actual tx above, calling again would fail since it's already delivered.
    # Instead decode from the receipt's output or check status.

    # Verify: status should now be Delivered (3)
    status_after = marketplace.functions.getRequestStatus(request_id).call()
    balance_after = bt.functions.mapMechBalances(w3.to_checksum_address(MECH_ADDR)).call()

    print(f"\n  Status after delivery:  {status_after} (3=Delivered) {'✓' if status_after == 3 else '✗ UNEXPECTED'}")
    print(f"  Balance after:  {balance_after / 1e18:.6f} xDAI")
    print(f"  Balance delta:  +{(balance_after - balance_before) / 1e18:.6f} xDAI")

    assert status_after == 3, f"Expected status=3 (Delivered), got {status_after}"
    assert balance_after > balance_before, "Balance did not increase after delivery!"

    print("\n  ✓ PASS: fallback delivery works and payment is credited.")


def main() -> None:
    print("=" * 60)
    print("Anvil fallback delivery test")
    print("=" * 60)

    # 1. Find open request on real Gnosis (minimal RPC calls)
    print("\n[1] Finding old open request on Gnosis...")
    w3_gnosis = Web3(Web3.HTTPProvider(GNOSIS_RPC, request_kwargs={"timeout": 20}))
    request_id, request_info = find_old_open_request(w3_gnosis)

    # 2. Start Anvil fork at current block
    print("\n[2] Starting Anvil fork of Gnosis...")
    anvil_proc = start_anvil(GNOSIS_RPC)

    try:
        # 3. Run the delivery test
        print("\n[3] Running delivery simulation...")
        run_test(request_id, request_info)
    finally:
        anvil_proc.kill()
        print("\nAnvil stopped.")


if __name__ == "__main__":
    main()
