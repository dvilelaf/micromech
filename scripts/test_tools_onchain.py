"""
Test on-chain: envía requests simultáneas por cada tool builtin usando la cuenta
master, espera la entrega on-chain (evento Deliver del mech), y verifica el resultado.

Todas las requests se envían en paralelo (threading) para verificar que el mech
procesa múltiples requests concurrentes sin congelarse.

Usage:
    MICROMECH_WALLET_PASSWORD=<password> uv run python scripts/test_tools_onchain.py
"""
import json
import os
import sys
import threading
import time
import uuid

if not os.environ.get("MICROMECH_WALLET_PASSWORD"):
    print("ERROR: MICROMECH_WALLET_PASSWORD env var not set", file=sys.stderr)
    sys.exit(1)

from web3 import Web3
from web3._utils.events import EventLogErrorFlags

from micromech.core.bridge import get_wallet
from micromech.core.config import MicromechConfig, register_plugin
from micromech.core.constants import IPFS_API_URL
from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid_hex
from micromech.runtime.contracts import load_marketplace_abi, load_mech_abi

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

TOOLS = {
    "echo":               "Will BTC reach 200k by 2027?",
    "prediction-offline": "Will ETH hit 5k by end of 2026?",
    "prediction-online":  "Will Solana flip Ethereum in market cap by 2028?",
    "local-llm":          "Explain blockchain in one sentence.",
}
RESPONSE_TIMEOUT = 300  # seconds on-chain parameter
WAIT_TIMEOUT     = 240  # seconds we wait for delivery

register_plugin()
w   = get_wallet()
cfg = MicromechConfig.load()

# Use public RPC to avoid QuikNode SSL issues in this script
w3  = Web3(Web3.HTTPProvider("https://rpc.gnosischain.com"))
cc  = cfg.chains["gnosis"]

mp_addr   = w3.to_checksum_address(cc.marketplace_address)
mech_addr = w3.to_checksum_address(cc.mech_address)
master    = w.key_storage.master_account.address

mp   = w3.eth.contract(address=mp_addr, abi=load_marketplace_abi())
mech = w3.eth.contract(address=mech_addr, abi=load_mech_abi())

fee   = mp.functions.fee().call()
value = cc.delivery_rate + fee

master_balance = w3.eth.get_balance(master) / 1e18
print("\n=== micromech on-chain tool test ===")
print(f"master:        {master}")
print(f"balance:       {master_balance:.4f} xDAI")
print(f"mech:          {mech_addr}")
print(f"marketplace:   {mp_addr}")
print(f"delivery_rate: {cc.delivery_rate/1e18:.4f} xDAI | fee: {fee/1e18:.4f} xDAI | total/req: {value/1e18:.4f} xDAI")
print(f"tools to test: {list(TOOLS.keys())}")
print(f"estimated cost: ~{len(TOOLS) * value / 1e18:.4f} xDAI\n")


def push_to_ipfs(prompt: str, tool: str) -> bytes:
    metadata = {
        "prompt": prompt,
        "tool": tool,
        "nonce": str(uuid.uuid4()),
        "schema_version": "2.0",
    }
    json_bytes = json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8")
    try:
        import requests as req_lib
        resp = req_lib.post(
            f"{IPFS_API_URL}/api/v0/add",
            files={"file": ("data", json_bytes, "application/octet-stream")},
            params={"pin": "true", "cid-version": "1"},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  [IPFS push warning: {e}]")
    cid_hex = compute_cid_hex(json_bytes)
    return cid_hex_to_multihash_bytes(cid_hex)


def send_request(tool: str, prompt: str) -> tuple[str | None, int]:
    """Returns (request_id_hex, tx_block) or (None, 0) on failure."""
    request_data = push_to_ipfs(prompt, tool)
    fn_call = mp.functions.request(
        request_data,
        cc.delivery_rate,
        PAYMENT_TYPE_NATIVE,
        mech_addr,
        RESPONSE_TIMEOUT,
        b"",
    )
    tx_dict = fn_call.build_transaction({
        "from": master,
        "value": value,
        "gas": 500_000,
        "gasPrice": w3.eth.gas_price,
        "nonce": w3.eth.get_transaction_count(master),
        "chainId": w3.eth.chain_id,
    })
    success, receipt_or_err = w.sign_and_send_transaction(tx_dict, master, "gnosis")
    if not success:
        return None, 0
    receipt = receipt_or_err
    if receipt.get("status") != 1:
        return None, 0

    parsed = mp.events.MarketplaceRequest().process_receipt(
        receipt, errors=EventLogErrorFlags.Discard
    )
    if not parsed:
        return None, receipt["blockNumber"]
    rid = parsed[0]["args"]["requestIds"][0]
    rid_hex = rid.hex() if isinstance(rid, bytes) else str(rid)
    return rid_hex, receipt["blockNumber"]


def _health_check() -> str:
    """Check mech HTTP health endpoint. Returns status string."""
    try:
        import requests as req_lib
        resp = req_lib.get("http://localhost:8090/api/health", timeout=5)
        if resp.status_code == 200:
            return f"OK ({resp.json().get('state', '?')})"
        return f"HTTP {resp.status_code}"
    except Exception as e:
        return f"UNREACHABLE ({e})"


def _send_worker(tool: str, prompt: str, result_list: list, lock: threading.Lock) -> None:
    """Thread worker: sends one request and appends result to result_list."""
    t0 = time.time()
    req_id, block = send_request(tool, prompt)
    elapsed = time.time() - t0
    with lock:
        if req_id:
            print(f"  [SENT {elapsed:.1f}s] {tool}: 0x{req_id[:16]}...  block {block}")
            result_list.append({
                "tool": tool, "prompt": prompt,
                "id": req_id, "block": block,
                "done": False, "t_send": elapsed,
            })
        else:
            print(f"  [FAIL  {elapsed:.1f}s] {tool}: send failed")


# Check mech health before sending
print(f"\n[HEALTH] mech before test: {_health_check()}")

# Send all requests in parallel
print(f"\n[SEND] Firing {len(TOOLS)} requests simultaneously...")
sent: list[dict] = []
lock = threading.Lock()
threads = [
    threading.Thread(target=_send_worker, args=(tool, prompt, sent, lock))
    for tool, prompt in TOOLS.items()
]
t_burst_start = time.time()
for t in threads:
    t.start()
for t in threads:
    t.join()
t_burst = time.time() - t_burst_start
print(f"  → All {len(sent)} requests submitted in {t_burst:.1f}s")

if not sent:
    print("\nNo requests sent. Aborting.")
    sys.exit(1)

print(f"\n[WAIT] Waiting for mech delivery (up to {WAIT_TIMEOUT}s)...")

# Poll on-chain for Deliver events from the mech contract
t_start    = time.time()
from_block = min(s["block"] for s in sent)

while True:
    pending = [s for s in sent if not s["done"]]
    if not pending:
        break
    elapsed = time.time() - t_start
    if elapsed > WAIT_TIMEOUT:
        print(f"  [TIMEOUT] still waiting for: {[s['tool'] for s in pending]}")
        break

    try:
        cur_block = w3.eth.block_number
        if cur_block > from_block:
            deliver_logs = mech.events.Deliver.get_logs(
                from_block=from_block, to_block=cur_block
            )
            for log in deliver_logs:
                rid_raw = log["args"]["requestId"]
                rid_hex = rid_raw.hex() if isinstance(rid_raw, bytes) else str(rid_raw)
                rid_hex_clean = rid_hex.lstrip("0x")
                for s in pending:
                    if s["id"].lstrip("0x").lower() == rid_hex_clean.lower():
                        raw_data = log["args"]["data"]
                        result_str = ""
                        try:
                            from micromech.core.constants import IPFS_GATEWAY
                            from micromech.ipfs.client import (
                                is_ipfs_multihash,
                                multihash_to_cid,
                            )
                            if is_ipfs_multihash(raw_data):
                                cid = multihash_to_cid(raw_data)
                                import requests as req_lib
                                r = req_lib.get(f"{IPFS_GATEWAY}{cid}", timeout=15)
                                r.raise_for_status()
                                result_data = r.json()
                                if isinstance(result_data.get("result"), str):
                                    try:
                                        result_data = json.loads(result_data["result"])
                                    except Exception:
                                        pass
                                result_str = json.dumps(result_data)[:200]
                            else:
                                result_str = raw_data.decode("utf-8", errors="replace")[:200]
                        except Exception as e:
                            result_str = f"(decode error: {e})"
                        s["done"]   = True
                        s["result"] = result_str
                        tx_hash = log["transactionHash"].hex()
                        print(f"  [DELIVERED] {s['tool']} — tx: {tx_hash[:20]}...")
                        print(f"              result: {result_str[:150]}")
            from_block = cur_block + 1
    except Exception as e:
        print(f"  [poll error: {e}]")

    # Periodic health check every ~30s
    if int(elapsed) % 30 == 0 and int(elapsed) > 0:
        print(f"  [HEALTH +{elapsed:.0f}s] mech: {_health_check()}")

    time.sleep(5)

# Summary
total_time = time.time() - t_burst_start
n_ok = sum(1 for s in sent if s.get("done"))

print("\n=== RESULTS ===")
for s in sent:
    status = "OK" if s.get("done") else "PENDING/TIMEOUT"
    result = s.get("result", "-")[:120]
    print(f"  [{status}] {s['tool']}: {result}")

print(f"\n  {n_ok}/{len(sent)} delivered in {total_time:.1f}s total")
print(f"  burst send time: {t_burst:.1f}s")
print(f"\n[HEALTH] mech after test:  {_health_check()}")
print(f"master balance after: {w3.eth.get_balance(master)/1e18:.4f} xDAI")
