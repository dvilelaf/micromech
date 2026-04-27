#!/usr/bin/env env -S uv run python3
"""
Recover all open marketplace requests older than 24h.

Scans MarketplaceRequest events for status=2 (RequestedAny) entries and
delivers them in batches via mech.deliverToMarketplace, crediting payment
to the mech's BalanceTracker.

⚠️  IMPORTANT: All requests are delivered with an empty payload b"{}".
    Requesters receive a formal delivery with no content. This is intentional
    for incident recovery — the payment goes to the mech, requesters get
    acknowledged delivery. Only use this for recovery operations.

⚠️  IMPORTANT: Stop the production mech before running real delivery.
    Both use the same Safe. Concurrent signing causes nonce races (GS026).

NOTE: MECH_ADDR, SAFE_ADDR, MARKETPLACE_ADDR match data/config.yaml.
      Update both if the mech is redeployed.

Modes:
  --anvil-test          Fork Gnosis locally, process 15 requests, verify — no keys needed.
  --mode discover       Scan blocks, save open requestIds to checkpoint file. No TX.
  --mode deliver        Re-validate on-chain, deliver via Safe. Needs AGENT_PRIVATE_KEY.
  --mode all            discover + deliver in one pass. Needs AGENT_PRIVATE_KEY.

Usage:
  python scripts/recover_open_requests.py --anvil-test
  python scripts/recover_open_requests.py --mode discover [--checkpoint PATH]
  AGENT_PRIVATE_KEY=0x... python scripts/recover_open_requests.py --mode deliver

Rate limits (real mode): 2s between get_logs, 1s between status checks, 10s between TXs.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from web3 import Web3

# ── Addresses (Gnosis mainnet) ─────────────────────────────────────────────────
MECH_ADDR = "0x33Ca1E117c4254b2eE8CD7Ef1621739431a37396"
SAFE_ADDR = "0x0EE0CA8A2fc8a5d9aa92a80Ae4e6A86DcAc81953"
MARKETPLACE_ADDR = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"

GNOSIS_RPCS = [
    "https://rpc.gnosischain.com",
    "https://gnosis-rpc.publicnode.com",
    "https://rpc.ankr.com/gnosis",
]
ANVIL_RPC = "http://127.0.0.1:8545"
ANVIL_PORT = 8545

GNOSIS_BLOCK_TIME = 5.2
BLOCKS_24H = int(24 * 3600 / GNOSIS_BLOCK_TIME)
BLOCKS_3D = int(3 * 24 * 3600 / GNOSIS_BLOCK_TIME)

DELAY_LOGS = 2.0  # seconds between get_logs calls
DELAY_STATUS = 1.0  # seconds between getRequestStatus calls
DELAY_TX = 10.0  # seconds between Safe TX submissions
BATCH_SIZE = 20  # requestIds per deliverToMarketplace call
LOG_WINDOW = 1000  # blocks per get_logs window (Gnosis public RPCs accept 1000-2000)

DEFAULT_CHECKPOINT = Path.home() / ".local" / "share" / "micromech" / "recover.json"
DEFAULT_LOOKBACK_DAYS = 30

_HEX32_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_PRIVKEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")

# ── ABIs ───────────────────────────────────────────────────────────────────────
MARKETPLACE_ABI = [
    {
        "inputs": [{"name": "requestId", "type": "bytes32"}],
        "name": "getRequestStatus",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "paymentType", "type": "bytes32"}],
        "name": "mapPaymentTypeBalanceTrackers",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "priorityMech", "type": "address"},
            {"indexed": True, "name": "requester", "type": "address"},
            {"indexed": False, "name": "numRequests", "type": "uint256"},
            {"indexed": False, "name": "requestIds", "type": "bytes32[]"},
            {"indexed": False, "name": "requestDatas", "type": "bytes[]"},
        ],
        "name": "MarketplaceRequest",
        "type": "event",
    },
]

MECH_ABI = [
    {
        "inputs": [
            {"name": "requestIds", "type": "bytes32[]"},
            {"name": "datas", "type": "bytes[]"},
        ],
        "name": "deliverToMarketplace",
        "outputs": [{"name": "deliveredRequests", "type": "bool[]"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "paymentType",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
]

BALANCE_TRACKER_ABI = [
    {
        "inputs": [{"name": "mech", "type": "address"}],
        "name": "mapMechBalances",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


# ── Helpers ────────────────────────────────────────────────────────────────────


def b32_to_hex(b: bytes) -> str:
    return "0x" + b.hex()


def hex_to_b32(s: str) -> bytes:
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _valid_hex32(s: str) -> bool:
    return bool(_HEX32_RE.match(s))


def _validate_private_key(pk: str) -> None:
    if not _PRIVKEY_RE.match(pk):
        raise ValueError("must be 0x + 64 hex chars (32 bytes)")


def _confirm(prompt: str) -> bool:
    try:
        return input(f"{prompt} [y/N] ").strip().lower() == "y"
    except (EOFError, KeyboardInterrupt):
        return False


def load_checkpoint(path: Path) -> dict:
    """Load checkpoint JSON, sanitising entries to valid hex32 strings."""
    if path.exists():
        try:
            data = json.loads(path.read_text())
            data["open_requests"] = [h for h in data.get("open_requests", []) if _valid_hex32(h)]
            data["delivered"] = [h for h in data.get("delivered", []) if _valid_hex32(h)]
            return data
        except Exception as e:
            print(f"[warn] Checkpoint unreadable ({e}) — starting fresh")
    return {
        "open_requests": [],
        "delivered": [],
        "last_scanned_block": None,
        "scan_from_block": None,
    }


def save_checkpoint(path: Path, data: dict) -> None:
    """Write checkpoint with owner-only permissions (600)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    path.chmod(0o600)


def _connect(rpc_urls: list[str], timeout: int = 30) -> tuple[Web3, str]:
    """Connect to the first working RPC. Returns (w3, url)."""
    for url in rpc_urls:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": timeout}))
            if w3.is_connected():
                return w3, url
        except Exception:
            continue
    raise RuntimeError(f"All RPCs unreachable: {rpc_urls}")


def _make_marketplace(w3: Web3):
    return w3.eth.contract(address=w3.to_checksum_address(MARKETPLACE_ADDR), abi=MARKETPLACE_ABI)


def _revalidate_open(
    w3: Web3,
    request_ids: list[bytes],
    delay: float = DELAY_STATUS,
    rpc_urls: list[str] | None = None,
) -> list[bytes]:
    """Check status on-chain for each id. Returns only those still at status=2.

    If rpc_urls is provided, rotates to the next RPC on failure.
    """
    rpc_idx = 0
    marketplace = _make_marketplace(w3)
    still_open, skipped = [], 0
    for rid in request_ids:
        n_attempts = len(rpc_urls) if rpc_urls else 1
        for _ in range(n_attempts):
            try:
                if marketplace.functions.getRequestStatus(rid).call() == 2:
                    still_open.append(rid)
                else:
                    skipped += 1
                break
            except Exception as e:
                print(f"  [warn] status check {b32_to_hex(rid)[:18]}: {type(e).__name__}")
                if rpc_urls:
                    rpc_idx += 1
                    url = rpc_urls[rpc_idx % len(rpc_urls)]
                    w3_new = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
                    marketplace = _make_marketplace(w3_new)
        time.sleep(delay)
    if skipped:
        print(f"  Re-validation: {skipped} no longer status=2 — filtered out")
    return still_open


# ── Discovery ─────────────────────────────────────────────────────────────────


def discover_open_requests(
    rpc_urls: list[str],
    scan_from: int,
    scan_to: int,
    *,
    max_open: int | None = None,
    delay_logs: float = DELAY_LOGS,
    delay_status: float = DELAY_STATUS,
    checkpoint: Path | None = None,
    resume_from: int | None = None,
) -> list[bytes]:
    """Scan [scan_from, scan_to] for status=2 requestIds with RPC rotation.

    Saves progress to checkpoint after each window (resumable).
    Returns list of 32-byte requestId bytes.
    """
    if scan_from >= scan_to:
        print(f"[warn] scan_from ({scan_from}) >= scan_to ({scan_to}) — nothing to scan")
        return []

    cp = (
        load_checkpoint(checkpoint)
        if checkpoint
        else {
            "open_requests": [],
            "delivered": [],
            "last_scanned_block": None,
            "scan_from_block": None,
        }
    )
    open_set = set(cp.get("open_requests", []))
    delivered_set = set(cp.get("delivered", []))
    seen: set[str] = set()

    start = resume_from if resume_from is not None else scan_from
    rpc_idx = 0

    def _w3_and_contract():
        url = rpc_urls[rpc_idx % len(rpc_urls)]
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        return w3, _make_marketplace(w3)

    n_windows = max(1, (scan_to - start) // LOG_WINDOW + 1)
    print(
        f"  Scanning {n_windows} windows of {LOG_WINDOW} blocks "
        f"({start} → {scan_to}), {delay_logs:.0f}s between calls"
    )

    for wi, from_b in enumerate(range(start, scan_to + 1, LOG_WINDOW)):
        to_b = min(from_b + LOG_WINDOW - 1, scan_to)

        # get_logs with RPC rotation
        w3, marketplace = _w3_and_contract()
        logs = None
        for _ in range(len(rpc_urls)):
            try:
                logs = marketplace.events.MarketplaceRequest.get_logs(
                    from_block=from_b, to_block=to_b
                )
                break
            except Exception as e:
                print(f"  [warn] get_logs {from_b}-{to_b}: {type(e).__name__} — rotating RPC")
                rpc_idx += 1
                w3, marketplace = _w3_and_contract()

        if logs is None:
            print(f"  [error] All RPCs failed for {from_b}-{to_b} — skipping")
            time.sleep(delay_logs)
            continue

        # Collect unseen requestIds
        new_ids: list[bytes] = []
        for log in logs:
            for rid in log["args"]["requestIds"]:
                h = b32_to_hex(rid)
                if h not in seen and h not in delivered_set:
                    seen.add(h)
                    new_ids.append(rid)

        # Status-check new candidates
        for rid in new_ids:
            h = b32_to_hex(rid)
            for _ in range(len(rpc_urls)):
                try:
                    if marketplace.functions.getRequestStatus(rid).call() == 2:
                        open_set.add(h)
                        print(f"    open: {h[:18]}... (total: {len(open_set)})")
                    break
                except Exception:
                    rpc_idx += 1
                    w3, marketplace = _w3_and_contract()
            time.sleep(delay_status)

        # Persist checkpoint (BLK-3: scan_from_block is sticky once set)
        if checkpoint:
            if cp.get("scan_from_block") is None:
                cp["scan_from_block"] = scan_from
            cp["open_requests"] = sorted(open_set)
            cp["last_scanned_block"] = to_b
            save_checkpoint(checkpoint, cp)

        if wi % 10 == 0:
            pct = 100 * (from_b - start) / max(1, scan_to - start)
            print(f"  Progress: {pct:.0f}% block {from_b}, open: {len(open_set)}")

        if max_open and len(open_set) >= max_open:
            print(f"  Reached target {max_open} — stopping discovery.")
            break

        time.sleep(delay_logs)

    result = [hex_to_b32(h) for h in sorted(open_set)]
    print(f"\n  Discovery complete: {len(result)} open request(s)")
    return result


# ── Delivery (Anvil impersonation) ─────────────────────────────────────────────


def deliver_batch_anvil(
    w3: Web3,
    batch: list[bytes],
    safe_addr: str,
    mech_addr: str,
) -> tuple[bool, list[bytes]]:
    """Deliver batch via impersonated Safe (Anvil only).

    Returns (tx_succeeded, list_of_delivered_ids).
    """
    mech = w3.eth.contract(address=w3.to_checksum_address(mech_addr), abi=MECH_ABI)
    datas = [b"{}"] * len(batch)
    tx = mech.functions.deliverToMarketplace(batch, datas).build_transaction(
        {
            "from": safe_addr,
            "gas": 500_000 + 50_000 * len(batch),
            "gasPrice": w3.to_wei(1, "gwei"),
            "nonce": w3.eth.get_transaction_count(safe_addr),
        }
    )
    tx_hash = w3.eth.send_transaction(tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    ok = receipt["status"] == 1
    print(
        f"  TX 0x{tx_hash.hex()[:18]}... [{'OK' if ok else 'REVERTED'}] — {len(batch)} request(s)"
    )
    return ok, (batch if ok else [])


# ── Delivery (real Safe via safe_eth) ─────────────────────────────────────────


def deliver_batch_real(
    batch: list[bytes],
    safe_addr: str,
    mech_addr: str,
    rpc_url: str,
    private_key: str,
    w3: Web3,
) -> tuple[bool, list[bytes]]:
    """Deliver batch via real Safe transaction.

    BLK-1: waits for on-chain receipt before marking as delivered.
    BLK-2: pre-simulates to determine which requests will actually succeed.
    Returns (tx_succeeded, list_of_delivered_ids).
    """
    from safe_eth.eth import EthereumClient
    from safe_eth.safe import Safe

    datas = [b"{}"] * len(batch)
    mech = w3.eth.contract(address=w3.to_checksum_address(mech_addr), abi=MECH_ABI)

    # BLK-2: Simulate to filter requests that will succeed.
    # On simulation failure, abort rather than risk a full-batch revert.
    try:
        flags = mech.functions.deliverToMarketplace(batch, datas).call({"from": safe_addr})
        pairs = [(r, d) for r, d, ok in zip(batch, datas, flags) if ok]
        n_filtered = len(batch) - len(pairs)
        if n_filtered:
            print(f"  Simulation: {n_filtered} request(s) not deliverable — filtered out")
    except Exception as e:
        print(f"  [warn] Simulation failed ({type(e).__name__}) — aborting batch for safety")
        return True, []

    if not pairs:
        print("  Simulation: batch empty after filtering — skipping")
        return True, []

    eff_batch = [r for r, _ in pairs]
    eff_datas = [d for _, d in pairs]

    call_hex: str = mech.functions.deliverToMarketplace(eff_batch, eff_datas).build_transaction(
        {"from": safe_addr}
    )["data"]
    call_bytes = bytes.fromhex(call_hex[2:] if call_hex.startswith("0x") else call_hex)

    ec = EthereumClient(rpc_url)
    safe_instance = Safe(Web3.to_checksum_address(safe_addr), ec)
    safe_tx = safe_instance.build_multisig_tx(
        to=Web3.to_checksum_address(mech_addr), value=0, data=call_bytes
    )
    safe_tx.sign(private_key)
    tx_hash_bytes, _ = safe_tx.execute(private_key)
    tx_hash = "0x" + tx_hash_bytes.hex()
    print(f"  TX {tx_hash[:20]}... submitted ({len(eff_batch)} request(s))")

    # BLK-1: Wait for on-chain confirmation
    try:
        receipt = ec.w3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=180)
        if receipt["status"] != 1:
            print(f"  TX {tx_hash[:20]}... REVERTED — nothing marked as delivered")
            return False, []
        print(f"  TX {tx_hash[:20]}... CONFIRMED ✓ ({len(eff_batch)} delivered)")
        return True, eff_batch
    except Exception as e:
        print(f"  [warn] Receipt timeout ({type(e).__name__}) — marking as undelivered for safety")
        return False, []


def deliver_all(
    open_requests: list[bytes],
    *,
    mode: str,
    w3: Web3,
    private_key: str | None = None,
    rpc_url: str | None = None,
    delay_tx: float = DELAY_TX,
    batch_size: int = BATCH_SIZE,
    checkpoint: Path | None = None,
    rpc_urls: list[str] | None = None,
) -> int:
    """Deliver all open requests in batches. Returns count of delivered requests."""
    safe_addr = Web3.to_checksum_address(SAFE_ADDR)
    mech_addr = Web3.to_checksum_address(MECH_ADDR)

    cp = load_checkpoint(checkpoint) if checkpoint else {"open_requests": [], "delivered": []}
    delivered_set: set[str] = set(cp.get("delivered", []))

    pending = [r for r in open_requests if b32_to_hex(r) not in delivered_set]
    n_batches = (len(pending) + batch_size - 1) // batch_size
    print(f"\n  {len(pending)} request(s) to deliver in {n_batches} batch(es) of {batch_size}")

    total = 0
    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        bn = i // batch_size + 1
        print(f"\n  Batch {bn}/{n_batches}: {len(batch)} request(s)")

        # BLK-4: Re-validate status before each batch in real mode
        if mode == "real":
            batch = _revalidate_open(w3, batch, delay=0.3, rpc_urls=rpc_urls)
            if not batch:
                print("  All in batch already delivered — skipping")
                continue

        try:
            if mode == "anvil":
                _, delivered_ids = deliver_batch_anvil(w3, batch, safe_addr, mech_addr)
            else:
                _, delivered_ids = deliver_batch_real(
                    batch, safe_addr, mech_addr, rpc_url, private_key, w3
                )
            for r in delivered_ids:
                delivered_set.add(b32_to_hex(r))
            total += len(delivered_ids)
        except Exception as e:
            print(f"  [error] Batch {bn} failed: {type(e).__name__}")

        if checkpoint:
            cp["delivered"] = sorted(delivered_set)
            save_checkpoint(checkpoint, cp)

        if i + batch_size < len(pending):
            print(f"  Waiting {delay_tx:.0f}s...")
            time.sleep(delay_tx)

    return total


# ── Anvil test ─────────────────────────────────────────────────────────────────


def start_anvil(fork_url: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [
            "anvil",
            "--fork-url",
            fork_url,
            "--port",
            str(ANVIL_PORT),
            "--block-time",
            "1",
            "--silent",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    for _ in range(30):
        time.sleep(0.5)
        if proc.poll() is not None:  # D7: detect early crash
            raise RuntimeError(
                f"Anvil crashed (exit {proc.returncode}). Is port {ANVIL_PORT} already in use?"
            )
        try:
            if w3.is_connected():
                print(f"  Anvil ready at block {w3.eth.block_number}")
                return proc
        except Exception:
            pass
    proc.kill()
    raise RuntimeError("Anvil did not start in 15s.")


def run_anvil_test() -> None:
    """End-to-end Anvil test: find 15 open requests, deliver in batches, verify."""
    print("=" * 60)
    print("Anvil recovery test")
    print("=" * 60)

    TARGET = 15
    rpc_url = GNOSIS_RPCS[0]

    print(f"\n[1] Discovering up to {TARGET} open requests on Gnosis (>3 days old)...")
    w3_gnosis, _ = _connect(GNOSIS_RPCS, timeout=20)
    current = w3_gnosis.eth.block_number
    scan_to = current - BLOCKS_24H
    scan_from = current - BLOCKS_3D

    if scan_from >= scan_to:
        print("ERROR: empty scan range — check block constants")
        sys.exit(1)

    open_requests = discover_open_requests(
        GNOSIS_RPCS,
        scan_from=scan_from,
        scan_to=scan_to,
        max_open=TARGET,
        delay_logs=0.3,
        delay_status=0.3,
    )
    if not open_requests:
        print("No open requests found — cannot run test.")
        sys.exit(1)

    print("\n[2] Starting Anvil fork of Gnosis...")
    anvil_proc = start_anvil(rpc_url)

    try:
        w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
        safe_addr = Web3.to_checksum_address(SAFE_ADDR)
        mech_addr = Web3.to_checksum_address(MECH_ADDR)
        marketplace = _make_marketplace(w3)
        mech = w3.eth.contract(address=mech_addr, abi=MECH_ABI)

        payment_type = mech.functions.paymentType().call()
        bt_addr = marketplace.functions.mapPaymentTypeBalanceTrackers(payment_type).call()
        bt = w3.eth.contract(address=w3.to_checksum_address(bt_addr), abi=BALANCE_TRACKER_ABI)

        balance_before = bt.functions.mapMechBalances(mech_addr).call()
        print(f"\n  BalanceTracker: {bt_addr}")
        print(f"  Balance before: {balance_before / 1e18:.6f} xDAI")

        for rid in open_requests:
            s = marketplace.functions.getRequestStatus(rid).call()
            assert s == 2, f"Expected status=2, got {s} for {rid.hex()[:16]}"
        print(f"  All {len(open_requests)} confirmed status=2 on fork ✓")

        w3.provider.make_request("anvil_impersonateAccount", [safe_addr])
        w3.provider.make_request("anvil_setBalance", [safe_addr, hex(10 * 10**18)])

        print(f"\n[3] Delivering {len(open_requests)} request(s) in batches of {BATCH_SIZE}...")
        total = deliver_all(open_requests, mode="anvil", w3=w3, delay_tx=2.0, batch_size=BATCH_SIZE)

        balance_after = bt.functions.mapMechBalances(mech_addr).call()
        delta = balance_after - balance_before
        print(f"\n  Balance after:  {balance_after / 1e18:.6f} xDAI")
        print(f"  Balance delta:  +{delta / 1e18:.6f} xDAI")

        for rid in open_requests[:3]:
            s = marketplace.functions.getRequestStatus(rid).call()
            assert s == 3, f"Expected status=3, got {s} for {rid.hex()[:16]}"
        print("  Status spot-check: first 3 delivered ✓")

        assert balance_after > balance_before, "Balance did not increase!"
        assert total == len(open_requests), f"Expected {len(open_requests)} delivered, got {total}"
        print(f"\n  ✓ PASS — {total} request(s) delivered, +{delta / 1e18:.6f} xDAI credited.")

    finally:
        anvil_proc.kill()
        print("\nAnvil stopped.")


# ── Real-mode entrypoints ──────────────────────────────────────────────────────


def _get_private_key() -> str:
    pk = os.environ.get("AGENT_PRIVATE_KEY", "")
    try:
        _validate_private_key(pk)
    except ValueError as e:
        print(f"AGENT_PRIVATE_KEY invalid: {e}", file=sys.stderr)
        sys.exit(1)
    return pk


def cmd_discover(args: argparse.Namespace) -> None:
    checkpoint = Path(args.checkpoint)
    cp = load_checkpoint(checkpoint)
    rpc_list = [args.rpc] if args.rpc else GNOSIS_RPCS

    w3, _ = _connect(rpc_list)
    current = w3.eth.block_number
    scan_to = current - BLOCKS_24H

    # BLK-3: sticky scan_from_block — reuse stored value on resume to avoid gaps
    lookback = int(args.lookback_days * 24 * 3600 / GNOSIS_BLOCK_TIME)
    computed_scan_from = current - lookback
    stored_scan_from = cp.get("scan_from_block")
    if stored_scan_from is not None:
        scan_from = stored_scan_from
        print(f"Resume: using stored scan_from_block={scan_from}")
        diff_days = abs(scan_from - computed_scan_from) * GNOSIS_BLOCK_TIME / 86400
        if diff_days >= 1:
            print(
                f"  [warn] --lookback-days would scan from {computed_scan_from} "
                f"but stored is {scan_from} ({diff_days:.1f} day diff) — stored takes precedence"
            )
    else:
        scan_from = computed_scan_from

    if scan_from >= scan_to:
        print(
            f"[error] scan_from ({scan_from}) >= scan_to ({scan_to}). Try --lookback-days N.",
            file=sys.stderr,
        )
        sys.exit(1)

    resume_from = cp.get("last_scanned_block")
    if resume_from is not None:
        resume_from += 1
        if resume_from > scan_to:
            print(
                f"Scan already complete (scanned up to {resume_from - 1}). "
                f"Run --mode deliver to deliver."
            )
            return
        print(f"Resuming from block {resume_from}")

    print(f"\nDiscovery: blocks {scan_from}–{scan_to} ({args.lookback_days} days minus 24h)")
    open_requests = discover_open_requests(
        rpc_list,
        scan_from=scan_from,
        scan_to=scan_to,
        delay_logs=DELAY_LOGS,
        delay_status=DELAY_STATUS,
        checkpoint=checkpoint,
        resume_from=resume_from,
    )
    print(f"\nFound {len(open_requests)} open request(s). Checkpoint: {checkpoint}")


def cmd_deliver(args: argparse.Namespace) -> None:
    checkpoint = Path(args.checkpoint)
    cp = load_checkpoint(checkpoint)
    rpc_list = [args.rpc] if args.rpc else GNOSIS_RPCS

    stored_ids = cp.get("open_requests", [])
    if not stored_ids:
        print("No open requests in checkpoint. Run --mode discover first.")
        sys.exit(1)

    w3, rpc_url = _connect(rpc_list)

    # Per-batch re-validation (BLK-4) inside deliver_all handles filtering.
    open_requests = [hex_to_b32(h) for h in stored_ids]

    # OPS-1 + D6: explicit confirmation with summary
    n_batches = (len(open_requests) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"""
⚠️  DELIVERY SUMMARY
   Safe:        {SAFE_ADDR}
   Mech:        {MECH_ADDR}
   Requests:    {len(open_requests)} checkpoint entries, {n_batches} batch(es) of ≤{BATCH_SIZE}
                (each batch is re-validated on-chain before delivery)
   Payload:     b\"{{}}\"  — requesters receive an empty delivery in exchange for payment
   Delay:       {DELAY_TX}s between batches

   ⚠ Stop the production mech BEFORE proceeding — concurrent Safe signing
     from both processes causes GS026 nonce races.
   ⚠ --rpc override uses a single RPC URL with no automatic failover during delivery.
""")
    if not _confirm("Proceed with real delivery?"):
        print("Aborted.")
        sys.exit(0)

    private_key = _get_private_key()
    total = deliver_all(
        open_requests,
        mode="real",
        w3=w3,
        private_key=private_key,
        rpc_url=rpc_url,
        delay_tx=DELAY_TX,
        batch_size=BATCH_SIZE,
        checkpoint=checkpoint,
        rpc_urls=rpc_list,
    )
    print(f"\nDone — {total} request(s) delivered.")


# ── CLI ────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--anvil-test", action="store_true", help="Run Anvil end-to-end test")
    parser.add_argument("--mode", choices=["discover", "deliver", "all"], default="discover")
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT))
    parser.add_argument("--rpc", default=None, help="Override RPC URL")
    parser.add_argument(
        "--lookback-days",
        type=float,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Days to look back (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    args = parser.parse_args()

    # LOW-2: Reject non-HTTPS RPCs in real mode (Anvil uses http locally — exempt)
    if args.rpc and not args.anvil_test and not args.rpc.startswith("https://"):
        print(
            f"[error] --rpc must use HTTPS. Got: {args.rpc}",
            file=sys.stderr,
        )
        sys.exit(1)

    # OPS-3: Fail-fast if key is missing for modes that need it
    if not args.anvil_test and args.mode in ("deliver", "all"):
        if not os.environ.get("AGENT_PRIVATE_KEY"):
            print(
                "AGENT_PRIVATE_KEY env var required for modes 'deliver' and 'all'.", file=sys.stderr
            )
            sys.exit(1)

    if args.anvil_test:
        run_anvil_test()
        return

    if args.mode == "discover":
        cmd_discover(args)
    elif args.mode == "deliver":
        cmd_deliver(args)
    elif args.mode == "all":
        cmd_discover(args)
        # OPS-2: verify discovery completed before delivering
        cp = load_checkpoint(Path(args.checkpoint))
        if cp.get("last_scanned_block") is None:
            print("[error] Discovery did not complete — aborting deliver phase.", file=sys.stderr)
            sys.exit(1)
        cmd_deliver(args)


if __name__ == "__main__":
    main()
