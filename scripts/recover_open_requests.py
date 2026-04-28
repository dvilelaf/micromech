#!/usr/bin/env -S uv run python3
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

Reads mech, marketplace, and Safe addresses from config.yaml by default.
Reads the Safe signer private key from wallet.json using wallet_password.
The script is standalone: it does not need to run from the micromech repo.

Modes:
  --anvil-test          Fork Gnosis locally, process 15 requests, verify — no keys needed.
  --mode discover       Scan blocks, save open requestIds to checkpoint file. No TX.
  --mode deliver        Re-validate on-chain, deliver via Safe. Needs wallet_password
                        (or AGENT_PRIVATE_KEY override).
  --mode all            discover + deliver in one pass. Needs wallet_password
                        (or AGENT_PRIVATE_KEY override).

Usage:
  python recover_open_requests.py --config /path/config.yaml --wallet /path/wallet.json --anvil-test
  python recover_open_requests.py --config /path/config.yaml --wallet /path/wallet.json --mode discover
  wallet_password=... python recover_open_requests.py --config /path/config.yaml --wallet /path/wallet.json --mode deliver

Rate limits (real mode): 2s between get_logs, 1s between status checks, 10s between TXs.
"""

import argparse
import getpass
import json
import logging
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

from web3 import Web3

ANVIL_RPC = "http://127.0.0.1:8545"
ANVIL_PORT = 8545

GNOSIS_BLOCK_TIME = 5.2
BLOCKS_24H = int(24 * 3600 / GNOSIS_BLOCK_TIME)
BLOCKS_3D = int(3 * 24 * 3600 / GNOSIS_BLOCK_TIME)

# Marketplace contract deployment block (2025-02-20). Used as the default scan
# start so --mode discover covers the full contract history without needing
# --lookback-days. Verified via binary search on Gnosis mainnet.
MARKETPLACE_DEPLOY_BLOCK = 38_661_963

DELAY_LOGS = 2.0  # seconds between get_logs calls
DELAY_STATUS = 1.0  # seconds between getRequestStatus calls
DELAY_TX = 10.0  # seconds between Safe TX submissions
RPC_RETRY_BASE = 1.0
RPC_RETRY_MAX = 30.0
BATCH_SIZE = 50  # requestIds per deliverToMarketplace call
LOG_WINDOW = 1000  # blocks per get_logs window (Gnosis public RPCs accept 1000-2000)
MECH_DISCOVERY_WINDOW = 1_000_000  # CreateMech is sparse; public RPCs handle large sparse ranges.

DEFAULT_CHECKPOINT = Path(__file__).resolve().with_name("recover.json")
DEFAULT_QUEUE = Path(__file__).resolve().with_name("recover_queue.sqlite")
DEFAULT_LOG = Path(__file__).resolve().with_name("recover.log")
DEFAULT_MECHS_CACHE = Path(__file__).resolve().with_name("recover_mechs.json")
DEFAULT_BLOCKSCOUT_API = "https://gnosis.blockscout.com/api/v2"

_HEX32_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_PRIVKEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_LOGGER = logging.getLogger("recover_open_requests")
_IWA_CHAIN_INTERFACE = None
_IWA_CHAIN_NAME: str | None = None

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
        "inputs": [],
        "name": "numUndeliveredRequests",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "requestId", "type": "bytes32"}],
        "name": "mapRequestIdInfos",
        "outputs": [
            {"name": "requester", "type": "address"},
            {"name": "priorityMech", "type": "address"},
            {"name": "deliveryMech", "type": "address"},
            {"name": "requestBlockNumber", "type": "uint256"},
            {"name": "maxDeliveryRate", "type": "uint256"},
            {"name": "paymentType", "type": "bytes32"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "mech", "type": "address"},
            {"indexed": True, "name": "serviceId", "type": "uint256"},
            {"indexed": True, "name": "mechFactory", "type": "address"},
        ],
        "name": "CreateMech",
        "type": "event",
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
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "deliveryMech", "type": "address"},
            {"indexed": False, "name": "requesters", "type": "address[]"},
            {"indexed": False, "name": "numDeliveries", "type": "uint256"},
            {"indexed": False, "name": "requestIds", "type": "bytes32[]"},
            {"indexed": False, "name": "deliveredRequests", "type": "bool[]"},
        ],
        "name": "MarketplaceDelivery",
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
    {
        "inputs": [],
        "name": "numUndeliveredRequests",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "size", "type": "uint256"},
            {"name": "offset", "type": "uint256"},
        ],
        "name": "getUndeliveredRequestIds",
        "outputs": [{"name": "requestIds", "type": "bytes32[]"}],
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


class RuntimeConfig:
    """Resolved addresses and files for a recovery run."""

    def __init__(
        self,
        *,
        chain: str,
        mech_addr: str,
        safe_addr: str,
        marketplace_addr: str,
        config_path: Path,
        wallet_path: Path,
        delivery_rate: int | None = None,
    ) -> None:
        self.chain = chain
        self.mech_addr = mech_addr
        self.safe_addr = safe_addr
        self.marketplace_addr = marketplace_addr
        self.config_path = config_path
        self.wallet_path = wallet_path
        self.delivery_rate = delivery_rate


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


def setup_logging(log_path: Path, verbose: bool = False) -> None:
    """Configure console and file logging for long recovery runs."""
    level = logging.DEBUG if verbose else logging.INFO
    _LOGGER.setLevel(logging.DEBUG)
    _LOGGER.handlers.clear()
    _LOGGER.propagate = False

    log_path = log_path.expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    )

    _LOGGER.addHandler(console)
    _LOGGER.addHandler(file_handler)
    _LOGGER.info("logging initialized path=%s level=%s", log_path, logging.getLevelName(level))


def log(msg: str, *args, level: int = logging.INFO) -> None:
    _LOGGER.log(level, msg, *args)


def _redact_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(str(url))
    if not parsed.scheme or not parsed.netloc:
        return str(url)
    path = parsed.path.rsplit("/", 1)[0] + "/..." if "/" in parsed.path else parsed.path
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _backoff_sleep(attempt: int, *, base: float = RPC_RETRY_BASE, cap: float = RPC_RETRY_MAX) -> None:
    delay = min(cap, base * (2 ** max(0, attempt)))
    log("RPC backoff sleep %.1fs", delay, level=logging.DEBUG)
    time.sleep(delay)


def setup_iwa_rpc(chain_name: str, enabled: bool = True) -> None:
    """Use iwa ChainInterfaces for RPC retry/rotation when available."""
    global _IWA_CHAIN_INTERFACE, _IWA_CHAIN_NAME
    _IWA_CHAIN_INTERFACE = None
    _IWA_CHAIN_NAME = None
    if not enabled:
        log("iwa RPC disabled; using explicit Web3 RPC list")
        return
    try:
        from iwa.core.chain import ChainInterfaces

        ci = ChainInterfaces().get(chain_name)
        if ci is None:
            log("iwa has no ChainInterface for %s; using explicit RPC list", chain_name, level=logging.WARNING)
            return
        _IWA_CHAIN_INTERFACE = ci
        _IWA_CHAIN_NAME = chain_name
        log("using iwa ChainInterface for chain=%s (RPC retry/rotation enabled)", chain_name)
    except Exception as e:
        log("iwa RPC setup failed (%s); using explicit RPC list", type(e).__name__, level=logging.WARNING)


def _rpc_call(operation, *, name: str):
    """Execute an RPC operation via iwa retry when configured."""
    if _IWA_CHAIN_INTERFACE is not None:
        return _IWA_CHAIN_INTERFACE.with_retry(operation, operation_name=name)

    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            return operation()
        except Exception as e:
            last_exc = e
            log("%s failed: %s", name, type(e).__name__, level=logging.WARNING)
            _backoff_sleep(attempt)
    raise last_exc or RuntimeError(f"{name} failed")


def _rpc_web3(default_w3: Web3 | None = None):
    if _IWA_CHAIN_INTERFACE is not None:
        return _IWA_CHAIN_INTERFACE.web3
    return default_w3


def _safe_eth_rpc_url(rpc_urls: list[str]) -> str:
    """Return a concrete HTTPS RPC URL suitable for safe-eth."""
    if _IWA_CHAIN_INTERFACE is not None:
        current = getattr(_IWA_CHAIN_INTERFACE, "current_rpc", None)
        if isinstance(current, str) and current.startswith("https://"):
            return current
    for url in rpc_urls:
        if url.startswith("https://"):
            return url
    raise RuntimeError("No HTTPS RPC URL available for Safe transaction submission")


def _default_rpc_urls(chain_name: str) -> list[str]:
    """Return RPC URLs from iwa/secrets.env; no script-level hardcoded RPCs."""
    if _IWA_CHAIN_INTERFACE is not None:
        rpcs = getattr(_IWA_CHAIN_INTERFACE.chain, "rpcs", None) or []
        if rpcs:
            return [str(r) for r in rpcs]
        rpc = getattr(_IWA_CHAIN_INTERFACE.chain, "rpc", None)
        if rpc:
            return [str(rpc)]
    try:
        from iwa.core.chain import ChainInterfaces

        ci = ChainInterfaces().get(chain_name)
        if ci is not None:
            rpcs = getattr(ci.chain, "rpcs", None) or []
            if rpcs:
                return [str(r) for r in rpcs]
            rpc = getattr(ci.chain, "rpc", None)
            if rpc:
                return [str(rpc)]
    except Exception as e:
        log("could not load RPCs from iwa: %s", type(e).__name__, level=logging.WARNING)
    raise RuntimeError(
        f"No RPCs available for {chain_name}. Configure {chain_name}_rpc in secrets.env or pass --rpc."
    )


def _rpc_list(args: argparse.Namespace, chain_name: str) -> list[str]:
    return [args.rpc] if args.rpc else _default_rpc_urls(chain_name)


def _find_free_port(preferred: int | None = None) -> int:
    ports = []
    if preferred is not None:
        ports.extend(range(preferred, preferred + 100))
    ports.append(0)
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return int(sock.getsockname()[1])
            except OSError:
                continue
    raise RuntimeError("No free localhost port found for Anvil")


def _load_yaml(path: Path) -> dict:
    import yaml

    return yaml.safe_load(path.read_text()) or {}


def _resolve_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    config_path = Path(args.config).expanduser().resolve()
    wallet_path = Path(args.wallet).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    if not wallet_path.exists() and not args.anvil_test:
        raise FileNotFoundError(f"wallet not found: {wallet_path}")

    data = _load_yaml(config_path)
    micromech = data.get("plugins", {}).get("micromech", {})
    chain_cfg = micromech.get("chains", {}).get(args.chain)
    if not chain_cfg:
        raise ValueError(f"chain '{args.chain}' not found under plugins.micromech.chains")

    mech_addr = args.mech or chain_cfg.get("mech_address")
    marketplace_addr = args.marketplace or chain_cfg.get("marketplace_address")
    if not mech_addr:
        raise ValueError("mech_address missing in config; pass --mech")
    if not marketplace_addr:
        raise ValueError("marketplace_address missing in config; pass --marketplace")

    safe_addr = args.safe or _safe_from_config(data, args.chain, args.service_key)
    if not safe_addr:
        safe_addr = _safe_from_wallet(wallet_path, args.chain) if wallet_path.exists() else None
    if not safe_addr:
        raise ValueError("Safe multisig not found in config/wallet; pass --safe")

    return RuntimeConfig(
        chain=args.chain,
        mech_addr=Web3.to_checksum_address(mech_addr),
        safe_addr=Web3.to_checksum_address(safe_addr),
        marketplace_addr=Web3.to_checksum_address(marketplace_addr),
        config_path=config_path,
        wallet_path=wallet_path,
        delivery_rate=chain_cfg.get("delivery_rate"),
    )


def _safe_from_config(data: dict, chain: str, service_key: str | None) -> str | None:
    services = data.get("plugins", {}).get("olas", {}).get("services", {}) or {}
    if service_key:
        service = services.get(service_key)
        return service.get("multisig_address") if service else None

    matches = []
    for key, service in services.items():
        if service.get("chain_name") == chain and service.get("multisig_address"):
            matches.append((key, service["multisig_address"]))
    if len(matches) == 1:
        return matches[0][1]
    if len(matches) > 1:
        raise ValueError(
            f"multiple OLAS services found for chain '{chain}'; pass --service-key"
        )
    return None


def _safe_from_wallet(wallet_path: Path, chain: str) -> str | None:
    try:
        data = json.loads(wallet_path.read_text())
    except Exception:
        return None
    matches = []
    for addr, account in (data.get("accounts") or {}).items():
        if account.get("signers") and chain in (account.get("chains") or []):
            matches.append(addr)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"multiple Safe accounts found in wallet for chain '{chain}'; pass --safe")
    return None


def _private_key_from_wallet(wallet_path: Path, safe_addr: str, password: str) -> str:
    from iwa.core.keys import KeyStorage

    wallet_data = json.loads(wallet_path.read_text())
    safe_entry = (wallet_data.get("accounts") or {}).get(safe_addr)
    if safe_entry is None:
        for addr, entry in (wallet_data.get("accounts") or {}).items():
            if addr.lower() == safe_addr.lower():
                safe_entry = entry
                break
    if not safe_entry:
        raise ValueError(f"Safe {safe_addr} not found in wallet.json")

    signers = safe_entry.get("signers") or []
    if not signers:
        raise ValueError(f"Safe {safe_addr} has no signer entries in wallet.json")

    ks = KeyStorage(path=wallet_path, password=password)
    for signer in signers:
        try:
            pk = ks._get_private_key(signer)
        except Exception:
            pk = None
        if pk:
            return pk if pk.startswith("0x") else "0x" + pk
    raise ValueError("No Safe signer in wallet.json could be decrypted with the provided password")


def load_checkpoint(path: Path) -> dict:
    """Load checkpoint JSON, sanitising entries to valid hex32 strings."""
    if path.exists():
        try:
            data = json.loads(path.read_text())
            data["open_requests"] = [h for h in data.get("open_requests", []) if _valid_hex32(h)]
            data["delivered"] = [h for h in data.get("delivered", []) if _valid_hex32(h)]
            return data
        except Exception as e:
            log("Checkpoint unreadable (%s) — starting fresh", e, level=logging.WARNING)
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


class RequestQueue:
    """Persistent queue for discovered marketplace request IDs."""

    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'open',
                    discovered_block INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    delivered_at INTEGER,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_requests_status_block "
                "ON requests(status, discovered_block)"
            )

    def enqueue_open(self, request_id: str, block_number: int | None = None) -> None:
        now = int(time.time())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO requests(request_id, status, discovered_block, updated_at)
                VALUES (?, 'open', ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    status = CASE
                        WHEN requests.status IN ('delivered', 'skipped') THEN requests.status
                        ELSE 'open'
                    END,
                    discovered_block = COALESCE(requests.discovered_block, excluded.discovered_block),
                    updated_at = excluded.updated_at
                """,
                (request_id, block_number, now),
            )

    def get_open(self, limit: int) -> list[bytes]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT request_id
                FROM requests
                WHERE status = 'open'
                ORDER BY COALESCE(discovered_block, 0), request_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [hex_to_b32(row[0]) for row in rows]

    def mark_delivering(self, request_ids: list[bytes]) -> None:
        self._mark(request_ids, "delivering", increment_attempts=True)

    def mark_open(self, request_ids: list[bytes], error: str | None = None) -> None:
        self._mark(request_ids, "open", error=error)

    def mark_delivered(self, request_ids: list[bytes]) -> None:
        now = int(time.time())
        ids = [b32_to_hex(r) for r in request_ids]
        if not ids:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                UPDATE requests
                SET status = 'delivered', delivered_at = ?, updated_at = ?, last_error = NULL
                WHERE request_id = ?
                """,
                [(now, now, rid) for rid in ids],
            )

    def mark_skipped(self, request_ids: list[bytes], reason: str) -> None:
        self._mark(request_ids, "skipped", error=reason)

    def remember_skipped(
        self,
        request_id: str,
        block_number: int | None = None,
        reason: str | None = None,
    ) -> None:
        """Persist a request that should not be retried by future discovery runs."""
        now = int(time.time())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO requests(request_id, status, discovered_block, last_error, updated_at)
                VALUES (?, 'skipped', ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    status = CASE
                        WHEN requests.status = 'delivered' THEN requests.status
                        ELSE 'skipped'
                    END,
                    discovered_block = COALESCE(requests.discovered_block, excluded.discovered_block),
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (request_id, block_number, reason, now),
            )

    def _mark(
        self,
        request_ids: list[bytes],
        status: str,
        *,
        error: str | None = None,
        increment_attempts: bool = False,
    ) -> None:
        ids = [b32_to_hex(r) for r in request_ids]
        if not ids:
            return
        now = int(time.time())
        attempts_expr = "attempts + 1" if increment_attempts else "attempts"
        with self._connect() as conn:
            conn.executemany(
                f"""
                UPDATE requests
                SET status = ?, updated_at = ?, last_error = ?, attempts = {attempts_expr}
                WHERE request_id = ?
                """,
                [(status, now, error, rid) for rid in ids],
            )

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM requests GROUP BY status"
            ).fetchall()
        return {str(status): int(count) for status, count in rows}

    def all_request_ids(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT request_id FROM requests").fetchall()
        return {str(row[0]) for row in rows}

    def count_open(self) -> int:
        return self.counts().get("open", 0)


def _connect(rpc_urls: list[str], timeout: int = 30) -> tuple[Web3, str]:
    """Connect to the first working RPC. Returns (w3, url)."""
    if _IWA_CHAIN_INTERFACE is not None:
        w3 = _IWA_CHAIN_INTERFACE.web3
        _rpc_call(lambda: w3.eth.block_number, name="iwa block_number probe")
        return w3, _safe_eth_rpc_url(rpc_urls)

    for attempt in range(3):
        for url in rpc_urls:
            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": timeout}))
                if w3.is_connected():
                    log("connected rpc=%s", _redact_url(url))
                    return w3, url
                log("RPC not connected: %s", _redact_url(url), level=logging.WARNING)
            except Exception as e:
                log("RPC connect failed %s: %s", _redact_url(url), type(e).__name__, level=logging.WARNING)
        _backoff_sleep(attempt)
    raise RuntimeError(f"All RPCs unreachable: {rpc_urls}")


def _make_marketplace(w3: Web3, marketplace_addr: str):
    return w3.eth.contract(address=w3.to_checksum_address(marketplace_addr), abi=MARKETPLACE_ABI)


def _call_status_with_rotation(
    rpc_urls: list[str],
    marketplace_addr: str,
    request_id: bytes,
    *,
    start_idx: int = 0,
) -> tuple[int | None, int, object]:
    """Call getRequestStatus with RPC rotation and exponential backoff."""
    last_exc: Exception | None = None
    for attempt in range(max(1, len(rpc_urls) * 3)):
        rpc_idx = start_idx + attempt
        url = rpc_urls[rpc_idx % len(rpc_urls)]
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
            marketplace = _make_marketplace(w3, marketplace_addr)
            return marketplace.functions.getRequestStatus(request_id).call(), rpc_idx, marketplace
        except Exception as e:
            last_exc = e
            log(
                "getRequestStatus %s via %s failed: %s",
                b32_to_hex(request_id)[:18],
                _redact_url(url),
                type(e).__name__,
                level=logging.WARNING,
            )
            if (attempt + 1) % len(rpc_urls) == 0:
                _backoff_sleep(attempt // len(rpc_urls))
    if last_exc:
        log(
            "getRequestStatus exhausted for %s: %s",
            b32_to_hex(request_id)[:18],
            type(last_exc).__name__,
            level=logging.ERROR,
        )
    return None, start_idx, None


def _revalidate_open(
    w3: Web3,
    request_ids: list[bytes],
    marketplace_addr: str,
    delay: float = DELAY_STATUS,
    rpc_urls: list[str] | None = None,
    cutoff_block: int | None = None,
    cutoff_timestamp: int | None = None,
) -> list[bytes]:
    """Check status/age on-chain. Returns only still-status=2 and old enough.

    If rpc_urls is provided, rotates to the next RPC on failure.
    """
    rpc_idx = 0
    still_open, skipped_status, skipped_age = [], 0, 0
    for rid in request_ids:
        n_attempts = len(rpc_urls) if rpc_urls else 1
        for _ in range(n_attempts):
            try:
                marketplace = _make_marketplace(_rpc_web3(w3), marketplace_addr)
                status = _rpc_call(
                    lambda: marketplace.functions.getRequestStatus(rid).call(),
                    name=f"revalidate {b32_to_hex(rid)[:18]}",
                )
                if status == 2:
                    if cutoff_block is not None:
                        info = _rpc_call(
                            lambda: marketplace.functions.mapRequestIdInfos(rid).call(),
                            name=f"revalidate info {b32_to_hex(rid)[:18]}",
                        )
                        request_block = int(info[3])
                        if not _request_marker_is_old_enough(
                            request_block,
                            cutoff_block=cutoff_block,
                            cutoff_timestamp=cutoff_timestamp,
                        ):
                            skipped_age += 1
                            break
                    still_open.append(rid)
                else:
                    skipped_status += 1
                break
            except Exception as e:
                log("status check %s: %s", b32_to_hex(rid)[:18], type(e).__name__, level=logging.WARNING)
                if rpc_urls:
                    rpc_idx += 1
                    url = rpc_urls[rpc_idx % len(rpc_urls)]
                    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        time.sleep(delay)
    if skipped_status:
        log("Re-validation: %s no longer status=2 — filtered out", skipped_status)
    if skipped_age:
        log("Re-validation: %s younger than 24h cutoff — filtered out", skipped_age)
    return still_open


def _request_marker_is_old_enough(
    request_marker: int,
    *,
    cutoff_block: int | None,
    cutoff_timestamp: int | None,
) -> bool:
    """Return whether a request marker is older than the 24h cutoff.

    Some deployed Marketplace versions expose a field named requestBlockNumber
    that contains a Unix timestamp. Treat large values as timestamps and normal
    chain-height-sized values as block numbers.
    """
    if request_marker >= 1_000_000_000:
        if cutoff_timestamp is None:
            return True
        return request_marker <= cutoff_timestamp
    if cutoff_block is None:
        return True
    return request_marker <= cutoff_block


# ── Discovery ─────────────────────────────────────────────────────────────────


def discover_open_requests(
    rpc_urls: list[str],
    marketplace_addr: str,
    scan_from: int,
    scan_to: int,
    *,
    max_open: int | None = None,
    delay_logs: float = DELAY_LOGS,
    delay_status: float = DELAY_STATUS,
    checkpoint: Path | None = None,
    resume_from: int | None = None,
    queue: RequestQueue | None = None,
) -> list[bytes]:
    """Scan [scan_from, scan_to] for status=2 requestIds with RPC rotation.

    Saves progress to checkpoint after each window (resumable).
    Returns list of 32-byte requestId bytes.
    """
    if scan_from >= scan_to:
        log("scan_from (%s) >= scan_to (%s) — nothing to scan", scan_from, scan_to, level=logging.WARNING)
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
        if _IWA_CHAIN_INTERFACE is not None:
            w3 = _IWA_CHAIN_INTERFACE.web3
            return w3, _make_marketplace(w3, marketplace_addr)
        url = rpc_urls[rpc_idx % len(rpc_urls)]
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        return w3, _make_marketplace(w3, marketplace_addr)

    n_windows = max(1, (scan_to - start) // LOG_WINDOW + 1)
    log(
        "Scanning %s windows of %s blocks (%s -> %s), %.0fs between calls",
        n_windows,
        LOG_WINDOW,
        start,
        scan_to,
        delay_logs,
    )

    for wi, from_b in enumerate(range(start, scan_to + 1, LOG_WINDOW)):
        to_b = min(from_b + LOG_WINDOW - 1, scan_to)

        # get_logs with RPC rotation
        w3, marketplace = _w3_and_contract()
        logs = None
        for _ in range(len(rpc_urls)):
            try:
                logs = _rpc_call(
                    lambda: _make_marketplace(
                        _rpc_web3(w3),
                        marketplace_addr,
                    ).events.MarketplaceRequest.get_logs(
                        from_block=from_b,
                        to_block=to_b,
                    ),
                    name=f"get_logs {from_b}-{to_b}",
                )
                break
            except Exception as e:
                log("get_logs %s-%s: %s — rotating RPC", from_b, to_b, type(e).__name__, level=logging.WARNING)
                rpc_idx += 1
                w3, marketplace = _w3_and_contract()

        if logs is None:
            msg = f"All RPCs failed for {from_b}-{to_b}; aborting to avoid scan gaps"
            raise RuntimeError(msg)

        # Collect unseen requestIds
        new_ids: list[bytes] = []
        for event_log in logs:
            for rid in event_log["args"]["requestIds"]:
                h = b32_to_hex(rid)
                if h not in seen and h not in delivered_set:
                    seen.add(h)
                    new_ids.append(rid)

        # Status-check new candidates
        for rid in new_ids:
            h = b32_to_hex(rid)
            for _ in range(len(rpc_urls)):
                try:
                    status = _rpc_call(
                        lambda: _make_marketplace(
                            _rpc_web3(w3),
                            marketplace_addr,
                        ).functions.getRequestStatus(rid).call(),
                        name=f"getRequestStatus {h[:18]}",
                    )
                    if status == 2:
                        open_set.add(h)
                        if queue:
                            queue.enqueue_open(h, from_b)
                        log("open: %s... total=%s queue=%s", h[:18], len(open_set), bool(queue))
                    break
                except Exception:
                    rpc_idx += 1
                    w3, marketplace = _w3_and_contract()
            time.sleep(delay_status)
            if max_open and len(open_set) >= max_open:
                break

        # Persist checkpoint (BLK-3: scan_from_block is sticky once set)
        if checkpoint:
            if cp.get("scan_from_block") is None:
                cp["scan_from_block"] = scan_from
            cp["open_requests"] = sorted(open_set)
            cp["last_scanned_block"] = to_b
            save_checkpoint(checkpoint, cp)

        if wi % 10 == 0:
            pct = 100 * (from_b - start) / max(1, scan_to - start)
            log("Progress: %.0f%% block %s open=%s", pct, from_b, len(open_set))

        if max_open and len(open_set) >= max_open:
            log("Reached target %s — stopping discovery.", max_open)
            break

        time.sleep(delay_logs)

    result = [hex_to_b32(h) for h in sorted(open_set)]
    log("Discovery complete: %s open request(s)", len(result))
    return result


def _load_mech_sources(args: argparse.Namespace) -> list[str]:
    """Load explicit priority mech addresses for queue-based discovery."""
    values: list[str] = []
    if args.priority_mechs:
        values.extend(x.strip() for x in args.priority_mechs.split(","))
    if args.priority_mechs_file:
        path = Path(args.priority_mechs_file).expanduser()
        values.extend(
            line.split("#", 1)[0].strip()
            for line in path.read_text().splitlines()
        )

    mechs: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        try:
            mech = Web3.to_checksum_address(value)
        except ValueError as exc:
            raise ValueError(f"invalid mech address: {value}") from exc
        key = mech.lower()
        if key not in seen:
            seen.add(key)
            mechs.append(mech)
    return mechs


def _load_mechs_cache(path: Path, marketplace_addr: str) -> dict:
    if not path.exists():
        return {"marketplace": marketplace_addr, "last_scanned_block": None, "mechs": []}
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        log("Mechs cache unreadable (%s) — rebuilding", exc, level=logging.WARNING)
        return {"marketplace": marketplace_addr, "last_scanned_block": None, "mechs": []}
    if str(data.get("marketplace", "")).lower() != marketplace_addr.lower():
        log("Mechs cache marketplace mismatch — rebuilding", level=logging.WARNING)
        return {"marketplace": marketplace_addr, "last_scanned_block": None, "mechs": []}
    mechs = []
    seen = set()
    for value in data.get("mechs", []):
        try:
            mech = Web3.to_checksum_address(value)
        except ValueError:
            continue
        key = mech.lower()
        if key not in seen:
            seen.add(key)
            mechs.append(mech)
    return {
        "marketplace": Web3.to_checksum_address(marketplace_addr),
        "last_scanned_block": data.get("last_scanned_block"),
        "mechs": mechs,
    }


def _save_mechs_cache(path: Path, data: dict) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(path)
    path.chmod(0o600)


def _blockscout_get_json(
    api_base: str,
    path: str,
    params: dict | None = None,
    *,
    retries: int = 5,
) -> dict:
    query = urllib.parse.urlencode(params or {})
    url = api_base.rstrip("/") + path
    if query:
        url += "?" + query
    req = urllib.request.Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "micromech-recover/1.0",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            delay = min(30.0, 2.0**attempt)
            log("Blockscout request failed (%s); retrying in %.1fs", type(exc).__name__, delay, level=logging.WARNING)
            time.sleep(delay)
    raise last_exc or RuntimeError("Blockscout request failed")


def discover_priority_mechs_blockscout(
    marketplace_addr: str,
    *,
    cache_path: Path,
    from_block: int = MARKETPLACE_DEPLOY_BLOCK,
    to_block: int | None = None,
    api_base: str = DEFAULT_BLOCKSCOUT_API,
    refresh: bool = False,
    max_pages: int | None = None,
) -> list[str]:
    """Discover mechs from Blockscout address logs when RPC getLogs is unavailable."""
    marketplace_addr = Web3.to_checksum_address(marketplace_addr)
    cache = (
        {"marketplace": marketplace_addr, "last_scanned_block": None, "mechs": []}
        if refresh
        else _load_mechs_cache(cache_path, marketplace_addr)
    )
    mechs = list(cache.get("mechs", []))
    seen = {m.lower() for m in mechs}
    cached_to = int(cache["last_scanned_block"]) if cache.get("last_scanned_block") else None
    stop_block = cached_to if cached_to is not None and not refresh else from_block - 1

    if to_block is None and cached_to is not None and not refresh:
        log("Using cached mech list: %s mech(s), scanned to block %s", len(mechs), cached_to)
        return mechs

    create_topic = Web3.keccak(text="CreateMech(address,uint256,address)").hex()
    request_topic = Web3.keccak(text="MarketplaceRequest(address,address,uint256,bytes32[],bytes[])").hex()
    if not create_topic.startswith("0x"):
        create_topic = "0x" + create_topic
    if not request_topic.startswith("0x"):
        request_topic = "0x" + request_topic

    path = f"/addresses/{marketplace_addr}/logs"
    params: dict[str, str | int] = {}
    pages = 0
    lowest_seen: int | None = None
    highest_seen = cached_to or 0
    log("Discovering mechs from Blockscout logs: %s", api_base)

    while True:
        data = _blockscout_get_json(api_base, path, params)
        items = data.get("items", [])
        pages += 1
        if not items:
            break

        for item in items:
            block_number = int(item["block_number"])
            lowest_seen = block_number if lowest_seen is None else min(lowest_seen, block_number)
            highest_seen = max(highest_seen, block_number)
            if to_block is not None and block_number > to_block:
                continue
            if block_number <= stop_block:
                break
            topics = item.get("topics") or []
            if not topics:
                continue
            topic0 = str(topics[0]).lower()
            mech = None
            if topic0 == create_topic.lower() and len(topics) >= 2:
                mech = Web3.to_checksum_address("0x" + str(topics[1])[-40:])
            elif topic0 == request_topic.lower():
                for parameter in (item.get("decoded") or {}).get("parameters") or []:
                    if parameter.get("name") == "priorityMech":
                        mech = Web3.to_checksum_address(parameter["value"])
                        break
                if mech is None and len(topics) >= 2:
                    mech = Web3.to_checksum_address("0x" + str(topics[1])[-40:])
            if mech is None:
                continue
            key = mech.lower()
            if key not in seen:
                seen.add(key)
                mechs.append(mech)
                log("priority mech discovered via Blockscout: %s total=%s", mech, len(mechs))

        _save_mechs_cache(
            cache_path,
            {
                "marketplace": marketplace_addr,
                "last_scanned_block": cached_to,
                "blockscout_partial": True,
                "mechs": mechs,
            },
        )
        if lowest_seen is not None and lowest_seen <= stop_block:
            break
        if max_pages is not None and pages >= max_pages:
            log("Blockscout mech discovery stopped at max_pages=%s", max_pages, level=logging.WARNING)
            break
        next_params = data.get("next_page_params")
        if not next_params:
            break
        params = next_params
        if pages % 50 == 0:
            log("Blockscout mech discovery progress: pages=%s lowest_block=%s mechs=%s", pages, lowest_seen, len(mechs))
        time.sleep(0.2)

    if to_block is not None:
        highest_seen = max(highest_seen, to_block)
    cache = {
        "marketplace": marketplace_addr,
        "last_scanned_block": highest_seen or to_block,
        "blockscout_partial": max_pages is not None and pages >= max_pages,
        "mechs": mechs,
    }
    _save_mechs_cache(cache_path, cache)
    log("Blockscout mech discovery complete: %s mech(s), pages=%s. Cache: %s", len(mechs), pages, cache_path)
    return mechs


def discover_priority_mechs(
    rpc_urls: list[str],
    marketplace_addr: str,
    *,
    cache_path: Path,
    from_block: int = MARKETPLACE_DEPLOY_BLOCK,
    to_block: int | None = None,
    window: int = MECH_DISCOVERY_WINDOW,
    refresh: bool = False,
) -> list[str]:
    """Discover Olas mech addresses from Marketplace CreateMech events."""
    if window <= 0:
        raise ValueError("mech discovery window must be > 0")

    marketplace_addr = Web3.to_checksum_address(marketplace_addr)
    cache = (
        {"marketplace": marketplace_addr, "last_scanned_block": None, "mechs": []}
        if refresh
        else _load_mechs_cache(cache_path, marketplace_addr)
    )
    mechs = list(cache.get("mechs", []))
    seen = {m.lower() for m in mechs}

    w3, _ = _connect(rpc_urls)
    web3 = _rpc_web3(w3)
    if to_block is None:
        to_block = _rpc_call(lambda: web3.eth.block_number, name="current block")

    start = from_block
    if cache.get("last_scanned_block") is not None and not refresh:
        start = max(start, int(cache["last_scanned_block"]) + 1)

    if start > to_block:
        log("Mechs cache up to date: %s mech(s), scanned to block %s", len(mechs), cache.get("last_scanned_block"))
        return mechs

    n_windows = max(1, (to_block - start) // window + 1)
    log(
        "Discovering mechs from CreateMech logs: %s windows of %s blocks (%s -> %s)",
        n_windows,
        window,
        start,
        to_block,
    )

    for wi, from_b in enumerate(range(start, to_block + 1, window)):
        to_b = min(from_b + window - 1, to_block)
        logs = None
        last_exc: Exception | None = None
        for url in rpc_urls:
            try:
                direct_w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
                direct_marketplace = _make_marketplace(direct_w3, marketplace_addr)
                logs = direct_marketplace.events.CreateMech.get_logs(
                    from_block=from_b,
                    to_block=to_b,
                )
                break
            except Exception as exc:
                last_exc = exc
                log(
                    "CreateMech logs %s-%s via %s failed (%s)",
                    from_b,
                    to_b,
                    _redact_url(url),
                    type(exc).__name__,
                    level=logging.DEBUG,
                )
        if logs is None:
            log(
                "CreateMech logs %s-%s failed (%s); aborting to avoid gaps",
                from_b,
                to_b,
                type(last_exc).__name__ if last_exc else "unknown",
                level=logging.ERROR,
            )
            raise last_exc or RuntimeError(f"CreateMech logs {from_b}-{to_b} failed")

        for event_log in logs:
            mech = Web3.to_checksum_address(event_log["args"]["mech"])
            key = mech.lower()
            if key not in seen:
                seen.add(key)
                mechs.append(mech)
                log("mech discovered: %s total=%s", mech, len(mechs))

        cache = {
            "marketplace": marketplace_addr,
            "last_scanned_block": to_b,
            "mechs": mechs,
        }
        _save_mechs_cache(cache_path, cache)
        if wi % 100 == 0:
            log("Mech discovery progress: block %s/%s mechs=%s", to_b, to_block, len(mechs))
        time.sleep(DELAY_LOGS)

    log("Mech discovery complete: %s mech(s). Cache: %s", len(mechs), cache_path)
    return mechs


def discover_open_requests_from_mech_queues(
    rpc_urls: list[str],
    marketplace_addr: str,
    mech_addresses: list[str],
    *,
    page_size: int = 100,
    delay_status: float = DELAY_STATUS,
    checkpoint: Path | None = None,
    queue: RequestQueue | None = None,
    max_open: int | None = None,
    payment_type: bytes | None = None,
    delivery_rate: int | None = None,
    cutoff_block: int | None = None,
    cutoff_timestamp: int | None = None,
) -> list[bytes]:
    """Read priority mech queues and keep marketplace-expired requests only."""
    if page_size <= 0:
        raise ValueError("page_size must be > 0")
    if not mech_addresses:
        raise ValueError("at least one priority mech address is required")

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
    known_set = open_set | delivered_set
    if queue:
        known_set |= queue.all_request_ids()

    w3, _ = _connect(rpc_urls)
    web3 = _rpc_web3(w3)
    marketplace = _make_marketplace(web3, marketplace_addr)
    total_candidates = 0

    log(
        "Mech-queue discovery: %s priority mech(s), page_size=%s",
        len(mech_addresses),
        page_size,
    )
    for mech_addr in mech_addresses:
        mech_addr = Web3.to_checksum_address(mech_addr)
        mech = web3.eth.contract(address=web3.to_checksum_address(mech_addr), abi=MECH_ABI)
        try:
            n_undelivered = _rpc_call(
                lambda mech=mech: mech.functions.numUndeliveredRequests().call(),
                name=f"mech {mech_addr[:10]} numUndeliveredRequests",
            )
        except Exception as exc:
            log(
                "mech %s: cannot read numUndeliveredRequests (%s)",
                mech_addr,
                type(exc).__name__,
                level=logging.WARNING,
            )
            continue

        log("mech %s: %s undelivered candidate(s)", mech_addr, n_undelivered)
        offset = 0
        while offset < n_undelivered:
            size = min(page_size, n_undelivered - offset)
            try:
                request_ids = _rpc_call(
                    lambda mech=mech, size=size, offset=offset: mech.functions.getUndeliveredRequestIds(
                        size,
                        offset,
                    ).call(),
                    name=f"mech {mech_addr[:10]} getUndeliveredRequestIds {offset}+{size}",
                )
            except Exception as exc:
                log(
                    "mech %s: page offset=%s size=%s failed (%s)",
                    mech_addr,
                    offset,
                    size,
                    type(exc).__name__,
                    level=logging.WARNING,
                )
                break

            total_candidates += len(request_ids)
            page_opened = 0
            page_skipped = 0
            page_known = 0
            for rid in request_ids:
                rid = bytes(rid)
                h = b32_to_hex(rid)
                if h in known_set:
                    page_known += 1
                    continue

                request_block: int | None = None
                if payment_type is not None:
                    try:
                        info = _rpc_call(
                            lambda rid=rid: marketplace.functions.mapRequestIdInfos(rid).call(),
                            name=f"mapRequestIdInfos {h[:18]}",
                        )
                    except Exception as exc:
                        log("request info failed %s: %s", h[:18], type(exc).__name__, level=logging.WARNING)
                        time.sleep(delay_status)
                        continue

                    request_payment_type = bytes(info[-1])
                    request_block = int(info[3])
                    if request_payment_type != payment_type:
                        log("skip paymentType mismatch: %s...", h[:18], level=logging.DEBUG)
                        known_set.add(h)
                        page_skipped += 1
                        if queue:
                            queue.remember_skipped(h, request_block, "payment_type_mismatch")
                        time.sleep(delay_status)
                        continue
                    if not _request_marker_is_old_enough(
                        request_block,
                        cutoff_block=cutoff_block,
                        cutoff_timestamp=cutoff_timestamp,
                    ):
                        log(
                            "skip younger than 24h cutoff marker %s: %s...",
                            request_block,
                            h[:18],
                            level=logging.DEBUG,
                        )
                        known_set.add(h)
                        page_skipped += 1
                        time.sleep(delay_status)
                        continue
                    max_delivery_rate = int(info[4])
                    if delivery_rate is not None and max_delivery_rate < delivery_rate:
                        log(
                            "skip maxDeliveryRate %s < %s: %s...",
                            max_delivery_rate,
                            delivery_rate,
                            h[:18],
                            level=logging.DEBUG,
                        )
                        known_set.add(h)
                        page_skipped += 1
                        if queue:
                            queue.remember_skipped(h, request_block, "max_delivery_rate_too_low")
                        time.sleep(delay_status)
                        continue

                try:
                    status = _rpc_call(
                        lambda rid=rid: marketplace.functions.getRequestStatus(rid).call(),
                        name=f"getRequestStatus {h[:18]}",
                    )
                except Exception as exc:
                    log("status failed %s: %s", h[:18], type(exc).__name__, level=logging.WARNING)
                    time.sleep(delay_status)
                    continue

                if status == 2:
                    open_set.add(h)
                    known_set.add(h)
                    if queue:
                        queue.enqueue_open(h, request_block)
                    page_opened += 1
                    log("open via mech queue: %s... total=%s", h[:18], len(open_set))
                else:
                    known_set.add(h)
                    if status == 3 and queue:
                        queue.remember_skipped(h, request_block, "already_delivered")
                    page_skipped += 1
                time.sleep(delay_status)
                if max_open and len(open_set) >= max_open:
                    break

            if n_undelivered >= page_size * 4:
                log(
                    "mech %s progress: offset=%s/%s page=%s known=%s opened=%s skipped=%s queue=%s",
                    mech_addr,
                    offset + size,
                    n_undelivered,
                    len(request_ids),
                    page_known,
                    page_opened,
                    page_skipped,
                    queue.counts() if queue else {},
                )

            if checkpoint:
                cp["open_requests"] = sorted(open_set)
                cp["mech_queue_source_count"] = len(mech_addresses)
                cp["mech_queue_candidates_seen"] = total_candidates
                save_checkpoint(checkpoint, cp)

            if max_open and len(open_set) >= max_open:
                break
            offset += size

        if max_open and len(open_set) >= max_open:
            break

    result = [hex_to_b32(h) for h in sorted(open_set)]
    log(
        "Mech-queue discovery complete: %s open request(s), %s candidate(s) checked",
        len(result),
        total_candidates,
    )
    return result


# ── Delivery (Anvil impersonation) ─────────────────────────────────────────────


def deliver_batch_anvil(
    w3: Web3,
    batch: list[bytes],
    safe_addr: str,
    mech_addr: str,
    marketplace_addr: str,
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
    delivered_ids = _delivered_ids_from_receipt_or_status(
        w3,
        receipt,
        batch,
        marketplace_addr,
    ) if ok else []
    log(
        "TX 0x%s... [%s] — %s/%s delivered",
        tx_hash.hex()[:18],
        "OK" if ok else "REVERTED",
        len(delivered_ids),
        len(batch),
    )
    return ok, delivered_ids


# ── Delivery (real Safe via safe_eth) ─────────────────────────────────────────


def deliver_batch_real(
    batch: list[bytes],
    safe_addr: str,
    mech_addr: str,
    marketplace_addr: str,
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
            log("Simulation: %s request(s) not deliverable — filtered out", n_filtered)
    except Exception as e:
        log("Simulation failed (%s) — aborting batch for safety", type(e).__name__, level=logging.WARNING)
        return False, []

    if not pairs:
        log("Simulation: batch empty after filtering — skipping")
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
    log("TX %s... submitted (%s request(s))", tx_hash[:20], len(eff_batch))

    # BLK-1: Wait for on-chain confirmation
    try:
        receipt = ec.w3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=180)
        if receipt["status"] != 1:
            log("TX %s... REVERTED — nothing marked as delivered", tx_hash[:20], level=logging.ERROR)
            return False, []
        delivered_ids = _delivered_ids_from_receipt_or_status(
            ec.w3,
            receipt,
            eff_batch,
            marketplace_addr,
        )
        log(
            "TX %s... CONFIRMED (%s/%s delivered)",
            tx_hash[:20],
            len(delivered_ids),
            len(eff_batch),
        )
        return True, delivered_ids
    except Exception as e:
        log("Receipt timeout (%s) — marking as undelivered for safety", type(e).__name__, level=logging.WARNING)
        return False, []


def _delivered_ids_from_receipt_or_status(
    w3: Web3,
    receipt,
    request_ids: list[bytes],
    marketplace_addr: str,
) -> list[bytes]:
    """Return ids accepted by marketplace, using event flags then status fallback."""
    marketplace = _make_marketplace(w3, marketplace_addr)

    try:
        events = marketplace.events.MarketplaceDelivery().process_receipt(receipt)
        wanted = {b32_to_hex(r): r for r in request_ids}
        delivered: list[bytes] = []
        for event in events:
            args = event["args"]
            for rid, ok in zip(args["requestIds"], args["deliveredRequests"]):
                h = b32_to_hex(rid)
                if ok and h in wanted:
                    delivered.append(wanted[h])
        if delivered:
            return delivered
        if events:
            return []
    except Exception as e:
        log("Could not decode delivery flags (%s); checking status", type(e).__name__, level=logging.WARNING)

    delivered = []
    for rid in request_ids:
        try:
            if _rpc_call(
                lambda: _make_marketplace(
                    _rpc_web3(w3),
                    marketplace_addr,
                ).functions.getRequestStatus(rid).call(),
                name=f"post-tx status {b32_to_hex(rid)[:18]}",
            ) == 3:
                delivered.append(rid)
        except Exception as e:
            log("post-TX status check %s: %s", b32_to_hex(rid)[:18], type(e).__name__, level=logging.WARNING)
    return delivered


def deliver_all(
    open_requests: list[bytes],
    *,
    runtime: RuntimeConfig,
    mode: str,
    w3: Web3,
    private_key: str | None = None,
    rpc_url: str | None = None,
    delay_tx: float = DELAY_TX,
    batch_size: int = BATCH_SIZE,
    checkpoint: Path | None = None,
    rpc_urls: list[str] | None = None,
    queue: RequestQueue | None = None,
    cutoff_block: int | None = None,
    cutoff_timestamp: int | None = None,
) -> int:
    """Deliver all open requests in batches. Returns count of delivered requests."""
    safe_addr = Web3.to_checksum_address(runtime.safe_addr)
    mech_addr = Web3.to_checksum_address(runtime.mech_addr)
    marketplace_addr = Web3.to_checksum_address(runtime.marketplace_addr)

    cp = load_checkpoint(checkpoint) if checkpoint else {"open_requests": [], "delivered": []}
    delivered_set: set[str] = set(cp.get("delivered", []))

    pending = [r for r in open_requests if b32_to_hex(r) not in delivered_set]
    n_batches = (len(pending) + batch_size - 1) // batch_size
    log("%s request(s) to deliver in %s batch(es) of %s", len(pending), n_batches, batch_size)

    total = 0
    for i in range(0, len(pending), batch_size):
        original_batch = pending[i : i + batch_size]
        batch = original_batch
        bn = i // batch_size + 1
        log("Batch %s/%s: %s request(s)", bn, n_batches, len(batch))
        if queue:
            queue.mark_delivering(batch)

        try:
            if mode == "anvil":
                tx_ok, delivered_ids = deliver_batch_anvil(
                    w3,
                    batch,
                    safe_addr,
                    mech_addr,
                    marketplace_addr,
                )
            else:
                tx_ok, delivered_ids = deliver_batch_real(
                    batch,
                    safe_addr,
                    mech_addr,
                    marketplace_addr,
                    rpc_url,
                    private_key,
                    w3,
                )
            for r in delivered_ids:
                delivered_set.add(b32_to_hex(r))
            total += len(delivered_ids)
            if queue:
                if tx_ok:
                    queue.mark_delivered(delivered_ids)
                    not_delivered = [r for r in batch if r not in set(delivered_ids)]
                    queue.mark_skipped(not_delivered, "not_delivered_after_tx")
                else:
                    queue.mark_open(batch, "tx_failed_or_simulation_failed")
        except Exception as e:
            log("Batch %s failed: %s: %s", bn, type(e).__name__, e, level=logging.ERROR)
            if queue:
                queue.mark_open(batch, type(e).__name__)

        if checkpoint:
            cp["delivered"] = sorted(delivered_set)
            save_checkpoint(checkpoint, cp)

        if i + batch_size < len(pending):
            log("Waiting %.0fs...", delay_tx)
            time.sleep(delay_tx)

    return total


# ── Anvil test ─────────────────────────────────────────────────────────────────


def start_anvil(fork_url: str, port: int | None = None) -> tuple[subprocess.Popen, str]:
    port = _find_free_port(port)
    anvil_rpc = f"http://127.0.0.1:{port}"
    log("Starting Anvil fork port=%s fork_url=%s", port, _redact_url(fork_url))
    proc = subprocess.Popen(
        [
            "anvil",
            "--fork-url",
            fork_url,
            "--port",
            str(port),
            "--block-time",
            "1",
            "--silent",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    w3 = Web3(Web3.HTTPProvider(anvil_rpc))
    for _ in range(30):
        time.sleep(0.5)
        if proc.poll() is not None:  # D7: detect early crash
            stderr = (proc.stderr.read() if proc.stderr else "").strip()
            raise RuntimeError(
                f"Anvil crashed (exit {proc.returncode}) on port {port}: {stderr}"
            )
        try:
            if w3.is_connected():
                log("Anvil ready at block %s", w3.eth.block_number)
                return proc, anvil_rpc
        except Exception:
            pass
    proc.kill()
    stderr = (proc.stderr.read() if proc.stderr else "").strip()
    raise RuntimeError("Anvil did not start in 15s.")


def run_anvil_test(runtime: RuntimeConfig, max_requests: int = 15) -> None:
    """End-to-end Anvil test: find 15 open requests, deliver in batches, verify."""
    print("=" * 60)
    print("Anvil recovery test")
    print("=" * 60)

    TARGET = max_requests
    rpc_url = _default_rpc_urls(runtime.chain)[0]

    print(f"\n[1] Discovering up to {TARGET} open requests on Gnosis (>3 days old)...")
    w3_gnosis, _ = _connect(_default_rpc_urls(runtime.chain), timeout=20)
    current = w3_gnosis.eth.block_number
    scan_to = current - BLOCKS_24H
    scan_from = current - BLOCKS_3D

    if scan_from >= scan_to:
        log("empty scan range — check block constants", level=logging.ERROR)
        sys.exit(1)

    open_requests = discover_open_requests(
        _default_rpc_urls(runtime.chain),
        runtime.marketplace_addr,
        scan_from=scan_from,
        scan_to=scan_to,
        max_open=TARGET,
        delay_logs=0.05,
        delay_status=0.05,
    )
    if not open_requests:
        log("No open requests found — cannot run test.", level=logging.ERROR)
        sys.exit(1)

    print("\n[2] Starting Anvil fork of Gnosis...")
    anvil_proc, anvil_rpc = start_anvil(rpc_url)

    try:
        w3 = Web3(Web3.HTTPProvider(anvil_rpc))
        safe_addr = Web3.to_checksum_address(runtime.safe_addr)
        mech_addr = Web3.to_checksum_address(runtime.mech_addr)
        marketplace = _make_marketplace(w3, runtime.marketplace_addr)
        mech = w3.eth.contract(address=mech_addr, abi=MECH_ABI)

        payment_type = mech.functions.paymentType().call()
        bt_addr = marketplace.functions.mapPaymentTypeBalanceTrackers(payment_type).call()
        bt = w3.eth.contract(address=w3.to_checksum_address(bt_addr), abi=BALANCE_TRACKER_ABI)

        balance_before = bt.functions.mapMechBalances(mech_addr).call()
        log("BalanceTracker: %s", bt_addr)
        log("Balance before: %.6f xDAI", balance_before / 1e18)

        for rid in open_requests:
            s = marketplace.functions.getRequestStatus(rid).call()
            assert s == 2, f"Expected status=2, got {s} for {rid.hex()[:16]}"
        log("All %s confirmed status=2 on fork", len(open_requests))

        w3.provider.make_request("anvil_impersonateAccount", [safe_addr])
        w3.provider.make_request("anvil_setBalance", [safe_addr, hex(10 * 10**18)])

        log("Delivering %s request(s) in batches of %s...", len(open_requests), BATCH_SIZE)
        total = deliver_all(
            open_requests,
            runtime=runtime,
            mode="anvil",
            w3=w3,
            delay_tx=2.0,
            batch_size=BATCH_SIZE,
        )

        balance_after = bt.functions.mapMechBalances(mech_addr).call()
        delta = balance_after - balance_before
        log("Balance after: %.6f xDAI", balance_after / 1e18)
        log("Balance delta: +%.6f xDAI", delta / 1e18)

        n_spot = min(3, len(open_requests))
        for rid in open_requests[:n_spot]:
            s = marketplace.functions.getRequestStatus(rid).call()
            assert s == 3, f"Expected status=3, got {s} for {rid.hex()[:16]}"
        log("Status spot-check: first %s delivered", n_spot)

        assert balance_after > balance_before, "Balance did not increase!"
        assert total == len(open_requests), f"Expected {len(open_requests)} delivered, got {total}"
        log("PASS — %s request(s) delivered, +%.6f xDAI credited.", total, delta / 1e18)

    finally:
        anvil_proc.kill()
        anvil_proc.wait(timeout=5)
        log("Anvil stopped.")


# ── Real-mode entrypoints ──────────────────────────────────────────────────────


def _get_private_key(runtime: RuntimeConfig, password_env: str) -> str:
    env_pk = os.environ.get("AGENT_PRIVATE_KEY", "")
    if env_pk:
        try:
            _validate_private_key(env_pk)
        except ValueError as e:
            log("AGENT_PRIVATE_KEY invalid: %s", e, level=logging.ERROR)
            sys.exit(1)
        return env_pk

    password = os.environ.get(password_env)
    if password is None:
        password = getpass.getpass("Wallet password: ")
    try:
        pk = _private_key_from_wallet(runtime.wallet_path, runtime.safe_addr, password)
        _validate_private_key(pk)
        return pk
    except Exception as e:
        log("Could not decrypt Safe signer from wallet.json: %s", type(e).__name__, level=logging.ERROR)
        sys.exit(1)


def cmd_discover(args: argparse.Namespace, runtime: RuntimeConfig) -> None:
    checkpoint = Path(args.checkpoint)
    queue = RequestQueue(Path(args.queue))
    cp = load_checkpoint(checkpoint)
    rpc_list = _rpc_list(args, runtime.chain)

    if args.discovery_source == "mech-queues":
        try:
            mechs = _load_mech_sources(args)
            if not mechs:
                w3, _ = _connect(rpc_list)
                current = _rpc_call(lambda: _rpc_web3(w3).eth.block_number, name="current block")
                if args.lookback_days is not None:
                    mech_from_block = current - int(args.lookback_days * 24 * 3600 / GNOSIS_BLOCK_TIME)
                else:
                    mech_from_block = MARKETPLACE_DEPLOY_BLOCK
                if args.mech_discovery_provider in ("rpc", "auto"):
                    try:
                        mechs = discover_priority_mechs(
                            rpc_list,
                            runtime.marketplace_addr,
                            cache_path=Path(args.mechs_cache),
                            from_block=mech_from_block,
                            to_block=current,
                            window=args.mech_discovery_window,
                            refresh=args.refresh_mechs,
                        )
                    except Exception as exc:
                        if args.mech_discovery_provider == "rpc":
                            raise
                        log(
                            "RPC mech discovery failed (%s); falling back to Blockscout",
                            type(exc).__name__,
                            level=logging.WARNING,
                        )
                        mechs = []
                if not mechs and args.mech_discovery_provider in ("blockscout", "auto"):
                    mechs = discover_priority_mechs_blockscout(
                        runtime.marketplace_addr,
                        cache_path=Path(args.mechs_cache),
                        from_block=mech_from_block,
                        to_block=current,
                        api_base=args.blockscout_api,
                        refresh=args.refresh_mechs,
                        max_pages=args.blockscout_max_pages,
                    )
        except Exception as exc:
            log("priority mech source: %s", exc, level=logging.ERROR)
            sys.exit(1)
        if not mechs:
            log("No priority mechs found; cannot use mech-queues discovery", level=logging.ERROR)
            sys.exit(1)
        w3, _ = _connect(rpc_list)
        current = _rpc_call(lambda: _rpc_web3(w3).eth.block_number, name="current block")
        latest_ts = int(_rpc_call(lambda: _rpc_web3(w3).eth.get_block(current)["timestamp"], name="latest timestamp"))
        cutoff_block = current - BLOCKS_24H
        cutoff_timestamp = latest_ts - 24 * 3600
        log(
            "Filtering mech queues by request marker older than 24h: block <= %s or timestamp <= %s",
            cutoff_block,
            cutoff_timestamp,
        )
        mech_contract = _rpc_web3(w3).eth.contract(
            address=_rpc_web3(w3).to_checksum_address(runtime.mech_addr),
            abi=MECH_ABI,
        )
        payment_type = _rpc_call(
            lambda: mech_contract.functions.paymentType().call(),
            name="mech paymentType",
        )
        log("Filtering mech queues by paymentType=%s", "0x" + bytes(payment_type).hex())
        if runtime.delivery_rate is not None:
            log("Filtering mech queues by maxDeliveryRate >= %s", runtime.delivery_rate)
        open_requests = discover_open_requests_from_mech_queues(
            rpc_list,
            runtime.marketplace_addr,
            mechs,
            page_size=args.mech_queue_page_size,
            delay_status=args.mech_queue_delay_status,
            checkpoint=checkpoint,
            queue=queue,
            max_open=args.max_discover,
            payment_type=bytes(payment_type),
            delivery_rate=runtime.delivery_rate,
            cutoff_block=cutoff_block,
            cutoff_timestamp=cutoff_timestamp,
        )
        log("Found %s open request(s). Checkpoint: %s", len(open_requests), checkpoint)
        log("Queue: %s counts=%s", queue.path, queue.counts())
        return

    w3, _ = _connect(rpc_list)
    current = _rpc_call(lambda: _rpc_web3(w3).eth.block_number, name="current block")
    scan_to = current - BLOCKS_24H

    # BLK-3: sticky scan_from_block — reuse stored value on resume to avoid gaps.
    # Default: scan from contract deployment (full history). Override with --lookback-days.
    if args.lookback_days is not None:
        computed_scan_from = current - int(args.lookback_days * 24 * 3600 / GNOSIS_BLOCK_TIME)
    else:
        computed_scan_from = MARKETPLACE_DEPLOY_BLOCK
    stored_scan_from = cp.get("scan_from_block")
    if stored_scan_from is not None:
        scan_from = stored_scan_from
        log("Resume: using stored scan_from_block=%s", scan_from)
        diff_days = abs(scan_from - computed_scan_from) * GNOSIS_BLOCK_TIME / 86400
        if diff_days >= 1:
            log(
                "computed scan_from would be %s but stored is %s (%.1f day diff) — stored takes precedence",
                computed_scan_from,
                scan_from,
                diff_days,
                level=logging.WARNING,
            )
    else:
        scan_from = computed_scan_from

    if scan_from >= scan_to:
        log(
            "scan_from (%s) >= scan_to (%s). Try --lookback-days N.",
            scan_from,
            scan_to,
            level=logging.ERROR,
        )
        sys.exit(1)

    resume_from = cp.get("last_scanned_block")
    if resume_from is not None:
        resume_from += 1
        if resume_from > scan_to:
            log(
                "Scan already complete (scanned up to %s). Run --mode deliver to deliver.",
                resume_from - 1,
            )
            return
        log("Resuming from block %s", resume_from)

    origin = f"block {scan_from} (contract deployment)" if args.lookback_days is None else f"{args.lookback_days} days"
    log("Discovery: blocks %s-%s (from %s, excluding last 24h)", scan_from, scan_to, origin)
    open_requests = discover_open_requests(
        rpc_list,
        runtime.marketplace_addr,
        scan_from=scan_from,
        scan_to=scan_to,
        delay_logs=DELAY_LOGS,
        delay_status=DELAY_STATUS,
        checkpoint=checkpoint,
        resume_from=resume_from,
        queue=queue,
        max_open=args.max_discover,
    )
    log("Found %s open request(s). Checkpoint: %s", len(open_requests), checkpoint)
    log("Queue: %s counts=%s", queue.path, queue.counts())


def cmd_deliver(args: argparse.Namespace, runtime: RuntimeConfig) -> None:
    checkpoint = Path(args.checkpoint)
    queue = RequestQueue(Path(args.queue))
    cp = load_checkpoint(checkpoint)
    rpc_list = _rpc_list(args, runtime.chain)

    queued_ids = queue.get_open(args.max_deliver or 1_000_000_000)
    stored_ids = cp.get("open_requests", [])
    if queued_ids:
        open_requests = queued_ids
        source = f"queue {queue.path}"
    elif stored_ids:
        open_requests = [hex_to_b32(h) for h in stored_ids]
        source = f"checkpoint {checkpoint}"
    else:
        log("No open requests in queue/checkpoint. Run --mode discover first.", level=logging.ERROR)
        sys.exit(1)

    w3, rpc_url = _connect(rpc_list)

    # OPS-1 + D6: explicit confirmation with summary
    n_batches = (len(open_requests) + args.batch_size - 1) // args.batch_size
    print(f"""
⚠️  DELIVERY SUMMARY
   Config:      {runtime.config_path}
   Wallet:      {runtime.wallet_path}
   Chain:       {runtime.chain}
   Safe:        {runtime.safe_addr}
   Mech:        {runtime.mech_addr}
   Marketplace: {runtime.marketplace_addr}
   Source:      {source}
   Queue:       {queue.path} counts={queue.counts()}
   Requests:    {len(open_requests)} entries, {n_batches} batch(es) of ≤{args.batch_size}
                (status is re-validated on-chain before delivery)
   Payload:     b\"{{}}\"  — requesters receive an empty delivery in exchange for payment
   Delay:       {DELAY_TX}s between batches

   ⚠ Stop the production mech BEFORE proceeding — concurrent Safe signing
     from both processes causes GS026 nonce races.
   ⚠ --rpc override uses a single RPC URL with no automatic failover during delivery.
""")
    if not _confirm("Proceed with real delivery?"):
        log("Aborted.", level=logging.WARNING)
        sys.exit(0)

    private_key = _get_private_key(runtime, args.password_env)
    total = deliver_all(
        open_requests,
        runtime=runtime,
        mode="real",
        w3=w3,
        private_key=private_key,
        rpc_url=rpc_url,
        delay_tx=DELAY_TX,
        batch_size=args.batch_size,
        checkpoint=checkpoint,
        rpc_urls=rpc_list,
        queue=queue,
    )
    log("Done — %s request(s) delivered.", total)


def cmd_all(args: argparse.Namespace, runtime: RuntimeConfig) -> None:
    """Run discovery and delivery concurrently through the persistent queue."""
    checkpoint = Path(args.checkpoint)
    queue = RequestQueue(Path(args.queue))
    rpc_list = _rpc_list(args, runtime.chain)
    w3, rpc_url = _connect(rpc_list)

    print(f"""
⚠️  PIPELINE SUMMARY
   Config:      {runtime.config_path}
   Wallet:      {runtime.wallet_path}
   Chain:       {runtime.chain}
   Safe:        {runtime.safe_addr}
   Mech:        {runtime.mech_addr}
   Marketplace: {runtime.marketplace_addr}
   Checkpoint:  {checkpoint}
   Queue:       {queue.path} counts={queue.counts()}
   Payload:     b\"{{}}\"
   Delivery:    sequential Safe TXs, batches of ≤{args.batch_size}

   Discovery and delivery will run at the same time. Safe submissions remain
   sequential to avoid nonce races.
""")
    if not _confirm("Proceed with real discovery+delivery?"):
        log("Aborted.", level=logging.WARNING)
        sys.exit(0)

    private_key = _get_private_key(runtime, args.password_env)
    done = threading.Event()
    errors: list[BaseException] = []

    def _producer() -> None:
        try:
            cmd_discover(args, runtime)
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=_producer, name="recover-discovery", daemon=True)
    thread.start()

    total = 0
    while True:
        batch = queue.get_open(args.batch_size)
        if batch:
            total += deliver_all(
                batch,
                runtime=runtime,
                mode="real",
                w3=w3,
                private_key=private_key,
                rpc_url=rpc_url,
                delay_tx=0,
                batch_size=args.batch_size,
                checkpoint=checkpoint,
                rpc_urls=rpc_list,
                queue=queue,
            )
            log("Waiting %.0fs before next Safe TX...", DELAY_TX)
            time.sleep(DELAY_TX)
            continue

        if done.is_set():
            break
        time.sleep(2)

    thread.join()
    if errors:
        log("discovery failed: %s", type(errors[0]).__name__, level=logging.ERROR)
        sys.exit(1)
    log("Done — %s request(s) delivered. Queue counts=%s", total, queue.counts())


# ── CLI ────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--anvil-test", action="store_true", help="Run Anvil end-to-end test")
    parser.add_argument("--mode", choices=["discover", "deliver", "all"], default="discover")
    parser.add_argument("--config", default="data/config.yaml", help="Path to config.yaml")
    parser.add_argument("--wallet", default="data/wallet.json", help="Path to wallet.json")
    parser.add_argument("--chain", default="gnosis", help="Chain key under plugins.micromech.chains")
    parser.add_argument("--service-key", default=None, help="OLAS service key if config has multiple")
    parser.add_argument("--mech", default=None, help="Override mech address from config.yaml")
    parser.add_argument("--safe", default=None, help="Override Safe multisig address from config.yaml")
    parser.add_argument("--marketplace", default=None, help="Override marketplace address from config.yaml")
    parser.add_argument(
        "--password-env",
        default="wallet_password",
        help="Environment variable containing the wallet password",
    )
    parser.add_argument(
        "--anvil-max-requests",
        type=int,
        default=15,
        help="Maximum requests to recover during --anvil-test",
    )
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT))
    parser.add_argument("--queue", default=str(DEFAULT_QUEUE), help="Path to SQLite request queue")
    parser.add_argument("--log", default=str(DEFAULT_LOG), help="Path to log file")
    parser.add_argument("--mechs-cache", default=str(DEFAULT_MECHS_CACHE), help="Path to priority mech cache")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Maximum request IDs per deliverToMarketplace transaction",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug console logs")
    parser.add_argument("--no-iwa", action="store_true", help="Disable iwa RPC retry/rotation")
    parser.add_argument(
        "--discovery-source",
        choices=["marketplace-logs", "mech-queues"],
        default="marketplace-logs",
        help="How --mode discover finds request IDs",
    )
    parser.add_argument(
        "--priority-mechs",
        default=None,
        help="Comma-separated priority mech addresses for --discovery-source mech-queues",
    )
    parser.add_argument(
        "--priority-mechs-file",
        default=None,
        help="File with one priority mech address per line for --discovery-source mech-queues",
    )
    parser.add_argument(
        "--mech-queue-page-size",
        type=int,
        default=250,
        help="Number of request IDs per getUndeliveredRequestIds page",
    )
    parser.add_argument(
        "--mech-queue-delay-status",
        type=float,
        default=0.05,
        help="Delay between per-request mech-queue checks; iwa handles RPC retry/rotation",
    )
    parser.add_argument(
        "--mech-discovery-window",
        type=int,
        default=MECH_DISCOVERY_WINDOW,
        help="Block window for CreateMech log discovery",
    )
    parser.add_argument(
        "--mech-discovery-provider",
        choices=["auto", "rpc", "blockscout"],
        default="auto",
        help="How to discover priority mechs when no explicit list is provided",
    )
    parser.add_argument(
        "--blockscout-api",
        default=DEFAULT_BLOCKSCOUT_API,
        help="Blockscout API v2 base URL for mech discovery fallback",
    )
    parser.add_argument(
        "--blockscout-max-pages",
        type=int,
        default=None,
        help="Optional safety cap for Blockscout address log pages",
    )
    parser.add_argument(
        "--refresh-mechs",
        action="store_true",
        help="Ignore recover_mechs.json and rebuild priority mech cache",
    )
    parser.add_argument(
        "--max-discover",
        type=int,
        default=None,
        help="Stop discovery after this many open requests are found",
    )
    parser.add_argument(
        "--max-deliver",
        type=int,
        default=None,
        help="Maximum queued requests to deliver in this run",
    )
    parser.add_argument("--rpc", default=None, help="Override RPC URL")
    parser.add_argument(
        "--lookback-days",
        type=float,
        default=None,
        help="Days to look back (default: from contract deployment block 38661963 / 2025-02-20)",
    )
    args = parser.parse_args()
    setup_logging(Path(args.log), verbose=args.verbose)
    if args.batch_size <= 0:
        log("--batch-size must be > 0", level=logging.ERROR)
        sys.exit(1)

    # LOW-2: Reject non-HTTPS RPCs in real mode (Anvil uses http locally — exempt)
    if args.rpc and not args.anvil_test and not args.rpc.startswith("https://"):
        log("--rpc must use HTTPS. Got: %s", args.rpc, level=logging.ERROR)
        sys.exit(1)

    try:
        runtime = _resolve_runtime_config(args)
    except Exception as e:
        log("runtime config: %s", e, level=logging.ERROR)
        sys.exit(1)

    setup_iwa_rpc(runtime.chain, enabled=(not args.rpc and not args.no_iwa))

    if args.anvil_test:
        run_anvil_test(runtime, max_requests=args.anvil_max_requests)
        return

    if args.mode == "discover":
        cmd_discover(args, runtime)
    elif args.mode == "deliver":
        cmd_deliver(args, runtime)
    elif args.mode == "all":
        cmd_all(args, runtime)


if __name__ == "__main__":
    main()
