"""Shared fixtures for integration tests.

Session-scoped Anvil fork management: forks are started automatically
when integration tests begin and killed when the session ends.
Each test gets snapshot/revert isolation via per-test fixtures in the
individual test files.
"""

import os
import subprocess
import time

import pytest
from web3 import Web3

# ---------------------------------------------------------------------------
# Anvil fork configuration
# ---------------------------------------------------------------------------

_ANVIL_BIN = os.path.expanduser("~/.foundry/bin/anvil")

_CHAIN_PORTS: dict[str, int] = {
    "gnosis": 18545,
    "base": 18546,
    "ethereum": 18547,
    "polygon": 18548,
    "optimism": 18549,
    "arbitrum": 18550,
    "celo": 18551,
}

_RPC_KEYS: dict[str, str] = {
    "gnosis": "gnosis_rpc",
    "base": "base_rpc",
    "ethereum": "ethereum_rpc",
    "polygon": "polygon_rpc",
    "optimism": "optimism_rpc",
    "arbitrum": "arbitrum_rpc",
    "celo": "celo_rpc",
}

_SECRETS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "secrets.env")


def _read_rpcs() -> dict[str, str]:
    """Read RPC URLs from secrets.env (first URL if comma-separated)."""
    rpcs: dict[str, str] = {}
    try:
        with open(_SECRETS_PATH) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    rpcs[key.strip()] = val.strip().split(",")[0]
    except FileNotFoundError:
        pass
    return rpcs


def _is_port_responding(port: int) -> bool:
    """Check if an Anvil fork is already running on this port."""
    try:
        w3 = Web3(
            Web3.HTTPProvider(
                f"http://localhost:{port}",
                request_kwargs={"timeout": 2},
            )
        )
        return w3.is_connected()
    except Exception:
        return False


def _fork_is_stale(port: int, rpc_url: str, max_lag: int = 3) -> bool:
    """Return True if the running fork on *port* is too far behind the live chain.

    Some RPC providers (e.g. rpc.gnosis.gateway.fm) have a very short sliding
    window — once the chain advances past the fork block the provider refuses
    to serve it, causing BlockOutOfRangeError in every subsequent call.

    We kill and restart the fork whenever it lags the live chain by more than
    *max_lag* blocks.
    """
    try:
        local_w3 = Web3(Web3.HTTPProvider(f"http://localhost:{port}", request_kwargs={"timeout": 2}))
        local_block = local_w3.eth.block_number
    except Exception:
        return False  # can't connect locally — not our problem here

    try:
        live_w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 5}))
        live_block = live_w3.eth.block_number
    except Exception:
        return False  # can't reach live RPC — leave the existing fork alone

    lag = live_block - local_block
    if lag > max_lag:
        print(f"  anvil: port {port} fork is stale (local={local_block}, live={live_block}, lag={lag})")
        return True
    return False


def _wait_ready(port: int, proc: subprocess.Popen, timeout: int = 30) -> None:
    """Block until Anvil fork responds on the given port."""
    deadline = time.monotonic() + timeout
    url = f"http://localhost:{port}"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Anvil exited early on port {port} (code {proc.returncode})")
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 2}))
            if w3.is_connected():
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Anvil not ready on port {port} after {timeout}s")


# ---------------------------------------------------------------------------
# Session-scoped fixture: auto-start Anvil forks
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _anvil_forks():
    """Start Anvil forks for every chain that has an RPC in secrets.env.

    - Reuses forks that are already running (manual ``just anvil-fork``).
    - Sets ANVIL_<CHAIN> and ANVIL_URL env vars so test fixtures find them.
    - Kills only the processes it started on teardown.
    """
    if not os.path.isfile(_ANVIL_BIN):
        pytest.skip(f"Anvil not installed at {_ANVIL_BIN}")

    secrets = _read_rpcs()
    started: dict[str, subprocess.Popen] = {}  # only processes WE started

    for chain, port in _CHAIN_PORTS.items():
        rpc_key = _RPC_KEYS[chain]
        rpc_url = secrets.get(rpc_key)

        if _is_port_responding(port):
            # Reuse if fresh; kill and restart if the fork is stale.
            if rpc_url and not rpc_url.startswith("#") and _fork_is_stale(port, rpc_url):
                subprocess.run(
                    f"lsof -ti:{port} | xargs -r kill -9 2>/dev/null",
                    shell=True,
                    capture_output=True,
                )
                time.sleep(0.5)
            else:
                env_var = f"ANVIL_{chain.upper()}"
                os.environ[env_var] = f"http://localhost:{port}"
                print(f"  anvil: {chain}:{port} already running (reusing)")
                continue

        if not rpc_url or rpc_url.startswith("#"):
            continue

        # Kill any stale listeners on the port (belt-and-suspenders)
        subprocess.run(
            f"lsof -ti:{port} | xargs -r kill 2>/dev/null",
            shell=True,
            capture_output=True,
        )

        proc = subprocess.Popen(
            [
                _ANVIL_BIN,
                "--fork-url",
                rpc_url,
                "--port",
                str(port),
                "--auto-impersonate",
                "--silent",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        started[chain] = proc
        env_var = f"ANVIL_{chain.upper()}"
        os.environ[env_var] = f"http://localhost:{port}"

    # Set ANVIL_URL for test_anvil_e2e.py (defaults to gnosis)
    if "gnosis" in started or _is_port_responding(_CHAIN_PORTS["gnosis"]):
        os.environ["ANVIL_URL"] = f"http://localhost:{_CHAIN_PORTS['gnosis']}"

    os.environ["CHAINLIST_ENRICHMENT"] = "false"

    # Wait for all forks WE started (skip chains that fail)
    failed: list[str] = []
    for chain, proc in list(started.items()):
        port = _CHAIN_PORTS[chain]
        try:
            _wait_ready(port, proc)
            print(f"  anvil: {chain}:{port} started (pid {proc.pid})")
        except RuntimeError as exc:
            print(f"  anvil: {chain}:{port} FAILED to start ({exc})")
            proc.terminate()
            del started[chain]
            os.environ.pop(f"ANVIL_{chain.upper()}", None)
            failed.append(chain)

    if not started and not any(_is_port_responding(p) for p in _CHAIN_PORTS.values()):
        pytest.skip("No RPC URLs in secrets.env and no Anvil forks running")

    yield

    # Teardown: kill only what we started
    for chain, proc in started.items():
        proc.terminate()
    for chain, proc in started.items():
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


# ---------------------------------------------------------------------------
# Per-test fixture: reset iwa IPFS session
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_ipfs_session():
    """Reset iwa's global aiohttp session before each integration test.

    iwa.core.ipfs caches an aiohttp.ClientSession at module level.
    If a previous test closed the event loop, this cached session becomes
    orphaned and poisons all subsequent IPFS calls with 'Event loop is closed'.
    """
    try:
        import iwa.core.ipfs as ipfs_mod

        ipfs_mod._ASYNC_SESSION = None
    except (ImportError, AttributeError):
        pass
    yield
    try:
        import iwa.core.ipfs as ipfs_mod

        ipfs_mod._ASYNC_SESSION = None
    except (ImportError, AttributeError):
        pass
