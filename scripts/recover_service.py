#!/usr/bin/env python3
"""
Recover funds from an OLAS service stuck in ACTIVE_REGISTRATION.

Sequence: terminate -> unbond -> drain
(recovers OLAS bond + any xDAI in agent key)

Usage
-----
# 1. Start Anvil fork of Gnosis (test first!) -- or: just anvil-fork
anvil --fork-url <gnosis_rpc> --port 18545

# 2. Fund master with xDAI for gas on the fork
python scripts/anvil_fund.py <master_address> gnosis

# 3. Test run against Anvil (no real funds at risk)
python scripts/recover_service.py \\
    --service gnosis:3085 \\
    --wallet ~/Descargas/wallet.json \\
    --rpc http://localhost:18545

# 4. Once Anvil test succeeds, run on mainnet (omit --rpc)
python scripts/recover_service.py \\
    --service gnosis:3085 \\
    --wallet ~/Descargas/wallet.json

Requirements: pip install micromech[chain]
"""

import argparse
import getpass
import os
import shutil
import sys
import tempfile
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _confirm(prompt: str) -> None:
    """Prompt for confirmation; exit on Ctrl-C."""
    try:
        input(f"\n{prompt}  [Press ENTER to continue, Ctrl-C to abort] ")
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(0)


def _fund_anvil_account(
    rpc_url: str, address: str, amount_eth: float = 1.0
) -> None:
    """Use anvil_setBalance to fund an account on a local Anvil node."""
    import json
    import urllib.request

    amount_hex = hex(int(amount_eth * 10**18))
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "anvil_setBalance",
        "params": [address, amount_hex],
    }).encode()

    req = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        result = json.loads(resp.read())
    if result.get("error"):
        print(f"  WARNING: anvil_setBalance failed: {result['error']}")
    else:
        print(f"  Funded {address} with {amount_eth} xDAI on Anvil.")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recover funds from a stuck OLAS service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--service", required=True, metavar="CHAIN:ID",
        help="Service key (e.g. gnosis:3085)",
    )
    parser.add_argument(
        "--wallet", required=True, metavar="PATH",
        help="Path to wallet.json",
    )
    parser.add_argument(
        "--rpc", default=None, metavar="URL",
        help="RPC URL override (use http://localhost:18545 for Anvil).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Check state only -- do not send any transactions.",
    )
    args = parser.parse_args()

    wallet_path = Path(args.wallet).expanduser().resolve()
    if not wallet_path.exists():
        print(f"ERROR: wallet not found: {wallet_path}")
        sys.exit(1)

    is_anvil = bool(args.rpc and "localhost" in args.rpc)
    chain_name, service_id_str = args.service.split(":", 1)
    service_id = int(service_id_str)

    password = (
        os.environ.get("wallet_password")
        or getpass.getpass("Wallet password: ")
    )

    print(f"\n{'=' * 60}")
    print(f"  Service  : {args.service}")
    print(f"  Wallet   : {wallet_path}")
    print(f"  RPC      : {args.rpc or '(default gnosis)'}")
    print(f"  Anvil    : {'yes (TEST MODE)' if is_anvil else 'NO -- MAINNET'}")
    print(f"  Dry run  : {args.dry_run}")
    print(f"{'=' * 60}")

    if not is_anvil and not args.dry_run:
        print("\n!!! This will send REAL transactions on mainnet.")
        _confirm("Are you sure you want to continue on MAINNET?")

    tmpdir = tempfile.mkdtemp(prefix="recover_service_")
    try:
        _run(
            tmpdir=tmpdir,
            wallet_path=wallet_path,
            password=password,
            service_key=args.service,
            chain_name=chain_name,
            service_id=service_id,
            rpc_url=args.rpc,
            is_anvil=is_anvil,
            dry_run=args.dry_run,
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _run(
    *,
    tmpdir: str,
    wallet_path: Path,
    password: str,
    service_key: str,
    chain_name: str,
    service_id: int,
    rpc_url: str | None,
    is_anvil: bool,
    dry_run: bool,
) -> None:
    data_dir = Path(tmpdir) / "data"
    data_dir.mkdir()
    shutil.copy(wallet_path, data_dir / "wallet.json")

    # Set env vars BEFORE importing iwa (secrets are read at import time).
    os.environ["wallet_password"] = password
    if rpc_url:
        os.environ[f"{chain_name}_rpc"] = rpc_url

    # chdir so iwa relative paths (data/wallet.json, data/config.yaml,
    # data/activity.db) all land inside the isolated tmpdir.
    original_cwd = os.getcwd()
    os.chdir(tmpdir)
    try:
        _recover(
            service_key=service_key,
            chain_name=chain_name,
            service_id=service_id,
            rpc_url=rpc_url,
            is_anvil=is_anvil,
            dry_run=dry_run,
        )
    finally:
        os.chdir(original_cwd)


def _recover(
    *,
    service_key: str,
    chain_name: str,
    service_id: int,
    rpc_url: str | None,
    is_anvil: bool,
    dry_run: bool,
) -> None:
    # Late imports: iwa reads config/secrets at module load time.
    # Must happen after os.chdir() and env vars are set.
    from iwa.core.models import Config
    from iwa.core.wallet import Wallet
    from iwa.plugins.olas.contracts.service import ServiceState
    from iwa.plugins.olas.models import OlasConfig, Service
    from iwa.plugins.olas.service_manager import ServiceManager

    # [1] Load wallet
    print("\n[1/4] Loading wallet...")
    wallet = Wallet()
    master = wallet.master_account.address
    print(f"      Master address: {master}")

    # [2] Inject service into Config singleton (no config.yaml needed)
    print(f"\n[2/4] Configuring service {service_key}...")
    config = Config()
    olas_config = OlasConfig()
    # Find agent address: the first non-master account in the wallet.
    agent_address = None
    for addr in wallet.key_storage.accounts:
        if str(addr).lower() != str(master).lower():
            agent_address = str(addr)
            print(f"      Agent  address: {agent_address}")
            break

    service = Service(
        service_name="recovery",
        chain_name=chain_name,
        service_id=service_id,
        service_owner_eoa_address=master,
        agent_address=agent_address,
    )
    olas_config.add_service(service)
    config.plugins["olas"] = olas_config
    mgr = ServiceManager(wallet, service_key=service_key)

    # [3] Read on-chain state
    print(f"\n[3/4] Reading on-chain state (service {service_id})...")
    state_data = mgr.registry.get_service(service_id)
    state: ServiceState = state_data["state"]
    owner_on_chain = state_data.get("owner", "?")
    print(f"      State           : {state.name}")
    print(f"      Owner (on-chain): {owner_on_chain}")

    if str(owner_on_chain).lower() != str(master).lower():
        print(
            f"\n  WARNING: on-chain owner ({owner_on_chain}) "
            f"!= master ({master})."
        )
        print(
            "  Teardown will fail unless the wallet controls "
            "the owner address."
        )

    if dry_run:
        print("\n[DRY RUN] Not sending any transactions.")
        return

    # Fund master on Anvil for gas
    if is_anvil and rpc_url:
        print("\n  Funding master on Anvil for gas...")
        _fund_anvil_account(rpc_url, master, amount_eth=1.0)

    # [4] Terminate → unbond (if needed) → drain
    terminatable = {
        ServiceState.ACTIVE_REGISTRATION,
        ServiceState.FINISHED_REGISTRATION,
        ServiceState.DEPLOYED,
    }
    if state in terminatable:
        _confirm(f"[TERMINATE] service {service_id} on {chain_name}?")
        print("  Sending terminate TX...")
        if not mgr.terminate():
            print("ERROR: terminate() returned False. Check logs above.")
            sys.exit(1)
        print("  OK: service terminated.")
        # Re-read state: if no operators registered, the service goes to
        # PRE_REGISTRATION and the activation bond is returned inside the
        # terminate TX itself — no separate unbond is needed.
        state = mgr.registry.get_service(service_id)["state"]
        print(f"  State after terminate: {state.name}")
    elif state == ServiceState.TERMINATED_BONDED:
        print("\n  Already TERMINATED_BONDED — skipping terminate.")
    elif state == ServiceState.PRE_REGISTRATION:
        # Service was already terminated (bond returned in terminate TX).
        print("\n  Service is PRE_REGISTRATION — already terminated, bond returned.")
    else:
        print(f"\nERROR: Cannot terminate from state {state.name}.")
        sys.exit(1)

    if state == ServiceState.TERMINATED_BONDED:
        _confirm(
            f"[UNBOND] service {service_id} "
            "(returns OLAS bond to master)?"
        )
        print("  Sending unbond TX...")
        if not mgr.unbond():
            print("ERROR: unbond() returned False. Check logs above.")
            sys.exit(1)
        print("  OK: unbonded. OLAS bond released to master.")
    else:
        print(
            "  Bond already returned during terminate "
            f"(state: {state.name}) — skipping unbond."
        )

    _confirm(f"[DRAIN] all service accounts to {master}?")
    print("  Draining service accounts...")
    drained = mgr.drain_service(
        target_address=master, claim_rewards=False
    )
    if drained:
        print(f"  OK: drained {list(drained.keys())}.")
        for acct, amounts in drained.items():
            if isinstance(amounts, dict):
                for token, amount in amounts.items():
                    print(f"      {acct}: {amount:.6f} {token}")
            else:
                print(f"      {acct}: {amounts}")
    else:
        print("  Nothing to drain (accounts were empty).")

    print(f"\nDone. Master: {master}")
    print("  https://gnosisscan.io/address/" + master)


if __name__ == "__main__":
    main()
