"""Fund a wallet on Anvil forks with native token + OLAS.

Supports all 7 chains. Each chain has a different OLAS token address
and balanceOf storage slot.

Usage:
    python scripts/anvil_fund.py 0xADDRESS                  # fund on all running forks
    python scripts/anvil_fund.py 0xADDRESS gnosis            # fund on gnosis only
    python scripts/anvil_fund.py 0xADDRESS gnosis,base       # fund on gnosis + base
"""

import sys

import requests
from web3 import Web3

# Per-chain config: anvil port, OLAS token address, balanceOf storage slot, native symbol
CHAINS = {
    "gnosis": {
        "port": 18545,
        "olas_token": "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f",
        "olas_slot": 3,
        "native_symbol": "xDAI",
        "native_amount_hex": "0x152D02C7E14AF6800000",  # 100,000
    },
    "base": {
        "port": 18546,
        "olas_token": "0x54330d28ca3357F294334BDC454a032e7f353416",
        "olas_slot": 0,
        "native_symbol": "ETH",
        "native_amount_hex": "0x56BC75E2D63100000",  # 100 ETH
    },
    "ethereum": {
        "port": 18547,
        "olas_token": "0x0001A500A6B18995B03f44bb040A5fFc28E45CB0",
        "olas_slot": 3,
        "native_symbol": "ETH",
        "native_amount_hex": "0x56BC75E2D63100000",  # 100 ETH
    },
    "polygon": {
        "port": 18548,
        "olas_token": "0xFEF5d947472e72Efbb2E388c730B7428406F2F95",
        "olas_slot": 0,
        "native_symbol": "POL",
        "native_amount_hex": "0x152D02C7E14AF6800000",  # 100,000
    },
    "optimism": {
        "port": 18549,
        "olas_token": "0xFC2E6e6BCbd49ccf3A5f029c79984372DcBFE527",
        "olas_slot": 51,
        "native_symbol": "ETH",
        "native_amount_hex": "0x56BC75E2D63100000",  # 100 ETH
    },
    "arbitrum": {
        "port": 18550,
        "olas_token": "0x064F8B858C2A603e1b106a2039f5446D32dc81C1",
        "olas_slot": 51,
        "native_symbol": "ETH",
        "native_amount_hex": "0x56BC75E2D63100000",  # 100 ETH
    },
    "celo": {
        "port": 18551,
        "olas_token": "0xaCFfAe8e57Ec6E394Eb1b41939A8CF7892DbDc51",
        "olas_slot": 0,
        "native_symbol": "CELO",
        "native_amount_hex": "0x152D02C7E14AF6800000",  # 100,000
    },
}

OLAS_AMOUNT = 20_000


def _is_anvil_running(url: str) -> bool:
    """Check if Anvil is running on the given URL."""
    try:
        r = requests.post(url, json={
            "jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1,
        }, timeout=2)
        return "result" in r.json()
    except Exception:
        return False


def fund_chain(address: str, chain_name: str, chain_cfg: dict) -> bool:
    """Fund an address on a single Anvil fork. Returns True if successful."""
    url = f"http://localhost:{chain_cfg['port']}"

    if not _is_anvil_running(url):
        return False

    addr = address.lower()
    addr_clean = addr[2:]
    sym = chain_cfg["native_symbol"]

    # Set native balance
    requests.post(url, json={
        "jsonrpc": "2.0", "method": "anvil_setBalance",
        "params": [addr, chain_cfg["native_amount_hex"]], "id": 1,
    })

    # Set OLAS balance via storage slot manipulation
    olas = chain_cfg["olas_token"]
    slot_num = chain_cfg["olas_slot"]
    slot_input = bytes.fromhex(
        addr_clean.rjust(64, "0") + hex(slot_num)[2:].rjust(64, "0")
    )
    slot = "0x" + Web3.keccak(slot_input).hex()
    value = "0x" + hex(OLAS_AMOUNT * 10**18)[2:].rjust(64, "0")
    requests.post(url, json={
        "jsonrpc": "2.0", "method": "anvil_setStorageAt",
        "params": [olas, slot, value], "id": 2,
    })

    # Verify OLAS balance
    data = "0x70a08231" + addr_clean.rjust(64, "0")
    r = requests.post(url, json={
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": olas, "data": data}, "latest"], "id": 3,
    })
    resp = r.json()
    if "result" in resp:
        bal = int(resp["result"], 16) / 1e18
        print(f"  {chain_name}: {sym} funded + {bal:,.0f} OLAS")
    else:
        print(f"  {chain_name}: {sym} funded + OLAS verify failed (wrong slot?)")

    return True


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/anvil_fund.py 0xADDRESS [chain1,chain2,...]")
        sys.exit(1)

    address = sys.argv[1]
    chain_filter = sys.argv[2].split(",") if len(sys.argv) > 2 else None

    chains_to_fund = (
        {k: v for k, v in CHAINS.items() if k in chain_filter}
        if chain_filter
        else CHAINS
    )

    print(f"Funding {address}...")
    funded = 0
    for name, cfg in chains_to_fund.items():
        if fund_chain(address, name, cfg):
            funded += 1

    if funded == 0:
        print("No Anvil forks running. Start with: just anvil-fork")
        sys.exit(1)
    print(f"Done! Funded on {funded} chain(s).")


if __name__ == "__main__":
    main()
