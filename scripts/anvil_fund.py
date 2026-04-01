"""Fund a wallet on an Anvil fork with native token + OLAS."""

import sys

import requests
from web3 import Web3

ANVIL = "http://localhost:18545"
OLAS = "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f"
OLAS_SLOT = 3  # balanceOf storage slot on Gnosis


def fund(address: str) -> None:
    addr = address.lower()
    addr_clean = addr[2:]

    # 100 xDAI
    requests.post(ANVIL, json={
        "jsonrpc": "2.0", "method": "anvil_setBalance",
        "params": [addr, "0x56BC75E2D63100000"], "id": 1,
    })
    print("  xDAI: 100")

    # 20000 OLAS via storage slot
    slot_input = bytes.fromhex(addr_clean.rjust(64, "0") + "0" * 63 + "3")
    slot = "0x" + Web3.keccak(slot_input).hex()
    value = "0x" + hex(20000 * 10**18)[2:].rjust(64, "0")
    requests.post(ANVIL, json={
        "jsonrpc": "2.0", "method": "anvil_setStorageAt",
        "params": [OLAS, slot, value], "id": 2,
    })

    # Verify
    data = "0x70a08231" + addr_clean.rjust(64, "0")
    r = requests.post(ANVIL, json={
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": OLAS, "data": data}, "latest"], "id": 3,
    })
    bal = int(r.json()["result"], 16) / 1e18
    print(f"  OLAS: {bal:,.0f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/anvil_fund.py 0xADDRESS")
        sys.exit(1)
    addr = sys.argv[1]
    print(f"Funding {addr} on Anvil fork...")
    fund(addr)
    print("Done!")
