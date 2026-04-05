"""Demo: send random on-chain requests to micromech.

Auto-discovers all deployed chains and mech addresses from config.
Creates a temporary wallet, funds it via Anvil impersonation,
and sends marketplace.request() every 5 seconds.

Usage:
    uv run python scripts/demo_requests.py
"""

import json
import random
import sys
import time

from eth_account import Account
from web3 import Web3

TOOL_PROMPTS: dict[str, list[str]] = {
    "echo": [
        "Will BTC reach 100k by December 2025?",
        "Will Ethereum merge to proof-of-stake successfully?",
        "Will it rain in London tomorrow?",
    ],
    "llm": [
        "Explain quantum computing in one sentence.",
        "What is the capital of Japan?",
        "Summarize the plot of Romeo and Juliet in 20 words.",
        "Name three renewable energy sources.",
        "What is 42 * 17?",
    ],
    "prediction-offline": [
        "Will ETH hit 10k by 2027?",
        "Will the next US president be a Democrat?",
        "Will SpaceX land humans on Mars before 2030?",
        "Will Bitcoin dominance drop below 40% this year?",
        "Will the Fed cut interest rates in the next meeting?",
    ],
    "gemma4-api": [
        "What are the three laws of thermodynamics?",
        "Translate 'hello world' to French, German, and Japanese.",
        "What causes a rainbow?",
        "List 5 programming languages created after 2010.",
        "Explain blockchain in one paragraph.",
    ],
}

# ANSI
RST = "\033[0m"
B = "\033[1m"
DIM = "\033[2m"
CYN = "\033[36m"
GRN = "\033[32m"
YEL = "\033[33m"
RED = "\033[31m"
MAG = "\033[35m"
BLU = "\033[34m"

TOOL_CLR = {"echo": BLU, "llm": CYN, "prediction-offline": MAG, "gemma4-api": GRN}

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

# Anvil ports per chain
ANVIL_PORTS: dict[str, int] = {
    "gnosis": 18545, "base": 18546, "ethereum": 18547,
    "polygon": 18548, "optimism": 18549, "arbitrum": 18550, "celo": 18551,
}


def get_anvil_w3(chain: str) -> Web3 | None:
    """Connect to a running Anvil fork for this chain."""
    port = ANVIL_PORTS.get(chain)
    if not port:
        return None
    url = f"http://localhost:{port}"
    w3 = Web3(Web3.HTTPProvider(url))
    try:
        w3.eth.chain_id  # test connection
        return w3
    except Exception:
        return None


def fund_via_anvil(w3: Web3, address: str, amount_wei: int) -> bool:
    """Fund an address on Anvil via anvil_setBalance."""
    try:
        w3.provider.make_request("anvil_setBalance", [address, hex(amount_wei)])
        return True
    except Exception:
        return False


def main():
    from micromech.core.config import MicromechConfig
    from micromech.runtime.contracts import load_marketplace_abi

    cfg = MicromechConfig.load()

    # Discover deployed chains with running Anvil forks
    chains = []
    anvil_w3s: dict[str, Web3] = {}
    for name, cc in cfg.enabled_chains.items():
        if not cc.mech_address or not cc.marketplace_address:
            continue
        w3 = get_anvil_w3(name)
        if w3:
            chains.append(name)
            anvil_w3s[name] = w3

    if not chains:
        print(f"{RED}No deployed chains with running Anvil forks found.{RST}")
        print(f"{DIM}Start Anvil with: just anvil-fork{RST}")
        sys.exit(1)

    # Create temporary wallet
    acct = Account.create()
    sender = acct.address
    private_key = acct.key

    # Fund on all chains
    abi = load_marketplace_abi()
    marketplaces: dict[str, any] = {}
    fund_amount = Web3.to_wei(1, "ether")

    print(f"\n{B}micromech on-chain demo{RST}")
    print(f"{DIM}{'─' * 60}{RST}")
    print(f"  Sender: {DIM}{sender} (temporary){RST}")

    for ch in chains:
        w3 = anvil_w3s[ch]
        cc = cfg.chains[ch]
        ok = fund_via_anvil(w3, sender, fund_amount)
        bal = w3.from_wei(w3.eth.get_balance(sender), "ether")
        status = f"{GRN}funded {bal:.2f}{RST}" if ok else f"{RED}fund failed{RST}"
        print(f"  {CYN}{ch}{RST}: mech {DIM}{cc.mech_address}{RST}  [{status}]")
        marketplaces[ch] = w3.eth.contract(
            address=w3.to_checksum_address(cc.marketplace_address), abi=abi,
        )

    tools = list(TOOL_PROMPTS.keys())
    print(f"  Tools:  {', '.join(f'{TOOL_CLR.get(t, CYN)}{B}{t}{RST}' for t in tools)}")
    print(f"{DIM}{'─' * 60}{RST}")
    print(f"{DIM}Sending requests every 5s. Ctrl+C to stop.{RST}\n")

    count = 0
    try:
        while True:
            count += 1
            chain = random.choice(chains)
            tool = random.choice(tools)
            prompt = random.choice(TOOL_PROMPTS[tool])
            cc = cfg.chains[chain]
            w3 = anvil_w3s[chain]
            mp = marketplaces[chain]

            ts = time.strftime("%H:%M:%S")
            tc = TOOL_CLR.get(tool, CYN)
            print(f"{DIM}[{ts}]{RST}  #{count}  {CYN}{chain}{RST}  {tc}{B}{tool}{RST}")
            print(f"  {YEL}> {prompt}{RST}")

            t0 = time.time()
            try:
                request_data = json.dumps({"prompt": prompt, "tool": tool}).encode()
                fee = mp.functions.fee().call()
                value = cc.delivery_rate + fee

                fn_call = mp.functions.request(
                    request_data,
                    cc.delivery_rate,
                    PAYMENT_TYPE_NATIVE,
                    w3.to_checksum_address(cc.mech_address),
                    300,
                    b"",
                )

                tx = fn_call.build_transaction({
                    "from": sender,
                    "value": value,
                    "gas": 500_000,
                    "gasPrice": w3.eth.gas_price,
                    "nonce": w3.eth.get_transaction_count(sender),
                    "chainId": w3.eth.chain_id,
                })

                signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

                elapsed = time.time() - t0
                if receipt["status"] == 1:
                    h = tx_hash.hex()
                    print(f"  {GRN}OK{RST}  {DIM}{h[:20]}...  ({elapsed:.1f}s){RST}")
                else:
                    print(f"  {RED}TX reverted{RST}  {DIM}({elapsed:.1f}s){RST}")
            except Exception as e:
                elapsed = time.time() - t0
                print(f"  {RED}Error: {e}{RST}  {DIM}({elapsed:.1f}s){RST}")

            print()
            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped after {count} request(s).{RST}")


if __name__ == "__main__":
    main()
