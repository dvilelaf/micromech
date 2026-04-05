"""Demo: send random on-chain requests to micromech.

Auto-discovers all deployed chains and mech addresses from config.
Every 5 seconds, picks a random chain and tool, sends a marketplace request.

Usage:
    uv run python scripts/demo_requests.py
"""

import json
import random
import sys
import time

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


def send_signed_request(bridge, w3, marketplace, mech_addr, delivery_rate,
                        sender, key_storage, tool, prompt):
    """Build, sign, and send a marketplace.request() transaction."""
    request_data = json.dumps({"prompt": prompt, "tool": tool}).encode()

    fee = bridge.with_retry(lambda: marketplace.functions.fee().call())
    value = delivery_rate + fee

    fn_call = marketplace.functions.request(
        request_data,
        delivery_rate,
        PAYMENT_TYPE_NATIVE,
        w3.to_checksum_address(mech_addr),
        300,  # responseTimeout
        b"",  # paymentData
    )

    nonce = bridge.with_retry(lambda: w3.eth.get_transaction_count(sender))
    gas_price = bridge.with_retry(lambda: w3.eth.gas_price)
    chain_id = bridge.with_retry(lambda: w3.eth.chain_id)

    tx = fn_call.build_transaction({
        "from": sender,
        "value": value,
        "gas": 500_000,
        "gasPrice": gas_price,
        "nonce": nonce,
        "chainId": chain_id,
    })

    signed = key_storage.sign_transaction(tx, "master")
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

    if receipt["status"] != 1:
        return None
    return tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)


def main():
    from micromech.core.bridge import IwaBridge, get_wallet
    from micromech.core.config import MicromechConfig
    from micromech.runtime.contracts import load_marketplace_abi

    cfg = MicromechConfig.load()

    # Discover deployed chains
    chains = [
        name for name, cc in cfg.enabled_chains.items()
        if cc.mech_address and cc.marketplace_address
    ]
    if not chains:
        print(f"{RED}No deployed chains found in config.{RST}")
        sys.exit(1)

    # Init bridges and marketplace contracts per chain
    abi = load_marketplace_abi()
    bridges: dict[str, tuple] = {}
    for ch in chains:
        br = IwaBridge(chain_name=ch)
        w3 = br.web3
        cc = cfg.chains[ch]
        mp = w3.eth.contract(
            address=w3.to_checksum_address(cc.marketplace_address), abi=abi,
        )
        bridges[ch] = (br, w3, mp)

    # Wallet and key_storage for signing
    wallet = get_wallet()
    sender = wallet.master_account.address
    key_storage = wallet.key_storage
    tools = list(TOOL_PROMPTS.keys())

    # Header
    print(f"\n{B}micromech on-chain demo{RST}")
    print(f"{DIM}{'─' * 60}{RST}")
    print(f"  Sender: {DIM}{sender}{RST}")
    for ch in chains:
        cc = cfg.chains[ch]
        print(f"  {CYN}{ch}{RST}: mech {DIM}{cc.mech_address}{RST}")
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
            br, w3, mp = bridges[chain]

            ts = time.strftime("%H:%M:%S")
            tc = TOOL_CLR.get(tool, CYN)
            print(f"{DIM}[{ts}]{RST}  #{count}  {CYN}{chain}{RST}  {tc}{B}{tool}{RST}")
            print(f"  {YEL}> {prompt}{RST}")

            t0 = time.time()
            try:
                tx_hash = send_signed_request(
                    br, w3, mp, cc.mech_address, cc.delivery_rate,
                    sender, key_storage, tool, prompt,
                )
                elapsed = time.time() - t0
                if tx_hash:
                    print(f"  {GRN}OK{RST}  {DIM}{tx_hash[:20]}...  ({elapsed:.1f}s){RST}")
                else:
                    print(f"  {RED}TX reverted{RST}")
            except Exception as e:
                elapsed = time.time() - t0
                print(f"  {RED}Error: {e}{RST}  {DIM}({elapsed:.1f}s){RST}")

            print()
            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped after {count} request(s).{RST}")


if __name__ == "__main__":
    main()
