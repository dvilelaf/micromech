"""Demo script: sends random on-chain requests to micromech via MechMarketplace.

Usage:
    python scripts/demo_requests.py [--chain CHAIN] [--interval SECS]

Auto-discovers mech address, marketplace, and delivery rate from config.
Sends marketplace.request() transactions on-chain every N seconds.
The mech detects, executes, and delivers — visible in the dashboard.

Requires:
- iwa installed (for wallet + chain access)
- Wallet unlocked (wallet_password in env or via web wizard)
- Wallet funded with native token (for gas + delivery fees)
"""

import argparse
import json
import os
import random
import sys
import time

# ── Prompt presets per tool ──────────────────────────────────────────

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

# ── ANSI colors ──────────────────────────────────────────────────────

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"

TOOL_COLORS = {
    "echo": BLUE,
    "llm": CYAN,
    "prediction-offline": MAGENTA,
    "gemma4-api": GREEN,
}

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)


def colored_tool(tool: str) -> str:
    color = TOOL_COLORS.get(tool, CYAN)
    return f"{color}{BOLD}{tool}{RESET}"


def separator() -> str:
    return f"{DIM}{'─' * 70}{RESET}"


# ── Config discovery ─────────────────────────────────────────────────


def load_chain_config(chain_name: str) -> dict:
    """Load mech config for a chain from ~/.micromech/config.yaml.

    Returns dict with: mech_address, marketplace_address, delivery_rate, chain.
    """
    from micromech.core.config import MicromechConfig

    cfg = MicromechConfig.load()
    chain_cfg = cfg.chains.get(chain_name)
    if not chain_cfg:
        print(f"{RED}Chain '{chain_name}' not found in config.{RESET}")
        print(f"Available: {', '.join(cfg.chains.keys())}")
        sys.exit(1)

    if not chain_cfg.mech_address:
        print(f"{RED}No mech_address configured for '{chain_name}'.{RESET}")
        print("Run the setup wizard or deploy first.")
        sys.exit(1)

    return {
        "chain": chain_name,
        "mech_address": chain_cfg.mech_address,
        "marketplace_address": chain_cfg.marketplace_address,
        "delivery_rate": chain_cfg.delivery_rate,
    }


def get_available_tools(chain_cfg: dict) -> list[str]:
    """Return tools that have prompts defined."""
    return list(TOOL_PROMPTS.keys())


# ── On-chain request ─────────────────────────────────────────────────


def send_onchain_request(
    bridge, w3, marketplace_contract, mech_address: str,
    delivery_rate: int, tool: str, prompt: str,
) -> str | None:
    """Send a marketplace.request() transaction on-chain.

    Returns the tx hash hex string, or None on failure.
    """
    from micromech.core.bridge import get_wallet

    wallet = get_wallet()
    sender = wallet.master_account.address

    # Build request data (JSON-encoded prompt + tool)
    request_data = json.dumps({"prompt": prompt, "tool": tool}).encode()

    # Get marketplace fee
    fee = bridge.with_retry(
        lambda: marketplace_contract.functions.fee().call()
    )
    value = delivery_rate + fee

    # Build transaction
    tx_fn = marketplace_contract.functions.request(
        request_data,
        delivery_rate,
        PAYMENT_TYPE_NATIVE,
        w3.to_checksum_address(mech_address),
        300,  # responseTimeout (5 min)
        b"",  # paymentData
    )

    try:
        tx = bridge.with_retry(
            lambda: tx_fn.transact({
                "from": sender,
                "value": value,
                "gas": 500_000,
            })
        )
        receipt = bridge.with_retry(
            lambda: w3.eth.wait_for_transaction_receipt(tx, timeout=60)
        )
        if receipt["status"] != 1:
            print(f"  {RED}TX reverted!{RESET}")
            return None
        return tx.hex() if isinstance(tx, bytes) else str(tx)
    except Exception as e:
        print(f"  {RED}TX failed: {e}{RESET}")
        return None


# ── Display ──────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="micromech demo — send random on-chain requests"
    )
    parser.add_argument(
        "--chain", default="gnosis",
        help="Chain to send requests on (default: gnosis)",
    )
    parser.add_argument(
        "--interval", type=int, default=10,
        help="Seconds between requests (default: 10)",
    )
    parser.add_argument(
        "--tools", default=None,
        help="Comma-separated list of tools to use (default: all)",
    )
    args = parser.parse_args()

    # Load config
    chain_info = load_chain_config(args.chain)

    # Initialize bridge
    from micromech.core.bridge import IwaBridge
    from micromech.runtime.contracts import load_marketplace_abi

    bridge = IwaBridge(chain_name=args.chain)
    w3 = bridge.web3

    # Get sender address and balance
    from micromech.core.bridge import get_wallet
    wallet = get_wallet()
    sender = wallet.master_account.address
    balance_wei = bridge.with_retry(lambda: w3.eth.get_balance(sender))
    balance = float(w3.from_wei(balance_wei, "ether"))

    # Load marketplace contract
    marketplace_abi = load_marketplace_abi()
    marketplace = w3.eth.contract(
        address=w3.to_checksum_address(chain_info["marketplace_address"]),
        abi=marketplace_abi,
    )

    # Select tools
    if args.tools:
        available = [t.strip() for t in args.tools.split(",") if t.strip() in TOOL_PROMPTS]
    else:
        available = list(TOOL_PROMPTS.keys())

    if not available:
        print(f"{RED}No valid tools selected.{RESET}")
        sys.exit(1)

    # Display config
    delivery_cost = chain_info["delivery_rate"] / 1e18

    print(f"\n{BOLD}micromech on-chain demo{RESET}")
    print(separator())
    print(f"  Chain:       {CYAN}{args.chain}{RESET}")
    print(f"  Mech:        {DIM}{chain_info['mech_address']}{RESET}")
    print(f"  Marketplace: {DIM}{chain_info['marketplace_address']}{RESET}")
    print(f"  Sender:      {DIM}{sender}{RESET}")
    print(f"  Balance:     {GREEN}{balance:.4f}{RESET} native")
    print(f"  Cost/req:    ~{delivery_cost:.4f} + gas")
    print(f"  Interval:    {args.interval}s")
    print(f"  Tools:       {', '.join(colored_tool(t) for t in available)}")
    print(separator())

    if balance < delivery_cost * 3:
        print(f"\n{RED}Low balance! Need at least {delivery_cost * 3:.4f} native.{RESET}")
        sys.exit(1)

    print(f"\n{DIM}Sending on-chain requests. Ctrl+C to stop.{RESET}\n")

    # Main loop
    count = 0
    try:
        while True:
            count += 1
            tool = random.choice(available)
            prompt = random.choice(TOOL_PROMPTS[tool])

            ts = time.strftime("%H:%M:%S")
            print(f"{DIM}[{ts}]{RESET}  #{count}  {colored_tool(tool)}")
            print(f"  {YELLOW}> {prompt}{RESET}")

            t0 = time.time()
            tx_hash = send_onchain_request(
                bridge, w3, marketplace, chain_info["mech_address"],
                chain_info["delivery_rate"], tool, prompt,
            )
            elapsed = time.time() - t0

            if tx_hash:
                short_hash = tx_hash[:18] + "..."
                print(f"  {GREEN}TX sent{RESET}  {DIM}{short_hash}  ({elapsed:.1f}s){RESET}")
            else:
                print(f"  {RED}Failed to send{RESET}")

            print()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped after {count} request(s).{RESET}")


if __name__ == "__main__":
    main()
