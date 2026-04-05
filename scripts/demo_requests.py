"""Demo: send random on-chain requests to micromech and display results.

Auto-discovers deployed chains and mech addresses from config.
Creates a temporary wallet, funds it via Anvil, sends marketplace
requests every 5 seconds, and polls for results — all in a live table.

Usage:
    uv run python scripts/demo_requests.py
"""

import json
import random
import sys
import threading
import time
import warnings
from typing import Any

import requests as http_requests
from eth_account import Account
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text
from web3 import Web3

# Suppress web3 ABI mismatch warnings (marketplace ABI is minimal)
warnings.filterwarnings("ignore", message=".*MismatchedABI.*")

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

TOOL_STYLES = {
    "echo": "blue",
    "llm": "cyan",
    "prediction-offline": "magenta",
    "gemma4-api": "green",
}

PAYMENT_TYPE_NATIVE = bytes.fromhex(
    "ba699a34be8fe0e7725e93dcbce1701b0211a8ca61330aaeb8a05bf2ec7abed1"
)

ANVIL_PORTS: dict[str, int] = {
    "gnosis": 18545, "base": 18546, "ethereum": 18547,
    "polygon": 18548, "optimism": 18549, "arbitrum": 18550, "celo": 18551,
}


# ── Row model ────────────────────────────────────────────────────────

class RequestRow:
    def __init__(self, idx: int, chain: str, tool: str, prompt: str):
        self.idx = idx
        self.chain = chain
        self.tool = tool
        self.prompt = prompt
        self.status = "sending"
        self.request_id: str | None = None
        self.response: str | None = None
        self.elapsed: float = 0
        self.t0 = time.time()


# ── Helpers ──────────────────────────────────────────────────────────

def get_anvil_w3(chain: str) -> Web3 | None:
    port = ANVIL_PORTS.get(chain)
    if not port:
        return None
    w3 = Web3(Web3.HTTPProvider(f"http://localhost:{port}"))
    try:
        w3.eth.chain_id
        return w3
    except Exception:
        return None


def fund_via_anvil(w3: Web3, address: str, amount_wei: int) -> bool:
    try:
        w3.provider.make_request("anvil_setBalance", [address, hex(amount_wei)])
        return True
    except Exception:
        return False


def extract_request_id(receipt: dict, marketplace_addr: str) -> str | None:
    """Extract the first requestId from MarketplaceRequest event logs."""
    mp_lower = marketplace_addr.lower()
    for log in receipt.get("logs", []):
        log_addr = (log.get("address") or "").lower()
        if log_addr != mp_lower:
            continue
        # requestIds is in the data (non-indexed), decode from ABI
        # Simpler: topic[0] is event sig, data contains requestIds array
        # The requestIds bytes32[] is the 4th field in data
        topics = log.get("topics", [])
        data = log.get("data", b"")
        if isinstance(data, bytes) and len(data) >= 128 and len(topics) >= 1:
            # Skip to requestIds: offset(0) + offset(32) + numRequests(64) + ...
            # ABI decode: first 32 bytes = numRequests offset, complex.
            # Easier: just grab from decoded event if available
            pass
    # Fallback: use web3 receipt processing
    return None


def format_response(result_data: dict | None) -> str:
    """Format a mech response for display."""
    if not result_data:
        return ""

    # Prediction tools
    if "p_yes" in result_data:
        p_yes = result_data["p_yes"]
        p_no = result_data["p_no"]
        conf = result_data.get("confidence", 0)
        yes_bar = "█" * round(p_yes * 20)
        no_bar = "█" * (20 - round(p_yes * 20))
        return f"[green]{yes_bar}[/green][red]{no_bar}[/red] YES {p_yes:.0%} / NO {p_no:.0%} (conf {conf:.0%})"

    # LLM tools
    if "result" in result_data:
        text = result_data["result"]
        if len(text) > 120:
            text = text[:120] + "..."
        model = result_data.get("model", "")
        suffix = f" [dim]\\[{model.split('/')[-1]}][/dim]" if model else ""
        return text + suffix

    return json.dumps(result_data)[:120]


def build_table(rows: list[RequestRow], title: str) -> Table:
    """Build a rich Table from current rows."""
    table = Table(title=title, expand=True, border_style="dim", title_style="bold", show_lines=True)
    table.add_column("#", style="dim", width=4)
    table.add_column("Chain", width=8)
    table.add_column("Tool", width=20)
    table.add_column("Prompt", width=40, no_wrap=True)
    table.add_column("Status", width=12)
    table.add_column("Response", ratio=1)
    table.add_column("Time", width=6, justify="right")

    # Show last 15 rows
    for row in rows[-15:]:
        style = TOOL_STYLES.get(row.tool, "white")

        if row.status == "sending":
            status = Text("sending", style="yellow")
        elif row.status == "pending":
            status = Text("pending", style="yellow")
        elif row.status == "done":
            status = Text("done", style="green")
        elif row.status == "failed":
            status = Text("failed", style="red")
        else:
            status = Text(row.status, style="dim")

        prompt_text = row.prompt
        if len(prompt_text) > 38:
            prompt_text = prompt_text[:38] + "..."

        response_text = Text.from_markup(row.response or "") if row.response else Text("")

        elapsed = f"{row.elapsed:.1f}s" if row.elapsed > 0 else ""

        table.add_row(
            str(row.idx),
            row.chain,
            Text(row.tool, style=style),
            prompt_text,
            status,
            response_text,
            elapsed,
        )

    return table


# ── Result poller (background) ───────────────────────────────────────

def poll_results(
    rows: list[RequestRow],
    marketplaces: dict,
    anvil_w3s: dict,
    cfg: Any,
    stop_event: threading.Event,
):
    """Background thread: poll on-chain Deliver events for pending requests."""
    # Track last scanned block per chain
    last_block: dict[str, int] = {}

    while not stop_event.is_set():
        pending = {r.request_id: r for r in rows if r.status == "pending" and r.request_id}
        if not pending:
            stop_event.wait(2)
            continue

        for chain_name, w3 in anvil_w3s.items():
            mp = marketplaces.get(chain_name)
            if not mp:
                continue
            try:
                current = w3.eth.block_number
                from_blk = last_block.get(chain_name, current - 50)

                # Scan MarketplaceDelivery to detect delivered requests,
                # then fetch full response from mech's /result endpoint.
                logs = mp.events.MarketplaceDelivery.get_logs(
                    from_block=from_blk, to_block=current,
                )
                for log in logs:
                    rids = log.args.get("requestIds", [])
                    for rid in rids:
                        rid_hex = rid.hex() if isinstance(rid, bytes) else str(rid)
                        if rid_hex not in pending:
                            continue
                        row = pending[rid_hex]
                        # Fetch response from mech HTTP API
                        try:
                            cc = cfg.chains.get(chain_name)
                            host = cfg.runtime.host
                            if host in ("0.0.0.0", "::"):
                                host = "127.0.0.1"
                            url = f"http://{host}:{cfg.runtime.port}"
                            resp = http_requests.get(
                                f"{url}/result/{rid_hex}", timeout=3,
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                row.response = format_response(
                                    data.get("result"),
                                )
                        except Exception:
                            row.response = "delivered"
                        row.status = "done"
                        row.elapsed = time.time() - row.t0

                last_block[chain_name] = current
            except Exception:
                pass

        stop_event.wait(3)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    from micromech.core.config import MicromechConfig
    from micromech.runtime.contracts import load_marketplace_abi

    console = Console(force_terminal=True)
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
        console.print("[red]No deployed chains with running Anvil forks found.[/red]")
        console.print("[dim]Start Anvil with: just anvil-fork[/dim]")
        sys.exit(1)

    # Create temporary wallet + fund
    acct = Account.create()
    sender = acct.address
    private_key = acct.key

    abi = load_marketplace_abi()
    marketplaces: dict[str, any] = {}
    fund_amount = Web3.to_wei(1, "ether")

    console.print()
    console.print("[bold]micromech on-chain demo[/bold]")
    console.print(f"[dim]Sender: {sender} (temporary)[/dim]")

    for ch in chains:
        w3 = anvil_w3s[ch]
        cc = cfg.chains[ch]
        ok = fund_via_anvil(w3, sender, fund_amount)
        bal = w3.from_wei(w3.eth.get_balance(sender), "ether")
        status = f"[green]funded {bal:.2f}[/green]" if ok else "[red]failed[/red]"
        console.print(f"  [cyan]{ch}[/cyan]: {status}  mech [dim]{cc.mech_address}[/dim]")
        marketplaces[ch] = w3.eth.contract(
            address=w3.to_checksum_address(cc.marketplace_address), abi=abi,
        )

    # Exclude tools that require missing API keys
    import os
    tools = [t for t in TOOL_PROMPTS if t != "gemma4-api" or os.environ.get("GOOGLE_API_KEY")]

    # Detect mech HTTP API URL from config
    host = cfg.runtime.host
    port = cfg.runtime.port
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    base_url = f"http://{host}:{port}"

    console.print(f"  API: [dim]{base_url}[/dim]")
    console.print()

    # State
    rows: list[RequestRow] = []
    stop_event = threading.Event()

    # Start background poller
    poller = threading.Thread(
        target=poll_results,
        args=(rows, marketplaces, anvil_w3s, cfg, stop_event),
        daemon=True,
    )
    poller.start()

    count = 0
    try:
        with Live(
            build_table(rows, "micromech demo"),
            console=console,
            refresh_per_second=2,
            get_renderable=lambda: build_table(rows, "micromech demo"),
        ) as live:
            while True:
                count += 1
                chain = random.choice(chains)
                tool = random.choice(tools)
                prompt = random.choice(TOOL_PROMPTS[tool])
                cc = cfg.chains[chain]
                w3 = anvil_w3s[chain]
                mp = marketplaces[chain]

                row = RequestRow(count, chain, tool, prompt)
                rows.append(row)

                # Send on-chain request
                try:
                    request_data = json.dumps({"prompt": prompt, "tool": tool}).encode()
                    fee = mp.functions.fee().call()
                    value = cc.delivery_rate + fee

                    fn_call = mp.functions.request(
                        request_data, cc.delivery_rate, PAYMENT_TYPE_NATIVE,
                        w3.to_checksum_address(cc.mech_address), 300, b"",
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

                    if receipt["status"] == 1:
                        # Extract request_id from MarketplaceRequest event
                        req_id = None
                        try:
                            parsed = mp.events.MarketplaceRequest().process_receipt(
                                receipt, errors=mp.events.MarketplaceRequest.EventLogErrorFlags.Discard,
                            )
                            if parsed:
                                rid = parsed[0]["args"]["requestIds"][0]
                                req_id = rid.hex() if isinstance(rid, bytes) else str(rid)
                        except Exception:
                            pass

                        row.request_id = req_id
                        row.status = "pending"
                        row.elapsed = time.time() - row.t0
                    else:
                        row.status = "failed"
                        row.response = "TX reverted"
                        row.elapsed = time.time() - row.t0
                except Exception as e:
                    row.status = "failed"
                    row.response = str(e)[:80]
                    row.elapsed = time.time() - row.t0

                time.sleep(5)

    except KeyboardInterrupt:
        stop_event.set()
        console.print(f"\n[dim]Stopped after {count} request(s).[/dim]")


if __name__ == "__main__":
    main()
