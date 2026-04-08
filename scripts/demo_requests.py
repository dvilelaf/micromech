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
import uuid
import warnings
from typing import Any

from micromech.core.constants import IPFS_API_URL

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
    "local-llm": [
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
    "local-llm": "cyan",
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

def _parse_delivery_data(delivery_data: bytes) -> str:
    """Parse on-chain delivery data: IPFS multihash or raw JSON."""
    from micromech.ipfs.client import is_ipfs_multihash, multihash_to_cid

    if is_ipfs_multihash(delivery_data):
        cid = multihash_to_cid(delivery_data)
        # Try to fetch the actual result from IPFS gateway
        try:
            import requests as req_lib

            from micromech.core.constants import IPFS_GATEWAY
            gw = IPFS_GATEWAY
            resp = req_lib.get(f"{gw}{cid}", timeout=10)
            resp.raise_for_status()
            result_data = resp.json()
            # The response wraps tool output as a JSON string in "result"
            if isinstance(result_data.get("result"), str):
                try:
                    result_data = json.loads(result_data["result"])
                except (json.JSONDecodeError, TypeError):
                    pass
            formatted = format_response(result_data)
            if formatted:
                return formatted
        except Exception:
            pass
        return f"[dim]IPFS: {cid[:20]}...[/dim]"

    # Fallback: try raw JSON (delivery wrapper or direct result)
    try:
        result_data = json.loads(delivery_data)
        # Unwrap delivery envelope: {"requestId":..., "result": "<json>", ...}
        if isinstance(result_data.get("result"), str):
            try:
                result_data = json.loads(result_data["result"])
            except (json.JSONDecodeError, TypeError):
                pass
        return format_response(result_data)
    except Exception:
        return f"Delivered ({len(delivery_data)} bytes)"


def _fetch_result_from_api(request_id: str) -> str:
    """Fetch the response for a request from the micromech HTTP API."""
    try:
        import requests as req_lib

        resp = req_lib.get(
            f"http://localhost:8090/result/{request_id}",
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            result_data = data.get("result")
            if isinstance(result_data, dict):
                formatted = format_response(result_data)
                if formatted:
                    return formatted
            elif isinstance(result_data, str):
                try:
                    parsed = json.loads(result_data)
                    formatted = format_response(parsed)
                    if formatted:
                        return formatted
                except (json.JSONDecodeError, TypeError):
                    pass
                return result_data[:120]
    except Exception:
        pass
    return "[dim]delivered[/dim]"


def poll_results(
    rows: list[RequestRow],
    anvil_w3s: dict[str, Web3],
    marketplaces: dict[str, Any],
    cfg: Any,
    stop_event: threading.Event,
):
    """Background thread: poll on-chain MarketplaceDelivery events for completed requests.

    NOTE: The marketplace contract emits MarketplaceDelivery (not Deliver)
    when deliverToMarketplace is called. The Deliver event in the ABI has a
    different signature and is never emitted by the marketplace address.
    """
    # Track last polled block per chain
    last_block: dict[str, int] = {}
    for chain_name, w3 in anvil_w3s.items():
        try:
            last_block[chain_name] = w3.eth.block_number
        except Exception:
            pass

    while not stop_event.is_set():
        try:
            pending = [r for r in rows if r.status == "pending" and r.request_id]
            if not pending:
                stop_event.wait(2)
                continue

            # Build lookup of pending request_ids
            pending_ids: dict[str, RequestRow] = {}
            for row in pending:
                hex_str = row.request_id[2:] if row.request_id.startswith("0x") else row.request_id
                pending_ids[hex_str.lower()] = row

            # Poll each chain for MarketplaceDelivery events
            for chain_name, mp in marketplaces.items():
                w3 = anvil_w3s.get(chain_name)
                if not w3:
                    continue
                try:
                    current = w3.eth.block_number
                    from_block = last_block.get(chain_name, current)
                    if current <= from_block:
                        continue

                    logs = mp.events.MarketplaceDelivery.get_logs(
                        from_block=from_block + 1,
                        to_block=current,
                    )
                    last_block[chain_name] = current

                    for log in logs:
                        # MarketplaceDelivery has requestIds (bytes32[])
                        request_ids = log["args"]["requestIds"]
                        tx_hash = log["transactionHash"]

                        # Decode Deliver events from the MECH contract (not marketplace).
                        # Marketplace emits MarketplaceDelivery (no delivery data).
                        # Mech emits Deliver with args: requestId, data (IPFS multihash).
                        delivery_map: dict[str, bytes] = {}
                        try:
                            from web3._utils.events import EventLogErrorFlags
                            from micromech.runtime.contracts import load_mech_abi

                            receipt = w3.eth.get_transaction_receipt(tx_hash)
                            mech_addr = cfg.chains[chain_name].mech_address
                            mech_c = w3.eth.contract(
                                address=w3.to_checksum_address(mech_addr),
                                abi=load_mech_abi(),
                            )
                            deliver_logs = mech_c.events.Deliver().process_receipt(
                                receipt, errors=EventLogErrorFlags.Discard,
                            )
                            for dl in deliver_logs:
                                rid_d = dl["args"]["requestId"]
                                rid_hex_d = rid_d.hex() if isinstance(rid_d, bytes) else str(rid_d)
                                delivery_map[rid_hex_d.lower()] = dl["args"]["data"]
                        except Exception as _e:
                            import sys
                            print(f"[poller] Deliver decode error: {_e}", file=sys.stderr)

                        for rid in request_ids:
                            rid_hex = rid.hex() if isinstance(rid, bytes) else str(rid)
                            row = pending_ids.get(rid_hex.lower())
                            if row:
                                # Try to parse actual response from deliveryData
                                dd = delivery_map.get(rid_hex.lower())
                                if dd:
                                    try:
                                        row.response = _parse_delivery_data(dd)
                                    except Exception:
                                        row.response = "[green]delivered[/green]"
                                else:
                                    row.response = "[green]delivered[/green]"
                                row.status = "done"
                                row.elapsed = time.time() - row.t0
                except Exception:
                    pass

            stop_event.wait(3)
        except Exception as e:
            import sys
            print(f"[poller] Error: {e}", file=sys.stderr)
            stop_event.wait(5)


# ── IPFS helpers ────────────────────────────────────────────────────

def _push_request_to_ipfs(prompt: str, tool: str, api_url: str) -> bytes:
    """Push mech request metadata to IPFS and return multihash bytes.

    Follows the same flow as iwa's push_metadata_to_ipfs:
    1. Build Valory v2 metadata dict with nonce
    2. Serialize as pretty-printed JSON (indent=4, ensure_ascii=False)
    3. Push to IPFS
    4. Return multihash bytes (34 bytes: 0x12 0x20 + sha256 digest)
    """
    from micromech.ipfs.client import cid_hex_to_multihash_bytes, compute_cid_hex

    metadata = {
        "prompt": prompt,
        "tool": tool,
        "nonce": str(uuid.uuid4()),
        "schema_version": "2.0",
    }

    # Match Valory agent serialization format exactly
    json_bytes = json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8")

    # Push to IPFS (best-effort; if it fails, compute CID locally)
    try:
        import requests as req_lib

        resp = req_lib.post(
            f"{api_url}/api/v0/add",
            files={"file": ("data", json_bytes, "application/octet-stream")},
            params={"pin": "true", "cid-version": "1"},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception:
        pass  # CID is content-addressed; local computation is sufficient

    cid_hex = compute_cid_hex(json_bytes)
    return cid_hex_to_multihash_bytes(cid_hex)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    from micromech.core.config import MicromechConfig, register_plugin
    from micromech.runtime.contracts import load_marketplace_abi

    register_plugin()
    console = Console(force_terminal=True)
    cfg = MicromechConfig.load()

    # Discover deployed chains with running Anvil forks
    chains = []
    anvil_w3s: dict[str, Web3] = {}
    for name, cc in cfg.enabled_chains.items():
        if not cc.mech_address or not cc.marketplace_address:
            continue
        # Skip placeholder addresses (e.g. 0x3333...3333 from test data)
        addr_body = cc.mech_address[2:]  # strip 0x
        if len(set(addr_body.lower())) <= 1:
            console.print(f"  [yellow]Skipping {name}: mech_address looks like a placeholder ({cc.mech_address[:10]}...)[/yellow]")
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
    marketplaces: dict[str, Any] = {}
    fund_amount = Web3.to_wei(100, "ether")

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

    console.print()

    # State
    rows: list[RequestRow] = []
    stop_event = threading.Event()

    # Start on-chain poller (watches Deliver events, no HTTP dependency)
    poller = threading.Thread(
        target=poll_results,
        args=(rows, anvil_w3s, marketplaces, cfg, stop_event),
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
                    request_data = _push_request_to_ipfs(prompt, tool, IPFS_API_URL)
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
                            from web3._utils.events import EventLogErrorFlags
                            parsed = mp.events.MarketplaceRequest().process_receipt(
                                receipt, errors=EventLogErrorFlags.Discard,
                            )
                            if parsed:
                                rid = parsed[0]["args"]["requestIds"][0]
                                req_id = rid.hex() if isinstance(rid, bytes) else str(rid)
                        except Exception as e:
                            console.print(f"[red]Event parse error: {e}[/red]")

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
