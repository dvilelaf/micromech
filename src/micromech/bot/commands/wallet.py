"""Wallet command handler — addresses and balances (triton-style, MarkdownV2)."""

import asyncio
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import (
    bold_md,
    code_md,
    escape_md,
    explorer_link_md,
    format_token,
    split_md_blocks,
    user_error,
)
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.bridge import (
    check_address_balances,
    check_balances,
    get_wallet,
)
from micromech.core.config import MicromechConfig


def _short_addr(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}"


def _address_line(
    label: str,
    address: Optional[str],
    chain: str,
    native: Optional[float] = None,
    olas: Optional[float] = None,
    balance_known: bool = True,
) -> str:
    """Format one address row with optional balances.

    `balance_known=False` means the RPC fetch returned None (unknown) — we show
    "balance unknown" instead of silently rendering 0.0 (H4/B3).
    """
    if not address:
        return f"{escape_md(label)}: N/A"
    lnk = explorer_link_md(chain, address, _short_addr(address))
    line = f"{escape_md(label)}: {lnk}"
    if not balance_known:
        line += f"\n{code_md('balance unknown — RPC error')}"
        return line
    if native is not None:
        n_str = format_token(native, "xDAI")
        o_str = format_token(olas if olas is not None else 0.0, "OLAS")
        line += f"\n{code_md(n_str)} \\| {code_md(o_str)}"
    return line


async def _fetch_address_balance(
    chain_name: str, address: Optional[str]
) -> Optional[tuple[float, float]]:
    """Return (native, olas) for an address, None if unknown or missing."""
    if not address:
        return None
    return await asyncio.to_thread(check_address_balances, chain_name, address)


@authorized_only
@rate_limited
async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /wallet command."""
    if not update.message:
        return
    # Local import so tests can patch `micromech.core.bridge.get_service_info`.
    from micromech.core.bridge import get_service_info

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    status_msg = await update.message.reply_text("Fetching wallet info...")

    blocks = []

    # Master wallet block (shared across all chains)
    try:
        wallet = await asyncio.to_thread(get_wallet)
        master_addr = str(wallet.master_account.address)
        first_chain = next(iter(enabled))
        master_bal = await asyncio.to_thread(check_balances, first_chain)
        if master_bal is not None:
            mn, mo = master_bal
            master_line = _address_line("Master", master_addr, first_chain, mn, mo)
        else:
            master_line = _address_line("Master", master_addr, first_chain, balance_known=False)
        blocks.append(f"{bold_md('Wallet')}\n\n{master_line}")
    except Exception as e:
        blocks.append(f"{bold_md('Wallet')}\n\n{user_error('wallet', e)}")

    # Per-chain blocks — fetch all chains' balances in parallel (H3/B1).
    chain_infos = []
    for chain_name, chain_config in enabled.items():
        svc_info = get_service_info(chain_name)
        chain_infos.append((chain_name, chain_config, svc_info))

    # R2-L2: build a dict of tasks keyed by (chain, "safe"/"agent") so index
    # arithmetic can't silently corrupt results if a future refactor adds or
    # removes a balance fetch per chain.
    task_keys: list[tuple[str, str]] = []
    tasks = []
    for chain_name, _, svc_info in chain_infos:
        task_keys.append((chain_name, "safe"))
        tasks.append(_fetch_address_balance(chain_name, svc_info.get("multisig_address")))
        task_keys.append((chain_name, "agent"))
        tasks.append(_fetch_address_balance(chain_name, svc_info.get("agent_address")))
    all_results = await asyncio.gather(*tasks, return_exceptions=True)
    results_by_key: dict[tuple[str, str], Optional[tuple[float, float]]] = {}
    for key, res in zip(task_keys, all_results):
        if isinstance(res, Exception):
            results_by_key[key] = None
        else:
            results_by_key[key] = res  # type: ignore[assignment]

    for chain_name, chain_config, svc_info in chain_infos:
        multisig = svc_info.get("multisig_address")
        agent = svc_info.get("agent_address")
        service_id = svc_info.get("service_id")

        safe_bal = results_by_key.get((chain_name, "safe"))
        agent_bal = results_by_key.get((chain_name, "agent"))

        safe_native = safe_bal[0] if safe_bal else None
        safe_olas = safe_bal[1] if safe_bal else None
        agent_native = agent_bal[0] if agent_bal else None
        agent_olas = agent_bal[1] if agent_bal else None

        # Header emoji reflects funding state. If balance is unknown we can't judge →
        # show ❓ instead of a misleading ✅/⚠️ (security-md M2).
        fund_threshold = getattr(chain_config, "fund_threshold_xdai", None)
        if fund_threshold is None:
            emoji = "✅"
        elif multisig and safe_bal is None:
            emoji = "❓"
        elif agent and agent_bal is None:
            emoji = "❓"
        else:
            fund_ok = True
            if safe_native is not None and safe_native < fund_threshold:
                fund_ok = False
            if agent_native is not None and agent_native < fund_threshold:
                fund_ok = False
            emoji = "✅" if fund_ok else "⚠️"

        lines = [f"{emoji} {bold_md(chain_name.upper())}"]

        if multisig:
            lines.append(
                _address_line(
                    "Multisig",
                    multisig,
                    chain_name,
                    safe_native,
                    safe_olas,
                    balance_known=(safe_bal is not None),
                )
            )
        if agent:
            lines.append(
                _address_line(
                    "Agent",
                    agent,
                    chain_name,
                    agent_native,
                    agent_olas,
                    balance_known=(agent_bal is not None),
                )
            )
        if chain_config.mech_address:
            lines.append(_address_line("Mech", str(chain_config.mech_address), chain_name))
        if service_id:
            lines.append(f"Service ID: {code_md(str(service_id))}")

        if not multisig and not chain_config.mech_address:
            lines.append("Not deployed")

        blocks.append("\n".join(lines))

    messages = split_md_blocks(blocks)
    await status_msg.edit_text(
        messages[0],
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )
    for msg in messages[1:]:
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
