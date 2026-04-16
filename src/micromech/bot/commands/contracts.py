"""Contracts command — show staking contract info per chain (triton-style tree)."""

import asyncio
from datetime import datetime, timezone

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
from micromech.core.config import MicromechConfig

MAX_CONTRACTS_BLOCK_LENGTH = 1500  # smaller than default to keep tree chunks readable


def _epoch_countdown(epoch_end: datetime) -> str:
    """Format time-until-epoch as 'Hh Mm' or '-Hh Mm ⚠️' if already elapsed."""
    now = datetime.now(timezone.utc)
    seconds = int((epoch_end - now).total_seconds())
    if seconds >= 0:
        h, m = seconds // 3600, (seconds % 3600) // 60
        return f"{h}h {m}m"
    abs_s = abs(seconds)
    h, m = abs_s // 3600, (abs_s % 3600) // 60
    return f"-{h}h {m}m ⚠️"


def _fetch_contract(addr: str, chain: str):
    """Blocking: fetch all staking contract data for one chain."""
    from iwa.plugins.olas.contracts.staking import StakingContract

    c = StakingContract(address=addr, chain_name=chain)
    return {
        "staked": len(c.get_service_ids()),
        "max": c.max_num_services,
        "balance_olas": c.balance / 1e18,
        "min_stake_olas": c.min_staking_deposit / 1e18,
        "epoch_end": c.get_next_epoch_start(),
        "name": getattr(c, "name", None) or addr,
    }


async def _fetch_chain_contract(chain_name: str, staking_address: str) -> tuple[str, str]:
    """Fetch contract data for one chain. Returns (chain_name, formatted_block)."""
    if not staking_address:
        return (
            chain_name,
            f"{bold_md(chain_name.upper())}\nNo staking contract configured",
        )
    try:
        data = await asyncio.to_thread(_fetch_contract, staking_address, chain_name)
    except Exception as e:
        return (
            chain_name,
            f"{bold_md(chain_name.upper())}\n{user_error(f'contracts {chain_name}', e)}",
        )

    countdown = _epoch_countdown(data["epoch_end"])
    addr_link = explorer_link_md(
        chain_name, staking_address, f"{staking_address[:6]}...{staking_address[-4:]}"
    )
    contract_name = data["name"]

    if contract_name and contract_name != staking_address:
        header = bold_md(f"{chain_name.upper()} — {contract_name}")
    else:
        header = bold_md(chain_name.upper())

    slots = f"{data['staked']}/{data['max']}"
    lines = [
        header,
        f"├ Address: {addr_link}",
        f"├ Used slots: {code_md(slots)}",
        f"├ Stake: {code_md(format_token(data['min_stake_olas'], 'OLAS'))}",
        f"├ Balance: {code_md(format_token(data['balance_olas'], 'OLAS'))}",
        f"└ Epoch ends: {code_md(escape_md(countdown))}",
    ]
    return (chain_name, "\n".join(lines))


@authorized_only
@rate_limited
async def contracts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /contracts command — one staking contract per chain, tree format."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    wait_msg = await update.message.reply_text("Fetching contract data...")

    # H3/B1: fetch all chains in parallel.
    tasks = [_fetch_chain_contract(c, cfg.staking_address) for c, cfg in enabled.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    blocks = []
    for result in results:
        if isinstance(result, Exception):
            # R2-L1: consistent server-side log + categorized user message.
            blocks.append(user_error("contracts gather", result))
        else:
            _, block = result  # type: ignore[misc]
            blocks.append(block)

    header = bold_md("Staking Contracts") + "\n"
    messages = split_md_blocks(blocks, header=header, max_length=MAX_CONTRACTS_BLOCK_LENGTH)
    if not messages:
        await wait_msg.edit_text("No contract data available.")
        return

    await wait_msg.edit_text(
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
