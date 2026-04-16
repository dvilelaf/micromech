"""Last rewards command — show current accrued OLAS rewards per chain (MarkdownV2)."""

import asyncio
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import (
    bold_md,
    code_md,
    escape_md,
    format_currency,
    format_epoch_countdown,
    format_token,
    split_md_blocks,
    user_error,
)
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.bridge import get_olas_price_eur
from micromech.core.config import MicromechConfig


async def _fetch_chain_rewards(
    chain_name: str, lifecycles: dict
) -> tuple[str, Optional[dict], Optional[str]]:
    """Fetch staking status for one chain. Returns (chain_name, status, err)."""
    # Local import so tests can patch `micromech.core.bridge.get_service_info`.
    from micromech.core.bridge import get_service_info

    svc_key = get_service_info(chain_name).get("service_key")
    if not svc_key:
        return (chain_name, None, "Not deployed")
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        return (chain_name, None, "Lifecycle not available")
    try:
        status = await asyncio.to_thread(lifecycle.get_status, svc_key)
    except Exception as e:
        return (chain_name, None, user_error(f"rewards {chain_name}", e))
    if not status:
        return (chain_name, None, "Could not fetch status")
    return (chain_name, status, None)


def _format_rewards_block(chain_name: str, status: dict, olas_price: Optional[float]) -> str:
    rewards_raw = status.get("rewards")
    rewards = rewards_raw if rewards_raw is not None else 0.0
    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    state = status.get("staking_state", "unknown")
    contract_name = status.get("staking_contract_name")
    epoch_number = status.get("epoch_number", 0)
    epoch_end_utc = status.get("epoch_end_utc")

    emoji = "✅" if rewards > 0 else "❌"
    if contract_name:
        header = bold_md(f"{chain_name.upper()} — {contract_name}")
    else:
        header = bold_md(chain_name.upper())
    lines = [f"{emoji} {header}"]

    lines.append(f"State: {code_md(state)}")

    reward_str = format_token(rewards, "OLAS")
    if olas_price and rewards > 0:
        eur = rewards * olas_price
        lines.append(f"Accrued: {code_md(reward_str)} \\({escape_md(format_currency(eur))}\\)")
    else:
        lines.append(f"Accrued: {code_md(reward_str)}")

    lines.append(f"Deliveries: {code_md(f'{requests}/{required}')}")

    if required > 0 and requests >= required:
        lines.append("✅ On track to earn rewards")
    elif required > 0:
        remaining = required - requests
        lines.append(f"⚠️ Needs {escape_md(str(remaining))} more deliveries")

    if epoch_number or epoch_end_utc:
        lines.append(format_epoch_countdown(epoch_number, epoch_end_utc, 0))

    return "\n".join(lines)


@authorized_only
@rate_limited
async def last_rewards_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /last_rewards command — show accrued staking rewards per chain."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    lifecycles = context.bot_data.get("lifecycles", {})
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    wait_msg = await update.message.reply_text("Fetching rewards...")

    # H3/B1: fetch OLAS price + all chain rewards in parallel.
    olas_price_task = asyncio.to_thread(get_olas_price_eur)
    chain_tasks = [_fetch_chain_rewards(c, lifecycles) for c in enabled]
    results = await asyncio.gather(olas_price_task, *chain_tasks, return_exceptions=True)
    olas_price = results[0] if not isinstance(results[0], Exception) else None
    chain_results = results[1:]

    blocks = []
    for result in chain_results:
        if isinstance(result, Exception):
            # R2-L1: user_error for consistent log + categorized message.
            blocks.append(user_error("last_rewards gather", result))
            continue
        chain_name, status, err = result
        if err:
            blocks.append(f"{bold_md(chain_name.upper())}\n{err}")
        else:
            blocks.append(_format_rewards_block(chain_name, status, olas_price))

    header = bold_md("Accrued Rewards") + "\n"
    messages = split_md_blocks(blocks, header=header)
    if not messages:
        await wait_msg.edit_text("No reward data available.")
        return

    await wait_msg.edit_text(messages[0], parse_mode=ParseMode.MARKDOWN_V2)
    for msg in messages[1:]:
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
