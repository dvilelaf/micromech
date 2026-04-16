"""Status command handler — show per-chain mech status (triton-style)."""

import asyncio
from typing import Optional

from loguru import logger
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


def _request_emoji(requests: int, required: int) -> str:
    """✅ done / 🔄 in progress / ❌ idle — mirrors triton's heatmap logic."""
    if required > 0 and requests >= required:
        return "✅"
    if requests > 0:
        return "🔄"
    return "❌"


def _format_chain_status(
    chain_name: str,
    status: dict,
    olas_price: Optional[float],
) -> str:
    """Format status for a single chain in MarkdownV2 (triton style)."""
    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    emoji = _request_emoji(requests, required)

    lines = [f"{emoji} {bold_md(chain_name.upper())}"]

    service_id = status.get("service_id")
    if service_id:
        lines.append(f"ID: {escape_md(str(service_id))}")

    contract = status.get("staking_contract_name")
    if contract:
        lines.append(f"Contract: {escape_md(contract)}")

    staking_state = status.get("staking_state", "unknown")
    lines.append(f"State: {code_md(staking_state)}")

    rewards_raw = status.get("rewards")
    rewards = rewards_raw if rewards_raw is not None else 0.0
    reward_str = format_token(rewards, "OLAS")
    if olas_price and rewards > 0:
        eur = rewards * olas_price
        lines.append(f"Rewards: {code_md(reward_str)} \\({escape_md(format_currency(eur))}\\)")
    else:
        lines.append(f"Rewards: {code_md(reward_str)}")

    lines.append(f"Requests: {code_md(f'{requests}/{required}')}")

    lines.append(
        format_epoch_countdown(
            status.get("epoch_number", 0),
            status.get("epoch_end_utc"),
            status.get("remaining_epoch_seconds", 0),
        )
    )

    # Agent balance — None means "unknown", 0.0 means "empty" (H4/B3).
    agent_native = status.get("agent_balance_native")
    agent_olas = status.get("agent_balance_olas")
    if agent_native is not None:
        a_xdai = format_token(agent_native, "xDAI")
        a_olas = format_token(agent_olas if agent_olas is not None else 0.0, "OLAS")
        lines.append(f"Agent:  {code_md(a_xdai)} \\| {code_md(a_olas)}")

    safe_native = status.get("safe_balance_native")
    safe_olas = status.get("safe_balance_olas")
    if safe_native is not None:
        s_xdai = format_token(safe_native, "xDAI")
        s_olas = format_token(safe_olas if safe_olas is not None else 0.0, "OLAS")
        lines.append(f"Safe:   {code_md(s_xdai)} \\| {code_md(s_olas)}")

    contract_bal = status.get("contract_balance")
    if contract_bal is not None:
        lines.append(f"Contract balance: {code_md(format_token(contract_bal, 'OLAS'))}")

    return "\n".join(lines)


async def _fetch_chain_status_dict(
    chain_name: str, lifecycles: dict
) -> tuple[str, Optional[dict], Optional[str]]:
    """Fetch a chain's staking status + balances in parallel.

    Returns (chain_name, status_dict_or_None, error_message_or_None).

    R2-M5/B2: staking status and balances are fetched concurrently within
    the chain (not sequentially in get_status thread), then merged into
    the status dict for the formatter.
    """
    # Local import so tests can patch `micromech.core.bridge.get_service_info`.
    from micromech.core.bridge import get_service_info

    svc_key = get_service_info(chain_name).get("service_key")
    if not svc_key:
        return (chain_name, None, "Not deployed")
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        return (chain_name, None, "Lifecycle not available")

    # R3-B2: `asyncio.gather(..., return_exceptions=True)` never raises, so
    # the outer try/except that wrapped this call was dead code. Exceptions
    # are handled per-task below.
    status_task = asyncio.to_thread(lifecycle.get_status, svc_key)
    balances_task = asyncio.to_thread(lifecycle.get_balances)
    status, balances = await asyncio.gather(status_task, balances_task, return_exceptions=True)

    if isinstance(status, Exception):
        return (chain_name, None, user_error(f"status {chain_name}", status))
    if not status:
        return (chain_name, None, "Failed to fetch status")

    # R3-B1: surface balance fetch failures in the server-side log rather than
    # silently dropping them. The status block is still rendered with the
    # balance fields absent (which the formatter already handles) so the user
    # sees partial data instead of an error page.
    if isinstance(balances, Exception):
        logger.debug(
            "balances fetch failed for {}: {}",
            chain_name,
            type(balances).__name__,
        )
    elif isinstance(balances, dict):
        status = {**status, **balances}
    return (chain_name, status, None)


@authorized_only
@rate_limited
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /status command — show status for all enabled chains."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    lifecycles = context.bot_data.get("lifecycles", {})
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    status_msg = await update.message.reply_text("Fetching status...")

    # H3/B1: fetch OLAS price + all chains in parallel.
    # Price is cached (see bridge.get_olas_price_eur) so this rarely hits HTTP.
    olas_price_task = asyncio.to_thread(get_olas_price_eur)
    chain_tasks = [_fetch_chain_status_dict(c, lifecycles) for c in enabled]
    results = await asyncio.gather(olas_price_task, *chain_tasks, return_exceptions=True)
    olas_price = results[0] if not isinstance(results[0], Exception) else None
    chain_results = results[1:]

    blocks = []
    for result in chain_results:
        if isinstance(result, Exception):
            # R2-L1: route to user_error so server-side log + categorized
            # message stay consistent with in-chain error handling.
            blocks.append(user_error("status gather", result))
            continue
        chain_name, status, err = result
        if err:
            blocks.append(f"{bold_md(chain_name.upper())}\n{err}")
        else:
            blocks.append(_format_chain_status(chain_name, status, olas_price))

    # Uptime footer
    metrics = context.bot_data.get("metrics")
    if metrics:
        uptime = metrics.uptime_seconds
        h = uptime // 3600
        m = (uptime % 3600) // 60
        blocks.append(f"\nUptime: {code_md(f'{h}h {m}m')}")

    messages = split_md_blocks(blocks)
    await status_msg.edit_text(messages[0], parse_mode=ParseMode.MARKDOWN_V2)
    for msg in messages[1:]:
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
