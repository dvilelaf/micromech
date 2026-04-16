"""Status command handler — show per-chain mech status (triton-style)."""

import asyncio
from typing import Any, Optional

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
from micromech.core.bridge import check_balances, get_olas_price_eur
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
    pending_payment: Optional[float] = None,
    master_balances: Optional[tuple[float, float]] = None,
) -> str:
    """Format status for a single chain in MarkdownV2 (triton style)."""
    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    emoji = _request_emoji(requests, required)

    lines = [f"{emoji} {bold_md(chain_name.upper())}"]

    service_id = status.get("service_id")
    if service_id:
        lines.append(f"ID: {escape_md(str(service_id))}")

    if pending_payment is not None:
        lines.append(f"Pending payment: {code_md(format_token(pending_payment, 'xDAI'))}")

    rewards_raw = status.get("rewards")
    reward_str = format_token(rewards_raw, "OLAS")  # None → "? OLAS", 0.0 → "0.00 OLAS"
    if olas_price and rewards_raw is not None and rewards_raw > 0:
        eur = rewards_raw * olas_price
        lines.append(f"Rewards: {code_md(reward_str)} \\({escape_md(format_currency(eur))}\\)")
    else:
        lines.append(f"Rewards: {code_md(reward_str)}")

    lines.append(f"Epoch deliveries: {code_md(f'{requests}/{required}')}")

    lines.append(
        format_epoch_countdown(
            status.get("epoch_number", 0),
            status.get("epoch_end_utc"),
            status.get("remaining_epoch_seconds", 0),
        )
    )

    if master_balances is not None:
        m_xdai = format_token(master_balances[0], "xDAI")
        m_olas = format_token(master_balances[1], "OLAS")
        lines.append(f"Master: {code_md(m_xdai)} \\| {code_md(m_olas)}")

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

    contract = status.get("staking_contract_name")
    if contract:
        lines.append(f"Contract: {escape_md(contract)}")

    staking_state = status.get("staking_state", "unknown")
    lines.append(f"State: {code_md(staking_state)}")

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
    _gather_results = await asyncio.gather(status_task, balances_task, return_exceptions=True)
    status: Any = _gather_results[0]
    balances: Any = _gather_results[1]

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


def _fetch_pending_payments(config: MicromechConfig) -> dict[str, float]:
    """Fetch pending marketplace payments for all enabled chains (blocking).

    IwaBridge is lazy (no work in __init__), so creating one per chain is cheap.
    """
    from micromech.core.bridge import IwaBridge
    from micromech.core.marketplace import get_balance_tracker_address, get_pending_balance

    results: dict[str, float] = {}
    for name, cfg in config.enabled_chains.items():
        if not cfg.mech_address or not cfg.marketplace_address:
            continue
        try:
            bridge = IwaBridge(chain_name=name)
            bt_addr = get_balance_tracker_address(
                bridge, name, cfg.mech_address, cfg.marketplace_address
            )
            if bt_addr:
                results[name] = round(get_pending_balance(bridge, bt_addr, cfg.mech_address), 6)
        except Exception as e:
            logger.warning(
                "Pending payment fetch failed for {}: {}", name, e
            )
    return results


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

    # Fetch OLAS price, chain statuses, pending payments, and master balances in parallel.
    chain_names = list(enabled.keys())
    olas_price_task = asyncio.to_thread(get_olas_price_eur)
    pending_task = asyncio.to_thread(_fetch_pending_payments, config)
    chain_tasks = [_fetch_chain_status_dict(c, lifecycles) for c in chain_names]
    master_tasks = [asyncio.to_thread(check_balances, c) for c in chain_names]

    # Layout: [olas_price, pending, *chain_status(N), *master_balance(N)]
    # chain_tasks and master_tasks both iterate chain_names in the same order.
    all_tasks = [olas_price_task, pending_task, *chain_tasks, *master_tasks]
    results = await asyncio.gather(*all_tasks, return_exceptions=True)

    n = len(chain_names)
    olas_price: Optional[float] = results[0] if not isinstance(results[0], Exception) else None  # type: ignore[assignment]
    pending_payments = results[1] if not isinstance(results[1], Exception) else {}
    chain_results = results[2 : 2 + n]
    master_results = results[2 + n :]
    master_by_chain = {}
    for name, master_result in zip(chain_names, master_results):
        if not isinstance(master_result, Exception):
            master_by_chain[name] = master_result

    blocks = []
    for result in chain_results:
        if isinstance(result, Exception):
            blocks.append(user_error("status gather", result))
            continue
        chain_name, status, err = result  # type: ignore[misc]
        if err:
            blocks.append(f"{bold_md(chain_name.upper())}\n{err}")
        else:
            blocks.append(
                _format_chain_status(
                    chain_name,
                    status,  # type: ignore[arg-type]
                    olas_price,
                    pending_payment=pending_payments.get(chain_name),
                    master_balances=master_by_chain.get(chain_name),  # type: ignore[arg-type]
                )
            )

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
