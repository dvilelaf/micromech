"""Status command handler — show per-chain mech status."""

import asyncio
from typing import Optional

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html, format_balance
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig


def _format_chain_status(chain_name: str, status: dict) -> str:
    """Format status for a single chain."""
    lines = [bold(chain_name.upper())]

    state = status.get("staking_state", "unknown")
    is_staked = status.get("is_staked", False)
    emoji = "🟢" if is_staked and state == "STAKED" else "🔴" if state == "EVICTED" else "⚪"
    lines.append(f"{emoji} State: {code(state)}")

    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    lines.append(f"Deliveries: {code(f'{requests}/{required}')}")

    rewards = status.get("rewards", 0)
    lines.append(f"Rewards: {code(format_balance(rewards, 'OLAS'))}")

    return "\n".join(lines)


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

    blocks = []
    for chain_name, chain_config in enabled.items():
        from micromech.core.bridge import get_service_info
        svc_key = get_service_info(chain_name).get("service_key")
        if not svc_key:
            blocks.append(f"{bold(chain_name.upper())}\nNot deployed")
            continue
        lifecycle = lifecycles.get(chain_name)
        if not lifecycle:
            blocks.append(f"{bold(chain_name.upper())}\nLifecycle not available")
            continue
        try:
            status = await asyncio.to_thread(lifecycle.get_status, svc_key)
            if status:
                blocks.append(_format_chain_status(chain_name, status))
            else:
                blocks.append(f"{bold(chain_name.upper())}\nFailed to fetch status")
        except Exception as e:
            logger.error(f"Status error for {chain_name}: {e}")
            blocks.append(f"{bold(chain_name.upper())}\nError: {escape_html(str(e))}")

    # Add uptime from metrics if available
    metrics = context.bot_data.get("metrics")
    if metrics:
        uptime = metrics.uptime_seconds
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        blocks.append(f"\nUptime: {code(f'{hours}h {minutes}m')}")

    text = "\n\n".join(blocks)
    await status_msg.edit_text(text, parse_mode="HTML")
