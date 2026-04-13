"""Last rewards command — show current accrued OLAS rewards per chain."""

import asyncio

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html, format_balance
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig


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

    blocks = []
    for chain_name in enabled:
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
            if not status:
                blocks.append(f"{bold(chain_name.upper())}\nCould not fetch status")
                continue

            rewards = status.get("rewards", 0) or 0
            requests = status.get("requests_this_epoch", 0)
            required = status.get("required_requests", 0)
            state = status.get("staking_state", "unknown")

            emoji = "✅" if rewards > 0 else "❌"
            lines = [f"{emoji} {bold(chain_name.upper())}"]
            lines.append(f"State: {code(state)}")
            lines.append(f"Accrued: {code(format_balance(rewards, 'OLAS'))}")
            lines.append(f"Deliveries this epoch: {code(f'{requests}/{required}')}")

            # Estimate if on track for rewards
            if required > 0 and requests >= required:
                lines.append("✅ On track to earn rewards")
            elif required > 0:
                remaining = required - requests
                lines.append(f"⚠️ Needs {remaining} more deliveries")

            blocks.append("\n".join(lines))
        except Exception as e:
            logger.error(f"Last rewards error for {chain_name}: {e}")
            blocks.append(f"{bold(chain_name.upper())}\nError: {escape_html(str(e))}")

    text = f"{bold('Accrued Rewards')}\n\n" + "\n\n".join(blocks)
    await wait_msg.edit_text(text, parse_mode="HTML")
