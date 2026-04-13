"""Schedule command — show next checkpoint epoch per chain."""

import asyncio
from datetime import datetime, timezone

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig


def _format_timedelta(seconds: float) -> str:
    """Format seconds as human-readable time delta."""
    abs_s = abs(seconds)
    h = int(abs_s / 3600)
    m = int((abs_s % 3600) / 60)
    if seconds < 0:
        return f"overdue {h}h {m}m ⚠️"
    return f"in {h}h {m}m"


@authorized_only
@rate_limited
async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /schedule command — show next epoch end per chain."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    wait_msg = await update.message.reply_text("Loading schedule...")

    now = datetime.now(timezone.utc)
    entries: list[tuple[datetime, str, str]] = []  # (epoch_end, chain_name, countdown)

    async def fetch_epoch(chain_name: str, staking_address: str) -> None:
        def _sync(addr: str = staking_address, chain: str = chain_name):
            from iwa.plugins.olas.contracts.staking import StakingContract

            contract = StakingContract(address=addr, chain_name=chain)
            return contract.get_next_epoch_start()

        try:
            epoch_end = await asyncio.to_thread(_sync)
            seconds = (epoch_end - now).total_seconds()
            countdown = _format_timedelta(seconds)
            entries.append((epoch_end, chain_name, countdown))
        except Exception as e:
            logger.error(f"Schedule error for {chain_name}: {e}")
            entries.append(
                (datetime.max.replace(tzinfo=timezone.utc), chain_name, f"Error: {str(e)}")
            )

    tasks = [
        fetch_epoch(chain_name, chain_config.staking_address)
        for chain_name, chain_config in enabled.items()
        if chain_config.staking_address
    ]
    if not tasks:
        await wait_msg.edit_text("No staking contracts configured.")
        return

    await asyncio.gather(*tasks)

    # Sort by epoch end ascending
    entries.sort(key=lambda e: e[0])

    lines = [bold("Checkpoint Schedule") + "\n"]
    for epoch_end, chain_name, countdown in entries:
        if epoch_end == datetime.max.replace(tzinfo=timezone.utc):
            lines.append(f"{bold(chain_name.upper())}: {escape_html(countdown)}")
            continue
        local_time = epoch_end.astimezone()
        tz_name = local_time.strftime("%Z")
        time_str = f"{local_time.strftime('%H:%M')} {tz_name}"
        lines.append(f"{bold(chain_name.upper())}: {code(time_str)} ({escape_html(countdown)})")

    await wait_msg.edit_text("\n".join(lines), parse_mode="HTML")
