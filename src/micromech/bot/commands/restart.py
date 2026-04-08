"""Restart command handler — restart runtime via RuntimeManager."""

from pathlib import Path
from typing import Optional

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold
from micromech.bot.security import authorized_only
from micromech.runtime.manager import RuntimeManager

RESTART_TRIGGER = Path("/app/data/.update-request")


@authorized_only
async def restart_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle /restart command — restart the mech runtime."""
    if not update.message:
        return

    runtime: Optional[RuntimeManager] = context.bot_data.get(
        "runtime_manager",
    )
    if runtime:
        await update.message.reply_text("Restarting runtime...")
        try:
            success = await runtime.restart()
            if success:
                await update.message.reply_text(
                    bold("Runtime restarted successfully."),
                    parse_mode="HTML",
                )
            else:
                await update.message.reply_text(
                    "Restart failed. Check logs for details.",
                )
        except Exception as e:
            logger.error(
                f"Error in restart command: {e}",
                exc_info=True,
            )
            await update.message.reply_text(
                "Restart error. Check logs for details.",
            )
        return

    # Fallback: write restart trigger for updater sidecar
    try:
        RESTART_TRIGGER.parent.mkdir(parents=True, exist_ok=True)
        RESTART_TRIGGER.write_text("restart")
        await update.message.reply_text(
            "Restart requested via updater sidecar.",
        )
    except Exception as e:
        logger.error(
            f"Failed to write restart trigger: {e}",
            exc_info=True,
        )
        await update.message.reply_text(
            "Restart not available.",
        )
