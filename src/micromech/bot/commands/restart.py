"""Restart command handler — restart runtime via RuntimeManager."""

from typing import Optional

from loguru import logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold
from micromech.bot.security import authorized_only
from micromech.runtime.manager import RuntimeManager


@authorized_only
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /restart command — restart the mech runtime."""
    if not update.message:
        return

    runtime: Optional[RuntimeManager] = context.bot_data.get("runtime_manager")
    if not runtime:
        await update.message.reply_text("Runtime manager not available.")
        return

    await update.message.reply_text("Restarting runtime...")

    try:
        success = await runtime.restart()
        if success:
            await update.message.reply_text(f"{bold('Runtime restarted successfully.')}", parse_mode="HTML")
        else:
            error = runtime.error or "Unknown error"
            await update.message.reply_text(f"Restart failed: {error}")
    except Exception as e:
        logger.error(f"Error in restart command: {e}", exc_info=True)
        await update.message.reply_text(f"Restart error: {e}")
