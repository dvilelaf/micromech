"""Sell command — trigger auto-sell pipeline manually."""

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, escape_html
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig


@authorized_only
@rate_limited
async def sell_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /sell command — run auto-sell pipeline manually."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]

    if not config.auto_sell_enabled:
        await update.message.reply_text(
            "Auto-sell is disabled. Enable it in /settings.", parse_mode="HTML"
        )
        return

    msg = await update.message.reply_text(f"Running {bold('auto-sell')}...", parse_mode="HTML")

    try:
        from micromech.tasks.auto_sell import auto_sell_task
        from micromech.tasks.notifications import NotificationService

        bridges = context.bot_data.get("bridges")
        if not bridges:
            # Fallback: create bridges on the fly (slower but correct)
            import asyncio

            from micromech.core.bridge import create_bridges

            bridges = await asyncio.to_thread(create_bridges, config)

        notification = NotificationService()
        await auto_sell_task(bridges, notification, config, olas_floor_wei=None)

        await msg.edit_text("Auto-sell completed. Check notifications.", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Manual sell command failed: {e}", exc_info=True)
        await msg.edit_text(f"Error: {escape_html(str(e))}", parse_mode="HTML")
