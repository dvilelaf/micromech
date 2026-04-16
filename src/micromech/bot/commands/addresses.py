"""Addresses command — export wallet addresses as CSV (MarkdownV2)."""

import csv
import io
from datetime import datetime, timezone

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.security import authorized_only, rate_limited


@authorized_only
@rate_limited
async def addresses_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addresses — export all wallet tags and public addresses as CSV."""
    if not update.message:
        return

    try:
        from micromech.core.bridge import get_wallet

        wallet = get_wallet()
        rows = wallet.key_storage.export_addresses()

        if not rows:
            await update.message.reply_text("No accounts found in wallet.")
            return

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=["tag", "address", "type"])
        writer.writeheader()
        writer.writerows(rows)

        csv_bytes = io.BytesIO(buf.getvalue().encode("utf-8"))
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

        await update.message.reply_document(
            document=csv_bytes,
            filename=f"addresses_{timestamp}.csv",
        )
        logger.info(f"Exported {len(rows)} addresses via /addresses command")

    except Exception as e:
        logger.error(f"Error exporting addresses: {e}", exc_info=True)
        await update.message.reply_text("Failed to export addresses. Check logs.")
