"""Update command handler — check for updates and trigger manual update."""

import asyncio
from pathlib import Path

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.security import authorized_only

TRIGGER_PATH = Path("/app/data/.update-request")
RESULT_PATH = Path("/app/data/.update-result")

POLL_INTERVAL = 10
POLL_ATTEMPTS = 12


@authorized_only
async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /update command — trigger remote update via updater sidecar."""
    if not update.message:
        return

    wait_msg = await update.message.reply_text("Checking for updates...")

    try:
        RESULT_PATH.unlink(missing_ok=True)
        TRIGGER_PATH.write_text("update")

        for _ in range(POLL_ATTEMPTS):
            await asyncio.sleep(POLL_INTERVAL)

            if not RESULT_PATH.exists():
                continue

            result = RESULT_PATH.read_text().strip()
            RESULT_PATH.unlink(missing_ok=True)

            if result.startswith("updated:"):
                parts = result.split(":")
                await wait_msg.edit_text(f"Updating v{parts[1]} -> v{parts[2]}! Restarting...")
                return
            elif result.startswith("current:"):
                version = result.split(":")[1]
                await wait_msg.edit_text(f"Already at latest version (v{version})")
                return
            elif result.startswith("error:"):
                error = result.split(":", 1)[1]
                await wait_msg.edit_text(f"Update failed: {error}")
                return

        await wait_msg.edit_text("Timeout waiting for updater. Is the updater container running?")

    except Exception as e:
        logger.error(f"Error in update command: {e}", exc_info=True)
        await wait_msg.edit_text(f"Error: {e}")
