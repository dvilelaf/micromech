"""Logs command handler — send last 24h logs as a zip file."""

import io
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.security import authorized_only
from micromech.core.config import DEFAULT_CONFIG_DIR

LOG_DIR = DEFAULT_CONFIG_DIR / "logs"
LOG_FILE = LOG_DIR / "micromech.log"

MAX_ZIP_BYTES = 49 * 1024 * 1024  # 49 MB


def _collect_logs() -> list[tuple[str, bytes]]:
    """Collect log files from the last 24h."""
    files: list[tuple[str, bytes]] = []
    cutoff = time.time() - 86400

    if LOG_FILE.exists():
        files.append((LOG_FILE.name, LOG_FILE.read_bytes()))

    # Check for rotated/compressed logs
    if LOG_DIR.exists():
        for gz_file in sorted(LOG_DIR.glob("micromech.log.*.gz"), key=lambda f: f.name):
            if gz_file.stat().st_mtime >= cutoff:
                files.append((gz_file.name, gz_file.read_bytes()))

    # Also check /app/data for Docker deployments
    docker_log = Path("/app/data/micromech.log")
    if docker_log.exists() and not LOG_FILE.exists():
        files.append((docker_log.name, docker_log.read_bytes()))

    return files


def _build_zip(files: list[tuple[str, bytes]]) -> io.BytesIO:
    """Build a zip file in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files:
            compress = zipfile.ZIP_STORED if name.endswith(".gz") else zipfile.ZIP_DEFLATED
            zf.writestr(name, content, compress_type=compress)
    buf.seek(0)
    return buf


@authorized_only
async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /logs command — send last 24h logs as zip."""
    if not update.message:
        return

    await update.message.reply_text("Collecting logs...")

    try:
        files = _collect_logs()

        if not files:
            await update.message.reply_text("No log files found.")
            return

        zip_buf = _build_zip(files)
        zip_size = zip_buf.getbuffer().nbytes

        if zip_size > MAX_ZIP_BYTES:
            await update.message.reply_text(
                f"Log archive too large ({zip_size // 1024 // 1024} MB). "
                "Try accessing logs via SSH."
            )
            return

        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"micromech_logs_{timestamp}.zip"

        await update.message.reply_document(
            document=zip_buf,
            filename=filename,
        )
    except Exception as e:
        logger.error(f"Error collecting logs: {e}", exc_info=True)
        await update.message.reply_text(
            "Error collecting logs. Check server logs for details.",
        )
