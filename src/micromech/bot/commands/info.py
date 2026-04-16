"""Info command handler — version and runtime information (MarkdownV2)."""

import importlib.metadata
from typing import Optional

from loguru import logger
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold_md, code_md, escape_md
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig
from micromech.core.persistence import PersistentQueue
from micromech.runtime.metrics import MetricsCollector


def _get_tool_names(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    """Get tool names from bot_data cache or load them lazily.

    Caches the result in context.bot_data so subsequent /info calls don't
    re-run tool discovery (M2/B8).

    R2-L3: only cache SUCCESSFUL loads. If the import/load fails transiently
    (e.g. a bad plugin during startup), we retry on the next /info instead
    of caching an empty list forever.
    """
    cached = context.bot_data.get("tool_names")
    if cached is not None:
        return cached
    try:
        from micromech.tools.registry import ToolRegistry

        reg = ToolRegistry()
        reg.load_builtins()
        names = list(reg.tool_ids)
    except Exception:
        logger.debug("ToolRegistry load failed; not caching empty result")
        return []
    context.bot_data["tool_names"] = names
    return names


@authorized_only
@rate_limited
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /info command."""
    if not update.message:
        return

    # Versions
    try:
        mm_version = importlib.metadata.version("micromech")
    except importlib.metadata.PackageNotFoundError:
        mm_version = "unknown"

    try:
        iwa_version = importlib.metadata.version("iwa")
    except importlib.metadata.PackageNotFoundError:
        iwa_version = "unknown"

    config: MicromechConfig = context.bot_data["config"]
    metrics: Optional[MetricsCollector] = context.bot_data.get("metrics")
    queue: Optional[PersistentQueue] = context.bot_data.get("queue")

    lines = [bold_md("Micromech Info"), ""]
    lines.append(f"Version: {code_md(mm_version)}")
    lines.append(f"IWA: {code_md(iwa_version)}")

    if metrics:
        uptime = metrics.uptime_seconds
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        lines.append(f"Uptime: {code_md(f'{hours}h {minutes}m')}")

    enabled = config.enabled_chains
    chain_names = ", ".join(c.upper() for c in enabled)
    lines.append(f"Chains: {code_md(chain_names) if chain_names else escape_md('none')}")

    tool_names = _get_tool_names(context)
    if tool_names:
        lines.append(f"Tools: {code_md(', '.join(tool_names))}")

    if queue:
        counts = queue.count_by_status()
        total = sum(counts.values())
        delivered = counts.get("delivered", 0)
        failed = counts.get("failed", 0)
        lines.append(f"Queue: {code_md(f'{total} total, {delivered} delivered, {failed} failed')}")

    text = "\n".join(lines)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
