"""Info command handler — version and runtime information."""

import importlib.metadata
from typing import Optional

from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig
from micromech.core.persistence import PersistentQueue
from micromech.runtime.metrics import MetricsCollector


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

    lines = [bold("Micromech Info"), ""]
    lines.append(f"Version: {code(mm_version)}")
    lines.append(f"IWA: {code(iwa_version)}")

    # Uptime
    if metrics:
        uptime = metrics.uptime_seconds
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        lines.append(f"Uptime: {code(f'{hours}h {minutes}m')}")

    # Chains
    enabled = config.enabled_chains
    chain_names = ", ".join(c.upper() for c in enabled)
    lines.append(f"Chains: {code(chain_names) if chain_names else 'none'}")

    # Tools (auto-discovered)
    try:
        from micromech.tools.registry import ToolRegistry
        reg = ToolRegistry()
        reg.load_builtins()
        tool_names = reg.tool_ids
        if tool_names:
            lines.append(f"Tools: {code(', '.join(tool_names))}")
    except Exception:
        pass

    # Queue summary
    if queue:
        counts = queue.count_by_status()
        total = sum(counts.values())
        delivered = counts.get("delivered", 0)
        failed = counts.get("failed", 0)
        lines.append(f"Queue: {code(f'{total} total, {delivered} delivered, {failed} failed')}")

    text = "\n".join(lines)
    await update.message.reply_text(text, parse_mode="HTML")
