"""Queue command handler — show request queue status."""

from typing import Optional

from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.persistence import PersistentQueue


@authorized_only
@rate_limited
async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /queue command — show queue status."""
    if not update.message:
        return

    queue: Optional[PersistentQueue] = context.bot_data.get("queue")
    if not queue:
        await update.message.reply_text("Queue not available.")
        return

    # Status counts
    counts = queue.count_by_status()
    chain_counts = queue.count_by_chain()

    lines = [bold("Request Queue")]
    lines.append("")

    # Overall counts
    lines.append(f"Pending: {code(str(counts.get('pending', 0)))}")
    lines.append(f"Executing: {code(str(counts.get('executing', 0)))}")
    lines.append(f"Delivered: {code(str(counts.get('delivered', 0)))}")
    lines.append(f"Failed: {code(str(counts.get('failed', 0)))}")

    # Per-chain breakdown
    if chain_counts:
        lines.append("")
        lines.append(bold("Per Chain"))
        for chain, count in sorted(chain_counts.items()):
            lines.append(f"{chain}: {code(str(count))}")

    # Last 5 requests
    recent = queue.get_recent(limit=5)
    if recent:
        lines.append("")
        lines.append(bold("Recent Requests"))
        for record in recent:
            req = record.request
            tool = req.tool or "?"
            prompt_preview = (req.prompt[:40] + "...") if len(req.prompt) > 40 else req.prompt
            status_emoji = {
                "pending": "⏳",
                "executing": "⚙️",
                "executed": "✅",
                "delivered": "📦",
                "failed": "❌",
            }.get(req.status, "❓")
            lines.append(
                f"{status_emoji} {code(tool)}: {escape_html(prompt_preview)}"
            )

    text = "\n".join(lines)
    await update.message.reply_text(text, parse_mode="HTML")
