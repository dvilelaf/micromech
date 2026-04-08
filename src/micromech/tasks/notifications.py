"""Notification service — Telegram when configured, always logs."""

import asyncio
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from telegram import Bot


class NotificationService:
    """Send notifications via Telegram and/or log.

    If bot/chat_id are not passed, lazily resolves from secrets
    when first needed (allows scheduler to send Telegram notifications
    even when created before the bot starts).
    """

    def __init__(self, bot: Optional["Bot"] = None, chat_id: Optional[int] = None):
        self._bot = bot
        self._chat_id = chat_id
        self._resolve_attempts = 0
        self._max_resolve_attempts = 10

    def _resolve(self) -> None:
        """Try to resolve bot from the running Application (if any).

        Retries up to _max_resolve_attempts to handle the case where
        the scheduler fires before the Telegram bot has fully started.
        """
        if self._bot is not None or self._resolve_attempts >= self._max_resolve_attempts:
            return
        self._resolve_attempts += 1
        try:
            from micromech.secrets import secrets

            if not (secrets.telegram_token and secrets.telegram_chat_id):
                self._resolve_attempts = self._max_resolve_attempts
                return
            from micromech.bot import _application

            if _application is not None:
                self._bot = _application.bot
                self._chat_id = secrets.telegram_chat_id
        except Exception:
            pass

    @property
    def telegram_enabled(self) -> bool:
        self._resolve()
        return self._bot is not None and self._chat_id is not None

    async def send(self, title: str, message: str, level: str = "info") -> None:
        """Send notification. Always logs, optionally sends to Telegram."""
        log_msg = f"[{title}] {message}"
        getattr(logger, level.lower(), logger.info)(log_msg)

        if self.telegram_enabled:
            try:
                text = f"<b>{_escape_html(title)}</b>\n{_escape_html(message)}"
                await self._bot.send_message(
                    chat_id=self._chat_id,  # type: ignore[arg-type]
                    text=text,
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.warning("Telegram notification failed: {}", e)

    def send_sync(self, title: str, message: str, level: str = "info") -> None:
        """Sync wrapper for use in threaded tasks."""
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self.send(title, message, level))
            _pending_tasks.add(task)
            task.add_done_callback(_pending_tasks.discard)
        except RuntimeError:
            # No event loop — just log
            getattr(logger, level.lower(), logger.info)(f"[{title}] {message}")

    def _skip_resolve(self) -> None:
        """Mark resolve as exhausted (for tests)."""
        self._resolve_attempts = self._max_resolve_attempts


# Prevent GC of fire-and-forget notification tasks
_pending_tasks: set[asyncio.Task] = set()


def _escape_html(text: str) -> str:
    """Escape HTML special chars for Telegram."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
