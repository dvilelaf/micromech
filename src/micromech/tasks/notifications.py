"""Notification service — Telegram when configured, always logs."""

import asyncio
from typing import TYPE_CHECKING, List, Optional

from loguru import logger

if TYPE_CHECKING:
    from telegram import Bot

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 4

# Telegram parse modes (avoid top-level import of optional telegram package)
_PARSE_MODE_HTML = "HTML"


class NotificationService:
    """Send notifications via Telegram and/or log.

    Mirrors triton's NotificationService: takes a Bot instance and chat_id
    directly. If not provided, notifications are logged only (no Telegram).
    """

    def __init__(self, bot: Optional["Bot"] = None, chat_id: Optional[int] = None):
        self.bot = bot
        self.chat_id = chat_id

    @property
    def telegram_enabled(self) -> bool:
        return self.bot is not None and self.chat_id is not None

    async def send(self, title: str, message: str, level: str = "info") -> None:
        """Send notification. Always logs, optionally sends to Telegram."""
        log_msg = f"[{title}] {message}"
        getattr(logger, level.lower(), logger.info)(log_msg)

        if self.telegram_enabled:
            text = f"<b>{_escape_html(title)}</b>\n{_escape_html(message)}"
            await self.notify(text)

    async def notify(self, message: str, parse_mode: str = _PARSE_MODE_HTML) -> None:
        """Send a Telegram message with retry on network errors. Always logs."""
        logger.info(message)
        if not self.telegram_enabled:
            return

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await self.bot.send_message(  # type: ignore[union-attr]
                    chat_id=self.chat_id,  # type: ignore[arg-type]
                    text=message,
                    parse_mode=parse_mode,
                )
                return
            except Exception as e:
                # Lazy import so telegram package is optional at module level
                try:
                    from telegram.error import NetworkError, TimedOut

                    is_transient = isinstance(e, (TimedOut, NetworkError))
                except ImportError:
                    is_transient = False

                if is_transient and attempt < MAX_RETRIES:
                    delay = RETRY_BACKOFF_BASE * attempt
                    logger.warning(
                        "Telegram send failed (attempt {}/{}): {}."
                        " Retrying in {}s...",
                        attempt,
                        MAX_RETRIES,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    if attempt == MAX_RETRIES and is_transient:
                        logger.error(
                            "Failed to send notification after {} attempts: {}",
                            MAX_RETRIES,
                            e,
                        )
                    else:
                        logger.error(
                            "Failed to send notification to chat {}: {}",
                            self.chat_id,
                            e,
                        )
                    return

    async def send_messages(
        self, messages: List[str], parse_mode: str = _PARSE_MODE_HTML
    ) -> None:
        """Send multiple messages sequentially."""
        for msg in messages:
            await self.notify(msg, parse_mode)

    async def send_message(
        self, text: str, parse_mode: str = _PARSE_MODE_HTML
    ) -> None:
        """Alias for notify — matches triton interface."""
        await self.notify(text, parse_mode)

    def send_sync(self, title: str, message: str, level: str = "info") -> None:
        """Sync wrapper for use in threaded tasks (scheduler callbacks)."""
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self.send(title, message, level))
            _pending_tasks.add(task)
            task.add_done_callback(_pending_tasks.discard)
        except RuntimeError:
            # No event loop — just log
            getattr(logger, level.lower(), logger.info)("[%s] %s", title, message)

    def _skip_resolve(self) -> None:
        """No-op — kept for test compatibility."""


# Prevent GC of fire-and-forget notification tasks
_pending_tasks: set[asyncio.Task] = set()


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
