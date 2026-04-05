"""Tests for NotificationService."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.tasks.notifications import NotificationService, _escape_html


class TestEscapeHtml:
    def test_ampersand(self):
        assert _escape_html("A & B") == "A &amp; B"

    def test_angle_brackets(self):
        assert _escape_html("<script>alert(1)</script>") == "&lt;script&gt;alert(1)&lt;/script&gt;"

    def test_combined(self):
        assert _escape_html("a<b&c>d") == "a&lt;b&amp;c&gt;d"

    def test_no_special(self):
        assert _escape_html("Hello World") == "Hello World"

    def test_empty(self):
        assert _escape_html("") == ""


class TestNotificationServiceInit:
    def test_without_bot(self):
        ns = NotificationService()
        assert ns._bot is None
        assert ns._chat_id is None
        assert ns._resolve_attempts == 0

    def test_with_bot(self):
        bot = MagicMock()
        ns = NotificationService(bot=bot, chat_id=42)
        assert ns._bot is bot
        assert ns._chat_id == 42


class TestNotificationServiceTelegramEnabled:
    def test_enabled_with_bot_and_chat_id(self):
        ns = NotificationService(bot=MagicMock(), chat_id=42)
        assert ns.telegram_enabled is True

    def test_disabled_without_bot(self):
        ns = NotificationService()
        ns._skip_resolve()
        assert ns.telegram_enabled is False


class TestNotificationServiceSend:
    @pytest.mark.asyncio
    async def test_send_with_telegram(self):
        bot = AsyncMock()
        ns = NotificationService(bot=bot, chat_id=123)

        await ns.send("Title", "Hello")

        bot.send_message.assert_called_once_with(
            chat_id=123,
            text="<b>Title</b>\nHello",
            parse_mode="HTML",
        )

    @pytest.mark.asyncio
    async def test_send_escapes_html(self):
        bot = AsyncMock()
        ns = NotificationService(bot=bot, chat_id=1)

        await ns.send("A<B", "x&y")

        bot.send_message.assert_called_once_with(
            chat_id=1,
            text="<b>A&lt;B</b>\nx&amp;y",
            parse_mode="HTML",
        )

    @pytest.mark.asyncio
    async def test_send_without_telegram_just_logs(self):
        ns = NotificationService()
        ns._skip_resolve()
        # Should not raise
        await ns.send("Title", "msg")

    @pytest.mark.asyncio
    async def test_send_telegram_error_does_not_raise(self):
        bot = AsyncMock()
        bot.send_message.side_effect = Exception("network error")
        ns = NotificationService(bot=bot, chat_id=1)

        # Should not raise
        await ns.send("Title", "msg")


class TestNotificationServiceSendSync:
    def test_send_sync_without_loop_just_logs(self):
        ns = NotificationService()
        ns._skip_resolve()
        # No event loop — just logs, no crash
        ns.send_sync("Title", "msg")

    @pytest.mark.asyncio
    async def test_send_sync_with_running_loop(self):
        bot = AsyncMock()
        ns = NotificationService(bot=bot, chat_id=1)
        # Inside an async test there IS a running loop
        ns.send_sync("Title", "msg")
        # The task is created but may not have run yet — just verify no crash


class TestNotificationServiceResolve:
    def test_resolve_increments_attempts(self):
        ns = NotificationService()
        assert ns._resolve_attempts == 0
        # After checking telegram_enabled, attempts incremented
        _ = ns.telegram_enabled
        assert ns._resolve_attempts >= 1

    def test_resolve_does_not_set_bot_without_secrets(self):
        ns = NotificationService()
        with patch("micromech.tasks.notifications.NotificationService._resolve"):
            pass
        # Without telegram secrets, bot stays None
        ns._resolve()
        assert ns._bot is None
