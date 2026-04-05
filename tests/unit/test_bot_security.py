"""Tests for bot.security — authorized_only and rate_limited decorators."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestAuthorizedOnly:
    @pytest.mark.asyncio
    async def test_authorized_chat_calls_handler(self):
        with patch("micromech.bot.security.secrets") as mock_secrets:
            mock_secrets.telegram_chat_id = 42

            from micromech.bot.security import authorized_only

            handler = AsyncMock()
            wrapped = authorized_only(handler)

            update = MagicMock()
            update.effective_chat.id = 42
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_called_once_with(update, context)

    @pytest.mark.asyncio
    async def test_unauthorized_chat_blocked(self):
        with patch("micromech.bot.security.secrets") as mock_secrets:
            mock_secrets.telegram_chat_id = 42

            from micromech.bot.security import authorized_only

            handler = AsyncMock()
            wrapped = authorized_only(handler)

            update = MagicMock()
            update.effective_chat.id = 999
            update.effective_user.username = "hacker"
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_effective_chat_returns_early(self):
        with patch("micromech.bot.security.secrets") as mock_secrets:
            mock_secrets.telegram_chat_id = 42

            from micromech.bot.security import authorized_only

            handler = AsyncMock()
            wrapped = authorized_only(handler)

            update = MagicMock()
            update.effective_chat = None
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_not_called()


class TestRateLimited:
    @pytest.mark.asyncio
    async def test_first_call_allowed(self):
        with patch("micromech.bot.security._rate_limit_cache", {}):
            from micromech.bot.security import rate_limited

            handler = AsyncMock()
            wrapped = rate_limited(handler)

            update = MagicMock()
            update.effective_user.id = 1
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_rapid_second_call_blocked(self):
        import time

        cache = {1: time.time()}
        with patch("micromech.bot.security._rate_limit_cache", cache):
            from micromech.bot.security import rate_limited

            handler = AsyncMock()
            wrapped = rate_limited(handler)

            update = MagicMock()
            update.effective_user.id = 1
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_user_drops_request(self):
        """Requests with no effective_user are dropped (security fix)."""
        with patch("micromech.bot.security._rate_limit_cache", {}):
            from micromech.bot.security import rate_limited

            handler = AsyncMock()
            wrapped = rate_limited(handler)

            update = MagicMock()
            update.effective_user = None
            context = MagicMock()

            await wrapped(update, context)
            handler.assert_not_called()
