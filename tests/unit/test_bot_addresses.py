"""Tests for bot/commands/addresses.py."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1

_SECRETS_PATCH = {"telegram_chat_id": AUTHORIZED_CHAT_ID}


def _make_update(has_message=True):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    if has_message:
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()
    else:
        update.message = None
    return update


class TestAddressesCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.addresses import addresses_command

        update = _make_update(has_message=False)
        ctx = MagicMock()
        with patch("micromech.bot.security.secrets", **_SECRETS_PATCH):
            await addresses_command(update, ctx)

    @pytest.mark.asyncio
    async def test_exports_csv(self):
        from micromech.bot.commands.addresses import addresses_command

        rows = [{"tag": "master", "address": "0x" + "aa" * 20, "type": "EOA"}]
        wallet = MagicMock()
        wallet.key_storage.export_addresses.return_value = rows
        update = _make_update()
        ctx = MagicMock()
        with patch("micromech.bot.security.secrets", **_SECRETS_PATCH):
            with patch("micromech.bot.security._rate_limit_cache", {}):
                with patch("micromech.core.bridge.get_wallet", return_value=wallet):
                    await addresses_command(update, ctx)
        update.message.reply_document.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_wallet_sends_text(self):
        from micromech.bot.commands.addresses import addresses_command

        wallet = MagicMock()
        wallet.key_storage.export_addresses.return_value = []
        update = _make_update()
        ctx = MagicMock()
        with patch("micromech.bot.security.secrets", **_SECRETS_PATCH):
            with patch("micromech.bot.security._rate_limit_cache", {}):
                with patch("micromech.core.bridge.get_wallet", return_value=wallet):
                    await addresses_command(update, ctx)
        update.message.reply_text.assert_called_once()
        assert "No accounts" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_exception_sends_error_message(self):
        from micromech.bot.commands.addresses import addresses_command

        update = _make_update()
        ctx = MagicMock()
        with patch("micromech.bot.security.secrets", **_SECRETS_PATCH):
            with patch("micromech.bot.security._rate_limit_cache", {}):
                with patch(
                    "micromech.core.bridge.get_wallet",
                    side_effect=RuntimeError("fail"),
                ):
                    await addresses_command(update, ctx)
        update.message.reply_text.assert_called_once()
        assert "Failed" in update.message.reply_text.call_args[0][0]
