"""Tests for /update command — focus on rollback marker parsing."""

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.commands.update import update_command


class TestUpdateRollbackMessage(unittest.IsolatedAsyncioTestCase):
    """Bot must display the literal rollback marker from updater.sh.

    The bash updater writes `error:rolled_back_to_v$OLD` after a failed
    health check. The bot's `update_command` parses any `error:` prefix via
    `split(":", 1)`, which preserves version dots in the suffix.
    """

    async def _run(self, marker: str) -> str:
        mock_update = MagicMock(spec=Update)
        mock_update.effective_chat.id = 123456789
        mock_wait_msg = AsyncMock()
        mock_update.message.reply_text = AsyncMock(return_value=mock_wait_msg)
        mock_context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)

        mock_trigger = MagicMock(spec=Path)
        mock_result = MagicMock(spec=Path)
        mock_result.exists.return_value = True
        mock_result.read_text.return_value = marker

        with (
            patch("micromech.bot.security.secrets") as mock_settings,
            patch("micromech.bot.commands.update.TRIGGER_PATH", mock_trigger),
            patch("micromech.bot.commands.update.RESULT_PATH", mock_result),
            patch("micromech.bot.commands.update.POLL_INTERVAL", 0.01),
        ):
            mock_settings.telegram_chat_id = 123456789
            await update_command(mock_update, mock_context)

        mock_wait_msg.edit_text.assert_called_once()
        return mock_wait_msg.edit_text.call_args[0][0]

    async def test_rolled_back_versioned(self):
        text = await self._run("error:rolled_back_to_v0.5.1")
        # split(":", 1) must preserve dots in version.
        assert "rolled_back_to_v0.5.1" in text
        assert "Update failed" in text

    async def test_rolled_back_unknown_version(self):
        """Updater writes `vunknown` if OLD label was missing on the previous image."""
        text = await self._run("error:rolled_back_to_vunknown")
        assert "rolled_back_to_vunknown" in text
        assert "Update failed" in text

    async def test_rolled_back_with_semver_suffix(self):
        """Versions like 0.10.0-rc1 must survive the split intact."""
        text = await self._run("error:rolled_back_to_v0.10.0-rc1")
        assert "rolled_back_to_v0.10.0-rc1" in text

    async def test_existing_error_prefix_still_works(self):
        """Regression guard: the new rollback marker must not break old error: cases."""
        text = await self._run("error:pull_failed")
        assert "pull_failed" in text
