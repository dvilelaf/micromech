"""Tests for /update command rollback marker parsing."""

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.commands.update import update_command


class TestUpdateRollbackMessage(unittest.IsolatedAsyncioTestCase):
    """Bot must display explicit `rolled_back:<old>:<failed>` markers."""

    async def _run(self, marker: str) -> str:
        mock_update = MagicMock(spec=Update)
        mock_update.effective_chat.id = 123456789
        mock_wait_msg = AsyncMock()
        mock_update.message.reply_text = AsyncMock(return_value=mock_wait_msg)
        mock_context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)

        mock_trigger = MagicMock(spec=Path)
        mock_result = MagicMock(spec=Path)

        with (
            patch("micromech.bot.security.secrets") as mock_settings,
            patch("micromech.bot.commands.update.TRIGGER_PATH", mock_trigger),
            patch("micromech.bot.commands.update.RESULT_PATH", mock_result),
            patch("micromech.bot.commands.update.pop_update_result", return_value=marker),
            patch("micromech.bot.commands.update.POLL_INTERVAL", 0.01),
        ):
            mock_settings.telegram_chat_id = 123456789
            await update_command(mock_update, mock_context)

        mock_wait_msg.edit_text.assert_called_once()
        return mock_wait_msg.edit_text.call_args[0][0]

    async def test_rolled_back_versioned(self):
        text = await self._run("rolled_back:0.5.1:0.5.2")
        assert "rolled back to v0.5.1" in text
        assert "v0.5.2 failed" in text

    async def test_rolled_back_unknown_version(self):
        text = await self._run("rolled_back:unknown:0.5.2")
        assert "rolled back to vunknown" in text

    async def test_rolled_back_with_semver_suffix(self):
        text = await self._run("rolled_back:0.9.9:0.10.0-rc1")
        assert "v0.10.0-rc1 failed" in text

    async def test_existing_error_prefix_still_works(self):
        text = await self._run("error:pull_failed")
        assert "pull_failed" in text
