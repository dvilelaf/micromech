"""Tests for micromech/bot/app.py.

Covers:
- start_command / help_command (68-84)
- global_callback_handler: authorized/unauthorized/dispatch/error (89-128)
- error_handler (133)
- create_application: token missing, services wired, lifecycle error (143-193)

SAFETY NOTES:
- HTTPXRequest is mocked to prevent any real network client creation
- Application.builder() chain is fully mocked
- No real Telegram connections are made
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.bot.app import (
    ACTION_CHECKPOINT,
    ACTION_CLAIM,
    ACTION_MANAGE,
    ACTION_MANAGE_CONFIRM,
    ACTION_SETTINGS,
    create_application,
    error_handler,
    global_callback_handler,
    help_command,
    start_command,
)
from micromech.core.config import ChainConfig, MicromechConfig

AUTHORIZED_CHAT_ID = 12345
AUTHORIZED_USER_ID = 99999


def _make_update(
    chat_id: int = AUTHORIZED_CHAT_ID,
    has_message: bool = True,
    callback_data: str | None = None,
):
    update = MagicMock()
    update.effective_chat = MagicMock()
    update.effective_chat.id = chat_id
    update.effective_user = MagicMock()
    update.effective_user.id = AUTHORIZED_USER_ID
    update.effective_user.username = "test_user"
    if has_message:
        update.message = MagicMock()
        update.message.reply_text = AsyncMock()
    else:
        update.message = None

    if callback_data is not None:
        update.callback_query = MagicMock()
        update.callback_query.data = callback_data
        update.callback_query.answer = AsyncMock()
    else:
        update.callback_query = None
    return update


def _make_ctx(chat_id: int = AUTHORIZED_CHAT_ID):
    ctx = MagicMock()
    ctx.bot_data = {
        "config": MicromechConfig(),
        "lifecycles": {},
        "queue": None,
        "metrics": None,
        "runtime_manager": None,
    }
    return ctx


# ---------------------------------------------------------------------------
# start_command
# ---------------------------------------------------------------------------


class TestStartCommand:
    @pytest.mark.asyncio
    async def test_start_replies_with_welcome(self):
        update = _make_update()
        ctx = _make_ctx()
        with (
            patch("micromech.bot.security.secrets") as mock_sec,
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await start_command(update, ctx)
        update.message.reply_text.assert_called_once()
        text = update.message.reply_text.call_args[0][0]
        assert "Welcome" in text or "Micromech" in text

    @pytest.mark.asyncio
    async def test_start_no_message_returns_early(self):
        update = _make_update(has_message=False)
        ctx = _make_ctx()
        with (
            patch("micromech.bot.security.secrets") as mock_sec,
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            # No crash expected, message is None so returns early
            await start_command(update, ctx)


# ---------------------------------------------------------------------------
# help_command
# ---------------------------------------------------------------------------


class TestHelpCommand:
    @pytest.mark.asyncio
    async def test_help_lists_commands(self):
        update = _make_update()
        ctx = _make_ctx()
        with (
            patch("micromech.bot.security.secrets") as mock_sec,
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await help_command(update, ctx)
        update.message.reply_text.assert_called_once()
        text = update.message.reply_text.call_args[0][0]
        assert "/status" in text
        assert "/wallet" in text

    @pytest.mark.asyncio
    async def test_help_no_message_returns_early(self):
        update = _make_update(has_message=False)
        ctx = _make_ctx()
        with (
            patch("micromech.bot.security.secrets") as mock_sec,
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await help_command(update, ctx)


# ---------------------------------------------------------------------------
# global_callback_handler
# ---------------------------------------------------------------------------


class TestGlobalCallbackHandler:
    @pytest.mark.asyncio
    async def test_no_effective_chat_returns_early(self):
        update = MagicMock()
        update.effective_chat = None
        ctx = _make_ctx()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)

    @pytest.mark.asyncio
    async def test_unauthorized_chat_returns_early(self):
        update = _make_update(chat_id=9999999)
        ctx = _make_ctx()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        update = _make_update()
        update.callback_query = None
        ctx = _make_ctx()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)

    @pytest.mark.asyncio
    async def test_no_colon_in_data_answers_invalid(self):
        update = _make_update(callback_data="nodatacolon")
        ctx = _make_ctx()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        update.callback_query.answer.assert_called_once_with("Invalid request")

    @pytest.mark.asyncio
    async def test_unknown_action_answers_unknown(self):
        update = _make_update(callback_data="unknown_action:payload")
        ctx = _make_ctx()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        update.callback_query.answer.assert_called_with("Unknown action")

    @pytest.mark.asyncio
    async def test_claim_action_dispatches(self):
        update = _make_update(callback_data=f"{ACTION_CLAIM}:all")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.handle_claim_callback", new_callable=AsyncMock) as mock_h,
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        mock_h.assert_called_once_with(update, ctx, "all")

    @pytest.mark.asyncio
    async def test_checkpoint_action_dispatches(self):
        update = _make_update(callback_data=f"{ACTION_CHECKPOINT}:all")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.handle_checkpoint_callback", new_callable=AsyncMock) as mock_h,
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        mock_h.assert_called_once()

    @pytest.mark.asyncio
    async def test_settings_action_dispatches(self):
        update = _make_update(callback_data=f"{ACTION_SETTINGS}:toggle_x")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.handle_settings_callback", new_callable=AsyncMock) as mock_h,
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        mock_h.assert_called_once()

    @pytest.mark.asyncio
    async def test_manage_action_dispatches(self):
        update = _make_update(callback_data=f"{ACTION_MANAGE}:gnosis")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.handle_manage_callback", new_callable=AsyncMock) as mock_h,
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        mock_h.assert_called_once()

    @pytest.mark.asyncio
    async def test_manage_confirm_action_dispatches(self):
        update = _make_update(callback_data=f"{ACTION_MANAGE_CONFIRM}:yes")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch(
                "micromech.bot.app.handle_manage_confirm_callback", new_callable=AsyncMock
            ) as mock_h,
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        mock_h.assert_called_once()

    @pytest.mark.asyncio
    async def test_handler_exception_answers_error(self):
        update = _make_update(callback_data=f"{ACTION_CLAIM}:all")
        ctx = _make_ctx()
        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.handle_claim_callback", side_effect=Exception("boom")),
        ):
            mock_sec.telegram_chat_id = AUTHORIZED_CHAT_ID
            await global_callback_handler(update, ctx)
        update.callback_query.answer.assert_called_with("An error occurred")


# ---------------------------------------------------------------------------
# error_handler
# ---------------------------------------------------------------------------


class TestErrorHandler:
    @pytest.mark.asyncio
    async def test_error_handler_logs_without_crash(self):
        ctx = MagicMock()
        ctx.error = ValueError("test error")
        await error_handler(MagicMock(), ctx)


# ---------------------------------------------------------------------------
# create_application
# ---------------------------------------------------------------------------


def _mock_builder():
    """Fully mocked telegram Application builder chain."""
    mock_app = MagicMock()
    mock_app.bot_data = {}
    builder = MagicMock()
    builder.token.return_value = builder
    builder.request.return_value = builder
    builder.post_init.return_value = builder
    builder.build.return_value = mock_app
    return builder, mock_app


class TestCreateApplication:
    def test_create_application_raises_without_token(self):
        cfg = MicromechConfig()
        with patch("micromech.bot.app.secrets") as mock_sec:
            mock_sec.telegram_token = None
            with pytest.raises(ValueError, match="token"):
                create_application(cfg)

    def test_create_application_returns_app(self):
        cfg = MicromechConfig()
        builder, mock_app = _mock_builder()

        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.Application") as mock_app_cls,
            patch("micromech.bot.app.HTTPXRequest"),
            patch("micromech.bot.app.MechLifecycle"),
        ):
            mock_sec.telegram_token = MagicMock()
            mock_sec.telegram_token.get_secret_value.return_value = "fake_token"
            mock_app_cls.builder.return_value = builder
            result = create_application(cfg)

        assert result is mock_app

    def test_create_application_stores_services_in_bot_data(self):
        """create_application stores queue, metrics, runtime_manager in bot_data."""
        cfg = MicromechConfig()
        builder, mock_app = _mock_builder()
        mock_queue = MagicMock()
        mock_metrics = MagicMock()
        mock_runtime = MagicMock()

        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.Application") as mock_app_cls,
            patch("micromech.bot.app.HTTPXRequest"),
            patch("micromech.bot.app.MechLifecycle"),
        ):
            mock_sec.telegram_token = MagicMock()
            mock_sec.telegram_token.get_secret_value.return_value = "fake"
            mock_app_cls.builder.return_value = builder
            create_application(
                cfg,
                runtime_manager=mock_runtime,
                queue=mock_queue,
                metrics=mock_metrics,
            )

        assert mock_app.bot_data["queue"] is mock_queue
        assert mock_app.bot_data["metrics"] is mock_metrics
        assert mock_app.bot_data["runtime_manager"] is mock_runtime

    def test_create_application_lifecycle_error_is_caught(self):
        """MechLifecycle creation failure per chain is caught and logged."""
        cfg = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    marketplace_address="0x" + "a" * 40,
                    factory_address="0x" + "b" * 40,
                    staking_address="0x" + "c" * 40,
                )
            }
        )
        builder, mock_app = _mock_builder()

        with (
            patch("micromech.bot.app.secrets") as mock_sec,
            patch("micromech.bot.app.Application") as mock_app_cls,
            patch("micromech.bot.app.HTTPXRequest"),
            patch("micromech.bot.app.MechLifecycle", side_effect=Exception("no rpc")),
        ):
            mock_sec.telegram_token = MagicMock()
            mock_sec.telegram_token.get_secret_value.return_value = "t"
            mock_app_cls.builder.return_value = builder
            create_application(cfg)

        # Should not raise; lifecycles dict is empty
        assert mock_app.bot_data["lifecycles"] == {}
