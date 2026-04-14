"""Tests for micromech.bot.commands.settings — targets uncovered lines."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_update(text=None):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    update.message = MagicMock()
    update.message.reply_text = AsyncMock()
    update.message.text = text
    return update


def _make_context(config=None, user_data=None):
    ctx = MagicMock()
    cfg = config if config is not None else make_test_config()
    ctx.bot_data = {"config": cfg}
    ctx.user_data = user_data if user_data is not None else {}
    return ctx


def _make_callback_update(payload: str):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    query = AsyncMock()
    query.data = payload
    update.callback_query = query
    update.message = None
    return update


def _auth_patches():
    return [
        patch(
            "micromech.bot.security.secrets",
            telegram_chat_id=AUTHORIZED_CHAT_ID,
        ),
        patch("micromech.bot.security._rate_limit_cache", {}),
    ]


# ---------------------------------------------------------------------------
# _validate_setting_input
# ---------------------------------------------------------------------------


class TestValidateSettingInput:
    def _float_setting(self):
        return {"type": float, "min": 0.1, "max": 10.0}

    def _int_setting(self):
        return {"type": int, "min": 1, "max": 100}

    def test_valid_float(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "5.5")
        assert value == pytest.approx(5.5)
        assert error is None

    def test_invalid_float_returns_error(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "abc")
        assert value is None
        assert "number" in error

    def test_invalid_int_returns_error(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._int_setting(), "xyz")
        assert value is None
        assert "integer" in error

    def test_below_min(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "0.0")
        assert value is None
        assert "between" in error

    def test_above_max(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "99.9")
        assert value is None
        assert "between" in error

    def test_at_boundary_min(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "0.1")
        assert value == pytest.approx(0.1)
        assert error is None

    def test_at_boundary_max(self):
        from micromech.bot.commands.settings import _validate_setting_input

        value, error = _validate_setting_input(self._float_setting(), "10.0")
        assert value == pytest.approx(10.0)
        assert error is None


# ---------------------------------------------------------------------------
# _validate_fund_thresholds
# ---------------------------------------------------------------------------


class TestValidateFundThresholds:
    def test_threshold_exceeds_target(self):
        from micromech.bot.commands.settings import _validate_fund_thresholds

        config = make_test_config()
        config.fund_target_native = 0.5
        error = _validate_fund_thresholds(
            "fund_threshold_native", 1.0, config
        )
        assert error is not None
        assert "Threshold" in error

    def test_threshold_ok(self):
        from micromech.bot.commands.settings import _validate_fund_thresholds

        config = make_test_config()
        config.fund_target_native = 1.0
        error = _validate_fund_thresholds(
            "fund_threshold_native", 0.5, config
        )
        assert error is None

    def test_target_below_threshold(self):
        from micromech.bot.commands.settings import _validate_fund_thresholds

        config = make_test_config()
        config.fund_threshold_native = 0.5
        error = _validate_fund_thresholds(
            "fund_target_native", 0.2, config
        )
        assert error is not None
        assert "Target" in error

    def test_target_ok(self):
        from micromech.bot.commands.settings import _validate_fund_thresholds

        config = make_test_config()
        config.fund_threshold_native = 0.1
        error = _validate_fund_thresholds("fund_target_native", 1.0, config)
        assert error is None

    def test_unrelated_attr_returns_none(self):
        from micromech.bot.commands.settings import _validate_fund_thresholds

        config = make_test_config()
        error = _validate_fund_thresholds(
            "claim_threshold_olas", 5.0, config
        )
        assert error is None


# ---------------------------------------------------------------------------
# _format_settings
# ---------------------------------------------------------------------------


class TestFormatSettings:
    def test_contains_all_labels(self):
        from micromech.bot.commands.settings import _TOGGLES, _format_settings

        config = make_test_config()
        text = _format_settings(config)
        for _, _, label in _TOGGLES:
            assert label in text

    def test_shows_enabled_disabled(self):
        from micromech.bot.commands.settings import _format_settings

        config = make_test_config()
        config.fund_enabled = True
        config.auto_update_enabled = False
        text = _format_settings(config)
        assert "Enabled" in text
        assert "Disabled" in text


# ---------------------------------------------------------------------------
# _format_edit_status
# ---------------------------------------------------------------------------


class TestFormatEditStatus:
    def test_contains_rewards_section(self):
        from micromech.bot.commands.settings import _format_edit_status

        config = make_test_config()
        text = _format_edit_status(config)
        assert "Rewards" in text
        assert "Claim threshold" in text

    def test_fund_section_when_fund_enabled(self):
        from micromech.bot.commands.settings import _format_edit_status

        config = make_test_config()
        config.fund_enabled = True
        config.payment_withdraw_enabled = False
        text = _format_edit_status(config)
        assert "Funding" in text

    def test_fund_section_when_withdraw_enabled(self):
        from micromech.bot.commands.settings import _format_edit_status

        config = make_test_config()
        config.fund_enabled = False
        config.payment_withdraw_enabled = True
        text = _format_edit_status(config)
        assert "Funding" in text

    def test_fund_section_hidden_when_both_disabled(self):
        from micromech.bot.commands.settings import _format_edit_status

        config = make_test_config()
        config.fund_enabled = False
        config.payment_withdraw_enabled = False
        text = _format_edit_status(config)
        assert "Funding" not in text


# ---------------------------------------------------------------------------
# _build_settings_keyboard
# ---------------------------------------------------------------------------


class TestBuildSettingsKeyboard:
    def test_has_edit_values_button(self):
        from micromech.bot.commands.settings import _build_settings_keyboard

        config = make_test_config()
        kb = _build_settings_keyboard(config)
        all_data = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
        ]
        assert any("values" in d for d in all_data)

    def test_has_cancel_button(self):
        from micromech.bot.commands.settings import _build_settings_keyboard

        config = make_test_config()
        kb = _build_settings_keyboard(config)
        all_data = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
        ]
        assert any("cancel" in d for d in all_data)

    def test_toggle_buttons_present_for_all_toggles(self):
        from micromech.bot.commands.settings import (
            _TOGGLES,
            _build_settings_keyboard,
        )

        config = make_test_config()
        kb = _build_settings_keyboard(config)
        all_data = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
        ]
        for key, _, _ in _TOGGLES:
            assert any(key in d for d in all_data)


# ---------------------------------------------------------------------------
# _build_edit_keyboard
# ---------------------------------------------------------------------------


class TestBuildEditKeyboard:
    def test_has_back_button(self):
        from micromech.bot.commands.settings import _build_edit_keyboard

        config = make_test_config()
        kb = _build_edit_keyboard(config)
        all_data = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
        ]
        assert any("back" in d for d in all_data)

    def test_has_buttons_for_editable_settings(self):
        from micromech.bot.commands.settings import (
            EDITABLE_SETTINGS,
            _build_edit_keyboard,
        )

        config = make_test_config()
        kb = _build_edit_keyboard(config)
        all_data = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
        ]
        for key in EDITABLE_SETTINGS:
            assert any(key in d for d in all_data)


# ---------------------------------------------------------------------------
# settings_command
# ---------------------------------------------------------------------------


class TestSettingsCommand:
    @pytest.mark.asyncio
    async def test_sends_reply_with_keyboard(self):
        from micromech.bot.commands.settings import settings_command

        update = _make_update()
        ctx = _make_context()

        with _auth_patches()[0], _auth_patches()[1]:
            await settings_command(update, ctx)

        update.message.reply_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.settings import settings_command

        update = MagicMock()
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = AUTHORIZED_USER_ID
        update.message = None
        ctx = _make_context()

        with _auth_patches()[0], _auth_patches()[1]:
            await settings_command(update, ctx)
        # No exception means the early return worked

    @pytest.mark.asyncio
    async def test_clears_editing_state(self):
        from micromech.bot.commands.settings import settings_command

        update = _make_update()
        ctx = _make_context(user_data={"settings_editing": "claim_thr"})

        with _auth_patches()[0], _auth_patches()[1]:
            await settings_command(update, ctx)

        assert "settings_editing" not in ctx.user_data


# ---------------------------------------------------------------------------
# handle_settings_callback
# ---------------------------------------------------------------------------


class TestHandleSettingsCallback:
    @pytest.mark.asyncio
    async def test_cancel_deletes_message(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:cancel")
        ctx = _make_context(user_data={"settings_editing": "claim_thr"})

        await handle_settings_callback(update, ctx, "cancel")

        update.callback_query.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = MagicMock()
        update.callback_query = None
        ctx = _make_context()

        # Should not raise
        await handle_settings_callback(update, ctx, "cancel")

    @pytest.mark.asyncio
    async def test_values_shows_edit_page(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:values")
        ctx = _make_context()

        await handle_settings_callback(update, ctx, "values")

        update.callback_query.edit_message_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_back_shows_settings_page(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:back")
        ctx = _make_context()

        await handle_settings_callback(update, ctx, "back")

        update.callback_query.edit_message_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_edit_known_key_sets_state(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:edit:claim_thr")
        ctx = _make_context(user_data={})

        await handle_settings_callback(update, ctx, "edit:claim_thr")

        assert ctx.user_data.get("settings_editing") == "claim_thr"
        update.callback_query.edit_message_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_edit_unknown_key_answers_unknown(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:edit:nonexistent")
        ctx = _make_context(user_data={})

        await handle_settings_callback(update, ctx, "edit:nonexistent")

        update.callback_query.answer.assert_called_once_with("Unknown setting")

    @pytest.mark.asyncio
    async def test_edit_with_none_user_data_answers_session_error(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:edit:claim_thr")
        ctx = _make_context()
        ctx.user_data = None

        await handle_settings_callback(update, ctx, "edit:claim_thr")

        update.callback_query.answer.assert_called_with("Session error")

    @pytest.mark.asyncio
    async def test_toggle_on(self):
        from micromech.bot.commands.settings import handle_settings_callback
        from micromech.core.config import MicromechConfig

        config = make_test_config()
        config.fund_enabled = False
        update = _make_callback_update("settings:fund:on")
        ctx = _make_context(config=config)

        with patch.object(MicromechConfig, "save") as mock_save:
            await handle_settings_callback(update, ctx, "fund:on")
            mock_save.assert_called_once()

        assert config.fund_enabled is True
        update.callback_query.edit_message_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_toggle_off(self):
        from micromech.bot.commands.settings import handle_settings_callback
        from micromech.core.config import MicromechConfig

        config = make_test_config()
        config.fund_enabled = True
        update = _make_callback_update("settings:fund:off")
        ctx = _make_context(config=config)

        with patch.object(MicromechConfig, "save") as mock_save:
            await handle_settings_callback(update, ctx, "fund:off")
            mock_save.assert_called_once()

        assert config.fund_enabled is False

    @pytest.mark.asyncio
    async def test_invalid_toggle_format_answers_invalid(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:toomany:parts:here")
        ctx = _make_context()

        await handle_settings_callback(update, ctx, "toomany:parts:here")

        update.callback_query.answer.assert_called_with("Invalid request")

    @pytest.mark.asyncio
    async def test_unknown_toggle_key_answers_unknown(self):
        from micromech.bot.commands.settings import handle_settings_callback

        update = _make_callback_update("settings:badkey:on")
        ctx = _make_context()

        await handle_settings_callback(update, ctx, "badkey:on")

        update.callback_query.answer.assert_called_with("Unknown setting")


# ---------------------------------------------------------------------------
# handle_settings_text
# ---------------------------------------------------------------------------


class TestHandleSettingsText:
    @pytest.mark.asyncio
    async def test_no_editing_state_returns_early(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = _make_update(text="1.0")
        ctx = _make_context(user_data={})

        await handle_settings_text(update, ctx)

        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_user_data_returns_early(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = _make_update(text="1.0")
        ctx = _make_context()
        ctx.user_data = {}

        await handle_settings_text(update, ctx)

        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_key_clears_state_returns(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = _make_update(text="1.0")
        ctx = _make_context(
            user_data={"settings_editing": "nonexistent_key"}
        )

        await handle_settings_text(update, ctx)

        assert "settings_editing" not in ctx.user_data
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_value_replies_error(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = _make_update(text="not_a_number")
        ctx = _make_context(user_data={"settings_editing": "claim_thr"})

        await handle_settings_text(update, ctx)

        update.message.reply_text.assert_called_once()
        args = update.message.reply_text.call_args[0]
        assert "number" in args[0] or "valid" in args[0]

    @pytest.mark.asyncio
    async def test_out_of_range_replies_error(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = _make_update(text="9999.0")
        ctx = _make_context(user_data={"settings_editing": "claim_thr"})

        await handle_settings_text(update, ctx)

        update.message.reply_text.assert_called_once()
        args = update.message.reply_text.call_args[0]
        assert "between" in args[0]

    @pytest.mark.asyncio
    async def test_fund_threshold_error_replies_and_clears_state(self):
        from micromech.bot.commands.settings import handle_settings_text
        from micromech.core.config import MicromechConfig

        config = make_test_config()
        config.fund_target_native = 0.5  # target is low
        update = _make_update(text="1.0")  # threshold > target
        ctx = _make_context(
            config=config,
            user_data={"settings_editing": "fund_thr"},
        )

        with patch.object(MicromechConfig, "save"):
            await handle_settings_text(update, ctx)

        update.message.reply_text.assert_called_once()
        args = update.message.reply_text.call_args[0]
        assert "Invalid" in args[0]
        assert "settings_editing" not in ctx.user_data

    @pytest.mark.asyncio
    async def test_valid_save_replies_confirmation(self):
        from micromech.bot.commands.settings import handle_settings_text
        from micromech.core.config import MicromechConfig

        config = make_test_config()
        update = _make_update(text="5.0")
        ctx = _make_context(
            config=config,
            user_data={"settings_editing": "claim_thr"},
        )

        with patch.object(MicromechConfig, "save") as mock_save:
            await handle_settings_text(update, ctx)
            mock_save.assert_called_once()

        assert "settings_editing" not in ctx.user_data
        update.message.reply_text.assert_called_once()
        call_args = update.message.reply_text.call_args
        text_arg = (
            call_args[0][0]
            if call_args[0]
            else call_args[1].get("text", "")
        )
        assert "Saved" in text_arg

    @pytest.mark.asyncio
    async def test_valid_save_updates_config_attr(self):
        from micromech.bot.commands.settings import handle_settings_text
        from micromech.core.config import MicromechConfig

        config = make_test_config()
        update = _make_update(text="3.5")
        ctx = _make_context(
            config=config,
            user_data={"settings_editing": "claim_thr"},
        )

        with patch.object(MicromechConfig, "save"):
            await handle_settings_text(update, ctx)

        assert config.claim_threshold_olas == pytest.approx(3.5)

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.settings import handle_settings_text

        update = MagicMock()
        update.message = None
        ctx = _make_context(user_data={"settings_editing": "claim_thr"})

        await handle_settings_text(update, ctx)
        # No exception — early return
