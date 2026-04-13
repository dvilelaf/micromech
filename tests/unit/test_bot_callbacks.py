"""Tests for bot callback handlers: claim, manage, checkpoint.

These cover the handle_*_callback functions and _execute_action helpers
that were at 0% coverage (only the /command entry points had tests).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42


def _make_query(payload: str):
    """Return a MagicMock that looks like a CallbackQuery."""
    query = MagicMock()
    query.data = payload
    query.answer = AsyncMock()
    query.delete_message = AsyncMock()
    query.edit_message_text = AsyncMock()
    return query


def _make_update_with_query(payload: str):
    update = MagicMock()
    update.callback_query = _make_query(payload)
    update.message = None
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = 1
    return update


def _make_context(**bot_data_extras):
    ctx = MagicMock()
    ctx.bot_data = {"config": make_test_config(), **bot_data_extras}
    ctx.user_data = {}
    return ctx


# ===========================================================================
# Claim callbacks
# ===========================================================================

class TestClaimCallbacks:
    @pytest.mark.asyncio
    async def test_cancel_deletes_message(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = _make_update_with_query("cancel")
        ctx = _make_context()
        await handle_claim_callback(update, ctx, "cancel")
        update.callback_query.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = MagicMock()
        update.callback_query = None
        ctx = _make_context()
        await handle_claim_callback(update, ctx, "gnosis")  # must not raise

    @pytest.mark.asyncio
    async def test_all_chains_claim_success(self):
        from micromech.bot.commands.claim import handle_claim_callback

        lifecycle = MagicMock()
        lifecycle.claim_rewards.return_value = True
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "all")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_all_chains_nothing_to_claim(self):
        from micromech.bot.commands.claim import handle_claim_callback

        lifecycle = MagicMock()
        lifecycle.claim_rewards.return_value = False
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "all")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Nothing" in text or "Claim Report" in text

    @pytest.mark.asyncio
    async def test_all_chains_lifecycle_missing(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "all")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_all_chains_no_service_key_skipped(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await handle_claim_callback(update, ctx, "all")

        # Empty chain list → "Claim Report" with no items
        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_all_chains_exception_per_chain(self):
        from micromech.bot.commands.claim import handle_claim_callback

        lifecycle = MagicMock()
        lifecycle.claim_rewards.side_effect = RuntimeError("rpc error")
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "all")

        # Error included in report
        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Error" in text or "rpc error" in text

    @pytest.mark.asyncio
    async def test_single_chain_not_found(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = _make_update_with_query("base")  # not in config
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await handle_claim_callback(update, ctx, "base")

        update.callback_query.answer.assert_called()

    @pytest.mark.asyncio
    async def test_single_chain_claim_success(self):
        from micromech.bot.commands.claim import handle_claim_callback

        lifecycle = MagicMock()
        lifecycle.claim_rewards.return_value = True
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "claimed" in text.lower() or "Claimed" in text

    @pytest.mark.asyncio
    async def test_single_chain_no_lifecycle(self):
        from micromech.bot.commands.claim import handle_claim_callback

        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "not available" in text

    @pytest.mark.asyncio
    async def test_single_chain_exception(self):
        from micromech.bot.commands.claim import handle_claim_callback

        lifecycle = MagicMock()
        lifecycle.claim_rewards.side_effect = RuntimeError("chain error")
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_claim_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "failed" in text.lower() or "Claim failed" in text

    @pytest.mark.asyncio
    async def test_claim_command_no_staked_services(self):
        from micromech.bot.commands.claim import claim_command

        update = MagicMock()
        update.message = AsyncMock()
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = 1
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.core.bridge.get_service_info", return_value={}):
            await claim_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No staked" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_claim_command_single_chain(self):
        from micromech.bot.commands.claim import claim_command

        lifecycle = MagicMock()
        lifecycle.claim_rewards.return_value = True
        update = MagicMock()
        update.message = AsyncMock()
        sent_msg = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent_msg)
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = 1
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await claim_command(update, ctx)

        update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_claim_command_multi_chain_shows_keyboard(self):
        from micromech.bot.commands.claim import claim_command
        from micromech.core.config import ChainConfig
        from tests.conftest import make_test_config

        # Add a second chain so multi-chain keyboard appears
        config = make_test_config()
        from micromech.core.constants import CHAIN_DEFAULTS
        config.chains["base"] = ChainConfig(
            chain="base",
            marketplace_address=CHAIN_DEFAULTS["base"]["marketplace"],
            factory_address=CHAIN_DEFAULTS["base"]["factory"],
            staking_address=CHAIN_DEFAULTS["base"]["staking"],
        )

        update = MagicMock()
        update.message = AsyncMock()
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = 1
        ctx = _make_context(config=config)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await claim_command(update, ctx)

        update.message.reply_text.assert_called()

    def test_build_chain_keyboard(self):
        from micromech.bot.commands.claim import _build_chain_keyboard

        kb = _build_chain_keyboard({"gnosis": MagicMock(), "base": MagicMock()}, "claim")
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("All Chains" in b for b in buttons)


# ===========================================================================
# Checkpoint callbacks
# ===========================================================================

class TestCheckpointCallbacks:
    @pytest.mark.asyncio
    async def test_cancel_deletes_message(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        update = _make_update_with_query("cancel")
        ctx = _make_context()
        await handle_checkpoint_callback(update, ctx, "cancel")
        update.callback_query.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        update = MagicMock()
        update.callback_query = None
        ctx = _make_context()
        await handle_checkpoint_callback(update, ctx, "gnosis")

    @pytest.mark.asyncio
    async def test_all_chains_success(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.return_value = True
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "all")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Checkpoint called" in text

    @pytest.mark.asyncio
    async def test_all_chains_not_needed(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.return_value = False
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "all")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Not needed" in text

    @pytest.mark.asyncio
    async def test_all_chains_no_lifecycle(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "all")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Not needed" in text or "No chains" in text

    @pytest.mark.asyncio
    async def test_all_chains_exception_skipped(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.side_effect = RuntimeError("rpc")
        update = _make_update_with_query("all")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "all")

        # Chain goes to skipped list
        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Not needed" in text or "GNOSIS" in text

    @pytest.mark.asyncio
    async def test_single_chain_not_found(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        update = _make_update_with_query("base")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await handle_checkpoint_callback(update, ctx, "base")

        update.callback_query.answer.assert_called()

    @pytest.mark.asyncio
    async def test_single_chain_success(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.return_value = True
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Checkpoint called" in text

    @pytest.mark.asyncio
    async def test_single_chain_not_needed(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.return_value = False
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "not needed" in text.lower()

    @pytest.mark.asyncio
    async def test_single_chain_exception(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        lifecycle = MagicMock()
        lifecycle.checkpoint.side_effect = RuntimeError("boom")
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "failed" in text.lower()

    @pytest.mark.asyncio
    async def test_single_chain_no_lifecycle(self):
        from micromech.bot.commands.checkpoint import handle_checkpoint_callback

        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_checkpoint_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "not available" in text


# ===========================================================================
# Manage callbacks
# ===========================================================================

class TestManageCallbacks:
    @pytest.mark.asyncio
    async def test_cancel_deletes_message(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("cancel")
        ctx = _make_context()
        await handle_manage_callback(update, ctx, "cancel")
        update.callback_query.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_back_shows_chain_list(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("back")
        ctx = _make_context()
        await handle_manage_callback(update, ctx, "back")
        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = MagicMock()
        update.callback_query = None
        ctx = _make_context()
        await handle_manage_callback(update, ctx, "gnosis")

    @pytest.mark.asyncio
    async def test_chain_not_found(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("unknownchain")
        ctx = _make_context()

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await handle_manage_callback(update, ctx, "unknownchain")

        update.callback_query.answer.assert_called()

    @pytest.mark.asyncio
    async def test_chain_selected_not_deployed(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("gnosis")
        ctx = _make_context()

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await handle_manage_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Not deployed" in text

    @pytest.mark.asyncio
    async def test_chain_selected_shows_actions(self):
        from micromech.bot.commands.manage import handle_manage_callback

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "is_staked": True,
        }
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_callback(update, ctx, "gnosis")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_chain_selected_no_lifecycle(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "not available" in text

    @pytest.mark.asyncio
    async def test_unstake_shows_confirm(self):
        from micromech.bot.commands.manage import handle_manage_callback

        update = _make_update_with_query("gnosis:unstake")
        ctx = _make_context()
        await handle_manage_callback(update, ctx, "gnosis:unstake")
        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_stake_action_executes(self):
        from micromech.bot.commands.manage import handle_manage_callback

        lifecycle = MagicMock()
        lifecycle.stake.return_value = True
        update = _make_update_with_query("gnosis:stake")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_callback(update, ctx, "gnosis:stake")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_confirm_cancelled(self):
        from micromech.bot.commands.manage import handle_manage_confirm_callback

        update = _make_update_with_query("no")
        ctx = _make_context()
        await handle_manage_confirm_callback(update, ctx, "no")
        update.callback_query.answer.assert_called_with("Cancelled")

    @pytest.mark.asyncio
    async def test_confirm_yes_unstake(self):
        from micromech.bot.commands.manage import handle_manage_confirm_callback

        lifecycle = MagicMock()
        lifecycle.unstake.return_value = True
        update = _make_update_with_query("yes")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})
        ctx.user_data = {"manage_chain": "gnosis", "manage_action": "unstake"}

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_confirm_callback(update, ctx, "yes")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_confirm_yes_restake(self):
        from micromech.bot.commands.manage import handle_manage_confirm_callback

        lifecycle = MagicMock()
        lifecycle.unstake.return_value = True
        lifecycle.stake.return_value = True
        update = _make_update_with_query("yes")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})
        ctx.user_data = {"manage_chain": "gnosis", "manage_action": "restake"}

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_confirm_callback(update, ctx, "yes")

        update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_confirm_yes_restake_unstake_fails(self):
        from micromech.bot.commands.manage import handle_manage_confirm_callback

        lifecycle = MagicMock()
        lifecycle.unstake.return_value = False  # unstake fails → no stake
        update = _make_update_with_query("yes")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})
        ctx.user_data = {"manage_chain": "gnosis", "manage_action": "restake"}

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_confirm_callback(update, ctx, "yes")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "failed" in text.lower() or "Unstake failed" in text

    @pytest.mark.asyncio
    async def test_confirm_session_expired(self):
        from micromech.bot.commands.manage import handle_manage_confirm_callback

        update = _make_update_with_query("yes")
        ctx = _make_context()
        ctx.user_data = {}  # no stored chain/action
        await handle_manage_confirm_callback(update, ctx, "yes")
        update.callback_query.answer.assert_called()

    @pytest.mark.asyncio
    async def test_execute_action_no_chain_config(self):
        from micromech.bot.commands.manage import _execute_action

        query = _make_query("yes")
        config = make_test_config()

        with patch("micromech.core.bridge.get_service_info", return_value={}):
            await _execute_action(query, config, "base", "stake", {})

        query.answer.assert_called_with("Chain not configured")

    @pytest.mark.asyncio
    async def test_execute_action_exception(self):
        from micromech.bot.commands.manage import _execute_action

        lifecycle = MagicMock()
        lifecycle.stake.side_effect = RuntimeError("chain down")
        query = _make_query("yes")
        config = make_test_config()

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await _execute_action(query, config, "gnosis", "stake",
                                  {"gnosis": lifecycle})

        text = query.edit_message_text.call_args[0][0]
        assert "failed" in text.lower()

    @pytest.mark.asyncio
    async def test_chain_selected_status_fetch_fails(self):
        from micromech.bot.commands.manage import handle_manage_callback

        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError("rpc down")
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Error" in text

    @pytest.mark.asyncio
    async def test_chain_selected_status_none(self):
        from micromech.bot.commands.manage import handle_manage_callback

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = None
        update = _make_update_with_query("gnosis")
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await handle_manage_callback(update, ctx, "gnosis")

        text = update.callback_query.edit_message_text.call_args[0][0]
        assert "Could not fetch" in text
