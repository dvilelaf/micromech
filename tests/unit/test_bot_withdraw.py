"""Tests for /withdraw command handler."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS
from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1

_MECH_ADDR = "0x" + "a" * 40


def _make_config_with_mech(**kwargs) -> MicromechConfig:
    gnosis = CHAIN_DEFAULTS["gnosis"]
    return MicromechConfig(
        chains={
            "gnosis": ChainConfig(
                chain="gnosis",
                mech_address=_MECH_ADDR,
                marketplace_address=gnosis["marketplace"],
                factory_address=gnosis["factory"],
                staking_address=gnosis["staking"],
            )
        },
        **kwargs,
    )


def _make_bridge():
    bridge = MagicMock()
    bridge.wallet = MagicMock()
    bridge.wallet.safe_service = MagicMock()
    bridge.web3 = MagicMock()
    bridge.with_retry = MagicMock(side_effect=lambda fn, **kw: fn())
    return bridge


def _make_update():
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    sent_msg = AsyncMock()
    update.message = AsyncMock()
    update.message.reply_text = AsyncMock(return_value=sent_msg)
    return update, sent_msg


def _make_context(config=None, bridge=None, **extras):
    ctx = MagicMock()
    cfg = config or _make_config_with_mech()
    bridges = {"gnosis": bridge or _make_bridge()} if bridge is not False else {}
    ctx.bot_data = {"config": cfg, "bridges": bridges, **extras}
    return ctx


def _auth_patches():
    return [
        patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
        patch("micromech.bot.security._rate_limit_cache", {}),
    ]


def _make_callback_query(payload: str):
    query = AsyncMock()
    query.data = f"withdraw:{payload}"
    return query


def _make_callback_update(payload: str):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    update.callback_query = _make_callback_query(payload)
    return update


# ---------------------------------------------------------------------------
# withdraw_command
# ---------------------------------------------------------------------------


class TestWithdrawCommand:
    @pytest.mark.asyncio
    async def test_no_chains_with_mech(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, _ = _make_update()
        ctx = _make_context(config=make_test_config(), bridge=False)

        with _auth_patches()[0], _auth_patches()[1]:
            await withdraw_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_shows_balance(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_pending_balance",
                new=AsyncMock(return_value=5.0),
            ),
        ):
            await withdraw_command(update, ctx)

        update.message.reply_text.assert_called_once()
        sent_msg.edit_text.assert_called_once()
        call_kwargs = sent_msg.edit_text.call_args
        assert "5.000000 xDAI" in call_kwargs[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_no_pending(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_pending_balance",
                new=AsyncMock(return_value=0.0),
            ),
        ):
            await withdraw_command(update, ctx)

        sent_msg.edit_text.assert_called_once()
        assert "No pending" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_balance_unavailable(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_pending_balance",
                new=AsyncMock(return_value=None),
            ),
        ):
            await withdraw_command(update, ctx)

        sent_msg.edit_text.assert_called_once()
        assert "retrieve" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_multi_chain_shows_picker(self):
        from micromech.bot.commands.withdraw import withdraw_command

        gnosis = CHAIN_DEFAULTS["gnosis"]
        config = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    mech_address=_MECH_ADDR,
                    marketplace_address=gnosis["marketplace"],
                    factory_address=gnosis["factory"],
                    staking_address=gnosis["staking"],
                ),
                "base": ChainConfig(
                    chain="base",
                    mech_address=_MECH_ADDR,
                    marketplace_address=gnosis["marketplace"],
                    factory_address=gnosis["factory"],
                    staking_address=gnosis["staking"],
                ),
            }
        )
        bridges = {
            "gnosis": _make_bridge(),
            "base": _make_bridge(),
        }
        update, _ = _make_update()
        ctx = MagicMock()
        ctx.bot_data = {"config": config, "bridges": bridges}

        with _auth_patches()[0], _auth_patches()[1]:
            await withdraw_command(update, ctx)

        update.message.reply_text.assert_called_once()
        call_kwargs = update.message.reply_text.call_args
        assert "reply_markup" in call_kwargs[1]

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update = MagicMock()
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = AUTHORIZED_USER_ID
        update.message = None
        ctx = _make_context()

        with _auth_patches()[0], _auth_patches()[1]:
            await withdraw_command(update, ctx)


# ---------------------------------------------------------------------------
# handle_withdraw_callback
# ---------------------------------------------------------------------------


class TestWithdrawCallback:
    @pytest.mark.asyncio
    async def test_cancel_deletes_message(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("cancel")
        ctx = _make_context()

        await handle_withdraw_callback(update, ctx, "cancel")

        update.callback_query.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_chain_picker_shows_balance(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("gnosis")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._get_pending_balance",
            new=AsyncMock(return_value=2.5),
        ):
            await handle_withdraw_callback(update, ctx, "gnosis")

        update.callback_query.edit_message_text.assert_called()
        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "2.500000 xDAI" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_chain_picker_no_balance(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("gnosis")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._get_pending_balance",
            new=AsyncMock(return_value=0.0),
        ):
            await handle_withdraw_callback(update, ctx, "gnosis")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "No pending" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_chain_picker_balance_none(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("gnosis")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._get_pending_balance",
            new=AsyncMock(return_value=None),
        ):
            await handle_withdraw_callback(update, ctx, "gnosis")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "retrieve" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_chain_picker_unknown_chain(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("unknown")
        ctx = _make_context()

        await handle_withdraw_callback(update, ctx, "unknown")

        update.callback_query.answer.assert_called_with("Chain not found")

    @pytest.mark.asyncio
    async def test_confirm_success(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("confirm:gnosis")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._run_withdraw",
            new=AsyncMock(return_value=(True, "*GNOSIS*: Withdrawn `1.0 xDAI` to master")),
        ):
            await handle_withdraw_callback(update, ctx, "confirm:gnosis")

        update.callback_query.edit_message_text.assert_called()
        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "Withdrawn" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_confirm_inflight_rejected(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("confirm:gnosis")
        ctx = _make_context()
        ctx.bot_data["withdraw_inflight"] = {"gnosis"}

        await handle_withdraw_callback(update, ctx, "confirm:gnosis")

        update.callback_query.answer.assert_called_with("Withdrawal already in progress")

    @pytest.mark.asyncio
    async def test_confirm_exception_shows_error(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("confirm:gnosis")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._run_withdraw",
            new=AsyncMock(side_effect=RuntimeError("rpc down")),
        ):
            await handle_withdraw_callback(update, ctx, "confirm:gnosis")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "check logs" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_confirm_unknown_chain(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("confirm:unknown")
        ctx = _make_context()

        await handle_withdraw_callback(update, ctx, "confirm:unknown")

        update.callback_query.answer.assert_called_with("Chain not found")

    @pytest.mark.asyncio
    async def test_all_chains_success(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("all")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._run_withdraw",
            new=AsyncMock(return_value=(True, "*GNOSIS*: Withdrawn `5.0 xDAI` to master")),
        ):
            await handle_withdraw_callback(update, ctx, "all")

        update.callback_query.edit_message_text.assert_called()
        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "Withdrawn" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_all_chains_inflight_skipped(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("all")
        ctx = _make_context()
        ctx.bot_data["withdraw_inflight"] = {"gnosis"}

        await handle_withdraw_callback(update, ctx, "all")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "Already in progress" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_all_chains_exception(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("all")
        ctx = _make_context()

        with patch(
            "micromech.bot.commands.withdraw._run_withdraw",
            new=AsyncMock(side_effect=RuntimeError("fail")),
        ):
            await handle_withdraw_callback(update, ctx, "all")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "check logs" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_no_query_returns_early(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = MagicMock()
        update.callback_query = None
        ctx = _make_context()

        # Should not raise
        await handle_withdraw_callback(update, ctx, "cancel")


# ---------------------------------------------------------------------------
# _get_pending_balance
# ---------------------------------------------------------------------------


class TestGetPendingBalance:
    @pytest.mark.asyncio
    async def test_returns_balance(self):
        from micromech.bot.commands.withdraw import _get_pending_balance

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with (
            patch(
                "micromech.core.marketplace.get_balance_tracker_address",
                return_value="0x" + "b" * 40,
            ),
            patch(
                "micromech.core.marketplace.get_pending_balance",
                return_value=3.0,
            ),
        ):
            result = await _get_pending_balance(bridge, "gnosis", chain_config)

        assert result == 3.0

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        from micromech.bot.commands.withdraw import _get_pending_balance

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.core.marketplace.get_balance_tracker_address",
            side_effect=RuntimeError("rpc error"),
        ):
            result = await _get_pending_balance(bridge, "gnosis", chain_config)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_bt_address(self):
        from micromech.bot.commands.withdraw import _get_pending_balance

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.core.marketplace.get_balance_tracker_address",
            return_value=None,
        ):
            result = await _get_pending_balance(bridge, "gnosis", chain_config)

        assert result is None


# ---------------------------------------------------------------------------
# _run_withdraw
# ---------------------------------------------------------------------------


class TestRunWithdraw:
    @pytest.mark.asyncio
    async def test_success(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        bridge.web3.eth.get_balance.return_value = int(5e18)
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "c" * 40},
            ),
            patch(
                "micromech.core.marketplace.get_balance_tracker_address",
                return_value="0x" + "b" * 40,
            ),
            patch("micromech.tasks.payment_withdraw._withdraw"),
            patch("micromech.tasks.payment_withdraw._drain_mech_to_safe"),
            patch("micromech.tasks.payment_withdraw._transfer_to_master"),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "5.000000 xDAI" in msg

    @pytest.mark.asyncio
    async def test_no_multisig(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.core.bridge.get_service_info",
            return_value={},
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is False
        assert "No multisig" in msg

    @pytest.mark.asyncio
    async def test_no_bt_address(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "c" * 40},
            ),
            patch(
                "micromech.core.marketplace.get_balance_tracker_address",
                return_value=None,
            ),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is False
        assert "balance tracker" in msg

    @pytest.mark.asyncio
    async def test_transfer_to_master_fails_returns_partial_success(self):
        """If step 3 (_transfer_to_master) fails, funds are in Safe → return True with warning."""
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        bridge.web3.eth.get_balance.return_value = int(3e18)
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "c" * 40},
            ),
            patch(
                "micromech.core.marketplace.get_balance_tracker_address",
                return_value="0x" + "b" * 40,
            ),
            patch("micromech.tasks.payment_withdraw._withdraw"),
            patch("micromech.tasks.payment_withdraw._drain_mech_to_safe"),
            patch(
                "micromech.tasks.payment_withdraw._transfer_to_master",
                side_effect=RuntimeError("Safe tx failed"),
            ),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "drained to Safe" in msg
        assert "transfer to master failed" in msg
