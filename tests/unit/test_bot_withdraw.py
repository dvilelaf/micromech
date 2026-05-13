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


def _make_preview(pending, mech_wei, safe_wei=0):
    from micromech.tasks.payment_withdraw import PaymentWithdrawPreview

    return PaymentWithdrawPreview(
        chain_name="gnosis",
        multisig_address="0x" + "c" * 40,
        pending_xdai=pending,
        mech_balance_wei=mech_wei,
        safe_excess_wei=safe_wei,
        balance_tracker_address="0x" + "b" * 40,
    )


def _make_withdraw_result(
    status="withdrawn",
    mech_wei=0,
    transferred_wei=0,
    error=None,
    attempted_wei=None,
):
    from micromech.tasks.payment_withdraw import PaymentWithdrawResult

    return PaymentWithdrawResult(
        chain_name="gnosis",
        status=status,
        mech_withdrawn_wei=mech_wei,
        transferred_to_master_wei=transferred_wei,
        attempted_transfer_to_master_wei=(
            transferred_wei if attempted_wei is None else attempted_wei
        ),
        multisig_address="0x" + "c" * 40,
        transfer_error=error,
    )


class _AsyncLockProbe:
    def __init__(self):
        self.entered = False
        self.exited = False
        self.active = False

    async def __aenter__(self):
        self.entered = True
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        self.exited = True
        return False


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
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(5.0, 0)),
            ),
        ):
            await withdraw_command(update, ctx)

        update.message.reply_text.assert_called_once()
        sent_msg.edit_text.assert_called_once()
        call_kwargs = sent_msg.edit_text.call_args
        assert "Pending: `5.000000 xDAI`" in call_kwargs[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_no_pending(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(0.0, 0, 0)),
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
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(None, None, 0)),
            ),
        ):
            await withdraw_command(update, ctx)

        sent_msg.edit_text.assert_called_once()
        assert "retrieve" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_shows_stranded_mech_balance(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(0.0, int(31.44e18))),
            ),
        ):
            await withdraw_command(update, ctx)

        text = sent_msg.edit_text.call_args[0][0]
        assert "Mech: `31.440000 xDAI`" in text
        assert "Withdraw to master" in text

    @pytest.mark.asyncio
    async def test_single_chain_shows_stranded_safe_balance(self):
        from micromech.bot.commands.withdraw import withdraw_command

        update, sent_msg = _make_update()
        ctx = _make_context()

        with (
            _auth_patches()[0],
            _auth_patches()[1],
            patch(
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(0.0, 0, int(33.42e18))),
            ),
        ):
            await withdraw_command(update, ctx)

        text = sent_msg.edit_text.call_args[0][0]
        assert "Safe excess: `33.420000 xDAI`" in text
        assert "Withdraw to master" in text

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
            "micromech.bot.commands.withdraw._get_withdraw_preview",
            new=AsyncMock(return_value=_make_preview(2.5, 0)),
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

        with (
            patch(
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(0.0, 0, 0)),
            ),
        ):
            await handle_withdraw_callback(update, ctx, "gnosis")

        last_call = update.callback_query.edit_message_text.call_args_list[-1]
        assert "No pending" in last_call[0][0]

    @pytest.mark.asyncio
    async def test_chain_picker_balance_none(self):
        from micromech.bot.commands.withdraw import handle_withdraw_callback

        update = _make_callback_update("gnosis")
        ctx = _make_context()

        with (
            patch(
                "micromech.bot.commands.withdraw._get_withdraw_preview",
                new=AsyncMock(return_value=_make_preview(None, None, 0)),
            ),
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
# _run_withdraw
# ---------------------------------------------------------------------------


class TestRunWithdraw:
    @pytest.mark.asyncio
    async def test_success(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(return_value=_make_withdraw_result("withdrawn", int(5e18), int(5e18))),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "5.000000 xDAI" in msg

    @pytest.mark.asyncio
    async def test_no_funds(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(return_value=_make_withdraw_result("no_funds")),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "No pending" in msg

    @pytest.mark.asyncio
    async def test_executor_error_returns_user_error(self):
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(side_effect=RuntimeError("No multisig_address found")),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is False
        assert "Error" in msg

    @pytest.mark.asyncio
    async def test_transfer_to_master_fails_returns_partial_success(self):
        """If Safe→master fails, the bot reports the partial success."""
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]
        error = RuntimeError("Safe tx failed: https://rpc.example/v3/secret-api-token")

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(
                return_value=_make_withdraw_result(
                    "transfer_failed",
                    int(3e18),
                    0,
                    error,
                    attempted_wei=int(3e18),
                )
            ),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "drained to Safe" in msg
        assert "3.000000 xDAI" in msg
        assert "could not be transferred to master" in msg
        assert "Safe tx failed" in msg
        assert "secret-api-token" not in msg
        assert "[REDACTED]" in msg

    @pytest.mark.asyncio
    async def test_safe_only_transfer_failure_reports_safe_amount(self):
        """If Safe-only sweep fails, the bot reports the Safe amount, not 0 drained."""
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]
        error = RuntimeError("Safe tx failed")

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(
                return_value=_make_withdraw_result(
                    "transfer_failed",
                    0,
                    0,
                    error,
                    attempted_wei=int(33.42e18),
                )
            ),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config)

        assert ok is True
        assert "Safe sweep failed" in msg
        assert "33.420000 xDAI" in msg
        assert "remains in Safe" in msg
        assert "0.000000 xDAI drained" not in msg

    @pytest.mark.asyncio
    async def test_withdraw_sweeps_stranded_safe_balance_without_pending(self):
        """Manual /withdraw retries xDAI already stranded in the Safe."""
        from micromech.bot.commands.withdraw import _run_withdraw

        bridge = _make_bridge()
        chain_config = _make_config_with_mech().enabled_chains["gnosis"]

        with patch(
            "micromech.tasks.payment_withdraw.execute_payment_withdraw",
            new=AsyncMock(return_value=_make_withdraw_result("swept_safe", 0, int(33.42e18))),
        ):
            ok, msg = await _run_withdraw(bridge, "gnosis", chain_config, safe_reserve_xdai=0.5)

        assert ok is True
        assert "Swept" in msg
        assert "33.420000 xDAI" in msg
