"""Tests for MarkdownV2 bot command handlers: status, wallet, contracts, last_rewards, info."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42


def _make_update(message=True):
    update = MagicMock()
    if message:
        update.message = AsyncMock()
        sent = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent)
    else:
        update.message = None
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = 1
    return update


def _make_context(**extras):
    ctx = MagicMock()
    ctx.bot_data = {"config": make_test_config(), **extras}
    return ctx


def _sent_msg():
    """Return an AsyncMock that simulates a sent Telegram message."""
    msg = AsyncMock()
    msg.edit_text = AsyncMock()
    return msg


# ===========================================================================
# status.py helpers
# ===========================================================================


class TestRequestEmoji:
    def test_done(self):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(10, 10) == "✅"

    def test_above_required(self):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(12, 10) == "✅"

    def test_in_progress(self):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(3, 10) == "🔄"

    def test_idle(self):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(0, 10) == "❌"

    def test_no_required(self):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(0, 0) == "❌"


class TestFormatEpochTimer:
    def test_positive_remaining(self):
        from micromech.bot.formatting import format_epoch_countdown as _format_epoch_timer

        result = _format_epoch_timer(5, None, 7200)
        assert "Epoch 5" in result
        assert "2h" in result

    def test_negative_remaining(self):
        from micromech.bot.formatting import format_epoch_countdown as _format_epoch_timer

        result = _format_epoch_timer(5, None, -3600)
        assert "ended" in result
        assert "⚠️" in result

    def test_with_epoch_end_utc(self):
        from datetime import datetime, timedelta, timezone

        from micromech.bot.formatting import format_epoch_countdown as _format_epoch_timer

        future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        result = _format_epoch_timer(1, future, 0)
        assert "ends in" in result

    def test_epoch_end_utc_invalid(self):
        from micromech.bot.formatting import format_epoch_countdown as _format_epoch_timer

        # Falls back to remaining_seconds
        result = _format_epoch_timer(1, "bad-date", 3600)
        assert "1h" in result


class TestFormatChainStatus:
    def test_basic_fields(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 10,
            "required_requests": 10,
            "staking_state": "STAKED",
            "rewards": 5.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=None)
        assert "GNOSIS" in result
        assert "STAKED" in result
        assert "5.00 OLAS" in result
        assert "Epoch deliveries" in result

    def test_with_olas_price(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 5,
            "required_requests": 10,
            "staking_state": "STAKED",
            "rewards": 10.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=1.5)
        assert "€15" in result  # escaped as €15\.00 in MarkdownV2

    def test_agent_and_safe_balances(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "staking_state": "STAKED",
            "rewards": 0.0,
            "agent_balance_native": 1.0,
            "agent_balance_olas": 2.0,
            "safe_balance_native": 3.0,
            "safe_balance_olas": 4.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=None)
        assert "Agent" in result
        assert "Safe" in result

    def test_contract_balance(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
            "contract_balance": 100.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=None)
        assert "Contract balance" in result

    def test_contract_name_shown(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
            "staking_contract_name": "MyContract",
        }
        result = _format_chain_status("gnosis", status, olas_price=None)
        assert "MyContract" in result

    def test_pending_payment_shown(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=None, pending_payment=1.5)
        assert "Pending payment" in result
        assert "1.50 xDAI" in result

    def test_pending_payment_absent_when_none(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
        }
        result = _format_chain_status("gnosis", status, olas_price=None, pending_payment=None)
        assert "Pending payment" not in result

    def test_master_balances_shown(self):
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
        }
        result = _format_chain_status(
            "gnosis", status, olas_price=None, master_balances=(10.5, 200.0)
        )
        assert "Master" in result
        assert "10.50 xDAI" in result
        assert "200.00 OLAS" in result

    def test_field_order(self):
        """Verify field ordering: ID, Pending, Rewards, Epoch deliveries, Master, Agent, Safe, Contract, State, Contract balance."""
        from micromech.bot.commands.status import _format_chain_status

        status = {
            "service_id": 42,
            "requests_this_epoch": 1999,
            "required_requests": 21,
            "staking_state": "STAKED",
            "staking_contract_name": "Pearl Beta 2",
            "rewards": 5.0,
            "agent_balance_native": 0.15,
            "agent_balance_olas": 0.0,
            "safe_balance_native": 1.23,
            "safe_balance_olas": 0.0,
            "contract_balance": 10000.0,
        }
        result = _format_chain_status(
            "gnosis",
            status,
            olas_price=None,
            pending_payment=0.123,
            master_balances=(5.0, 100.0),
        )
        lines = result.split("\n")
        labels = [line.split(":")[0].strip() for line in lines if ":" in line]
        assert labels.index("Pending payment") < labels.index("Rewards")
        assert labels.index("Rewards") < labels.index("Epoch deliveries")
        assert labels.index("Master") < labels.index("Agent")
        assert labels.index("Safe") < labels.index("Contract")
        assert labels.index("State") < labels.index("Contract balance")


class TestStatusCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns(self):
        from micromech.bot.commands.status import status_command

        update = _make_update(message=False)
        ctx = _make_context()
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await status_command(update, ctx)

    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.core.config import MicromechConfig

        from micromech.bot.commands.status import status_command

        update = _make_update()
        ctx = _make_context(config=MicromechConfig(chains={}))
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await status_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_not_deployed(self):
        from micromech.bot.commands.status import status_command

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=None),
        ):
            await status_command(update, ctx)

        sent.edit_text.assert_called()

    @pytest.mark.asyncio
    async def test_lifecycle_not_available(self):
        from micromech.bot.commands.status import status_command

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=None),
        ):
            await status_command(update, ctx)

        sent.edit_text.assert_called()
        args = sent.edit_text.call_args[0][0]
        assert "not available" in args

    @pytest.mark.asyncio
    async def test_status_success(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 5.0,
            "requests_this_epoch": 10,
            "required_requests": 10,
            "epoch_number": 3,
            "epoch_end_utc": None,
            "remaining_epoch_seconds": 3600,
        }
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=1.5),
        ):
            await status_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "GNOSIS" in text

    @pytest.mark.asyncio
    async def test_status_with_pending_and_master(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 5.0,
            "requests_this_epoch": 1999,
            "required_requests": 21,
            "epoch_number": 3,
            "epoch_end_utc": None,
            "remaining_epoch_seconds": 3600,
        }
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=1.5),
            patch(
                "micromech.bot.commands.status._fetch_pending_payments",
                return_value={"gnosis": 1.234567},
            ),
            patch(
                "micromech.bot.commands.status.check_balances",
                return_value=(10.5, 200.0),
            ),
        ):
            await status_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "Pending payment" in text
        assert "1.23 xDAI" in text
        assert "Master" in text
        assert "10.50 xDAI" in text
        assert "200.00 OLAS" in text
        assert "Epoch deliveries" in text
        assert "1999/21" in text

    @pytest.mark.asyncio
    async def test_status_exception(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError("rpc error")
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=None),
        ):
            await status_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "Error" in text

    @pytest.mark.asyncio
    async def test_uptime_footer(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 0.0,
            "requests_this_epoch": 0,
            "required_requests": 10,
        }
        metrics = MagicMock()
        metrics.uptime_seconds = 7200
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle}, metrics=metrics)

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.status.get_olas_price_eur", return_value=None),
        ):
            await status_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "Uptime" in text


# ===========================================================================
# wallet.py
# ===========================================================================


class TestWalletCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns(self):
        from micromech.bot.commands.wallet import wallet_command

        update = _make_update(message=False)
        ctx = _make_context()
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await wallet_command(update, ctx)

    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.core.config import MicromechConfig

        from micromech.bot.commands.wallet import wallet_command

        update = _make_update()
        ctx = _make_context(config=MicromechConfig(chains={}))
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await wallet_command(update, ctx)

        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_wallet_success(self):
        from micromech.bot.commands.wallet import wallet_command

        wallet_mock = MagicMock()
        wallet_mock.master_account.address = "0x" + "a" * 40
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context()

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=wallet_mock),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(1.0, 2.0)),
            patch("micromech.bot.commands.wallet.check_address_balances", return_value=(0.5, 1.0)),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={
                    "multisig_address": "0x" + "b" * 40,
                    "agent_address": "0x" + "c" * 40,
                    "service_id": 42,
                },
            ),
        ):
            await wallet_command(update, ctx)

        sent.edit_text.assert_called()

    @pytest.mark.asyncio
    async def test_wallet_master_fails(self):
        from micromech.bot.commands.wallet import wallet_command

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context()

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch(
                "micromech.bot.commands.wallet.get_wallet", side_effect=RuntimeError("no wallet")
            ),
            patch(
                "micromech.bot.commands.wallet.check_address_balances", return_value=(None, None)
            ),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await wallet_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "Wallet" in text

    @pytest.mark.asyncio
    async def test_wallet_no_multisig_not_deployed(self):
        from micromech.bot.commands.wallet import wallet_command

        wallet_mock = MagicMock()
        wallet_mock.master_account.address = "0x" + "a" * 40
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context()

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=wallet_mock),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(1.0, 0.0)),
            patch(
                "micromech.bot.commands.wallet.check_address_balances", return_value=(None, None)
            ),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await wallet_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "Not deployed" in text


# ===========================================================================
# contracts.py
# ===========================================================================


class TestContractsCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns(self):
        from micromech.bot.commands.contracts import contracts_command

        update = _make_update(message=False)
        ctx = _make_context()
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await contracts_command(update, ctx)

    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.core.config import MicromechConfig

        from micromech.bot.commands.contracts import contracts_command

        update = _make_update()
        ctx = _make_context(config=MicromechConfig(chains={}))
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await contracts_command(update, ctx)

        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_staking_address(self):
        from micromech.bot.commands.contracts import contracts_command

        # Use a mock chain config where staking_address is None/empty
        chain_cfg = MagicMock()
        chain_cfg.staking_address = None
        config = MagicMock()
        config.enabled_chains = {"gnosis": chain_cfg}

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = MagicMock()
        ctx.bot_data = {"config": config}

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await contracts_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "No staking contract" in text

    @pytest.mark.asyncio
    async def test_contract_fetch_success(self):
        from micromech.bot.commands.contracts import contracts_command

        staking_address = "0x" + "a" * 40
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context()

        fake_epoch = MagicMock()
        from datetime import datetime, timedelta, timezone

        fake_epoch.get_next_epoch_start.return_value = datetime.now(timezone.utc) + timedelta(
            hours=3
        )
        fake_epoch.balance = 1000 * 1e18
        fake_epoch.min_staking_deposit = 100 * 1e18
        fake_epoch.max_num_services = 50
        fake_epoch.name = "TestContract"

        def _make_staking(*args, **kwargs):
            c = MagicMock()
            c.get_service_ids.return_value = list(range(10))
            c.max_num_services = 50
            c.balance = 1000 * 1e18
            c.min_staking_deposit = 100 * 1e18
            c.get_next_epoch_start.return_value = datetime.now(timezone.utc) + timedelta(hours=3)
            c.name = "TestContract"
            return c

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", _make_staking),
        ):
            await contracts_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "GNOSIS" in text

    @pytest.mark.asyncio
    async def test_contract_fetch_error(self):
        from micromech.bot.commands.contracts import contracts_command

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context()

        def _bad_staking(*args, **kwargs):
            raise RuntimeError("RPC down")

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("iwa.plugins.olas.contracts.staking.StakingContract", _bad_staking),
        ):
            await contracts_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "Error" in text


class TestEpochCountdown:
    def test_positive(self):
        from datetime import datetime, timedelta, timezone

        from micromech.bot.commands.contracts import _epoch_countdown

        future = datetime.now(timezone.utc) + timedelta(hours=5)
        result = _epoch_countdown(future)
        # Countdown truncates seconds so we get "4h 59m" — check no warning
        assert "h" in result
        assert "m" in result
        assert "⚠️" not in result
        assert "-" not in result

    def test_negative(self):
        from datetime import datetime, timedelta, timezone

        from micromech.bot.commands.contracts import _epoch_countdown

        past = datetime.now(timezone.utc) - timedelta(hours=1)
        result = _epoch_countdown(past)
        assert "⚠️" in result
        assert "-" in result


# ===========================================================================
# last_rewards.py
# ===========================================================================


class TestLastRewardsCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update(message=False)
        ctx = _make_context()
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await last_rewards_command(update, ctx)

    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.core.config import MicromechConfig

        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update()
        ctx = _make_context(config=MicromechConfig(chains={}))
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await last_rewards_command(update, ctx)

        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_not_deployed(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={}),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        sent.edit_text.assert_called()

    @pytest.mark.asyncio
    async def test_rewards_with_eur(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "rewards": 10.0,
            "requests_this_epoch": 10,
            "required_requests": 10,
            "staking_state": "STAKED",
            "staking_contract_name": "MyContract",
            "epoch_number": 5,
            "epoch_end_utc": None,
        }
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=2.0),
        ):
            await last_rewards_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "10.00 OLAS" in text
        assert "€20" in text  # dot is escaped as \. in MarkdownV2
        assert "Epoch 5" in text

    @pytest.mark.asyncio
    async def test_needs_more_deliveries(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "rewards": 0.0,
            "requests_this_epoch": 3,
            "required_requests": 10,
            "staking_state": "STAKED",
            "epoch_number": 0,
            "epoch_end_utc": None,
        }
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "7" in text  # 10 - 3 = 7 remaining

    @pytest.mark.asyncio
    async def test_error_handling(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError("rpc error")
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        sent.edit_text.assert_called()
        text = sent.edit_text.call_args[0][0]
        assert "Error" in text

    @pytest.mark.asyncio
    async def test_epoch_end_utc_future(self):
        from datetime import datetime, timedelta, timezone

        from micromech.bot.commands.last_rewards import last_rewards_command

        future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "rewards": 5.0,
            "requests_this_epoch": 10,
            "required_requests": 10,
            "staking_state": "STAKED",
            "epoch_number": 3,
            "epoch_end_utc": future,
        }
        update = _make_update()
        sent = _sent_msg()
        update.message.reply_text = AsyncMock(return_value=sent)
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.core.bridge.get_service_info", return_value={"service_key": "k"}),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "ends in" in text


# ===========================================================================
# info.py
# ===========================================================================


class TestInfoCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns(self):
        from micromech.bot.commands.info import info_command

        update = _make_update(message=False)
        ctx = _make_context()
        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await info_command(update, ctx)

    @pytest.mark.asyncio
    async def test_basic_info(self):
        from micromech.bot.commands.info import info_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await info_command(update, ctx)

        update.message.reply_text.assert_called_once()
        text = update.message.reply_text.call_args[0][0]
        assert "Micromech Info" in text
        assert "Version" in text
        assert "GNOSIS" in text

    @pytest.mark.asyncio
    async def test_with_metrics(self):
        from micromech.bot.commands.info import info_command

        metrics = MagicMock()
        metrics.uptime_seconds = 3660  # 1h 1m
        update = _make_update()
        ctx = _make_context(metrics=metrics)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await info_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "Uptime" in text
        assert "1h" in text

    @pytest.mark.asyncio
    async def test_with_queue(self):
        from micromech.bot.commands.info import info_command

        queue = MagicMock()
        queue.count_by_status.return_value = {"delivered": 50, "failed": 2, "pending": 1}
        update = _make_update()
        ctx = _make_context(queue=queue)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await info_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "Queue" in text
        assert "53 total" in text

    @pytest.mark.asyncio
    async def test_package_not_found(self):
        from micromech.bot.commands.info import info_command

        update = _make_update()
        ctx = _make_context()

        import importlib.metadata

        def _raise(name):
            raise importlib.metadata.PackageNotFoundError(name)

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("importlib.metadata.version", side_effect=_raise),
        ):
            await info_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "unknown" in text
