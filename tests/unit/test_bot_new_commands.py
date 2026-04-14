"""Tests for contracts, last_rewards, and schedule bot commands.

Strategy
--------
Each command is wrapped with @authorized_only and @rate_limited.
We bypass both by:
  - patching micromech.bot.security.secrets so telegram_chat_id matches the
    fake update's chat id
  - patching micromech.bot.security._rate_limit_cache to an empty dict so
    the first call per test is never rate-limited

asyncio.to_thread is patched to call the inner function synchronously so we
can control what StakingContract / lifecycle return without touching the
network.
"""

import contextlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _make_update(has_message=True):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    if has_message:
        sent_msg = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent_msg)
    else:
        update.message = None
    return update


def _make_context(config=None, lifecycles=None, **extra_bot_data):
    ctx = MagicMock()
    cfg = config or make_test_config()
    ctx.bot_data = {"config": cfg, "lifecycles": lifecycles or {}, **extra_bot_data}
    return ctx


@contextlib.contextmanager
def _auth():
    """Context manager that bypasses @authorized_only and @rate_limited."""
    with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
         patch("micromech.bot.security._rate_limit_cache", {}):
        yield


def _no_chains_config():
    """Config with no enabled chains."""
    cfg = make_test_config()
    cfg.chains = {}
    return cfg


# ---------------------------------------------------------------------------
# /contracts
# ---------------------------------------------------------------------------


class TestContractsCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.contracts import contracts_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with _auth():
            await contracts_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_no_chains_enabled(self):
        from micromech.bot.commands.contracts import contracts_command

        update = _make_update()
        ctx = _make_context(config=_no_chains_config())

        with _auth():
            await contracts_command(update, ctx)

        update.message.reply_text.assert_called_once()
        call_text = update.message.reply_text.call_args[0][0]
        assert "No chains" in call_text

    @pytest.mark.asyncio
    async def test_no_staking_address(self):
        """Chain without staking_address → shows 'No staking contract configured'."""
        from micromech.bot.commands.contracts import contracts_command

        # staking_address is a validated Ethereum address so it can't be empty string.
        # We simulate the "no staking address" branch by mocking enabled_chains with
        # a fake chain config that has falsy staking_address.
        fake_chain = MagicMock()
        fake_chain.staking_address = None

        cfg = make_test_config()
        update = _make_update()
        ctx = _make_context(config=cfg)

        with _auth(), patch.object(type(cfg), "enabled_chains", new_callable=PropertyMock, return_value={"gnosis": fake_chain}):
            await contracts_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        edited_text = sent_msg.edit_text.call_args[0][0]
        assert "No staking contract configured" in edited_text

    @pytest.mark.asyncio
    async def test_chain_success(self):
        """Happy path: to_thread returns valid contract data."""
        from micromech.bot.commands.contracts import contracts_command

        fetch_result = (3, 10, 5000.0, 100.0, "2h 0m")

        update = _make_update()
        ctx = _make_context()

        async def fake_to_thread(fn, *args, **kwargs):
            return fetch_result

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await contracts_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "GNOSIS" in text
        assert "Slots" in text

    @pytest.mark.asyncio
    async def test_chain_error(self):
        """If to_thread raises, the error is shown gracefully."""
        from micromech.bot.commands.contracts import contracts_command

        async def fake_to_thread(fn, *args, **kwargs):
            raise RuntimeError("rpc boom")

        update = _make_update()
        ctx = _make_context()

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await contracts_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "Error" in text or "rpc boom" in text

    def test_format_epoch_countdown_future(self):
        from micromech.bot.commands.contracts import _format_epoch_countdown

        future = datetime.now(timezone.utc) + timedelta(hours=3, minutes=30)
        result = _format_epoch_countdown(future)
        assert "h" in result
        assert "m" in result
        assert "overdue" not in result

    def test_format_epoch_countdown_overdue(self):
        from micromech.bot.commands.contracts import _format_epoch_countdown

        past = datetime.now(timezone.utc) - timedelta(hours=1, minutes=15)
        result = _format_epoch_countdown(past)
        assert "overdue" in result

    def test_explorer_link_known_chain(self):
        from micromech.bot.commands.contracts import _explorer_link

        link = _explorer_link("gnosis", "0xABCDef", "short")
        assert "gnosisscan.io" in link
        assert "0xABCDef" in link

    def test_explorer_link_unknown_chain_falls_back(self):
        from micromech.bot.commands.contracts import _explorer_link

        link = _explorer_link("polygon", "0x1234", "lbl")
        # falls back to gnosis explorer
        assert "gnosisscan.io" in link


# ---------------------------------------------------------------------------
# /last_rewards
# ---------------------------------------------------------------------------


class TestLastRewardsCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with _auth():
            await last_rewards_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_no_chains_enabled(self):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update()
        ctx = _make_context(config=_no_chains_config())

        with _auth():
            await last_rewards_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_service_key(self):
        """Chain where get_service_info returns no service_key → 'Not deployed'."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update()
        ctx = _make_context(lifecycles={})

        with _auth(), patch("micromech.core.bridge.get_service_info", return_value={}):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        assert "Not deployed" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_lifecycle_not_available(self):
        """service_key present but no lifecycle for chain → 'Lifecycle not available'."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        update = _make_update()
        ctx = _make_context(lifecycles={})  # gnosis key absent

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        assert "Lifecycle not available" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_chain_not_staked(self):
        """Lifecycle returns status with no rewards and low requests."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        status = {
            "staking_state": "NOT_STAKED",
            "rewards": 0,
            "requests_this_epoch": 0,
            "required_requests": 10,
        }

        async def fake_to_thread(fn, *args, **kwargs):
            return status

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "GNOSIS" in text
        assert "NOT_STAKED" in text

    @pytest.mark.asyncio
    async def test_chain_with_rewards_on_track(self):
        """Lifecycle returns rewards > 0 and requests >= required → on track."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        status = {
            "staking_state": "STAKED",
            "rewards": 1_500_000_000_000_000_000,  # 1.5e18 wei
            "requests_this_epoch": 10,
            "required_requests": 10,
        }

        async def fake_to_thread(fn, *args, **kwargs):
            return status

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "On track" in text

    @pytest.mark.asyncio
    async def test_chain_with_rewards_not_on_track(self):
        """Lifecycle returns requests < required → 'Needs N more deliveries'."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        status = {
            "staking_state": "STAKED",
            "rewards": 0,
            "requests_this_epoch": 3,
            "required_requests": 10,
        }

        async def fake_to_thread(fn, *args, **kwargs):
            return status

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Needs" in text
        assert "7" in text  # 10 - 3 = 7

    @pytest.mark.asyncio
    async def test_chain_status_none(self):
        """to_thread returns None → 'Could not fetch status'."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()

        async def fake_to_thread(fn, *args, **kwargs):
            return None

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Could not fetch status" in text

    @pytest.mark.asyncio
    async def test_chain_error(self):
        """to_thread raises → error block in message."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()

        async def fake_to_thread(fn, *args, **kwargs):
            raise RuntimeError("chain gone")

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with _auth(), patch(
            "micromech.core.bridge.get_service_info",
            return_value={"service_key": "0xkey"},
        ), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await last_rewards_command(update, ctx)  # must not raise

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Error" in text or "chain gone" in text


# ---------------------------------------------------------------------------
# /schedule
# ---------------------------------------------------------------------------


class TestScheduleCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.schedule import schedule_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with _auth():
            await schedule_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_no_chains_enabled(self):
        from micromech.bot.commands.schedule import schedule_command

        update = _make_update()
        ctx = _make_context(config=_no_chains_config())

        with _auth():
            await schedule_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_staking_address_shows_no_contracts(self):
        """All chains lack staking_address → 'No staking contracts configured'."""
        from micromech.bot.commands.schedule import schedule_command

        # staking_address is a validated Ethereum address so it can't be empty string.
        # We simulate the "no staking address" branch by mocking enabled_chains with
        # a fake chain config that has falsy staking_address.
        fake_chain = MagicMock()
        fake_chain.staking_address = None

        cfg = make_test_config()
        update = _make_update()
        ctx = _make_context(config=cfg)

        with _auth(), patch.object(type(cfg), "enabled_chains", new_callable=PropertyMock, return_value={"gnosis": fake_chain}):
            await schedule_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        assert "No staking contracts" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_epoch_in_future(self):
        """to_thread returns epoch in future → countdown shows 'in Xh Ym'."""
        from micromech.bot.commands.schedule import schedule_command

        future_epoch = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

        async def fake_to_thread(fn, *args, **kwargs):
            return future_epoch

        update = _make_update()
        ctx = _make_context()

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await schedule_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "GNOSIS" in text
        assert "in " in text

    @pytest.mark.asyncio
    async def test_epoch_in_past(self):
        """to_thread returns epoch in past → countdown shows 'overdue'."""
        from micromech.bot.commands.schedule import schedule_command

        past_epoch = datetime.now(timezone.utc) - timedelta(hours=1, minutes=20)

        async def fake_to_thread(fn, *args, **kwargs):
            return past_epoch

        update = _make_update()
        ctx = _make_context()

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await schedule_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "overdue" in text

    @pytest.mark.asyncio
    async def test_chain_error(self):
        """to_thread raises → error entry with datetime.max sentinel."""
        from micromech.bot.commands.schedule import schedule_command

        async def fake_to_thread(fn, *args, **kwargs):
            raise ConnectionError("node down")

        update = _make_update()
        ctx = _make_context()

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await schedule_command(update, ctx)  # must not raise

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        text = sent_msg.edit_text.call_args[0][0]
        assert "Error" in text or "node down" in text

    @pytest.mark.asyncio
    async def test_multiple_chains_both_appear(self):
        """Multiple chains each appear in output."""
        from micromech.bot.commands.schedule import schedule_command
        from micromech.core.config import ChainConfig, MicromechConfig

        now = datetime.now(timezone.utc)
        epochs = [now + timedelta(hours=1), now + timedelta(hours=3)]
        idx = iter(epochs)

        async def fake_to_thread(fn, *args, **kwargs):
            return next(idx)

        cfg = MicromechConfig(
            chains={
                "base": ChainConfig(
                    chain="base",
                    marketplace_address="0x9c7d6D8E5B8b3b75F1a1Bd9e8E8D8d8B8b8B8b8B",
                    factory_address="0x9c7d6D8E5B8b3b75F1a1Bd9e8E8D8d8B8b8B8b8B",
                    staking_address="0x2Ef503950Be67a98746F484DA0bB62d9d969E1C0",
                ),
                "gnosis": ChainConfig(
                    chain="gnosis",
                    marketplace_address="0x9c7d6D8E5B8b3b75F1a1Bd9e8E8D8d8B8b8B8b8B",
                    factory_address="0x9c7d6D8E5B8b3b75F1a1Bd9e8E8D8d8B8b8B8b8B",
                    staking_address="0x2Ef503950Be67a98746F484DA0bB62d9d969E1C0",
                ),
            }
        )

        update = _make_update()
        ctx = _make_context(config=cfg)

        with _auth(), patch("asyncio.to_thread", side_effect=fake_to_thread):
            await schedule_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        # Both chains should appear in output
        assert "BASE" in text
        assert "GNOSIS" in text

    def test_format_timedelta_future(self):
        from micromech.bot.commands.schedule import _format_timedelta

        result = _format_timedelta(7380)  # 2h 3m
        assert result == "in 2h 3m"

    def test_format_timedelta_past(self):
        from micromech.bot.commands.schedule import _format_timedelta

        result = _format_timedelta(-3660)  # 1h 1m overdue
        assert result == "overdue 1h 1m ⚠️"

    def test_format_timedelta_zero(self):
        from micromech.bot.commands.schedule import _format_timedelta

        result = _format_timedelta(0)
        assert result == "in 0h 0m"
