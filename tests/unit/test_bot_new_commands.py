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
    with (
        patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
        patch("micromech.bot.security._rate_limit_cache", {}),
    ):
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

        with (
            _auth(),
            patch.object(
                type(cfg),
                "enabled_chains",
                new_callable=PropertyMock,
                return_value={"gnosis": fake_chain},
            ),
        ):
            await contracts_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        edited_text = sent_msg.edit_text.call_args[0][0]
        assert "No staking contract configured" in edited_text

    @pytest.mark.asyncio
    async def test_chain_success(self):
        """Happy path: _fetch_contract returns dict with contract data."""
        from datetime import datetime, timedelta, timezone

        from micromech.bot.commands.contracts import contracts_command

        epoch_end = datetime.now(timezone.utc) + timedelta(hours=2)
        fetch_result = {
            "staked": 3,
            "max": 10,
            "balance_olas": 5000.0,
            "min_stake_olas": 100.0,
            "epoch_end": epoch_end,
            "name": "TestContract",
        }

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
        assert "Used slots" in text

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
        from micromech.bot.commands.contracts import _epoch_countdown

        future = datetime.now(timezone.utc) + timedelta(hours=3)
        result = _epoch_countdown(future)
        assert "h" in result
        assert "m" in result
        assert "⚠️" not in result

    def test_format_epoch_countdown_overdue(self):
        from micromech.bot.commands.contracts import _epoch_countdown

        past = datetime.now(timezone.utc) - timedelta(hours=1, minutes=15)
        result = _epoch_countdown(past)
        assert "⚠️" in result
        assert "-" in result

    def test_explorer_urls_known_chain(self):
        from micromech.bot.formatting import EXPLORER_URLS

        assert "gnosisscan.io" in EXPLORER_URLS["gnosis"]
        assert "basescan.org" in EXPLORER_URLS["base"]

    def test_explorer_urls_fallback_default(self):
        from micromech.bot.formatting import EXPLORER_URLS, explorer_link_md

        # Unknown chain: helper fails closed to plain code (no misleading link)
        result = explorer_link_md("polygon", "0xdeadbeef")
        assert "gnosisscan.io" not in result  # no longer misleading fallback
        fallback = EXPLORER_URLS.get("polygon", EXPLORER_URLS["gnosis"])
        assert "gnosisscan.io" in fallback


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

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
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
        lifecycle.get_status.return_value = {
            "staking_state": "NOT_STAKED",
            "rewards": 0,
            "requests_this_epoch": 0,
            "required_requests": 10,
        }

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
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
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 1.5,  # OLAS float (not raw wei)
            "requests_this_epoch": 10,
            "required_requests": 10,
        }

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
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
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 0,
            "requests_this_epoch": 3,
            "required_requests": 10,
        }

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Needs" in text
        assert "7" in text  # 10 - 3 = 7

    @pytest.mark.asyncio
    async def test_chain_status_none(self):
        """lifecycle.get_status returns None → 'Could not fetch status'."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = None

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Could not fetch status" in text

    @pytest.mark.asyncio
    async def test_chain_error(self):
        """lifecycle.get_status raises → error block in message."""
        from micromech.bot.commands.last_rewards import last_rewards_command

        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError("chain gone")

        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with (
            _auth(),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "0xkey"},
            ),
            patch("micromech.bot.commands.last_rewards.get_olas_price_eur", return_value=None),
        ):
            await last_rewards_command(update, ctx)  # must not raise

        sent_msg = update.message.reply_text.return_value
        text = sent_msg.edit_text.call_args[0][0]
        assert "Error" in text or "chain gone" in text
