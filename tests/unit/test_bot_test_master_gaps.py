"""Tests closing the gaps flagged by test-master in the Round 5 review.

Covers:
- HIGH1: wallet fund-emoji branches (✅/⚠️/❓) in wallet.py
- HIGH2: API-key leak assertion — user_error MUST NOT forward str(exc) to chat
- HIGH3: parse_mode=MARKDOWN_V2 contract on success paths
- HIGH4: MarkdownV2 injection via hostile on-chain names through handlers
- HIGH5: _categorize_exception branch coverage (incl. HttpValidationError regression)
- MED: _request_emoji boundary, weak assertions tightened
- LOW: explorer_link_md unknown chain, format_epoch_countdown boundary
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telegram.constants import ParseMode

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42
ADDR_MASTER = "0x" + "aa" * 20
ADDR_MULTISIG = "0x" + "bb" * 20
ADDR_AGENT = "0x" + "cc" * 20
# A fake RPC URL containing a "secret" we can grep the rendered message for.
# user_error must NEVER forward this substring to the Telegram chat.
LEAKY_URL = "https://eth.example.com/v2/SUPER_SECRET_API_KEY_abc123"
LEAKY_MARKER = "SUPER_SECRET_API_KEY_abc123"


# ---------------------------------------------------------------------------
# Shared fixtures (MED: promote ad-hoc helpers to fixtures)
# ---------------------------------------------------------------------------


@pytest.fixture
def auth_chat():
    """Patch the bot security layer so command handlers run under a known chat."""
    with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
        yield AUTHORIZED_CHAT_ID


@pytest.fixture
def update_ctx():
    """Return (update, context, sent_msg) triples where sent_msg is the reply."""
    update = MagicMock()
    update.message = AsyncMock()
    sent = AsyncMock()
    update.message.reply_text = AsyncMock(return_value=sent)
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = 1

    ctx = MagicMock()
    ctx.bot_data = {"config": make_test_config()}
    return update, ctx, sent


# ---------------------------------------------------------------------------
# HIGH5: _categorize_exception branch coverage
# ---------------------------------------------------------------------------


class TestCategorizeException:
    """R2-M6 regression + full branch coverage for `_categorize_exception`."""

    @pytest.mark.parametrize(
        "exc,expected",
        [
            (TimeoutError(), "RPC timeout"),
            (ConnectionError(), "RPC unavailable"),
            (OSError(), "RPC unavailable"),
            (ValueError("bad"), "Invalid data"),
            (TypeError("bad"), "Invalid data"),
        ],
    )
    def test_isinstance_branches(self, exc, expected):
        from micromech.bot.formatting import _categorize_exception

        assert _categorize_exception(exc) == expected

    def test_custom_timeout_by_name(self):
        """Third-party timeout types we can't import still classify correctly."""
        from micromech.bot.formatting import _categorize_exception

        cls = type("SomeLibTimeoutError", (Exception,), {})
        assert _categorize_exception(cls()) == "RPC timeout"

    def test_contract_revert_by_name(self):
        from micromech.bot.formatting import _categorize_exception

        cls = type("ContractLogicError", (Exception,), {})
        assert _categorize_exception(cls()) == "Contract reverted"

    def test_http_validation_error_regression(self):
        """R2-M6: `HttpValidationError` used to match the "http" substring and
        get classified as RPC unavailable. Now it must match "validation"
        first."""
        from micromech.bot.formatting import _categorize_exception

        cls = type("HttpValidationError", (Exception,), {})
        assert _categorize_exception(cls()) == "Invalid data"

    def test_http_error_by_name(self):
        from micromech.bot.formatting import _categorize_exception

        cls = type("HTTPError", (Exception,), {})
        assert _categorize_exception(cls()) == "RPC unavailable"

    def test_unknown_defaults_to_error(self):
        from micromech.bot.formatting import _categorize_exception

        cls = type("CompletelyNovelException", (Exception,), {})
        assert _categorize_exception(cls()) == "Error"


# ---------------------------------------------------------------------------
# HIGH2: API-key leak assertions
# ---------------------------------------------------------------------------


class TestUserErrorNeverLeaksSecret:
    """user_error MUST NOT forward `str(exc)` to the chat.

    This is the whole reason the helper exists: Web3/requests exceptions often
    embed the full RPC URL (with embedded API key) into the exception message,
    and we were interpolating that into `reply_text`. These tests pin the
    invariant at the boundary.
    """

    def test_user_error_output_does_not_contain_str_exc(self):
        from micromech.bot.formatting import user_error

        exc = RuntimeError(LEAKY_URL)
        rendered = user_error("status gnosis", exc)
        assert LEAKY_MARKER not in rendered
        assert "https://" not in rendered

    @pytest.mark.asyncio
    async def test_status_command_does_not_leak_rpc_url(self, auth_chat, update_ctx):
        from micromech.bot.commands.status import status_command

        update, ctx, sent = update_ctx
        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError(LEAKY_URL)
        lifecycle.get_balances.return_value = {}
        ctx.bot_data["lifecycles"] = {"gnosis": lifecycle}

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "k"},
            ),
            patch(
                "micromech.bot.commands.status.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await status_command(update, ctx)

        # Neither the status text nor any other reply may contain the key.
        all_text = " ".join(
            str(c.args[0]) if c.args else ""
            for c in sent.edit_text.call_args_list + update.message.reply_text.call_args_list
        )
        assert LEAKY_MARKER not in all_text
        assert LEAKY_URL not in all_text

    @pytest.mark.asyncio
    async def test_last_rewards_does_not_leak_rpc_url(self, auth_chat, update_ctx):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update, ctx, sent = update_ctx
        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError(LEAKY_URL)
        ctx.bot_data["lifecycles"] = {"gnosis": lifecycle}

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "k"},
            ),
            patch(
                "micromech.bot.commands.last_rewards.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await last_rewards_command(update, ctx)

        all_text = " ".join(
            str(c.args[0]) if c.args else ""
            for c in sent.edit_text.call_args_list + update.message.reply_text.call_args_list
        )
        assert LEAKY_MARKER not in all_text


# ---------------------------------------------------------------------------
# HIGH3: parse_mode=MARKDOWN_V2 assertion
# ---------------------------------------------------------------------------


class TestParseModeContract:
    """Success paths must call Telegram with parse_mode=MARKDOWN_V2.

    A refactor that silently drops the kwarg would render raw backslashes
    in chat and all text-only assertions would still pass. Pinning this
    kwarg is the cheapest way to prevent that regression.
    """

    @pytest.mark.asyncio
    async def test_status_uses_markdown_v2(self, auth_chat, update_ctx):
        from micromech.bot.commands.status import status_command

        update, ctx, sent = update_ctx
        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 1.0,
            "requests_this_epoch": 5,
            "required_requests": 10,
        }
        lifecycle.get_balances.return_value = {}
        ctx.bot_data["lifecycles"] = {"gnosis": lifecycle}

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "k"},
            ),
            patch(
                "micromech.bot.commands.status.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await status_command(update, ctx)

        # At least one rendering must use MarkdownV2.
        kwargs = sent.edit_text.call_args.kwargs
        assert kwargs.get("parse_mode") == ParseMode.MARKDOWN_V2

    @pytest.mark.asyncio
    async def test_info_uses_markdown_v2(self, auth_chat, update_ctx):
        from micromech.bot.commands.info import info_command

        update, ctx, _sent = update_ctx
        await info_command(update, ctx)
        kwargs = update.message.reply_text.call_args.kwargs
        assert kwargs.get("parse_mode") == ParseMode.MARKDOWN_V2


# ---------------------------------------------------------------------------
# HIGH4: MD-injection integration through handlers
# ---------------------------------------------------------------------------


class TestMdInjectionIntegration:
    """A hostile on-chain `staking_contract_name` must not smuggle formatting.

    These integration tests flow the hostile value through an actual command
    handler → `reply_text`, verifying the escape pipeline holds end-to-end.
    """

    HOSTILE = "*rekt*_[]`"

    @pytest.mark.asyncio
    async def test_last_rewards_escapes_hostile_contract_name(self, auth_chat, update_ctx):
        from micromech.bot.commands.last_rewards import last_rewards_command

        update, ctx, sent = update_ctx
        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 0.0,
            "requests_this_epoch": 0,
            "required_requests": 10,
            "staking_contract_name": self.HOSTILE,
        }
        ctx.bot_data["lifecycles"] = {"gnosis": lifecycle}

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "k"},
            ),
            patch(
                "micromech.bot.commands.last_rewards.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await last_rewards_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        # The hostile raw string must NOT appear unescaped.
        assert self.HOSTILE not in text
        # Each reserved char appearing must be backslash-prefixed.
        # Grab the stretch around "rekt" and check its neighbours.
        i = text.find("rekt")
        assert i != -1
        assert text[i - 2 : i] == "\\*", (
            f"expected escaped * before 'rekt', got {text[i - 2 : i + 4]!r}"
        )

    @pytest.mark.asyncio
    async def test_status_escapes_hostile_contract_name(self, auth_chat, update_ctx):
        from micromech.bot.commands.status import status_command

        update, ctx, sent = update_ctx
        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "rewards": 0.0,
            "requests_this_epoch": 0,
            "required_requests": 10,
            "staking_contract_name": self.HOSTILE,
        }
        lifecycle.get_balances.return_value = {}
        ctx.bot_data["lifecycles"] = {"gnosis": lifecycle}

        with (
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"service_key": "k"},
            ),
            patch(
                "micromech.bot.commands.status.get_olas_price_eur",
                return_value=None,
            ),
        ):
            await status_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert self.HOSTILE not in text
        # "Contract:" line must carry the escaped form.
        assert "\\*rekt\\*" in text


# ---------------------------------------------------------------------------
# HIGH1: wallet.py fund-emoji branches (✅/⚠️/❓)
# ---------------------------------------------------------------------------


class TestWalletFundEmoji:
    """The emoji selector in wallet.py was added precisely because H4/B3
    wanted to avoid showing ✅ when the balance is unknown. Every branch of
    that selector must have an assertion.

    We build the config with MagicMock because `fund_threshold_xdai` is not
    a declared field on the real ChainConfig model (wallet.py reads it via
    `getattr(cfg, "fund_threshold_xdai", None)`).
    """

    async def _render(self, master_bal, addr_bal, threshold):
        from micromech.bot.commands.wallet import wallet_command

        chain_cfg = MagicMock()
        chain_cfg.fund_threshold_xdai = threshold
        chain_cfg.mech_address = None

        config = MagicMock()
        config.enabled_chains = {"gnosis": chain_cfg}

        ctx = MagicMock()
        ctx.bot_data = {"config": config}

        update = MagicMock()
        update.message = AsyncMock()
        sent = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent)
        update.effective_chat.id = AUTHORIZED_CHAT_ID
        update.effective_user.id = 1

        wallet_mock = MagicMock()
        wallet_mock.master_account.address = ADDR_MASTER

        with (
            patch(
                "micromech.bot.security.secrets",
                telegram_chat_id=AUTHORIZED_CHAT_ID,
            ),
            patch(
                "micromech.bot.commands.wallet.get_wallet",
                return_value=wallet_mock,
            ),
            patch(
                "micromech.bot.commands.wallet.check_balances",
                return_value=master_bal,
            ),
            patch(
                "micromech.bot.commands.wallet.check_address_balances",
                return_value=addr_bal,
            ),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={
                    "multisig_address": ADDR_MULTISIG,
                    "agent_address": ADDR_AGENT,
                    "service_id": 42,
                },
            ),
        ):
            await wallet_command(update, ctx)
        return sent.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_ok_when_balance_above_threshold(self):
        text = await self._render(master_bal=(1.0, 0.0), addr_bal=(1.0, 1.0), threshold=0.5)
        assert "✅" in text
        assert "⚠️" not in text
        assert "❓" not in text

    @pytest.mark.asyncio
    async def test_warning_when_balance_below_threshold(self):
        text = await self._render(master_bal=(0.01, 0.0), addr_bal=(0.01, 0.0), threshold=10.0)
        assert "⚠️" in text

    @pytest.mark.asyncio
    async def test_unknown_when_balance_is_none(self):
        text = await self._render(master_bal=None, addr_bal=None, threshold=1.0)
        assert "❓" in text
        assert "balance unknown" in text


# ---------------------------------------------------------------------------
# MED: _request_emoji boundary cases
# ---------------------------------------------------------------------------


class TestRequestEmojiBoundaries:
    @pytest.mark.parametrize(
        "requests,required,expected",
        [
            (9, 10, "🔄"),  # just below the line
            (10, 10, "✅"),  # exactly on the line
            (11, 10, "✅"),  # above the line
            (0, 10, "❌"),  # idle
            (1, 10, "🔄"),  # barely started
            (0, 0, "❌"),  # no requirement, idle
            (5, 0, "🔄"),  # no requirement, but some activity — in progress
        ],
    )
    def test_boundaries(self, requests, required, expected):
        from micromech.bot.commands.status import _request_emoji

        assert _request_emoji(requests, required) == expected


# ---------------------------------------------------------------------------
# LOW: explorer_link_md unknown chain fail-closed
# ---------------------------------------------------------------------------


class TestExplorerLinkMd:
    def test_known_chain_renders_link(self):
        from micromech.bot.formatting import explorer_link_md

        result = explorer_link_md("gnosis", "0x" + "1" * 40, "short")
        assert "gnosisscan.io" in result
        assert "short" in result

    def test_unknown_chain_falls_closed_to_code_md(self):
        from micromech.bot.formatting import explorer_link_md

        result = explorer_link_md("polygon", "0x" + "1" * 40)
        # No link, just an inline code span — fail closed, per security-md L3.
        assert result.startswith("`")
        assert result.endswith("`")
        assert "polygonscan" not in result


# ---------------------------------------------------------------------------
# LOW: format_epoch_countdown boundary
# ---------------------------------------------------------------------------


class TestFormatEpochCountdownBoundary:
    def test_zero_remaining_is_not_warning(self):
        """`remaining_seconds == 0` is the edge between "ends in" and "ended".
        The current implementation treats 0 as "ends in: 0h 0m" (not warning).
        """
        from micromech.bot.formatting import format_epoch_countdown

        result = format_epoch_countdown(1, None, 0)
        assert "ends in" in result
        assert "⚠️" not in result

    def test_one_second_negative_is_warning(self):
        from micromech.bot.formatting import format_epoch_countdown

        result = format_epoch_countdown(1, None, -1)
        assert "ended" in result
        assert "⚠️" in result
