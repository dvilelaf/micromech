"""Extra tests to raise coverage on bridge.py, formatting.py, wallet.py and listener.py."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import micromech.core.bridge as bridge
from micromech.bot.formatting import split_message_blocks
from micromech.core.config import ChainConfig, MicromechConfig
from micromech.runtime.listener import EventListener

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ADDR = "0x" + "ab" * 20
CHAIN_CFG = ChainConfig(
    chain="gnosis",
    marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
    factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
    staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
    mech_address=ADDR,
)

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1


def _make_update(has_message=True):
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    if has_message:
        sent_msg = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent_msg)
        update.message.reply_document = AsyncMock()
    else:
        update.message = None
    return update


def _make_context(config=None):
    from tests.conftest import make_test_config

    ctx = MagicMock()
    ctx.bot_data = {"config": config or make_test_config()}
    return ctx


# ---------------------------------------------------------------------------
# bridge.py — IwaBridge properties (lines 46-71)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_bridge():
    bridge._cached_wallet = None
    bridge._cached_interfaces = None
    bridge._cached_key_storage = None
    bridge._service_info_cache.clear()
    yield
    bridge._cached_wallet = None
    bridge._cached_interfaces = None
    bridge._cached_key_storage = None
    bridge._service_info_cache.clear()


class TestIwaBridgeProperties:
    """Cover IwaBridge.wallet, chain_interface, web3 and with_retry (lines 46-71)."""

    def test_wallet_lazy_loads_and_caches(self):
        mock_wallet = MagicMock()
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.require_iwa"),
            patch("micromech.core.bridge.get_wallet", return_value=mock_wallet),
        ):
            b = bridge.IwaBridge(chain_name="gnosis")
            w = b.wallet
            assert w is mock_wallet
            # Second access returns cached (get_wallet called only once)
            w2 = b.wallet
            assert w2 is mock_wallet

    def test_chain_interface_lazy_loads(self):
        mock_ci_instance = MagicMock()
        mock_interfaces = MagicMock()
        mock_interfaces.get.return_value = mock_ci_instance

        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.require_iwa"),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces),
        ):
            b = bridge.IwaBridge(chain_name="gnosis")
            ci = b.chain_interface
            assert ci is mock_ci_instance
            # Second access hits cache
            ci2 = b.chain_interface
            assert ci2 is mock_ci_instance
            mock_interfaces.get.assert_called_once_with("gnosis")

    def test_chain_interface_unknown_chain_raises(self):
        mock_interfaces = MagicMock()
        mock_interfaces.get.return_value = None

        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.require_iwa"),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces),
        ):
            b = bridge.IwaBridge(chain_name="unknown_chain")
            with pytest.raises(ValueError, match="not found"):
                _ = b.chain_interface

    def test_web3_delegates_to_chain_interface(self):
        mock_ci = MagicMock()
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.require_iwa"),
        ):
            b = bridge.IwaBridge(chain_name="gnosis")
            b._chain_interface = mock_ci
            assert b.web3 is mock_ci.web3

    def test_with_retry_delegates(self):
        mock_ci = MagicMock()
        mock_ci.with_retry.return_value = "result"
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.require_iwa"),
        ):
            b = bridge.IwaBridge(chain_name="gnosis")
            b._chain_interface = mock_ci
            result = b.with_retry(lambda: 42)
            assert result == "result"


# ---------------------------------------------------------------------------
# bridge.py — create_bridges (lines 79-84)
# ---------------------------------------------------------------------------


class TestCreateBridges:
    def test_creates_bridge_per_chain(self):
        config = MagicMock()
        config.enabled_chains = {"gnosis": MagicMock(), "base": MagicMock()}

        with patch("micromech.core.bridge.IwaBridge") as MockBridge:
            MockBridge.side_effect = lambda chain_name: MagicMock()
            result = bridge.create_bridges(config)
        assert set(result.keys()) == {"gnosis", "base"}

    def test_skips_failed_chain(self):
        config = MagicMock()
        config.enabled_chains = {"gnosis": MagicMock(), "broken": MagicMock()}

        def side_effect(chain_name):
            if chain_name == "broken":
                raise RuntimeError("bad chain")
            return MagicMock()

        with patch("micromech.core.bridge.IwaBridge", side_effect=side_effect):
            result = bridge.create_bridges(config)
        assert "gnosis" in result
        assert "broken" not in result

    def test_returns_empty_on_iteration_error(self):
        config = MagicMock()
        type(config).enabled_chains = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("fail"))
        )
        with patch("micromech.core.bridge.IwaBridge"):
            result = bridge.create_bridges(config)
        assert result == {}


# ---------------------------------------------------------------------------
# bridge.py — get_wallet path B edge cases (lines 173-174)
# ---------------------------------------------------------------------------


class TestGetWalletPathB:
    """Cover AttributeError/TypeError fallback in path B (lines 173-174)."""

    def test_wallet_attribute_error_falls_through(self, tmp_path):
        (tmp_path / "wallet.json").touch()
        with patch("iwa.core.constants.WALLET_PATH", str(tmp_path / "wallet.json")):
            with patch("micromech.core.bridge.Wallet", side_effect=AttributeError("no attr")):
                with pytest.raises(RuntimeError, match="No wallet"):
                    bridge.get_wallet()

    def test_wallet_type_error_falls_through(self, tmp_path):
        (tmp_path / "wallet.json").touch()
        with patch("iwa.core.constants.WALLET_PATH", str(tmp_path / "wallet.json")):
            with patch("micromech.core.bridge.Wallet", side_effect=TypeError("bad type")):
                with pytest.raises(RuntimeError, match="No wallet"):
                    bridge.get_wallet()


# ---------------------------------------------------------------------------
# bridge.py — check_balances (lines 197-239)
# ---------------------------------------------------------------------------


class TestCheckBalancesFull:
    """H4/B3: check_balances now returns None on RPC failure (not (0.0, 0.0))."""

    def test_uses_cached_wallet_address(self):
        mock_wallet = MagicMock()
        mock_wallet.master_account.address = "0x" + "aa" * 20
        bridge._cached_wallet = mock_wallet
        bridge._cached_key_storage = None

        mock_ci = MagicMock()
        mock_ci.get.return_value = None  # chain not found → returns None
        bridge._cached_interfaces = mock_ci

        assert bridge.check_balances("gnosis") is None

    def test_returns_none_when_no_address_sources(self):
        bridge._cached_wallet = None
        bridge._cached_key_storage = None
        with patch("micromech.core.bridge._IWA_AVAILABLE", True):
            assert bridge.check_balances("gnosis") is None

    def test_returns_none_when_address_is_falsy(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = None
        bridge._cached_key_storage = mock_ks
        bridge._cached_wallet = None
        with patch("micromech.core.bridge._IWA_AVAILABLE", True):
            assert bridge.check_balances("gnosis") is None

    def test_returns_none_when_chain_not_found(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0x" + "bb" * 20
        bridge._cached_key_storage = mock_ks
        bridge._cached_wallet = None

        mock_interfaces = MagicMock()
        mock_interfaces.get.return_value = None
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces),
        ):
            assert bridge.check_balances("gnosis") is None

    def test_returns_native_and_olas_balances(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0x" + "cc" * 20
        bridge._cached_key_storage = mock_ks

        mock_ci_instance = MagicMock()
        mock_ci_instance.with_retry.side_effect = lambda fn: fn()
        mock_ci_instance.web3.eth.get_balance.return_value = 1_000_000_000_000_000_000
        mock_ci_instance.web3.from_wei.side_effect = lambda wei, unit: wei / 1e18

        mock_chain_model = MagicMock()
        mock_chain_model.get_token_address.return_value = "0x" + "dd" * 20
        mock_ci_instance.chain = mock_chain_model

        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call.return_value = 2_000_000_000_000_000_000
        mock_ci_instance.web3.eth.contract.return_value = mock_contract

        mock_interfaces = MagicMock()
        mock_interfaces.get.return_value = mock_ci_instance

        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces),
        ):
            native, olas = bridge.check_balances("gnosis")

        assert native == pytest.approx(1.0)
        assert olas == pytest.approx(2.0)

    def test_olas_balance_failure_returns_zero_olas(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0x" + "cc" * 20
        bridge._cached_key_storage = mock_ks

        mock_ci_instance = MagicMock()
        mock_ci_instance.with_retry.side_effect = lambda fn: fn()
        mock_ci_instance.web3.eth.get_balance.return_value = 500_000_000_000_000_000
        mock_ci_instance.web3.from_wei.side_effect = lambda wei, unit: wei / 1e18

        mock_chain_model = MagicMock()
        mock_chain_model.get_token_address.side_effect = Exception("token error")
        mock_ci_instance.chain = mock_chain_model

        mock_interfaces = MagicMock()
        mock_interfaces.get.return_value = mock_ci_instance

        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces),
        ):
            native, olas = bridge.check_balances("gnosis")

        assert native == pytest.approx(0.5)
        assert olas == 0.0

    def test_creates_cached_interfaces_when_none(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0x" + "ee" * 20
        bridge._cached_key_storage = mock_ks
        bridge._cached_wallet = None
        bridge._cached_interfaces = None

        mock_ci_instance = MagicMock()
        mock_ci_instance.get.return_value = None
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.ChainInterfaces", return_value=mock_ci_instance),
        ):
            result = bridge.check_balances("gnosis")
        assert result is None
        assert bridge._cached_interfaces is mock_ci_instance


# ---------------------------------------------------------------------------
# bridge.py — check_safe_balance (lines 251-273)
# ---------------------------------------------------------------------------


class TestCheckSafeBalance:
    def test_returns_none_without_iwa(self):
        with patch("micromech.core.bridge._IWA_AVAILABLE", False):
            assert bridge.check_safe_balance("gnosis") is None

    def test_returns_none_when_no_multisig(self):
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            assert bridge.check_safe_balance("gnosis") is None

    def test_returns_none_when_chain_not_found(self):
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "ff" * 20},
            ),
        ):
            mock_interfaces = MagicMock()
            mock_interfaces.get.return_value = None
            with patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces):
                assert bridge.check_safe_balance("gnosis") is None

    def test_returns_balance(self):
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "ff" * 20},
            ),
        ):
            mock_ci_instance = MagicMock()
            mock_ci_instance.with_retry.side_effect = lambda fn: fn()
            mock_ci_instance.web3.eth.get_balance.return_value = 3_000_000_000_000_000_000
            mock_ci_instance.web3.from_wei.side_effect = lambda wei, unit: wei / 1e18

            mock_interfaces = MagicMock()
            mock_interfaces.get.return_value = mock_ci_instance

            with patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces):
                balance = bridge.check_safe_balance("gnosis")
            assert balance == pytest.approx(3.0)

    def test_returns_none_on_exception(self):
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch(
                "micromech.core.bridge.get_service_info",
                side_effect=RuntimeError("boom"),
            ),
        ):
            assert bridge.check_safe_balance("gnosis") is None

    def test_creates_cached_interfaces_when_none(self):
        bridge._cached_interfaces = None
        with (
            patch("micromech.core.bridge._IWA_AVAILABLE", True),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "11" * 20},
            ),
        ):
            mock_ci_instance = MagicMock()
            mock_ci_instance.with_retry.side_effect = lambda fn: fn()
            mock_ci_instance.web3.eth.get_balance.return_value = 0
            mock_ci_instance.web3.from_wei.return_value = 0.0

            mock_interfaces = MagicMock()
            mock_interfaces.get.return_value = mock_ci_instance

            with patch("micromech.core.bridge.ChainInterfaces", return_value=mock_interfaces):
                bridge.check_safe_balance("gnosis")
            assert bridge._cached_interfaces is mock_interfaces


# ---------------------------------------------------------------------------
# bridge.py — _get_attr (line 284)
# ---------------------------------------------------------------------------


class TestGetAttr:
    def test_gets_from_dict(self):
        assert bridge._get_attr({"a": 1}, "a") == 1

    def test_gets_from_dict_with_default(self):
        assert bridge._get_attr({}, "missing", "default") == "default"

    def test_gets_from_plain_object(self):
        # Plain object — no .get method — uses getattr path
        class Obj:
            foo = "bar"

        assert bridge._get_attr(Obj(), "foo") == "bar"

    def test_object_missing_attr_uses_default(self):
        class Obj:
            pass

        assert bridge._get_attr(Obj(), "missing", 99) == 99


# ---------------------------------------------------------------------------
# bridge.py — get_service_info (lines 296-325)
# ---------------------------------------------------------------------------


class TestGetServiceInfo:
    def test_returns_cached_result(self):
        now = time.monotonic()
        bridge._service_info_cache["gnosis"] = (now, {"service_id": 42})
        result = bridge.get_service_info("gnosis")
        assert result["service_id"] == 42

    def test_returns_empty_when_no_olas_plugin(self):
        mock_config = MagicMock()
        mock_config.get_plugin_config.return_value = None

        with patch.dict(
            "sys.modules",
            {"iwa.core.models": MagicMock(Config=MagicMock(return_value=mock_config))},
        ):
            result = bridge.get_service_info("other_chain")
        assert result == {}

    def test_returns_empty_on_import_error(self):
        # Setting the module to None in sys.modules makes `from iwa.core.models import Config`
        # raise ImportError, which is the path we want to cover (line 322).
        with patch.dict("sys.modules", {"iwa.core.models": None}):
            result = bridge.get_service_info("gnosis")
        assert result == {}

    def test_returns_service_info_for_matching_chain(self):
        # Build a plain-class service whose attributes match the expected chain
        class FakeService:
            chain_name = "gnosis"
            service_id = 7
            multisig_address = "0x" + "aa" * 20
            agent_address = "0x" + "bb" * 20

        # Build a plain-class olas config whose .services is a plain dict
        class FakeOlas:
            services = {"s1": FakeService()}

        mock_config = MagicMock()
        mock_config.get_plugin_config.return_value = FakeOlas()

        with patch.dict(
            "sys.modules",
            {"iwa.core.models": MagicMock(Config=MagicMock(return_value=mock_config))},
        ):
            result = bridge.get_service_info("gnosis")

        assert result.get("service_id") == 7
        assert result.get("multisig_address") == "0x" + "aa" * 20

    def test_returns_empty_when_no_services(self):
        class FakeOlas:
            services = {}

        mock_config = MagicMock()
        mock_config.get_plugin_config.return_value = FakeOlas()

        with patch.dict(
            "sys.modules",
            {"iwa.core.models": MagicMock(Config=MagicMock(return_value=mock_config))},
        ):
            result = bridge.get_service_info("gnosis")
        assert result == {}

    def test_expired_cache_is_refreshed(self):
        # Inject a very old cache entry (expired)
        bridge._service_info_cache["gnosis"] = (0.0, {"service_id": 999})

        mock_config = MagicMock()
        mock_config.get_plugin_config.return_value = None  # returns fresh empty

        with patch.dict(
            "sys.modules",
            {"iwa.core.models": MagicMock(Config=MagicMock(return_value=mock_config))},
        ):
            result = bridge.get_service_info("gnosis")
        # Cache was stale → refreshed → no plugin → empty dict
        assert result == {}


# ---------------------------------------------------------------------------
# formatting.py — split_message_blocks (lines 45-61)
# ---------------------------------------------------------------------------


class TestSplitMessageBlocks:
    def test_empty_input_returns_empty(self):
        assert split_message_blocks([]) == []

    def test_single_block_fits(self):
        assert split_message_blocks(["hello"]) == ["hello"]

    def test_multiple_blocks_fit_in_one_message(self):
        blocks = ["block one", "block two", "block three"]
        result = split_message_blocks(blocks)
        assert len(result) == 1
        assert result[0] == "block one\n\nblock two\n\nblock three"

    def test_split_when_exceeds_limit(self):
        # 60 + 2 + 60 = 122 > 100 → split into two
        block_a = "a" * 60
        block_b = "b" * 60
        result = split_message_blocks([block_a, block_b], max_length=100)
        assert len(result) == 2
        assert result[0] == block_a
        assert result[1] == block_b

    def test_multiple_blocks_split_across_messages(self):
        blocks = ["x" * 50, "y" * 50, "z" * 50]
        result = split_message_blocks(blocks, max_length=100)
        assert len(result) == 3

    def test_blocks_exactly_at_limit(self):
        # 49 + 2 + 49 = 100 == max_length → fits in one
        block = "a" * 49
        result = split_message_blocks([block, block], max_length=100)
        assert len(result) == 1

    def test_single_block_larger_than_limit_still_included(self):
        big = "x" * 5000
        result = split_message_blocks([big], max_length=100)
        assert len(result) == 1
        assert result[0] == big

    def test_separator_only_added_between_blocks(self):
        assert split_message_blocks(["a", "b"], max_length=4096) == ["a\n\nb"]

    def test_many_small_blocks_packed_greedily(self):
        blocks = ["x" * 9] * 10
        result = split_message_blocks(blocks, max_length=50)
        assert len(result) > 1
        for msg in result:
            # Each message must be ≤50 chars (single block is 9, always fine)
            assert len(msg) <= 50


# ---------------------------------------------------------------------------
# wallet.py — lines 29-47, 81-83, 94-95, 98-100, 103
# ---------------------------------------------------------------------------


class TestWalletCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.wallet import wallet_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            await wallet_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_no_chains_returns_message(self):
        from micromech.bot.commands.wallet import wallet_command
        from tests.conftest import make_test_config

        config = make_test_config()
        config.chains = {}
        update = _make_update()
        ctx = _make_context(config=config)

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.security._rate_limit_cache", {}),
        ):
            await wallet_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_wallet_success_shows_master_and_chains(self):
        from micromech.bot.commands.wallet import wallet_command
        from tests.conftest import make_test_config

        config = make_test_config()
        update = _make_update()
        ctx = _make_context(config=config)

        mock_wallet = MagicMock()
        mock_wallet.master_account.address = "0x" + "aa" * 20

        # get_service_info is imported inline inside wallet_command — patch at source
        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.security._rate_limit_cache", {}),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=mock_wallet),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(1.5, 0.25)),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "bb" * 20, "service_id": 5},
            ),
        ):
            await wallet_command(update, ctx)

        status_msg = update.message.reply_text.return_value
        status_msg.edit_text.assert_called_once()
        text = status_msg.edit_text.call_args[0][0]
        assert "Master" in text

    @pytest.mark.asyncio
    async def test_wallet_failure_shows_unavailable(self):
        from micromech.bot.commands.wallet import wallet_command
        from tests.conftest import make_test_config

        config = make_test_config()
        update = _make_update()
        ctx = _make_context(config=config)

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.security._rate_limit_cache", {}),
            patch(
                "micromech.bot.commands.wallet.get_wallet",
                side_effect=RuntimeError("no wallet"),
            ),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await wallet_command(update, ctx)

        status_msg = update.message.reply_text.return_value
        status_msg.edit_text.assert_called_once()
        text = status_msg.edit_text.call_args[0][0]
        # H1: user_error now produces "Error (wallet) — check logs"
        assert "Error" in text or "check logs" in text

    @pytest.mark.asyncio
    async def test_wallet_chain_with_mech_address(self):
        """Cover branch: chain has mech_address (lines 97-100)."""
        from micromech.bot.commands.wallet import wallet_command
        from micromech.core.constants import CHAIN_DEFAULTS
        from tests.conftest import make_test_config

        gnosis = CHAIN_DEFAULTS["gnosis"]
        config = make_test_config(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    marketplace_address=gnosis["marketplace"],
                    factory_address=gnosis["factory"],
                    staking_address=gnosis["staking"],
                    mech_address="0x" + "cc" * 20,
                )
            }
        )
        update = _make_update()
        ctx = _make_context(config=config)

        mock_wallet = MagicMock()
        mock_wallet.master_account.address = "0x" + "aa" * 20

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
            patch("micromech.bot.security._rate_limit_cache", {}),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=mock_wallet),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(0.0, 0.0)),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await wallet_command(update, ctx)

        status_msg = update.message.reply_text.return_value
        status_msg.edit_text.assert_called_once()
        text = status_msg.edit_text.call_args[0][0]
        assert "Mech" in text

    @pytest.mark.asyncio
    async def test_wallet_chain_not_deployed(self):
        """Wallet command with no multisig and no mech → shows 'Not deployed'."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from micromech.bot.commands.wallet import wallet_command

        wallet_mock = MagicMock()
        wallet_mock.master_account.address = "0x" + "a" * 40

        update = MagicMock()
        update.message = AsyncMock()
        sent = AsyncMock()
        sent.edit_text = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent)
        update.effective_chat.id = 42
        update.effective_user.id = 99

        ctx = MagicMock()
        from tests.conftest import make_test_config

        ctx.bot_data = {"config": make_test_config()}

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=42),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=wallet_mock),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(1.0, 0.0)),
            patch(
                "micromech.bot.commands.wallet.check_address_balances",
                return_value=(None, None),
            ),
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            await wallet_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "Not deployed" in text

    @pytest.mark.asyncio
    async def test_wallet_chain_with_multisig(self):
        """Wallet command with a multisig address → shows 'Multisig'."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from micromech.bot.commands.wallet import wallet_command

        wallet_mock = MagicMock()
        wallet_mock.master_account.address = "0x" + "a" * 40

        update = MagicMock()
        update.message = AsyncMock()
        sent = AsyncMock()
        sent.edit_text = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent)
        update.effective_chat.id = 42
        update.effective_user.id = 99

        ctx = MagicMock()
        from tests.conftest import make_test_config

        ctx.bot_data = {"config": make_test_config()}

        with (
            patch("micromech.bot.security.secrets", telegram_chat_id=42),
            patch("micromech.bot.commands.wallet.get_wallet", return_value=wallet_mock),
            patch("micromech.bot.commands.wallet.check_balances", return_value=(1.0, 0.0)),
            patch(
                "micromech.bot.commands.wallet.check_address_balances",
                return_value=(0.5, 1.0),
            ),
            patch(
                "micromech.core.bridge.get_service_info",
                return_value={"multisig_address": "0x" + "ff" * 20},
            ),
        ):
            await wallet_command(update, ctx)

        text = sent.edit_text.call_args[0][0]
        assert "Multisig" in text


# ---------------------------------------------------------------------------
# listener.py — lines 71, 76-106, 187-208, 215-216, 279-280
# ---------------------------------------------------------------------------


class TestPollOnceFullPath:
    """Cover poll_once with new blocks and event resolution (lines 71, 76-106)."""

    @pytest.mark.asyncio
    async def test_sets_last_block_on_first_poll(self):
        from micromech.core.constants import DEFAULT_EVENT_LOOKBACK_BLOCKS

        mock_bridge = MagicMock()
        mock_bridge.with_retry.side_effect = lambda fn, **kw: fn()
        mock_bridge.web3.eth.block_number = 1000

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        assert listener._last_block is None

        with patch.object(listener, "_fetch_events", return_value=[]):
            await listener.poll_once()

        # _last_block is initialized to max(0, current - lookback)
        expected = max(0, 1000 - DEFAULT_EVENT_LOOKBACK_BLOCKS)
        assert listener._last_block == expected
        assert listener._polled_to_block == 1000

    @pytest.mark.asyncio
    async def test_resolves_requests_and_sets_polled_block(self):
        from micromech.core.models import MechRequest

        mock_bridge = MagicMock()
        mock_bridge.with_retry.side_effect = lambda fn, **kw: fn()
        mock_bridge.web3.eth.block_number = 2000

        req = MechRequest(request_id="r1", prompt="test", tool="echo")

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        listener._last_block = 1999

        with (
            patch.object(listener, "_fetch_events", return_value=[req]),
            patch.object(
                listener,
                "_resolve_request",
                new_callable=AsyncMock,
                return_value=req,
            ),
        ):
            result = await listener.poll_once()

        assert len(result) == 1
        assert result[0] is req
        assert listener._polled_to_block == 2000

    @pytest.mark.asyncio
    async def test_logs_truncated_prompt_branch(self):
        """Cover logging branch where prompt > 60 chars (lines 102-104)."""
        from micromech.core.models import MechRequest

        mock_bridge = MagicMock()
        mock_bridge.with_retry.side_effect = lambda fn, **kw: fn()
        mock_bridge.web3.eth.block_number = 500

        req = MechRequest(request_id="r1", prompt="x" * 100, tool="llm")

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        listener._last_block = 499

        with (
            patch.object(listener, "_fetch_events", return_value=[req]),
            patch.object(
                listener,
                "_resolve_request",
                new_callable=AsyncMock,
                return_value=req,
            ),
        ):
            result = await listener.poll_once()

        assert len(result) == 1


class TestFetchEventsChunking:
    """Cover mini-chunk fallback in _fetch_events (lines 187-208)."""

    def test_falls_back_to_mini_chunks(self):
        mock_bridge = MagicMock()
        mock_bridge.web3.to_checksum_address.side_effect = lambda a: a

        call_count = [0]

        def with_retry_side(fn, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("range too large")
            return []  # mini chunks succeed with empty

        mock_bridge.with_retry.side_effect = with_retry_side

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        listener._marketplace_contract = MagicMock()

        result = listener._fetch_events(100, 110)
        assert isinstance(result, list)

    def test_mini_chunk_failure_logs_warning(self):
        mock_bridge = MagicMock()
        mock_bridge.web3.to_checksum_address.side_effect = lambda a: a
        mock_bridge.with_retry.side_effect = Exception("always fails")

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        listener._marketplace_contract = MagicMock()

        result = listener._fetch_events(100, 110)
        assert result == []

    def test_parse_event_exception_is_caught(self):
        """Cover the parse exception catch in _fetch_events (lines 215-216)."""
        mock_bridge = MagicMock()
        mock_bridge.web3.to_checksum_address.side_effect = lambda a: a
        # Return a non-empty log list
        mock_bridge.with_retry.side_effect = lambda fn, **kw: [{"args": {}}]

        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=mock_bridge)
        listener._marketplace_contract = MagicMock()

        with patch.object(
            listener, "_parse_marketplace_event", side_effect=ValueError("bad event")
        ):
            result = listener._fetch_events(100, 200)
        assert result == []


class TestListenerRunBackoff:
    """Cover run() adaptive backoff capping at max_interval (lines 279-280 area)."""

    @pytest.mark.asyncio
    async def test_interval_caps_at_max(self):
        listener = EventListener(MicromechConfig(), CHAIN_CFG, bridge=None)
        sleep_calls = []
        call_count = [0]

        async def mock_poll():
            return []

        original_sleep = asyncio.sleep

        async def tracked_sleep(t):
            sleep_calls.append(t)
            call_count[0] += 1
            if call_count[0] >= 10:
                listener.stop()
            await original_sleep(0)

        listener.poll_once = mock_poll

        with (
            patch("asyncio.sleep", tracked_sleep),
            patch("micromech.runtime.listener.DEFAULT_EVENT_POLL_INTERVAL", 15),
        ):
            await listener.run(callback=AsyncMock())

        assert all(t <= 60 for t in sleep_calls)
        # After enough idle iterations the interval should reach 60 s
        assert sleep_calls[-1] == pytest.approx(60.0, abs=1.0)
