"""Tests for Telegram bot command handlers.

Strategy
--------
Each command is wrapped with @authorized_only and @rate_limited.
We bypass both by:
  - patching micromech.bot.security.secrets so telegram_chat_id matches the
    fake update's chat id
  - patching micromech.bot.security._rate_limit_cache to an empty dict so
    the first call per test is never rate-limited

All external I/O (bridge calls, wallet, filesystem) is mocked so tests run
without real credentials or network access.
"""

import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_test_config

AUTHORIZED_CHAT_ID = 42
AUTHORIZED_USER_ID = 1


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_update(has_message=True):
    """Return a MagicMock that looks like a Telegram Update."""
    update = MagicMock()
    update.effective_chat.id = AUTHORIZED_CHAT_ID
    update.effective_user.id = AUTHORIZED_USER_ID
    if has_message:
        # reply_text returns an object we can call edit_text on
        sent_msg = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock(return_value=sent_msg)
        update.message.reply_document = AsyncMock()
    else:
        update.message = None
    return update


def _make_context(**bot_data_extras):
    """Return a MagicMock context with sensible bot_data defaults."""
    ctx = MagicMock()
    ctx.bot_data = {"config": make_test_config(), **bot_data_extras}
    return ctx


def _auth_patches():
    """Context managers that bypass auth decorators."""
    return [
        patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID),
        patch("micromech.bot.security._rate_limit_cache", {}),
    ]


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

class TestStatusCommand:
    @pytest.mark.asyncio
    async def test_no_chains_enabled(self):
        from micromech.bot.commands.status import status_command

        config = make_test_config()
        config.chains = {}  # no enabled chains
        update = _make_update()
        ctx = _make_context(config=config)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await status_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.status import status_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await status_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_with_lifecycle_success(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = {
            "staking_state": "STAKED",
            "is_staked": True,
            "requests_this_epoch": 3,
            "required_requests": 10,
            "rewards": 1.5,
        }
        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})
        svc_info = {"service_key": "0xkey"}

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.core.bridge.get_service_info", return_value=svc_info):
            await status_command(update, ctx)

        update.message.reply_text.assert_called_once_with("Fetching status...")

    @pytest.mark.asyncio
    async def test_lifecycle_not_available(self):
        from micromech.bot.commands.status import status_command

        update = _make_update()
        ctx = _make_context(lifecycles={})  # no lifecycle for gnosis
        svc_info = {"service_key": "0xkey"}

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.core.bridge.get_service_info", return_value=svc_info):
            await status_command(update, ctx)

        update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_no_service_key(self):
        from micromech.bot.commands.status import status_command

        update = _make_update()
        ctx = _make_context(lifecycles={})
        svc_info = {}  # no service_key

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.core.bridge.get_service_info", return_value=svc_info):
            await status_command(update, ctx)

        update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_lifecycle_exception(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.side_effect = RuntimeError("rpc error")
        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})
        svc_info = {"service_key": "0xkey"}

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.core.bridge.get_service_info", return_value=svc_info):
            await status_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_with_metrics(self):
        from micromech.bot.commands.status import status_command

        lifecycle = MagicMock()
        lifecycle.get_status.return_value = None  # returns None → "Failed to fetch"
        metrics = MagicMock()
        metrics.uptime_seconds = 3700
        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle}, metrics=metrics)
        svc_info = {"service_key": "0xkey"}

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.core.bridge.get_service_info", return_value=svc_info):
            await status_command(update, ctx)

    def test_format_chain_status_evicted(self):
        from micromech.bot.commands.status import _format_chain_status

        result = _format_chain_status("gnosis", {
            "staking_state": "EVICTED",
            "is_staked": False,
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
        })
        assert "EVICTED" in result
        assert "🔴" in result

    def test_format_chain_status_not_staked(self):
        from micromech.bot.commands.status import _format_chain_status

        result = _format_chain_status("gnosis", {
            "staking_state": "NOT_STAKED",
            "is_staked": False,
            "requests_this_epoch": 0,
            "required_requests": 10,
            "rewards": 0.0,
        })
        assert "⚪" in result


# ---------------------------------------------------------------------------
# /info
# ---------------------------------------------------------------------------

class TestInfoCommand:
    @pytest.mark.asyncio
    async def test_basic_info(self):
        from micromech.bot.commands.info import info_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("importlib.metadata.version", return_value="1.2.3"):
            await info_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "1.2.3" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_with_metrics_and_queue(self):
        from micromech.bot.commands.info import info_command

        metrics = MagicMock()
        metrics.uptime_seconds = 7200
        queue = MagicMock()
        queue.count_by_status.return_value = {"pending": 2, "delivered": 10, "failed": 1}
        update = _make_update()
        ctx = _make_context(metrics=metrics, queue=queue)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("importlib.metadata.version", return_value="0.1.0"):
            await info_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "2h" in text or "Uptime" in text

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.info import info_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await info_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_package_not_found_shows_unknown(self):
        from importlib.metadata import PackageNotFoundError

        from micromech.bot.commands.info import info_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("importlib.metadata.version", side_effect=PackageNotFoundError):
            await info_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "unknown" in text


# ---------------------------------------------------------------------------
# /restart
# ---------------------------------------------------------------------------

class TestRestartCommand:
    @pytest.mark.asyncio
    async def test_restart_via_runtime_success(self):
        from micromech.bot.commands.restart import restart_command

        runtime = MagicMock()
        runtime.restart = AsyncMock(return_value=True)
        update = _make_update()
        ctx = _make_context(runtime_manager=runtime)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await restart_command(update, ctx)

        runtime.restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_restart_via_runtime_failure(self):
        from micromech.bot.commands.restart import restart_command

        runtime = MagicMock()
        runtime.restart = AsyncMock(return_value=False)
        update = _make_update()
        ctx = _make_context(runtime_manager=runtime)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await restart_command(update, ctx)

        # Should report failure
        calls = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("failed" in c.lower() or "Restart" in c for c in calls)

    @pytest.mark.asyncio
    async def test_restart_via_runtime_exception(self):
        from micromech.bot.commands.restart import restart_command

        runtime = MagicMock()
        runtime.restart = AsyncMock(side_effect=RuntimeError("boom"))
        update = _make_update()
        ctx = _make_context(runtime_manager=runtime)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await restart_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_restart_fallback_trigger_file(self, tmp_path):
        from micromech.bot.commands.restart import restart_command

        update = _make_update()
        ctx = _make_context()  # no runtime_manager

        trigger = tmp_path / ".update-request"

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.restart.RESTART_TRIGGER", trigger):
            await restart_command(update, ctx)

        assert trigger.exists()
        assert trigger.read_text() == "restart"

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.restart import restart_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await restart_command(update, ctx)  # must not raise


# ---------------------------------------------------------------------------
# /queue
# ---------------------------------------------------------------------------

class TestQueueCommand:
    @pytest.mark.asyncio
    async def test_no_queue_available(self):
        from micromech.bot.commands.queue_cmd import queue_command

        update = _make_update()
        ctx = _make_context()  # no queue key

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await queue_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "not available" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_with_queue(self):
        from micromech.bot.commands.queue_cmd import queue_command

        queue = MagicMock()
        queue.count_by_status.return_value = {"pending": 1, "delivered": 5}
        queue.count_by_chain.return_value = {"gnosis": 6}
        queue.get_recent.return_value = []
        update = _make_update()
        ctx = _make_context(queue=queue)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await queue_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "Queue" in text

    @pytest.mark.asyncio
    async def test_with_recent_requests(self):
        from micromech.bot.commands.queue_cmd import queue_command

        record = MagicMock()
        record.request.tool = "echo"
        record.request.prompt = "hello world"
        record.request.status = "delivered"
        queue = MagicMock()
        queue.count_by_status.return_value = {}
        queue.count_by_chain.return_value = {}
        queue.get_recent.return_value = [record]
        update = _make_update()
        ctx = _make_context(queue=queue)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await queue_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "echo" in text

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.queue_cmd import queue_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await queue_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_long_prompt_truncated(self):
        from micromech.bot.commands.queue_cmd import queue_command

        record = MagicMock()
        record.request.tool = "echo"
        record.request.prompt = "x" * 100  # longer than 40
        record.request.status = "pending"
        queue = MagicMock()
        queue.count_by_status.return_value = {}
        queue.count_by_chain.return_value = {}
        queue.get_recent.return_value = [record]
        update = _make_update()
        ctx = _make_context(queue=queue)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await queue_command(update, ctx)

        text = update.message.reply_text.call_args[0][0]
        assert "..." in text


# ---------------------------------------------------------------------------
# /update
# ---------------------------------------------------------------------------

class TestUpdateCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.update import update_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await update_command(update, ctx)  # must not raise

    @pytest.mark.asyncio
    async def test_timeout_when_no_result_file(self, tmp_path):
        from micromech.bot.commands.update import update_command

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"
        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.update.TRIGGER_PATH", trigger), \
             patch("micromech.bot.commands.update.RESULT_PATH", result_path), \
             patch("micromech.bot.commands.update.POLL_ATTEMPTS", 1), \
             patch("micromech.bot.commands.update.POLL_INTERVAL", 0), \
             patch("asyncio.sleep", AsyncMock()):
            await update_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        sent_msg.edit_text.assert_called_once()
        assert "Timeout" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_updated_result(self, tmp_path):
        from micromech.bot.commands.update import update_command

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"
        update = _make_update()
        ctx = _make_context()

        # Write result file inside fake sleep so it survives the initial unlink()
        async def fake_sleep(_interval):
            result_path.write_text("updated:0.0.15:0.0.16")

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.update.TRIGGER_PATH", trigger), \
             patch("micromech.bot.commands.update.RESULT_PATH", result_path), \
             patch("micromech.bot.commands.update.POLL_ATTEMPTS", 1), \
             patch("micromech.bot.commands.update.POLL_INTERVAL", 0), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            await update_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        assert "0.0.15" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_current_result(self, tmp_path):
        from micromech.bot.commands.update import update_command

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"
        update = _make_update()
        ctx = _make_context()

        async def fake_sleep(_interval):
            result_path.write_text("current:0.0.16")

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.update.TRIGGER_PATH", trigger), \
             patch("micromech.bot.commands.update.RESULT_PATH", result_path), \
             patch("micromech.bot.commands.update.POLL_ATTEMPTS", 1), \
             patch("micromech.bot.commands.update.POLL_INTERVAL", 0), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            await update_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        assert "latest" in sent_msg.edit_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_error_result(self, tmp_path):
        from micromech.bot.commands.update import update_command

        trigger = tmp_path / ".update-request"
        result_path = tmp_path / ".update-result"
        update = _make_update()
        ctx = _make_context()

        async def fake_sleep(_interval):
            result_path.write_text("error:network failure")

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.update.TRIGGER_PATH", trigger), \
             patch("micromech.bot.commands.update.RESULT_PATH", result_path), \
             patch("micromech.bot.commands.update.POLL_ATTEMPTS", 1), \
             patch("micromech.bot.commands.update.POLL_INTERVAL", 0), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            await update_command(update, ctx)

        sent_msg = update.message.reply_text.return_value
        assert "failed" in sent_msg.edit_text.call_args[0][0]


# ---------------------------------------------------------------------------
# /logs
# ---------------------------------------------------------------------------

class TestLogsCommand:
    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.logs import logs_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await logs_command(update, ctx)

    @pytest.mark.asyncio
    async def test_no_log_files(self):
        from micromech.bot.commands.logs import logs_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.logs._collect_logs", return_value=[]):
            await logs_command(update, ctx)

        calls = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("No log" in c for c in calls)

    @pytest.mark.asyncio
    async def test_sends_zip_document(self):
        from micromech.bot.commands.logs import logs_command

        update = _make_update()
        ctx = _make_context()
        fake_files = [("micromech.log", b"log line 1\nlog line 2\n")]

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.logs._collect_logs", return_value=fake_files), \
             patch("micromech.web.app._redact_sensitive", side_effect=lambda x: x):
            await logs_command(update, ctx)

        update.message.reply_document.assert_called_once()

    @pytest.mark.asyncio
    async def test_zip_too_large(self):
        from micromech.bot.commands.logs import MAX_ZIP_BYTES, logs_command

        update = _make_update()
        ctx = _make_context()
        # Build a mock zip buf that reports huge size
        big_zip = MagicMock()
        big_zip.getbuffer.return_value = MagicMock(nbytes=MAX_ZIP_BYTES + 1)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.commands.logs._collect_logs", return_value=[("a.log", b"x")]), \
             patch("micromech.bot.commands.logs._build_zip", return_value=big_zip), \
             patch("micromech.web.app._redact_sensitive", side_effect=lambda x: x):
            await logs_command(update, ctx)

        calls = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("large" in c.lower() for c in calls)

    def test_collect_logs_no_files(self, tmp_path):
        from micromech.bot.commands.logs import _collect_logs

        with patch("micromech.bot.commands.logs.LOG_FILE", tmp_path / "nonexistent.log"), \
             patch("micromech.bot.commands.logs.LOG_DIR", tmp_path):
            result = _collect_logs()
        assert result == []

    def test_collect_logs_with_existing_file(self, tmp_path):
        from micromech.bot.commands.logs import _collect_logs

        log_file = tmp_path / "micromech.log"
        log_file.write_bytes(b"some logs")

        with patch("micromech.bot.commands.logs.LOG_FILE", log_file), \
             patch("micromech.bot.commands.logs.LOG_DIR", tmp_path):
            result = _collect_logs()

        assert len(result) == 1
        assert result[0][0] == "micromech.log"

    def test_build_zip_content(self):
        from micromech.bot.commands.logs import _build_zip

        files = [("test.log", b"line1\nline2\n")]

        with patch("micromech.web.app._redact_sensitive", side_effect=lambda x: x):
            buf = _build_zip(files)

        buf.seek(0)
        with zipfile.ZipFile(buf) as zf:
            names = zf.namelist()
        assert "test.log" in names


# ---------------------------------------------------------------------------
# /wallet
# ---------------------------------------------------------------------------

class TestWalletCommand:
    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.bot.commands.wallet import wallet_command

        config = make_test_config()
        config.chains = {}
        update = _make_update()
        ctx = _make_context(config=config)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await wallet_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.wallet import wallet_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await wallet_command(update, ctx)

    @pytest.mark.asyncio
    async def test_wallet_info_fetched(self):
        from micromech.bot.commands.wallet import wallet_command

        wallet = MagicMock()
        wallet.master_account.address = "0x" + "ab" * 20
        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}), \
             patch("micromech.bot.commands.wallet.get_wallet", return_value=wallet), \
             patch("micromech.bot.commands.wallet.check_balances", return_value=(1.0, 2.0)), \
             patch("micromech.core.bridge.get_service_info", return_value={}):
            await wallet_command(update, ctx)

        update.message.reply_text.assert_called_with("Fetching wallet info...")

    def test_explorer_link(self):
        from micromech.bot.commands.wallet import _explorer_link

        link = _explorer_link("gnosis", "0xabc", "short")
        assert "gnosisscan" in link
        assert "0xabc" in link

    def test_explorer_link_unknown_chain(self):
        from micromech.bot.commands.wallet import _explorer_link

        link = _explorer_link("unknownchain", "0xabc", "addr")
        # Falls back to gnosis explorer
        assert "gnosisscan" in link


# ---------------------------------------------------------------------------
# /manage
# ---------------------------------------------------------------------------

class TestManageCommand:
    @pytest.mark.asyncio
    async def test_no_chains(self):
        from micromech.bot.commands.manage import manage_command

        config = make_test_config()
        config.chains = {}
        update = _make_update()
        ctx = _make_context(config=config)

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await manage_command(update, ctx)

        assert "No chains" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_shows_chain_keyboard(self):
        from micromech.bot.commands.manage import manage_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await manage_command(update, ctx)

        update.message.reply_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.manage import manage_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await manage_command(update, ctx)

    def test_build_chain_keyboard(self):
        from telegram import InlineKeyboardMarkup

        from micromech.bot.commands.manage import _build_chain_keyboard

        kb = _build_chain_keyboard({"gnosis": MagicMock(), "base": MagicMock()})
        assert isinstance(kb, InlineKeyboardMarkup)

    def test_build_actions_keyboard_staked(self):
        from micromech.bot.commands.manage import _build_actions_keyboard

        kb = _build_actions_keyboard("gnosis", {"staking_state": "STAKED", "is_staked": True})
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("Unstake" in b for b in buttons)

    def test_build_actions_keyboard_evicted(self):
        from micromech.bot.commands.manage import _build_actions_keyboard

        kb = _build_actions_keyboard("gnosis", {"staking_state": "EVICTED", "is_staked": False})
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("Restake" in b for b in buttons)

    def test_build_actions_keyboard_not_staked(self):
        from micromech.bot.commands.manage import _build_actions_keyboard

        kb = _build_actions_keyboard("gnosis", {"staking_state": "NOT_STAKED", "is_staked": False})
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("Stake" in b for b in buttons)


# ---------------------------------------------------------------------------
# /checkpoint
# ---------------------------------------------------------------------------

class TestCheckpointCommand:
    @pytest.mark.asyncio
    async def test_no_staked_services(self):
        from micromech.bot.commands.checkpoint import checkpoint_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.core.bridge.get_service_info", return_value={}):
            await checkpoint_command(update, ctx)

        assert "No staked" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_single_chain_runs_checkpoint(self):
        from micromech.bot.commands.checkpoint import checkpoint_command

        lifecycle = MagicMock()
        lifecycle.checkpoint.return_value = True
        update = _make_update()
        ctx = _make_context(lifecycles={"gnosis": lifecycle})

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.core.bridge.get_service_info",
                   return_value={"service_key": "0xkey"}):
            await checkpoint_command(update, ctx)

        update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_no_message_returns_early(self):
        from micromech.bot.commands.checkpoint import checkpoint_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID):
            await checkpoint_command(update, ctx)

    def test_build_chain_keyboard(self):
        from micromech.bot.commands.checkpoint import _build_chain_keyboard

        kb = _build_chain_keyboard({"gnosis": MagicMock(), "base": MagicMock()})
        # Multi-chain should include "All Chains"
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("All" in b for b in buttons)

    def test_build_chain_keyboard_single(self):
        from micromech.bot.commands.checkpoint import _build_chain_keyboard

        kb = _build_chain_keyboard({"gnosis": MagicMock()})
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        # Single chain: no "All Chains" button
        assert not any(b == "All Chains" for b in buttons)


# ---------------------------------------------------------------------------
# /settings
# ---------------------------------------------------------------------------

class TestSettingsCommand:
    def test_format_settings(self):
        from micromech.bot.commands.settings import _format_settings

        config = make_test_config()
        result = _format_settings(config)
        assert "Settings" in result

    def test_build_settings_keyboard(self):
        from micromech.bot.commands.settings import _build_settings_keyboard

        config = make_test_config()
        kb = _build_settings_keyboard(config)
        buttons = [btn.text for row in kb.inline_keyboard for btn in row]
        assert any("Close" in b for b in buttons)

    def test_get_set_value(self):
        from micromech.bot.commands.settings import _get_value, _set_value

        config = make_test_config()
        _set_value(config, "fund_enabled", True)
        assert _get_value(config, "fund_enabled") is True
        _set_value(config, "fund_enabled", False)
        assert _get_value(config, "fund_enabled") is False


# ---------------------------------------------------------------------------
# bot/app.py — module constants and start_command
# ---------------------------------------------------------------------------

class TestBotApp:
    def test_action_constants_defined(self):
        from micromech.bot import app as bot_app

        assert bot_app.ACTION_STATUS == "status"
        assert bot_app.ACTION_CLAIM == "claim"
        assert bot_app.ACTION_CHECKPOINT == "checkpoint"

    @pytest.mark.asyncio
    async def test_start_command(self):
        from micromech.bot.app import start_command

        update = _make_update()
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await start_command(update, ctx)

        update.message.reply_text.assert_called_once()
        assert "Micromech" in update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_start_command_no_message(self):
        from micromech.bot.app import start_command

        update = _make_update(has_message=False)
        ctx = _make_context()

        with patch("micromech.bot.security.secrets", telegram_chat_id=AUTHORIZED_CHAT_ID), \
             patch("micromech.bot.security._rate_limit_cache", {}):
            await start_command(update, ctx)  # must not raise
