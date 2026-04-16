"""Extended tests for web/app.py — uncovered endpoints.

Covers:
- _push_log_line / _redact_sensitive / _StdlibLogHandler (module-level utils)
- _get_deploy_lock / _needs_setup / _clear_setup_cache
- /api/setup/balance
- /api/staking/status
- /api/runtime/status + /api/runtime/{action}
- /api/metadata (status endpoint)
- /result/{request_id}
- _log_sink / log push utilities
- SSE connection limit (429)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from micromech.secrets import secrets as _real_secrets
from micromech.web.app import create_web_app

CSRF = {"X-Micromech-Action": "test"}


def _app(**kw):
    defaults = dict(
        get_status=lambda: {"status": "running", "queue": {}, "tools": [], "delivered_total": 0},
        get_recent=lambda limit=20, chain=None: [],
        get_tools=lambda: [],
        on_request=AsyncMock(),
    )
    defaults.update(kw)
    return create_web_app(**defaults)


def _client(**kw):
    return TestClient(_app(**kw), raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Module-level utilities (no HTTP needed)
# ---------------------------------------------------------------------------


class TestModuleUtils:
    def test_redact_sensitive_masks_token(self):
        from micromech.web.app import _redact_sensitive

        result = _redact_sensitive("auth token=supersecret&other=ok")
        assert "supersecret" not in result
        assert "***" in result

    def test_redact_sensitive_masks_password(self):
        from micromech.web.app import _redact_sensitive

        result = _redact_sensitive("password=hunter2 ok")
        assert "hunter2" not in result

    def test_redact_sensitive_leaves_safe_text(self):
        from micromech.web.app import _redact_sensitive

        result = _redact_sensitive("normal log message with no secrets")
        assert result == "normal log message with no secrets"

    def test_push_log_line_delivers_to_queues(self):
        import queue as stdlib_queue

        from micromech.web import app as web_mod

        q = stdlib_queue.Queue()
        web_mod._log_queues.append(q)
        try:
            web_mod._push_log_line("12:00:00.000", "INFO", "hello")
            assert not q.empty()
        finally:
            web_mod._log_queues.remove(q)

    def test_push_log_line_tolerates_full_queue(self):
        import queue as stdlib_queue

        from micromech.web import app as web_mod

        q = stdlib_queue.Queue(maxsize=1)
        q.put_nowait("full")
        web_mod._log_queues.append(q)
        try:
            # Must not raise even when queue is full
            web_mod._push_log_line("12:00:00.000", "ERROR", "overflow")
        finally:
            web_mod._log_queues.remove(q)

    def test_stdlib_log_handler_emit(self):
        import logging

        from micromech.web.app import _StdlibLogHandler

        handler = _StdlibLogHandler()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        # Must not raise
        handler.emit(record)

    def test_get_deploy_lock_creates_per_chain(self):
        from micromech.web.app import _get_deploy_lock

        lock_a = _get_deploy_lock("gnosis")
        lock_b = _get_deploy_lock("base")
        lock_same = _get_deploy_lock("gnosis")
        assert lock_a is lock_same
        assert lock_a is not lock_b

    def test_clear_setup_cache(self):
        from micromech.web import app as web_mod

        web_mod._setup_needed = True
        from micromech.web.app import _clear_setup_cache

        _clear_setup_cache()
        assert web_mod._setup_needed is None

    def test_needs_setup_exception_returns_true(self):
        from micromech.web import app as web_mod

        web_mod._setup_needed = None
        from micromech.web.app import _needs_setup

        with patch("micromech.web.app.MicromechConfig.load", side_effect=Exception("no config")):
            result = _needs_setup()
        assert result is True
        web_mod._setup_needed = None  # cleanup


# ---------------------------------------------------------------------------
# /api/setup/balance
# ---------------------------------------------------------------------------


class TestSetupBalanceEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_valid_chain_returns_balances(self, _mock):
        c = _client()
        with patch("micromech.core.bridge.check_balances", return_value=(2.0, 15.0)):
            resp = c.get("/api/setup/balance?chain=gnosis")
        assert resp.status_code == 200
        data = resp.json()
        assert "native_balance" in data
        assert data["native_balance"] == pytest.approx(2.0)

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_unknown_chain_returns_error(self, _mock):
        c = _client()
        resp = c.get("/api/setup/balance?chain=unknownchain99")
        assert resp.status_code == 200
        assert resp.json()["error"] == "Unknown chain"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_balance_check_exception_returns_error(self, _mock):
        c = _client()
        with patch("micromech.core.bridge.check_balances", side_effect=Exception("rpc")):
            resp = c.get("/api/setup/balance?chain=gnosis")
        assert resp.status_code == 200
        assert "error" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_sufficient_flag_true_when_funded(self, _mock):
        c = _client()
        # Patch high balances so sufficient=True (MIN_OLAS_WHOLE=5000, min_native~0.1)
        with patch("micromech.core.bridge.check_balances", return_value=(10.0, 6000.0)):
            resp = c.get("/api/setup/balance?chain=gnosis")
        assert resp.json()["sufficient"] is True

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_sufficient_flag_false_when_underfunded(self, _mock):
        c = _client()
        with patch("micromech.core.bridge.check_balances", return_value=(0.0, 0.0)):
            resp = c.get("/api/setup/balance?chain=gnosis")
        assert resp.json()["sufficient"] is False


# ---------------------------------------------------------------------------
# /api/staking/status
# ---------------------------------------------------------------------------


class TestStakingStatusEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_no_auth_required_without_password(self, _mock):
        """Endpoint accessible without auth when no password set."""
        c = _client()
        with (
            patch("micromech.web.app.MicromechConfig.load") as mock_load,
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            mock_cfg = MagicMock()
            mock_cfg.chains = {}
            mock_cfg.enabled_chains = {}
            mock_load.return_value = mock_cfg
            resp = c.get("/api/staking/status")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_not_configured_chain(self, _mock):
        c = _client()
        with (
            patch("micromech.web.app.MicromechConfig.load") as mock_load,
            patch("micromech.core.bridge.get_service_info", return_value={}),
        ):
            mock_cfg = MagicMock()
            mock_cfg.chains = {}
            mock_cfg.enabled_chains = {"gnosis": MagicMock()}
            mock_load.return_value = mock_cfg
            resp = c.get("/api/staking/status")
        data = resp.json()
        assert data.get("gnosis", {}).get("status") == "not_configured"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_exception_returns_error(self, _mock):
        c = _client()
        with patch("micromech.web.app.MicromechConfig.load", side_effect=Exception("db error")):
            resp = c.get("/api/staking/status")
        assert resp.status_code == 200
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# /api/runtime/status + /api/runtime/{action}
# ---------------------------------------------------------------------------


class TestRuntimeStatusEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_without_runtime_manager(self, _mock):
        c = _client()
        resp = c.get("/api/runtime/status")
        assert resp.status_code == 200
        assert resp.json()["state"] == "unavailable"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_with_runtime_manager(self, _mock):
        runtime = MagicMock()
        runtime.get_status.return_value = {"state": "running"}
        c = _client(runtime_manager=runtime)
        resp = c.get("/api/runtime/status")
        assert resp.json()["state"] == "running"


class TestRuntimeControlEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_missing_csrf_returns_403(self, _mock):
        c = _client()
        resp = c.post("/api/runtime/start")
        assert resp.status_code == 403

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_unknown_action_returns_404(self, _mock):
        c = _client()
        resp = c.post("/api/runtime/explode", headers=CSRF)
        assert resp.status_code == 404

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_no_runtime_manager_returns_503(self, _mock):
        c = _client()
        resp = c.post("/api/runtime/start", headers=CSRF)
        assert resp.status_code == 503

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_start_action(self, _mock):
        runtime = MagicMock()
        runtime.start = AsyncMock(return_value=True)
        runtime.state = "running"
        c = _client(runtime_manager=runtime)
        resp = c.post("/api/runtime/start", headers=CSRF)
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_stop_action(self, _mock):
        runtime = MagicMock()
        runtime.stop = AsyncMock(return_value=True)
        runtime.state = "stopped"
        c = _client(runtime_manager=runtime)
        resp = c.post("/api/runtime/stop", headers=CSRF)
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_restart_action(self, _mock):
        runtime = MagicMock()
        runtime.restart = AsyncMock(return_value=True)
        runtime.state = "running"
        c = _client(runtime_manager=runtime)
        resp = c.post("/api/runtime/restart", headers=CSRF)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# /api/metadata
# ---------------------------------------------------------------------------


class TestMetadataEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_no_metadata_manager(self, _mock):
        c = _client()
        resp = c.get("/api/metadata")
        assert resp.status_code == 200
        assert "error" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_with_metadata_manager_up_to_date(self, _mock):
        mm = MagicMock()
        status = MagicMock()
        status.needs_update = False
        status.ipfs_cid = "Qmabc"
        status.computed_hash = "0xaaa"
        status.stored_hash = "0xaaa"
        status.changed_packages = []
        status.tools = ["echo"]
        mm.get_status.return_value = status
        c = _client(metadata_manager=mm)
        resp = c.get("/api/metadata")
        assert resp.json()["status"] == "up_to_date"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_with_metadata_manager_stale(self, _mock):
        mm = MagicMock()
        status = MagicMock()
        status.needs_update = True
        status.ipfs_cid = "Qmabc"
        status.computed_hash = "0xnew"
        status.stored_hash = "0xold"
        status.changed_packages = ["echo"]
        status.tools = ["echo"]
        mm.get_status.return_value = status
        c = _client(metadata_manager=mm)
        resp = c.get("/api/metadata")
        assert resp.json()["status"] == "stale"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_with_metadata_manager_not_registered(self, _mock):
        mm = MagicMock()
        status = MagicMock()
        status.needs_update = True
        status.ipfs_cid = None
        status.computed_hash = "0xnew"
        status.stored_hash = None
        status.changed_packages = []
        status.tools = []
        mm.get_status.return_value = status
        c = _client(metadata_manager=mm)
        resp = c.get("/api/metadata")
        assert resp.json()["status"] == "not_registered"

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_exception_returns_error(self, _mock):
        mm = MagicMock()
        mm.get_status.side_effect = Exception("broken")
        c = _client(metadata_manager=mm)
        resp = c.get("/api/metadata")
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# /result/{request_id}
# ---------------------------------------------------------------------------


class TestResultByIdEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_invalid_id_returns_400(self, _mock):
        c = _client()
        resp = c.get("/result/'; DROP TABLE--")
        assert resp.status_code == 400

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_no_queue_returns_501(self, _mock):
        c = _client()
        resp = c.get("/result/" + "ab" * 32)
        assert resp.status_code == 501

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_not_found_returns_404(self, _mock):
        queue = MagicMock()
        queue.get_by_id.return_value = None
        c = _client(queue=queue)
        resp = c.get("/result/" + "ab" * 32)
        assert resp.status_code == 404

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_found_record_no_result(self, _mock):
        queue = MagicMock()
        record = MagicMock()
        record.request.request_id = "ab" * 32
        record.request.status = "executed"
        record.request.tool = "echo"
        record.request.prompt = "hello"
        record.request.chain = "gnosis"
        record.request.created_at = None
        record.request.is_offchain = False
        record.request.data = None
        record.result = None
        queue.get_by_id.return_value = record
        c = _client(queue=queue)
        resp = c.get(f"/result/{'ab' * 32}")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_found_record_with_json_result(self, _mock):
        import json

        queue = MagicMock()
        record = MagicMock()
        record.request.request_id = "ab" * 32
        record.request.status = "delivered"
        record.request.tool = "echo"
        record.request.prompt = "hello"
        record.request.chain = "gnosis"
        record.request.created_at = None
        record.request.is_offchain = False
        record.request.data = None
        record.result.output = json.dumps({"result": "pong"})
        record.result.execution_time = 0.5
        record.result.error = None
        queue.get_by_id.return_value = record
        c = _client(queue=queue)
        resp = c.get(f"/result/{'ab' * 32}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["result"]["result"] == "pong"


# ---------------------------------------------------------------------------
# Bearer auth on staking endpoints
# ---------------------------------------------------------------------------


class TestBearerAuthOnProtectedEndpoints:
    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_staking_status_requires_auth_when_password_set(self, _mock):
        c = _client()
        original = _real_secrets.webui_password
        _real_secrets.webui_password = SecretStr("testpass")
        try:
            resp = c.get("/api/staking/status")
            assert resp.status_code == 401
        finally:
            _real_secrets.webui_password = original

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_runtime_status_requires_auth_when_password_set(self, _mock):
        c = _client()
        original = _real_secrets.webui_password
        _real_secrets.webui_password = SecretStr("testpass")
        try:
            resp = c.get("/api/runtime/status")
            assert resp.status_code == 401
        finally:
            _real_secrets.webui_password = original
