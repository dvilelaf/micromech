"""Additional tests for web/app.py — covering missed lines.

Targets:
- _get_sse_semaphore (66-68)
- _StdlibLogHandler exception path (126-127)
- _needs_setup cached path (192-197)
- Token query param auth (280)
- setup_state: rate-limited, wallet cache, step logic (338, 350-398)
- setup_wallet: password too long, wallet fail, PermissionError (430, 447-448, 507-509)
- setup_secrets GET/POST (518, 528-530, 541, 559-561)
- api_setup_tools + tools_save missing CSRF (745-753, 770, 775)
- karma_status endpoint (1105-1175)
- management_action service_key fallback (1261-1264)
- metrics endpoints with queue data (961, 969, 976, 983, 990)
- health_check with metrics + chains (1186-1188, 1194)
- result with non-JSON output (937-938)
- _record_to_dict IPFS CID exception (1355-1356)
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from pydantic import SecretStr

from micromech.secrets import secrets as _real_secrets
from micromech.web.app import _get_sse_semaphore, _StdlibLogHandler, create_web_app

CSRF = {"X-Micromech-Action": "test"}


def _app(**kw):
    defaults = dict(
        get_status=lambda: {
            "status": "running",
            "queue": {"pending": 1},
            "tools": [],
            "delivered_total": 5,
        },
        get_recent=lambda limit=20, chain=None: [],
        get_tools=lambda: [],
        on_request=AsyncMock(),
    )
    defaults.update(kw)
    return create_web_app(**defaults)


def _client(**kw):
    return TestClient(_app(**kw), raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# _get_sse_semaphore — lazy init
# ---------------------------------------------------------------------------

class TestGetSseSemaphore:
    def test_returns_semaphore(self):
        import micromech.web.app as app_mod
        # Reset so we can test the branch
        original = app_mod._sse_semaphore
        app_mod._sse_semaphore = None
        try:
            sem = _get_sse_semaphore()
            assert sem is not None
        finally:
            app_mod._sse_semaphore = original

    def test_returns_same_instance(self):
        sem1 = _get_sse_semaphore()
        sem2 = _get_sse_semaphore()
        assert sem1 is sem2


# ---------------------------------------------------------------------------
# _StdlibLogHandler emit exception path
# ---------------------------------------------------------------------------

class TestStdlibLogHandlerException:
    def test_emit_exception_is_swallowed(self):
        handler = _StdlibLogHandler()
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname="", lineno=0, msg="hello", args=(), exc_info=None
        )
        # Make record.created invalid to trigger exception
        record.created = "not-a-float"
        # Should not raise
        handler.emit(record)


# ---------------------------------------------------------------------------
# _needs_setup — cached path
# ---------------------------------------------------------------------------

class TestNeedsSetupCached:
    def test_uses_cached_false(self):
        import micromech.web.app as app_mod
        original = app_mod._setup_needed
        app_mod._setup_needed = False
        try:
            # Should return cached value without calling load
            result = app_mod._needs_setup()
            assert result is False
        finally:
            app_mod._setup_needed = original

    def test_uses_cached_true(self):
        import micromech.web.app as app_mod
        original = app_mod._setup_needed
        app_mod._setup_needed = True
        try:
            result = app_mod._needs_setup()
            assert result is True
        finally:
            app_mod._setup_needed = original


# ---------------------------------------------------------------------------
# Token query param auth (?token=)
# ---------------------------------------------------------------------------

class TestTokenQueryParamAuth:
    def test_valid_token_param_allows_access(self):
        c = _client()
        _real_secrets.webui_password = SecretStr("securepass")
        try:
            resp = c.get("/api/status?token=securepass")
            assert resp.status_code == 200
        finally:
            _real_secrets.webui_password = None


# ---------------------------------------------------------------------------
# /api/setup/state
# ---------------------------------------------------------------------------

class TestSetupStateEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_rate_limited_returns_429(self, _mock):
        """Rate limiting applies to setup/state."""
        from micromech.web import app as app_mod
        c = _client()
        # Patch rate limiter to always return True
        with patch.object(app_mod, "_rate_limited", return_value=True):
            resp = c.get("/api/setup/state")
        assert resp.status_code == 429
        # rate_limit dependency raises HTTPException → {"detail": "..."}
        assert "detail" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_with_cached_key_storage(self, _mock):
        """setup_state reports wallet_exists=True when _cached_key_storage is set."""
        c = _client()
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0xABCDEF1234567890AbCdEf1234567890aBcDeF12"
        with patch("micromech.core.bridge._cached_key_storage", mock_ks), \
             patch("micromech.core.bridge._cached_wallet", None), \
             patch("micromech.core.config.MicromechConfig.load", side_effect=Exception("no cfg")):
            resp = c.get("/api/setup/state")
        assert resp.status_code == 200
        data = resp.json()
        assert data["wallet_exists"] is True
        assert data["wallet_address"] is not None

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_with_cached_wallet(self, _mock):
        """setup_state reports wallet_exists=True when _cached_wallet is set."""
        c = _client()
        mock_wallet = MagicMock()
        mock_wallet.master_account.address = "0xABCDEF1234567890AbCdEf1234567890aBcDeF12"
        with patch("micromech.core.bridge._cached_key_storage", None), \
             patch("micromech.core.bridge._cached_wallet", mock_wallet), \
             patch("micromech.core.config.MicromechConfig.load", side_effect=Exception("no cfg")):
            resp = c.get("/api/setup/state")
        assert resp.status_code == 200
        data = resp.json()
        assert data["wallet_exists"] is True

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_step_complete_when_chain_setup(self, _mock):
        """Step = 'complete' when at least one chain is set up."""
        c = _client()
        mock_cfg = MagicMock()
        chain_cfg = MagicMock()
        chain_cfg.setup_complete = True
        chain_cfg.detect_setup_state.return_value = "complete"
        chain_cfg.mech_address = "0x1234"
        mock_cfg.chains = {"gnosis": chain_cfg}
        with patch("micromech.core.bridge._cached_key_storage", None), \
             patch("micromech.core.bridge._cached_wallet", MagicMock()), \
             patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg), \
             patch("micromech.core.bridge.get_service_info", return_value={
                 "service_id": 1, "service_key": "0xabc", "multisig_address": "0xdef"
             }):
            resp = c.get("/api/setup/state")
        assert resp.status_code == 200
        assert resp.json()["step"] == "complete"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_step_deploy_when_chains_but_not_setup(self, _mock):
        """Step = 'deploy' when chains exist but none complete."""
        c = _client()
        mock_cfg = MagicMock()
        chain_cfg = MagicMock()
        chain_cfg.setup_complete = False
        chain_cfg.detect_setup_state.return_value = "pending"
        chain_cfg.mech_address = None
        mock_cfg.chains = {"gnosis": chain_cfg}
        mock_wallet = MagicMock()
        mock_wallet.master_account.address = "0xAAA"
        with patch("micromech.core.bridge._cached_key_storage", None), \
             patch("micromech.core.bridge._cached_wallet", mock_wallet), \
             patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg), \
             patch("micromech.core.bridge.get_service_info", return_value={}):
            resp = c.get("/api/setup/state")
        assert resp.status_code == 200
        assert resp.json()["step"] == "deploy"


# ---------------------------------------------------------------------------
# /api/setup/wallet
# ---------------------------------------------------------------------------

class TestSetupWalletEndpoint:
    def test_password_too_long_returns_400(self):
        c = _client()
        resp = c.post(
            "/api/setup/wallet",
            json={"password": "x" * 129},
            headers=CSRF,
        )
        assert resp.status_code == 400
        assert "too long" in resp.json()["error"]

    def test_wallet_create_failure_returns_500(self):
        c = _client()
        with patch("asyncio.to_thread", side_effect=Exception("disk full")):
            resp = c.post(
                "/api/setup/wallet",
                json={"password": "validpassword"},
                headers=CSRF,
            )
        assert resp.status_code == 500

    def test_permission_error_returns_403(self):
        c = _client()
        with patch("asyncio.to_thread", side_effect=PermissionError("bad pw")):
            resp = c.post(
                "/api/setup/wallet",
                json={"password": "validpassword"},
                headers=CSRF,
            )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# /api/setup/secrets
# ---------------------------------------------------------------------------

class TestSetupSecretsEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_get_rate_limited(self, _mock):
        from micromech.web import app as app_mod
        c = _client()
        with patch.object(app_mod, "_rate_limited", return_value=True):
            resp = c.get("/api/setup/secrets")
        assert resp.status_code == 429
        assert "detail" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_get_exception_returns_500(self, _mock):
        c = _client()
        with patch("micromech.core.secrets_file.read_secrets_file", side_effect=Exception("io")):
            resp = c.get("/api/setup/secrets")
        assert resp.status_code == 500
        assert "error" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_post_rate_limited(self, _mock):
        from micromech.web import app as app_mod
        c = _client()
        with patch.object(app_mod, "_rate_limited", return_value=True):
            resp = c.post("/api/setup/secrets", json={}, headers=CSRF)
        assert resp.status_code == 429
        assert "detail" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_post_exception_returns_500(self, _mock):
        c = _client()
        with patch("micromech.core.secrets_file.write_secrets", side_effect=Exception("io")):
            resp = c.post("/api/setup/secrets", json={"telegram_bot_token": "abc"}, headers=CSRF)
        assert resp.status_code == 500
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# /api/setup/tools
# ---------------------------------------------------------------------------

class TestSetupToolsEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_get_tools_list(self, _mock):
        """GET /api/setup/tools returns list of tool packages."""
        c = _client()
        mock_tools = [
            {"name": "web_search", "description": "Search", "version": "1.0",
             "allowed_tools": ["web_search"], "source": "builtin"},
        ]
        mock_cfg = MagicMock()
        mock_cfg.disabled_tools = []
        with patch("micromech.ipfs.metadata.scan_tool_packages", return_value=mock_tools), \
             patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg):
            resp = c.get("/api/setup/tools")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["name"] == "web_search"
        assert data[0]["enabled"] is True

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_post_tools_missing_csrf_returns_403(self, _mock):
        """POST /api/setup/tools without CSRF header returns 403."""
        c = _client()
        resp = c.post("/api/setup/tools", json={"disabled_tools": []})
        assert resp.status_code == 403

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_post_tools_invalid_list_returns_400(self, _mock):
        """POST /api/setup/tools with non-list disabled_tools returns 400."""
        c = _client()
        resp = c.post("/api/setup/tools", json={"disabled_tools": "not-a-list"}, headers=CSRF)
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# /api/karma
# ---------------------------------------------------------------------------

class TestKarmaEndpoint:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_no_mech_address_returns_error(self, _mock):
        """Chain without mech_address returns karma=None."""
        mock_cfg = MagicMock()
        chain_cfg = MagicMock()
        chain_cfg.mech_address = None
        mock_cfg.enabled_chains = {"gnosis": chain_cfg}
        with patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg):
            c = _client()
            resp = c.get("/api/karma")
        assert resp.status_code == 200
        data = resp.json()
        assert "gnosis" in data
        assert data["gnosis"]["karma"] is None

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_bridge_exception_returns_error(self, _mock):
        """Exception in karma check returns error key."""
        mock_cfg = MagicMock()
        chain_cfg = MagicMock()
        chain_cfg.mech_address = "0x1234"
        chain_cfg.marketplace_address = "0xabc"
        mock_cfg.enabled_chains = {"gnosis": chain_cfg}
        with patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg), \
             patch("micromech.core.bridge.IwaBridge", side_effect=Exception("rpc")):
            c = _client()
            resp = c.get("/api/karma")
        assert resp.status_code == 200
        data = resp.json()
        assert "gnosis" in data
        assert data["gnosis"]["karma"] is None

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_outer_exception_returns_error(self, _mock):
        """Exception in thread returns error dict."""
        with patch("asyncio.to_thread", side_effect=Exception("thread fail")):
            c = _client()
            resp = c.get("/api/karma")
        assert resp.status_code == 200
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# /api/health with metrics
# ---------------------------------------------------------------------------

class TestHealthCheckWithMetrics:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_health_includes_metrics_when_provided(self, _mock):
        """Health check includes uptime/requests/deliveries from metrics."""
        mock_metrics = MagicMock()
        mock_metrics.uptime_seconds = 3600
        mock_metrics.requests_received = 42
        mock_metrics.deliveries_completed = 38

        def get_status():
            return {"status": "running", "queue": {}, "chains": ["gnosis"], "delivered_total": 38}

        c = TestClient(_app(get_status=get_status, metrics=mock_metrics), raise_server_exceptions=False)
        resp = c.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["uptime"] == 3600
        assert data["requests_received"] == 42
        assert data["deliveries_completed"] == 38

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_health_includes_chains(self, _mock):
        """Health check populates per-chain status."""

        def get_status():
            return {"status": "running", "queue": {}, "chains": ["gnosis", "base"], "delivered_total": 0}

        c = TestClient(_app(get_status=get_status), raise_server_exceptions=False)
        resp = c.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "gnosis" in data["chains"]
        assert "base" in data["chains"]


# ---------------------------------------------------------------------------
# Metrics endpoints with queue data
# ---------------------------------------------------------------------------

class TestMetricsEndpointsWithQueue:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_metrics_events_with_since(self, _mock):
        """GET /api/metrics/events?since= calls get_events_since."""
        mock_metrics = MagicMock()
        mock_metrics.get_events_since.return_value = [{"ts": 1.0, "type": "request"}]
        c = TestClient(_app(metrics=mock_metrics), raise_server_exceptions=False)
        resp = c.get("/api/metrics/events?since=1000.0")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_metrics_tools_with_queue(self, _mock):
        """GET /api/metrics/tools returns queue.tool_stats()."""
        mock_queue = MagicMock()
        mock_queue.tool_stats.return_value = [{"tool": "web_search", "count": 5}]
        c = TestClient(_app(queue=mock_queue), raise_server_exceptions=False)
        resp = c.get("/api/metrics/tools")
        assert resp.status_code == 200
        assert resp.json()[0]["tool"] == "web_search"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_metrics_daily_with_queue(self, _mock):
        """GET /api/metrics/daily returns queue.daily_stats()."""
        mock_queue = MagicMock()
        mock_queue.daily_stats.return_value = [{"date": "2024-01-01", "count": 3}]
        c = TestClient(_app(queue=mock_queue), raise_server_exceptions=False)
        resp = c.get("/api/metrics/daily")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_metrics_monthly_with_queue(self, _mock):
        """GET /api/metrics/monthly returns queue.monthly_stats()."""
        mock_queue = MagicMock()
        mock_queue.monthly_stats.return_value = [{"month": "2024-01", "count": 10}]
        c = TestClient(_app(queue=mock_queue), raise_server_exceptions=False)
        resp = c.get("/api/metrics/monthly")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_metrics_channels_with_queue(self, _mock):
        """GET /api/metrics/channels returns queue.onchain_offchain_counts()."""
        mock_queue = MagicMock()
        mock_queue.onchain_offchain_counts.return_value = {"onchain": 8, "offchain": 2}
        c = TestClient(_app(queue=mock_queue), raise_server_exceptions=False)
        resp = c.get("/api/metrics/channels")
        assert resp.status_code == 200
        assert resp.json()["onchain"] == 8


# ---------------------------------------------------------------------------
# /api/management — service_key fallback from service info
# ---------------------------------------------------------------------------

class TestManagementServiceKeyFallback:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_service_key_from_bridge_when_not_in_body(self, _mock):
        """Management action falls back to get_service_info for service_key."""
        mock_cfg = MagicMock()
        mock_lc = MagicMock()
        mock_lc.stake.return_value = True
        with patch("micromech.core.config.MicromechConfig.load", return_value=mock_cfg), \
             patch("micromech.management.MechLifecycle", return_value=mock_lc), \
             patch("micromech.core.bridge.get_service_info", return_value={"service_key": "0xabc123"}):
            c = _client()
            resp = c.post(
                "/api/management/stake",
                json={"chain": "gnosis"},  # No service_key in body
                headers=CSRF,
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True


# ---------------------------------------------------------------------------
# /result/{id} — non-JSON output path
# ---------------------------------------------------------------------------

class TestResultNonJsonOutput:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_non_json_result_output_returns_raw(self, _mock):
        """Result with non-JSON output returns raw string."""
        mock_result = MagicMock()
        mock_result.output = "plain text result"
        mock_record = MagicMock()
        mock_record.request.request_id = "0xabc123"
        mock_record.request.chain = "gnosis"
        mock_record.request.status = "done"
        mock_record.request.tool = "web_search"
        mock_record.request.prompt = "test prompt"
        mock_record.request.created_at = None
        mock_record.request.is_offchain = False
        mock_record.request.data = None
        mock_record.result = mock_result
        mock_queue = MagicMock()
        mock_queue.get_by_id.return_value = mock_record
        c = TestClient(_app(queue=mock_queue), raise_server_exceptions=False)
        resp = c.get("/result/0xabc123")
        assert resp.status_code == 200
        data = resp.json()
        assert "result" in data
        assert data["result"]["raw"] == "plain text result"


# ---------------------------------------------------------------------------
# /api/setup/deploy — already in progress
# ---------------------------------------------------------------------------

class TestSetupDeployInProgress:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_deploy_already_in_progress_returns_409(self, _mock):
        """Second deploy to same chain returns 409."""
        import asyncio

        from micromech.web import app as app_mod

        c = _client()
        # Create a locked asyncio.Lock by patching _get_deploy_lock
        locked = asyncio.Lock()

        async def _lock_it():
            await locked.acquire()

        asyncio.run(_lock_it())
        try:
            with patch.object(app_mod, "_get_deploy_lock", return_value=locked):
                resp = c.post(
                    "/api/setup/deploy",
                    json={"chain": "gnosis"},
                    headers=CSRF,
                )
            assert resp.status_code == 409
        finally:
            locked.release()
