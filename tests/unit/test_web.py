"""Tests for the web UI."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.core.models import MechRequest, MechResponse, RequestRecord, ToolResult
from micromech.web.app import create_web_app, get_auth_token

AUTH_TOKEN = get_auth_token()


def _make_record(req_id: str, status: str = "pending", tool: str = "echo") -> RequestRecord:
    req = MechRequest.model_construct(
        request_id=req_id,
        status=status,
        tool=tool,
        prompt=f"prompt for {req_id}",
        sender="",
        data=b"",
        extra_params={},
        created_at=None,
        timeout=300,
        delivery_method="marketplace",
        is_offchain=False,
        error=None,
    )
    result = None
    if status in ("executed", "delivered"):
        result = ToolResult(output="ok", execution_time=1.23)
    return RequestRecord.model_construct(request=req, result=result, response=None, updated_at=None)


@pytest.fixture
def web_client() -> TestClient:
    def get_status():
        return {
            "status": "running",
            "queue": {"pending": 2, "executing": 1, "executed": 0, "delivered": 10, "failed": 0},
            "tools": ["echo", "llm"],
            "delivered_total": 10,
        }

    def get_recent(limit, chain=None):
        return [
            _make_record("r1", "pending"),
            _make_record("r2", "executed", "llm"),
            _make_record("r3", "delivered"),
        ]

    def get_tools():
        return [{"id": "echo", "version": "0.1.0"}, {"id": "llm", "version": "0.1.0"}]

    async def on_request(req):
        pass

    app = create_web_app(get_status, get_recent, get_tools, on_request)
    return TestClient(app)


class TestDashboard:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_renders_html(self, mock_setup, web_client: TestClient):
        resp = web_client.get(f"/?token={AUTH_TOKEN}")
        assert resp.status_code == 200
        assert "micromech" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_tabs(self, mock_setup, web_client: TestClient):
        resp = web_client.get(f"/?token={AUTH_TOKEN}")
        assert "Overview" in resp.text
        assert "Live Activity" in resp.text
        assert "Charts" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_chart_js(self, mock_setup, web_client: TestClient):
        resp = web_client.get(f"/?token={AUTH_TOKEN}")
        assert "chart.js" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_sse_connection(self, mock_setup, web_client: TestClient):
        resp = web_client.get(f"/?token={AUTH_TOKEN}")
        assert "EventSource" in resp.text
        assert "/api/metrics/stream" in resp.text

    def test_redirects_to_setup_when_not_configured(self, web_client: TestClient):
        with patch("micromech.web.app._needs_setup", return_value=True):
            resp = web_client.get(
                f"/?token={AUTH_TOKEN}",
                follow_redirects=False,
            )
            assert resp.status_code == 302
            assert "/setup" in resp.headers["location"]

    def test_setup_page_renders(self, web_client: TestClient):
        resp = web_client.get(f"/setup?token={AUTH_TOKEN}")
        assert resp.status_code == 200
        assert "setup" in resp.text.lower()
        assert "micromech" in resp.text.lower()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_dashboard_js_does_not_reference_undefined_auth_token(
        self, mock_setup, web_client: TestClient
    ):
        """Regression: dashboard.html must use authHeaders() / authQueryParam(),
        never a bare AUTH_TOKEN identifier (which is not declared)."""
        resp = web_client.get(f"/?token={AUTH_TOKEN}")
        assert resp.status_code == 200
        # A bare AUTH_TOKEN reference would be a ReferenceError in the browser.
        # Rule out `X-Auth-Token:` header literals and the authHeaders helper.
        import re

        matches = re.findall(r"(?<![\w-])AUTH_TOKEN(?![\w-])", resp.text)
        assert matches == [], (
            f"dashboard.html references undefined AUTH_TOKEN identifier "
            f"({len(matches)} occurrences). Use authHeaders() instead."
        )


class TestAPIEndpoints:
    def test_api_status(self, web_client: TestClient):
        resp = web_client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["queue"]["pending"] == 2

    def test_api_requests(self, web_client: TestClient):
        resp = web_client.get("/api/requests?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["request_id"] == "r1"
        assert data[1]["tool"] == "llm"

    def test_api_tools(self, web_client: TestClient):
        resp = web_client.get("/api/tools")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["id"] == "echo"


class TestMetricsAPI:
    """Test /api/metrics/* endpoints."""

    def test_metrics_live(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/live")
        assert resp.status_code == 200
        data = resp.json()
        assert "queue" in data
        assert "delivered_total" in data

    def test_metrics_events_empty(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/events")
        assert resp.status_code == 200
        # No metrics collector wired, returns []
        assert resp.json() == []

    def test_metrics_tools_no_queue(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/tools")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_metrics_daily_no_queue(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/daily")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_metrics_monthly_no_queue(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/monthly")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_metrics_channels_no_queue(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/channels")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"onchain": 0, "offchain": 0}

    def test_metrics_stream_endpoint_exists(self, web_client: TestClient):
        """SSE endpoint is registered and reachable."""
        # We can't easily test a streaming infinite generator in sync tests.
        # Verify the route exists by checking the app routes.
        routes = [r.path for r in web_client.app.routes]
        assert "/api/metrics/stream" in routes


class TestMetricsAPIWithCollector:
    """Test metrics API with a real MetricsCollector."""

    def test_live_with_metrics(self):
        from micromech.runtime.metrics import MetricsCollector

        mc = MetricsCollector()
        mc.record_request_received("r1", "echo", False)

        def get_status():
            return {
                "queue": {"pending": 1},
                "delivered_total": 0,
                "metrics": mc.get_live_snapshot(),
            }

        app = create_web_app(get_status, lambda lim: [], lambda: [], lambda r: None, metrics=mc)
        client = TestClient(app)

        resp = client.get("/api/metrics/live")
        data = resp.json()
        assert data["live"]["requests_received"] == 1

    def test_events_with_metrics(self):
        from micromech.runtime.metrics import MetricsCollector

        mc = MetricsCollector()
        mc.record_request_received("r1", "echo", False)
        mc.record_execution_done("r1", "echo", 1.5)

        app = create_web_app(
            lambda: {"queue": {}, "delivered_total": 0},
            lambda lim: [],
            lambda: [],
            lambda r: None,
            metrics=mc,
        )
        client = TestClient(app)

        resp = client.get("/api/metrics/events?limit=10")
        data = resp.json()
        assert len(data) == 2
        assert data[0]["event"] == "request_received"
        assert data[1]["event"] == "execution_done"


class TestChainAPI:
    """Test chain-related API endpoints."""

    def test_api_chains(self, web_client: TestClient):
        resp = web_client.get("/api/chains")
        assert resp.status_code == 200
        data = resp.json()
        assert "echo" in data or "gnosis" in data or isinstance(data, list)

    def test_api_requests_with_chain_filter(self, web_client: TestClient):
        resp = web_client.get("/api/requests?limit=10&chain=gnosis")
        assert resp.status_code == 200

    def test_api_metrics_tools_with_chain(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/tools?chain=gnosis")
        assert resp.status_code == 200

    def test_api_metrics_daily_with_chain(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/daily?chain=gnosis")
        assert resp.status_code == 200

    def test_api_metrics_monthly_with_chain(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/monthly?chain=gnosis")
        assert resp.status_code == 200

    def test_api_metrics_channels_with_chain(self, web_client: TestClient):
        resp = web_client.get("/api/metrics/channels?chain=gnosis")
        assert resp.status_code == 200

    def test_record_to_dict_includes_chain(self):
        from micromech.web.app import _record_to_dict

        record = _make_record("r1", "pending")
        d = _record_to_dict(record)
        assert "chain" in d


class TestManagementAPI:
    """Test the /api/management/{action} endpoint."""

    CSRF = {"X-Micromech-Action": "test", "X-Auth-Token": AUTH_TOKEN}

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_stake_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.stake.return_value = True
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["action"] == "stake"

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_unstake_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.unstake.return_value = True
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/unstake",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_claim_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.claim_rewards.return_value = True
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/claim",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["action"] == "claim"

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_checkpoint_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.checkpoint.return_value = True
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/checkpoint",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_status_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.get_status.return_value = {"service_id": 42}
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/status",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["data"]["service_id"] == 42

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_unknown_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        resp = web_client.post(
            "/api/management/bogus",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        assert "Unknown action" in data["error"]

    @patch("micromech.web.app.MicromechConfig")
    def test_management_exception(self, mock_cfg_cls, web_client: TestClient):
        mock_cfg_cls.load.side_effect = RuntimeError("config error")

        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        # Error message is sanitized — no raw exception in response
        assert "server logs" in data["error"].lower() or "failed" in data["error"].lower()

    def test_management_csrf_required(self, web_client: TestClient):
        """Requests without X-Micromech-Action header are rejected."""
        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
            headers={"X-Auth-Token": AUTH_TOKEN},
        )
        assert resp.status_code == 403

    def test_management_auth_required(self, web_client: TestClient):
        """Requests without X-Auth-Token header are rejected."""
        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
            headers={"X-Micromech-Action": "test"},
        )
        assert resp.status_code == 401


class TestToolsHotReload:
    """Tests for /api/tools/reload and /api/setup/tools save flow."""

    CSRF = {"X-Micromech-Action": "tools", "X-Auth-Token": AUTH_TOKEN}

    def _make_client(self, reload_tools_fn=None) -> TestClient:
        async def on_request(req):
            pass

        app = create_web_app(
            get_status=lambda: {"status": "running"},
            get_recent=lambda lim, chain=None: [],
            get_tools=lambda: [],
            on_request=on_request,
            reload_tools=reload_tools_fn,
        )
        return TestClient(app)

    def test_reload_success(self):
        called = {"n": 0}

        def fake_reload():
            called["n"] += 1
            return ["echo", "another"]

        client = self._make_client(reload_tools_fn=fake_reload)
        resp = client.post("/api/tools/reload", headers=self.CSRF)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "reloaded"
        assert data["tools"] == ["echo", "another"]
        assert called["n"] == 1

    def test_reload_not_wired_returns_501(self):
        """If the server wasn't wired with reload_tools, endpoint reports 501."""
        client = self._make_client(reload_tools_fn=None)
        resp = client.post("/api/tools/reload", headers=self.CSRF)
        assert resp.status_code == 501
        assert "not available" in resp.json()["error"].lower()

    def test_reload_requires_csrf(self):
        client = self._make_client(reload_tools_fn=lambda: [])
        resp = client.post(
            "/api/tools/reload",
            headers={"X-Auth-Token": AUTH_TOKEN},
        )
        assert resp.status_code == 403

    def test_reload_requires_auth(self):
        client = self._make_client(reload_tools_fn=lambda: [])
        resp = client.post(
            "/api/tools/reload",
            headers={"X-Micromech-Action": "tools"},
        )
        assert resp.status_code == 401

    def test_reload_sanitizes_error_response(self):
        """The 500 response must NOT leak the raw exception message —
        it could contain filesystem paths or stack details."""
        def broken_reload():
            raise RuntimeError("/opt/secret/wallet.json: permission denied")

        client = self._make_client(reload_tools_fn=broken_reload)
        resp = client.post("/api/tools/reload", headers=self.CSRF)
        assert resp.status_code == 500
        body = resp.json()
        assert "/opt/secret/wallet.json" not in body["error"]
        assert "permission denied" not in body["error"]
        assert "reload failed" in body["error"].lower()

    def test_reload_supports_async_callable(self):
        """The endpoint must await coroutine-returning reloaders so the
        real MechServer.reload_tools (which is async) works."""
        async def async_reload():
            return ["echo", "llm"]

        client = self._make_client(reload_tools_fn=async_reload)
        resp = client.post("/api/tools/reload", headers=self.CSRF)
        assert resp.status_code == 200
        assert resp.json()["tools"] == ["echo", "llm"]

    def test_reload_is_rate_limited(self):
        """Too many reloads from the same client get 429."""
        from micromech.web.app import _RATE_LIMITS, _rate_counters

        # Clear state so other tests don't poison the counter.
        _rate_counters.clear()
        max_req, _window = _RATE_LIMITS["/api/tools/reload"]

        client = self._make_client(reload_tools_fn=lambda: [])
        for _ in range(max_req):
            resp = client.post("/api/tools/reload", headers=self.CSRF)
            assert resp.status_code == 200
        resp = client.post("/api/tools/reload", headers=self.CSRF)
        assert resp.status_code == 429
        assert "rate limit" in resp.json()["error"].lower()
        _rate_counters.clear()

    @patch("micromech.web.app.MicromechConfig")
    def test_setup_tools_save_reports_hot_reloadable(self, mock_cfg_cls):
        """POST /api/setup/tools returns hot_reloadable=True when wired."""
        mock_cfg = MagicMock()
        mock_cfg.disabled_tools = []
        mock_cfg_cls.load.return_value = mock_cfg

        client = self._make_client(reload_tools_fn=lambda: [])
        resp = client.post(
            "/api/setup/tools",
            json={"disabled_tools": ["echo_tool"]},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert data["disabled_tools"] == ["echo_tool"]
        assert data["hot_reloadable"] is True
        mock_cfg.save.assert_called_once()

    @patch("micromech.web.app.MicromechConfig")
    def test_setup_tools_save_without_reload_wired(self, mock_cfg_cls):
        """When reload_tools is not wired, hot_reloadable is False."""
        mock_cfg = MagicMock()
        mock_cfg.disabled_tools = []
        mock_cfg_cls.load.return_value = mock_cfg

        client = self._make_client(reload_tools_fn=None)
        resp = client.post(
            "/api/setup/tools",
            json={"disabled_tools": []},
            headers=self.CSRF,
        )
        assert resp.status_code == 200
        assert resp.json()["hot_reloadable"] is False


class TestRecordToDict:
    """Test the _record_to_dict helper."""

    def test_pending_record(self):
        from micromech.web.app import _record_to_dict

        record = _make_record("r1", "pending")
        d = _record_to_dict(record)
        assert d["request_id"] == "r1"
        assert d["status"] == "pending"
        assert d["tool"] == "echo"
        assert "execution_time" not in d
        assert "tx_hash" not in d

    def test_executed_record(self):
        from micromech.web.app import _record_to_dict

        record = _make_record("r2", "executed", "llm")
        d = _record_to_dict(record)
        assert d["execution_time"] == 1.23
        assert d["error"] is None

    def test_delivered_record_with_response(self):
        from micromech.web.app import _record_to_dict

        req = MechRequest.model_construct(
            request_id="r3",
            status="delivered",
            tool="echo",
            prompt="test",
            sender="",
            data=b"",
            extra_params={},
            created_at=None,
            timeout=300,
            delivery_method="marketplace",
            is_offchain=False,
            error=None,
        )
        resp = MechResponse.model_construct(
            request_id="r3",
            delivery_tx_hash="0xdeadbeef",
        )
        record = RequestRecord.model_construct(
            request=req,
            result=ToolResult(output="ok", execution_time=0.5),
            response=resp,
            updated_at=None,
        )
        d = _record_to_dict(record)
        assert d["tx_hash"] == "0xdeadbeef"


class TestSetupAPI:
    def test_setup_state(self, web_client: TestClient):
        mock_wallet = MagicMock()
        mock_wallet.address = "0x" + "11" * 20
        mock_module = MagicMock(Wallet=MagicMock(return_value=mock_wallet))
        with (
            patch.dict("sys.modules", {"iwa.core.wallet": mock_module}),
            patch("micromech.core.bridge._cached_wallet", None),
        ):
            resp = web_client.get("/api/setup/state")
        assert resp.status_code == 200
        data = resp.json()
        assert "wallet_exists" in data
        assert "step" in data

    def test_setup_chains(self, web_client: TestClient):
        resp = web_client.get("/api/setup/chains")
        assert resp.status_code == 200
        chains = resp.json()
        assert isinstance(chains, list)
        assert len(chains) >= 1
        assert chains[0]["name"] == "gnosis"


class TestHostBinding:
    """Verify the server only binds to localhost (security)."""

    def test_default_host_is_localhost(self):
        from micromech.core.constants import DEFAULT_HOST

        assert DEFAULT_HOST == "127.0.0.1", f"DEFAULT_HOST must be 127.0.0.1, got {DEFAULT_HOST}"

    def test_default_host_is_not_all_interfaces(self):
        from micromech.core.constants import DEFAULT_HOST

        assert DEFAULT_HOST != "0.0.0.0", (
            "DEFAULT_HOST must NOT be 0.0.0.0 — web UI must only be accessible from localhost"
        )

    def test_cli_web_default_host(self):
        """CLI web command defaults to 127.0.0.1."""
        import inspect

        from micromech.cli import web

        sig = inspect.signature(web)
        host_param = sig.parameters.get("host")
        assert host_param is not None
        assert host_param.default.default == "127.0.0.1"


class TestHealthAPI:
    def test_health_check(self, web_client: TestClient):
        resp = web_client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestRecordToDictIpfsCid:
    """Verify _record_to_dict includes request IPFS CID."""

    def test_request_ipfs_cid_from_multihash(self):
        from micromech.core.models import MechRequest, RequestRecord
        from micromech.web.app import _record_to_dict

        req = MechRequest.model_construct(
            request_id="abc123",
            chain="gnosis",
            status="delivered",
            tool="echo",
            prompt="test",
            data=bytes.fromhex("1220" + "ab" * 32),  # valid multihash
            created_at=None,
            is_offchain=False,
        )
        record = RequestRecord.model_construct(request=req, result=None, response=None)
        d = _record_to_dict(record)
        assert d["request_ipfs_cid"] is not None
        assert d["request_ipfs_cid"].startswith("b")  # bafkrei...

    def test_request_ipfs_cid_none_for_raw_data(self):
        from micromech.core.models import MechRequest, RequestRecord
        from micromech.web.app import _record_to_dict

        req = MechRequest.model_construct(
            request_id="abc123",
            chain="gnosis",
            status="pending",
            tool="echo",
            prompt="test",
            data=b'{"prompt":"test"}',  # raw JSON, not multihash
            created_at=None,
            is_offchain=False,
        )
        record = RequestRecord.model_construct(request=req, result=None, response=None)
        d = _record_to_dict(record)
        assert d["request_ipfs_cid"] is None
