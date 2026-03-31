"""Tests for the web UI."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.core.models import MechRequest, MechResponse, RequestRecord, ToolResult
from micromech.web.app import create_web_app


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
        resp = web_client.get("/")
        assert resp.status_code == 200
        assert "micromech" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_tabs(self, mock_setup, web_client: TestClient):
        resp = web_client.get("/")
        assert "Overview" in resp.text
        assert "Live Activity" in resp.text
        assert "Charts" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_chart_js(self, mock_setup, web_client: TestClient):
        resp = web_client.get("/")
        assert "chart.js" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_has_sse_connection(self, mock_setup, web_client: TestClient):
        resp = web_client.get("/")
        assert "EventSource" in resp.text
        assert "/api/metrics/stream" in resp.text

    def test_redirects_to_setup_when_not_configured(self, web_client: TestClient):
        with patch("micromech.web.app._needs_setup", return_value=True):
            resp = web_client.get("/", follow_redirects=False)
            assert resp.status_code == 302
            assert "/setup" in resp.headers["location"]

    def test_setup_page_renders(self, web_client: TestClient):
        resp = web_client.get("/setup")
        assert resp.status_code == 200
        assert "setup" in resp.text.lower()
        assert "micromech" in resp.text.lower()


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
            lambda lim: [], lambda: [], lambda r: None,
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

    CSRF = {"X-Micromech-Action": "test"}

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
        )
        assert resp.status_code == 403


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
            patch("micromech.web.app.DEFAULT_CONFIG_PATH") as mock_path,
            patch.dict("sys.modules", {"iwa.core.wallet": mock_module}),
            patch("micromech.core.bridge._cached_wallet", None),
        ):
            mock_path.exists.return_value = False
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


class TestHealthAPI:
    def test_health_check(self, web_client: TestClient):
        resp = web_client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data
