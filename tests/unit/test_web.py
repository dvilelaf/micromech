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

    def get_recent(limit):
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
    def test_renders_html(self, web_client: TestClient):
        resp = web_client.get("/")
        assert resp.status_code == 200
        assert "micromech" in resp.text
        assert "Pending" in resp.text

    def test_shows_queue_counts(self, web_client: TestClient):
        resp = web_client.get("/")
        assert "2" in resp.text  # pending count
        assert "10" in resp.text  # delivered total

    def test_shows_tools(self, web_client: TestClient):
        resp = web_client.get("/")
        assert "echo" in resp.text
        assert "llm" in resp.text

    def test_shows_requests(self, web_client: TestClient):
        resp = web_client.get("/")
        assert "r1" in resp.text
        assert "r2" in resp.text


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


class TestManagementAPI:
    """Test the /api/management/{action} endpoint."""

    @patch("micromech.management.MechLifecycle")
    @patch("micromech.web.app.MicromechConfig")
    def test_stake_action(self, mock_cfg_cls, mock_lc_cls, web_client: TestClient):
        mock_lc = MagicMock()
        mock_lc.stake.return_value = True
        mock_lc_cls.return_value = mock_lc

        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
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
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        assert "config error" in data["error"]


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
