"""Tests for the web UI."""

import pytest
from fastapi.testclient import TestClient

from micromech.core.models import MechRequest, RequestRecord, ToolResult
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
