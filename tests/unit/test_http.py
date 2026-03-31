"""Tests for HTTP endpoints."""

import pytest
from fastapi.testclient import TestClient

from micromech.core.constants import STATUS_PENDING
from micromech.core.models import MechRequest
from micromech.runtime.http import create_app


@pytest.fixture
def received_requests() -> list:
    return []


@pytest.fixture
def client(received_requests: list) -> TestClient:
    async def on_request(req: MechRequest) -> None:
        received_requests.append(req)

    def get_status() -> dict:
        return {
            "status": "running",
            "queue": {STATUS_PENDING: len(received_requests)},
            "tools": ["echo", "llm"],
            "delivered_total": 42,
        }

    app = create_app(on_request=on_request, get_status=get_status)
    return TestClient(app)


class TestHealthEndpoint:
    def test_health(self, client: TestClient):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestStatusEndpoint:
    def test_status(self, client: TestClient):
        resp = client.get("/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["tools"] == ["echo", "llm"]
        assert data["delivered_total"] == 42


class TestRequestEndpoint:
    def test_submit_basic(self, client: TestClient, received_requests: list):
        resp = client.post(
            "/request",
            json={"prompt": "Will ETH hit 10k?", "tool": "llm"},
        )
        assert resp.status_code == 202
        data = resp.json()
        assert "request_id" in data
        assert data["status"] == "accepted"
        assert len(received_requests) == 1
        assert received_requests[0].prompt == "Will ETH hit 10k?"
        assert received_requests[0].tool == "llm"
        assert received_requests[0].is_offchain is True

    def test_submit_with_custom_id(self, client: TestClient, received_requests: list):
        resp = client.post(
            "/request",
            json={"prompt": "test", "request_id": "custom-123"},
        )
        assert resp.status_code == 202
        assert resp.json()["request_id"] == "custom-123"
        assert received_requests[0].request_id == "custom-123"

    def test_submit_with_sender(self, client: TestClient, received_requests: list):
        addr = "0x" + "a" * 40
        resp = client.post(
            "/request",
            json={"prompt": "test", "sender": addr},
        )
        assert resp.status_code == 202

    def test_submit_invalid_sender(self, client: TestClient):
        resp = client.post(
            "/request",
            json={"prompt": "test", "sender": "bad"},
        )
        assert resp.status_code == 400

    def test_submit_with_extra_params(self, client: TestClient, received_requests: list):
        resp = client.post(
            "/request",
            json={
                "prompt": "test",
                "tool": "llm",
                "extra_params": {"model": "qwen", "temperature": 0.5},
            },
        )
        assert resp.status_code == 202
        assert received_requests[0].extra_params["model"] == "qwen"

    def test_submit_missing_prompt(self, client: TestClient):
        resp = client.post("/request", json={"tool": "echo"})
        assert resp.status_code == 422  # Pydantic validation error

    def test_default_tool_is_echo(self, client: TestClient, received_requests: list):
        resp = client.post("/request", json={"prompt": "test"})
        assert resp.status_code == 202
        assert received_requests[0].tool == "echo"

    def test_submit_with_chain(self, client: TestClient, received_requests: list):
        resp = client.post(
            "/request",
            json={"prompt": "test", "chain": "gnosis"},
        )
        assert resp.status_code == 202
        assert received_requests[0].chain == "gnosis"

    def test_default_chain_is_gnosis(self, client: TestClient, received_requests: list):
        resp = client.post("/request", json={"prompt": "test"})
        assert resp.status_code == 202
        assert received_requests[0].chain == "gnosis"


class TestChainValidation:
    """Test chain validation when get_status returns chain list."""

    def test_rejects_unknown_chain(self):
        async def on_request(req):
            pass

        def get_status():
            return {
                "status": "running",
                "chains": ["gnosis", "base"],
                "queue": {},
                "tools": [],
                "delivered_total": 0,
            }

        app = create_app(on_request=on_request, get_status=get_status)
        c = TestClient(app)

        resp = c.post("/request", json={"prompt": "test", "chain": "solana"})
        assert resp.status_code == 400
        assert "solana" in resp.json()["detail"]

    def test_accepts_valid_chain(self):
        received = []

        async def on_request(req):
            received.append(req)

        def get_status():
            return {
                "status": "running",
                "chains": ["gnosis", "base"],
                "queue": {},
                "tools": [],
                "delivered_total": 0,
            }

        app = create_app(on_request=on_request, get_status=get_status)
        c = TestClient(app)

        resp = c.post("/request", json={"prompt": "test", "chain": "base"})
        assert resp.status_code == 202
        assert received[0].chain == "base"
