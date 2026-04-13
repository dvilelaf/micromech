"""Extra tests for runtime/http.py covering missed lines.

Covers:
- Rate limiting path (line 101)
- on_request exception (lines 139-141)
- /result endpoint with record (lines 154-180)
- Root redirect (lines 190-192)
"""

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.core.models import MechRequest, RequestRecord, ToolResult
from micromech.runtime.http import create_app

AUTH = {"X-Micromech-Action": "test"}


def _make_app(on_request=None, get_result=None):
    """Create a test app with optional callbacks."""
    if on_request is None:
        async def on_request(req): pass

    def get_status():
        return {"status": "running", "queue": {}, "tools": [], "delivered_total": 0}

    return create_app(on_request=on_request, get_status=get_status, get_result=get_result)


def _make_record(request_id="r1", output='{"answer": 42}', error=None, has_result=True):
    req = MechRequest(
        request_id=request_id,
        chain="gnosis",
        prompt="test",
        tool="echo",
    )
    result = ToolResult(output=output, execution_time=0.5, error=error) if has_result else None
    return RequestRecord(request=req, result=result)


# ---------------------------------------------------------------------------
# Root redirect
# ---------------------------------------------------------------------------

class TestRootRedirect:
    def test_root_redirects_to_dashboard(self):
        client = TestClient(_make_app(), follow_redirects=False)
        resp = client.get("/")
        assert resp.status_code == 302
        assert "/dashboard" in resp.headers["location"]


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

class TestRateLimiting:
    def test_rate_limited_returns_429(self):
        client = TestClient(_make_app())
        with patch("micromech.web.app._rate_limited", return_value=True), \
             patch("micromech.web.app._get_client_ip", return_value="1.2.3.4"):
            resp = client.post("/request", json={"prompt": "test"}, headers=AUTH)
        assert resp.status_code == 429
        assert "Rate limit" in resp.json()["error"]


# ---------------------------------------------------------------------------
# on_request exception
# ---------------------------------------------------------------------------

class TestOnRequestException:
    def test_on_request_exception_returns_500(self):
        async def bad_request(req):
            raise RuntimeError("db error")

        client = TestClient(_make_app(on_request=bad_request), raise_server_exceptions=False)
        with patch("micromech.web.app._rate_limited", return_value=False), \
             patch("micromech.web.app._get_client_ip", return_value="1.2.3.4"):
            resp = client.post("/request", json={"prompt": "test"}, headers=AUTH)
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# /result endpoint
# ---------------------------------------------------------------------------

class TestResultEndpoint:
    def test_result_not_configured_returns_501(self):
        """get_result=None raises 501."""
        client = TestClient(_make_app(get_result=None))
        resp = client.get("/result/anyid")
        assert resp.status_code == 501

    def test_result_not_found_returns_404(self):
        """get_result returning None raises 404."""
        client = TestClient(_make_app(get_result=lambda rid: None))
        resp = client.get("/result/nonexistent")
        assert resp.status_code == 404

    def test_result_with_json_output(self):
        """Record with valid JSON output returns parsed result."""
        record = _make_record(output='{"answer": 42}')
        client = TestClient(_make_app(get_result=lambda rid: record))
        resp = client.get("/result/r1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["result"] == {"answer": 42}
        assert data["execution_time"] == 0.5
        assert data["error"] is None

    def test_result_with_non_json_output_returns_raw(self):
        """Non-JSON output falls back to raw dict."""
        record = _make_record(output="plain text output")
        client = TestClient(_make_app(get_result=lambda rid: record))
        resp = client.get("/result/r1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["result"] == {"raw": "plain text output"}

    def test_result_with_tool_error(self):
        """ToolResult.error takes priority over output."""
        record = _make_record(output="", error="tool failed")
        client = TestClient(_make_app(get_result=lambda rid: record))
        resp = client.get("/result/r1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["error"] == "tool failed"
        assert data["result"] is None

    def test_result_without_tool_result(self):
        """Record with no result returns status only."""
        record = _make_record(has_result=False)
        client = TestClient(_make_app(get_result=lambda rid: record))
        resp = client.get("/result/r1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["result"] is None
        assert data["error"] is None
