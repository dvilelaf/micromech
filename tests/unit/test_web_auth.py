"""Tests for web UI Bearer authentication middleware.

Coverage:
- No password configured → all requests pass through (first install / setup wizard)
- Password configured → protected /api/* endpoints require Bearer token
- Setup endpoints (/api/setup/*) always accessible without auth
- /api/health always accessible without auth
- SSE endpoints authenticate via ?token= query param
- webui_password is written to secrets.env on new wallet creation
- In-memory secrets singleton is updated immediately after wallet creation
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from pydantic import SecretStr

from micromech.secrets import secrets as _real_secrets
from micromech.web.app import create_web_app

PASSWORD = "s3cr3tPassw0rd!"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app():
    """Minimal create_web_app() for auth testing."""
    return create_web_app(
        get_status=lambda: {
            "status": "running",
            "queue": {},
            "tools": [],
            "delivered_total": 0,
        },
        get_recent=lambda limit=20, chain=None: [],
        get_tools=lambda: [],
        on_request=AsyncMock(),
    )


@contextmanager
def _password_active(password: str | None):
    """Temporarily set webui_password on the real secrets singleton.

    The middleware reads the singleton at request time, so the context
    must wrap the actual HTTP calls, not just app creation.
    """
    original = _real_secrets.webui_password
    _real_secrets.webui_password = SecretStr(password) if password else None
    try:
        yield
    finally:
        _real_secrets.webui_password = original


def _client() -> TestClient:
    return TestClient(_make_app(), raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# When no password is set (first install / wizard phase)
# ---------------------------------------------------------------------------

class TestNoPassword:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_status_accessible_without_auth(self, _mock):
        client = _client()
        with _password_active(None):
            resp = client.get("/api/status")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_chains_accessible_without_auth(self, _mock):
        client = _client()
        with _password_active(None):
            resp = client.get("/api/chains")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_setup_wizard_accessible(self, _mock):
        client = _client()
        with _password_active(None):
            resp = client.get("/api/setup/state")
        assert resp.status_code == 200

    def test_health_accessible(self):
        client = _client()
        with _password_active(None):
            resp = client.get("/api/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# When password IS set (normal operation after first wizard)
# ---------------------------------------------------------------------------

class TestWithPassword:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_protected_endpoint_returns_401_without_auth(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status")
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_401_response_is_json(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status")
        assert resp.headers["content-type"].startswith("application/json")
        assert "Unauthorized" in resp.text

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_401_includes_www_authenticate_bearer(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status")
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_valid_bearer_token_allows_access(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status", headers={"Authorization": f"Bearer {PASSWORD}"})
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_wrong_bearer_token_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status", headers={"Authorization": "Bearer wrongpassword"})
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_basic_auth_not_accepted(self, _mock):
        """Must use Bearer, not Basic — avoids relying on browser native dialog."""
        import base64
        encoded = base64.b64encode(f"admin:{PASSWORD}".encode()).decode()
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status", headers={"Authorization": f"Basic {encoded}"})
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_empty_bearer_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/status", headers={"Authorization": "Bearer "})
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_multiple_protected_endpoints_all_require_auth(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            for path in ["/api/status", "/api/chains", "/api/tools", "/api/staking/status"]:
                assert client.get(path).status_code == 401, f"{path} should require auth"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_multiple_protected_endpoints_all_accept_token(self, _mock):
        client = _client()
        headers = {"Authorization": f"Bearer {PASSWORD}"}
        with _password_active(PASSWORD):
            for path in ["/api/status", "/api/chains", "/api/tools"]:
                assert client.get(path, headers=headers).status_code == 200, (
                    f"{path} should be accessible with valid token"
                )


# ---------------------------------------------------------------------------
# Endpoints that bypass auth regardless of password
# ---------------------------------------------------------------------------

class TestBypassedEndpoints:
    def test_health_always_accessible(self):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/health")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_setup_state_always_accessible(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/setup/state")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_setup_chains_always_accessible(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/setup/chains")
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=True)
    def test_setup_page_always_accessible(self, _mock):
        """/setup wizard is always reachable so first-install works."""
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/setup")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Dashboard (/) requires auth when password is set
# ---------------------------------------------------------------------------

class TestDashboardAuth:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_dashboard_blocked_without_auth(self, _mock):
        """/ must NOT return 200 when a password is configured and no token given."""
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/", follow_redirects=False)
        assert resp.status_code != 200, "Dashboard must be protected when WEBUI_PASSWORD is set"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_dashboard_redirects_to_setup_without_auth(self, _mock):
        """Unauthenticated browser request to / is redirected to /setup."""
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == "/setup"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_dashboard_accessible_with_bearer_token(self, _mock):
        """/ returns 200 when correct Bearer token is provided."""
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/", headers={"Authorization": f"Bearer {PASSWORD}"})
        assert resp.status_code == 200

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_dashboard_blocked_without_password_set(self, _mock):
        """/ is accessible when no password is configured (first install)."""
        client = _client()
        with _password_active(None):
            resp = client.get("/")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# SSE authentication via ?token= query param
# ---------------------------------------------------------------------------

class TestSSEAuthentication:
    # The auth middleware runs BEFORE the SSE generator starts, so it rejects
    # without producing a streaming body.  We only test rejection here — the
    # "valid token allows access" case is already covered by the regular endpoint
    # tests above (the middleware logic is identical for all /api/* paths).

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_sse_without_token_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/metrics/stream")
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_sse_with_wrong_token_param_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/metrics/stream?token=wrongtoken")
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_logs_stream_without_token_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/logs/stream")
        assert resp.status_code == 401

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_logs_stream_with_wrong_token_returns_401(self, _mock):
        client = _client()
        with _password_active(PASSWORD):
            resp = client.get("/api/logs/stream?token=wrongtoken")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Wallet creation writes webui_password to secrets
# ---------------------------------------------------------------------------

class TestWalletCreationWritesWebUIPassword:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_webui_password_written_on_new_wallet(self, _mock):
        """When a new wallet is created via the wizard, webui_password is persisted."""
        client = _client()
        mock_result = {"address": "0xabc", "mnemonic": "word " * 12, "created": True}

        with (
            patch("micromech.web.app.asyncio.to_thread", AsyncMock(return_value=mock_result)),
            patch("micromech.core.secrets_file.write_secret") as mock_write,
        ):
            resp = client.post(
                "/api/setup/wallet",
                json={"password": "TestPassword123"},
                headers={"X-Micromech-Action": "wallet"},
            )

        assert resp.status_code == 200
        written_keys = {call.args[0] for call in mock_write.call_args_list}
        assert "wallet_password" in written_keys
        assert "webui_password" in written_keys

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_webui_password_not_written_on_unlock(self, _mock):
        """Re-unlocking an existing wallet must NOT overwrite webui_password."""
        client = _client()
        mock_result = {"address": "0xabc", "mnemonic": None, "created": False}

        with (
            patch("micromech.web.app.asyncio.to_thread", AsyncMock(return_value=mock_result)),
            patch("micromech.core.secrets_file.write_secret") as mock_write,
        ):
            resp = client.post(
                "/api/setup/wallet",
                json={"password": "TestPassword123"},
                headers={"X-Micromech-Action": "wallet"},
            )

        assert resp.status_code == 200
        assert mock_write.call_count == 0, "Should not write any secret on re-unlock"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_in_memory_secrets_updated_immediately(self, _mock):
        """Singleton is updated so auth kicks in without a server restart."""
        client = _client()
        mock_result = {"address": "0xabc", "mnemonic": "word " * 12, "created": True}
        original_password = _real_secrets.webui_password

        with (
            patch("micromech.web.app.asyncio.to_thread", AsyncMock(return_value=mock_result)),
            patch("micromech.core.secrets_file.write_secret"),
        ):
            client.post(
                "/api/setup/wallet",
                json={"password": "NewPassword!1"},
                headers={"X-Micromech-Action": "wallet"},
            )

        try:
            assert _real_secrets.webui_password is not None
            assert _real_secrets.webui_password.get_secret_value() == "NewPassword!1"
        finally:
            _real_secrets.webui_password = original_password
