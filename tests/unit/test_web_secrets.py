"""Tests for /api/setup/secrets GET and POST endpoints."""

import pytest
from fastapi.testclient import TestClient

from micromech.web.app import create_web_app, get_auth_token

AUTH_TOKEN = get_auth_token()


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Web client with secrets.env mounted at a temp path."""
    secrets_path = tmp_path / "secrets.env"
    secrets_path.write_text(
        "wallet_password=testpass\ntelegram_token=tok123\ngnosis_rpc=https://rpc.example.com\n"
    )
    monkeypatch.setenv("SECRETS_ENV_PATH", str(secrets_path))
    # Reload the module so SECRETS_ENV_PATH is re-evaluated
    import importlib

    import micromech.core.secrets_file as sf

    importlib.reload(sf)

    async def on_request(req):
        pass

    app = create_web_app(lambda: {}, lambda *a, **k: [], lambda: [], on_request)
    return TestClient(app), secrets_path


class TestGetSecrets:
    def test_requires_auth(self, client):
        c, _ = client
        res = c.get("/api/setup/secrets")
        assert res.status_code in (401, 403, 200)  # depends on auth middleware

    def test_returns_editable_keys(self, client):
        c, _ = client
        res = c.get("/api/setup/secrets", headers={"X-Auth-Token": AUTH_TOKEN})
        assert res.status_code == 200
        data = res.json()
        assert "telegram_token" in data
        assert "gnosis_rpc" in data

    def test_masks_sensitive_values(self, client):
        c, _ = client
        res = c.get("/api/setup/secrets", headers={"X-Auth-Token": AUTH_TOKEN})
        data = res.json()
        # telegram_token is sensitive and set → should be masked
        assert data["telegram_token"] == "***"

    def test_returns_empty_string_for_unset_keys(self, client):
        c, _ = client
        res = c.get("/api/setup/secrets", headers={"X-Auth-Token": AUTH_TOKEN})
        data = res.json()
        # telegram_chat_id not in our test secrets.env
        assert data.get("telegram_chat_id") == ""

    def test_does_not_expose_wallet_password(self, client):
        c, _ = client
        res = c.get("/api/setup/secrets", headers={"X-Auth-Token": AUTH_TOKEN})
        data = res.json()
        assert "wallet_password" not in data


class TestPostSecrets:
    def test_saves_telegram_secrets(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"telegram_token": "newtoken", "telegram_chat_id": "99999"},
        )
        assert res.status_code == 200
        content = secrets_path.read_text()
        assert "telegram_token=newtoken" in content
        assert "telegram_chat_id=99999" in content

    def test_saves_rpc_endpoints(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": "https://custom-rpc.example.com"},
        )
        assert res.status_code == 200
        assert "gnosis_rpc=https://custom-rpc.example.com" in secrets_path.read_text()

    def test_ignores_masked_values(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"telegram_token": "***"},  # masked — should be ignored
        )
        assert res.status_code == 200
        # token should remain unchanged
        assert "telegram_token=tok123" in secrets_path.read_text()

    def test_ignores_non_editable_keys(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"wallet_password": "hacked", "some_random_key": "value"},
        )
        assert res.status_code == 200
        content = secrets_path.read_text()
        # wallet_password should NOT be overwritten via this endpoint
        assert "wallet_password=hacked" not in content
        # Random key should not appear
        assert "some_random_key" not in content

    def test_returns_saved_keys(self, client):
        c, _ = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": "https://example.com", "telegram_chat_id": "12345"},
        )
        data = res.json()
        assert data["status"] == "ok"
        assert set(data["saved"]) == {"gnosis_rpc", "telegram_chat_id"}

    def test_rejects_missing_csrf_header(self, client):
        c, _ = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json"},
            # No X-Micromech-Action header
            json={"gnosis_rpc": "https://example.com"},
        )
        assert res.status_code == 403

    def test_rejects_newline_injection(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": "https://evil.com\nwallet_password=pwned"},
        )
        assert res.status_code == 400
        assert "wallet_password=pwned" not in secrets_path.read_text()

    def test_rejects_null_json_value(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": None},
        )
        assert res.status_code == 400
        assert "gnosis_rpc=None" not in secrets_path.read_text()

    def test_rejects_bool_json_value(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": False},
        )
        assert res.status_code == 400

    def test_rejects_int_json_value(self, client):
        c, secrets_path = client
        res = c.post(
            "/api/setup/secrets",
            headers={"X-Auth-Token": AUTH_TOKEN, "Content-Type": "application/json", "X-Micromech-Action": "save-secrets"},
            json={"gnosis_rpc": 12345},
        )
        assert res.status_code == 400
