"""Shared test fixtures for the unit test suite.

Key concern: MicromechSecrets loads from secrets.env at import time. On
developer machines, WEBUI_PASSWORD and WALLET_PASSWORD may be set, which
would cause auth-protected endpoints to return 401 in tests that don't
provide credentials. This module-level fixture clears both secrets for the
duration of each test so the auth middleware lets requests through.
"""

import pytest


@pytest.fixture(autouse=True)
def _clear_webui_secrets(monkeypatch):
    """Ensure webui_password and wallet_password are None during tests.

    Tests that explicitly need a password set should set it themselves
    within the test body; this fixture only prevents the production
    secrets.env from leaking into the test suite.
    """
    import micromech.secrets as secrets_mod

    monkeypatch.setattr(secrets_mod.secrets, "webui_password", None)
    monkeypatch.setattr(secrets_mod.secrets, "wallet_password", None)
