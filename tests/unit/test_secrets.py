"""Tests for micromech.secrets — MicromechSecrets loading."""

from unittest.mock import patch

from pydantic import SecretStr

from micromech.secrets import MicromechSecrets


class TestMicromechSecrets:
    def test_defaults_are_none(self):
        with patch.dict("os.environ", {}, clear=True):
            s = MicromechSecrets()
        assert s.wallet_password is None
        assert s.telegram_token is None
        assert s.telegram_chat_id is None
        assert s.health_url is None
        assert s.micromech_auth_token is None

    def test_telegram_fields_loaded(self):
        with patch.dict(
            "os.environ",
            {"TELEGRAM_TOKEN": "tok123", "TELEGRAM_CHAT_ID": "42"},
            clear=True,
        ):
            s = MicromechSecrets()
        assert s.telegram_token.get_secret_value() == "tok123"
        assert s.telegram_chat_id == 42

    def test_wallet_password_is_secret(self):
        with patch.dict("os.environ", {"WALLET_PASSWORD": "hunter2"}, clear=True):
            s = MicromechSecrets()
        assert isinstance(s.wallet_password, SecretStr)
        assert s.wallet_password.get_secret_value() == "hunter2"

    def test_extra_fields_ignored(self):
        with patch.dict("os.environ", {"SOME_UNKNOWN_FIELD": "x"}, clear=True):
            s = MicromechSecrets()
        assert not hasattr(s, "some_unknown_field")

    def test_health_url_loaded(self):
        with patch.dict("os.environ", {"HEALTH_URL": "https://example.com/ping"}, clear=True):
            s = MicromechSecrets()
        assert s.health_url == "https://example.com/ping"
