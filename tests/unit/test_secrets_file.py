"""Tests for secrets_file helper (read/write secrets.env)."""

import pytest

from micromech.core.secrets_file import (
    EDITABLE_KEYS,
    SENSITIVE_KEYS,
    read_secrets_file,
    write_secret,
    write_secrets,
)


class TestReadSecretsFile:
    def test_returns_empty_dict_when_file_missing(self, tmp_path):
        path = tmp_path / "secrets.env"
        assert read_secrets_file(path) == {}

    def test_parses_key_value_pairs(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("wallet_password=hunter2\ntelegram_token=abc123\n")
        result = read_secrets_file(p)
        assert result["wallet_password"] == "hunter2"
        assert result["telegram_token"] == "abc123"

    def test_ignores_comments_and_blank_lines(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("# This is a comment\n\nwallet_password=secret\n\n# Another comment\n")
        result = read_secrets_file(p)
        assert result == {"wallet_password": "secret"}

    def test_value_with_equals_sign(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("some_url=https://example.com?a=b\n")
        result = read_secrets_file(p)
        assert result["some_url"] == "https://example.com?a=b"

    def test_empty_file(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("")
        assert read_secrets_file(p) == {}


class TestWriteSecret:
    def test_creates_file_when_missing(self, tmp_path):
        p = tmp_path / "secrets.env"
        write_secret("wallet_password", "newpass", path=p)
        assert p.exists()
        assert "wallet_password=newpass" in p.read_text()

    def test_updates_existing_key(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("wallet_password=old\ntelegram_token=tok\n")
        write_secret("wallet_password", "new", path=p)
        content = p.read_text()
        assert "wallet_password=new" in content
        assert "wallet_password=old" not in content
        assert "telegram_token=tok" in content

    def test_appends_new_key(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("wallet_password=secret\n")
        write_secret("telegram_token", "tok123", path=p)
        content = p.read_text()
        assert "wallet_password=secret" in content
        assert "telegram_token=tok123" in content

    def test_preserves_comments(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("# My comment\nwallet_password=old\n# Another\n")
        write_secret("wallet_password", "new", path=p)
        content = p.read_text()
        assert "# My comment" in content
        assert "# Another" in content
        assert "wallet_password=new" in content

    def test_roundtrip(self, tmp_path):
        p = tmp_path / "secrets.env"
        write_secret("gnosis_rpc", "https://rpc.example.com", path=p)
        result = read_secrets_file(p)
        assert result["gnosis_rpc"] == "https://rpc.example.com"

    def test_rejects_newline_in_value(self, tmp_path):
        p = tmp_path / "secrets.env"
        with pytest.raises(ValueError, match="forbidden"):
            write_secret("gnosis_rpc", "https://evil.com\nwallet_password=pwned", path=p)
        # File must not have been written
        assert not p.exists()

    def test_rejects_carriage_return_in_value(self, tmp_path):
        p = tmp_path / "secrets.env"
        with pytest.raises(ValueError, match="forbidden"):
            write_secret("gnosis_rpc", "https://evil.com\rwallet_password=pwned", path=p)

    def test_rejects_null_byte_in_value(self, tmp_path):
        p = tmp_path / "secrets.env"
        with pytest.raises(ValueError):
            write_secret("gnosis_rpc", "https://evil.com\x00", path=p)

    def test_rejects_pure_digit_key(self, tmp_path):
        p = tmp_path / "secrets.env"
        with pytest.raises(ValueError, match="at least one letter"):
            write_secret("123", "value", path=p)

    def test_rejects_underscore_only_key(self, tmp_path):
        p = tmp_path / "secrets.env"
        with pytest.raises(ValueError):
            write_secret("___", "value", path=p)

    def test_no_leftover_tmp_file_on_success(self, tmp_path):
        p = tmp_path / "secrets.env"
        write_secret("gnosis_rpc", "https://rpc.example.com", path=p)
        tmp_files = list(tmp_path.glob(".secrets_*.tmp"))
        assert tmp_files == [], f"Leftover tmp file(s): {tmp_files}"

    def test_file_created_with_mode_600(self, tmp_path):
        p = tmp_path / "secrets.env"
        write_secret("wallet_password", "pass", path=p)
        mode = p.stat().st_mode & 0o777
        assert mode == 0o600, f"Expected 0o600, got {oct(mode)}"

    def test_file_permissions_enforced_on_existing_file(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("wallet_password=old\n")
        p.chmod(0o644)  # simulate world-readable
        write_secret("wallet_password", "new", path=p)
        mode = p.stat().st_mode & 0o777
        assert mode == 0o600, f"Expected 0o600 after update, got {oct(mode)}"


class TestWriteSecrets:
    def test_writes_multiple_keys_atomically(self, tmp_path):
        p = tmp_path / "secrets.env"
        write_secrets({"wallet_password": "pass", "telegram_token": "tok"}, path=p)
        result = read_secrets_file(p)
        assert result["wallet_password"] == "pass"
        assert result["telegram_token"] == "tok"

    def test_single_read_write_not_per_key(self, tmp_path):
        """write_secrets must touch the file only once, not once per key."""
        p = tmp_path / "secrets.env"
        p.write_text("existing=value\n")
        write_secrets({"key1": "v1", "key2": "v2", "key3": "v3"}, path=p)
        result = read_secrets_file(p)
        assert result["existing"] == "value"
        assert result["key1"] == "v1"
        assert result["key2"] == "v2"
        assert result["key3"] == "v3"

    def test_empty_updates_is_noop(self, tmp_path):
        p = tmp_path / "secrets.env"
        p.write_text("wallet_password=existing\n")
        write_secrets({}, path=p)
        assert read_secrets_file(p)["wallet_password"] == "existing"

    def test_rejects_injection_before_writing(self, tmp_path):
        """All keys validated before any file write — partial writes must not occur."""
        p = tmp_path / "secrets.env"
        p.write_text("existing=value\n")
        original = p.read_text()
        with pytest.raises(ValueError):
            write_secrets(
                {"gnosis_rpc": "https://ok.com", "base_rpc": "bad\nvalue"},
                path=p,
            )
        # File must be completely unchanged
        assert p.read_text() == original


class TestWriteSecretsCommentedTemplates:
    def test_uncomments_template_placeholder(self, tmp_path):
        """Commented-out template lines like '# telegram_token=' are replaced in-place."""
        p = tmp_path / "secrets.env"
        p.write_text("# Telegram bot\n# telegram_token=\n# telegram_chat_id=\n")
        write_secrets({"telegram_token": "abc123", "telegram_chat_id": "999"}, path=p)
        result = read_secrets_file(p)
        assert result["telegram_token"] == "abc123"
        assert result["telegram_chat_id"] == "999"
        # Should not be duplicated at the end
        content = p.read_text()
        assert content.count("telegram_token=") == 1
        assert content.count("telegram_chat_id=") == 1

    def test_commented_template_with_existing_value_replaced(self, tmp_path):
        """'# key=default' placeholder is replaced, not duplicated."""
        p = tmp_path / "secrets.env"
        p.write_text("# gnosis_rpc=https://example.com\n")
        write_secrets({"gnosis_rpc": "https://new.rpc"}, path=p)
        result = read_secrets_file(p)
        assert result["gnosis_rpc"] == "https://new.rpc"
        assert p.read_text().count("gnosis_rpc=") == 1


class TestConstants:
    def test_editable_keys_includes_rpc_and_telegram(self):
        assert "telegram_token" in EDITABLE_KEYS
        assert "telegram_chat_id" in EDITABLE_KEYS
        assert "gnosis_rpc" in EDITABLE_KEYS

    def test_sensitive_keys_includes_wallet_and_telegram(self):
        assert "wallet_password" in SENSITIVE_KEYS
        assert "telegram_token" in SENSITIVE_KEYS

    def test_wallet_password_not_in_editable(self):
        # wallet_password is set automatically, never via the secrets editor UI
        assert "wallet_password" not in EDITABLE_KEYS
