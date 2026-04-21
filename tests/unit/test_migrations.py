"""Tests for micromech.migrations."""

from pathlib import Path

from micromech.migrations import migrate_removed_fields, migrate_schema


def _write_config(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestMigrateRemovedFields:
    def test_no_removed_fields_dict_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    foo: 1\n")
        assert migrate_removed_fields(cfg, {}) == []

    def test_missing_config_returns_empty(self, tmp_path):
        assert migrate_removed_fields(tmp_path / "nope.yaml", {"old": None}) == []

    def test_no_stale_keys_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    current: 1\n")
        assert migrate_removed_fields(cfg, {"old": None}) == []

    def test_removes_obsolete_field(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    old_key: 99\n")
        result = migrate_removed_fields(cfg, {"old_key": None})
        assert result == ["old_key"]
        assert "old_key" not in cfg.read_text()

    def test_renames_field(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    old_name: 42\n")
        result = migrate_removed_fields(cfg, {"old_name": "new_name"})
        assert result == ["old_name"]
        text = cfg.read_text()
        assert "old_name" not in text
        assert "new_name" in text

    def test_rename_skips_if_new_key_exists(self, tmp_path):
        """If new_key already present, don't overwrite it."""
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    old_name: 1\n    new_name: 99\n")
        migrate_removed_fields(cfg, {"old_name": "new_name"})
        text = cfg.read_text()
        assert "old_name" not in text
        assert "99" in text

    def test_parse_error_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(": bad: yaml: {{\n")
        result = migrate_removed_fields(cfg, {"old": None})
        assert result == []

    def test_unlink_called_on_write_error(self, tmp_path, monkeypatch):
        """os.unlink is attempted on tmp file when os.replace fails."""
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    old: 1\n")
        import os

        unlinked = []
        monkeypatch.setattr(os, "replace", lambda *a: (_ for _ in ()).throw(OSError("err")))
        monkeypatch.setattr(os, "unlink", lambda p: unlinked.append(p))
        migrate_removed_fields(cfg, {"old": None})
        assert len(unlinked) == 1

    def test_write_error_returns_stale_list(self, tmp_path, monkeypatch):
        """Even if writing fails, the list of stale keys is returned."""
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    old: 1\n")
        import os

        monkeypatch.setattr(os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
        result = migrate_removed_fields(cfg, {"old": None})
        assert result == ["old"]


class TestMigrateSchema:
    def test_missing_config_returns_empty(self, tmp_path):
        assert migrate_schema(tmp_path / "nope.yaml") == []

    def test_parse_error_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text(": bad: yaml: {{\n")
        assert migrate_schema(cfg) == []

    def test_no_micromech_section_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  other: {}\n")
        assert migrate_schema(cfg) == []

    def test_no_pending_migrations_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("plugins:\n  micromech:\n    schema_version: 1\n")
        assert migrate_schema(cfg) == []
