"""Tests for the CLI commands."""

from pathlib import Path

from typer.testing import CliRunner

from micromech.cli import app
from micromech.core.config import MicromechConfig
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue

runner = CliRunner()


class TestVersionCommand:
    def test_version(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "micromech" in result.output


class TestInitCommand:
    def test_init_creates_config(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        result = runner.invoke(app, ["init", "--config", str(config_path)])
        assert result.exit_code == 0
        assert config_path.exists()
        assert "created" in result.output.lower()

    def test_init_refuses_overwrite(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("existing")
        result = runner.invoke(app, ["init", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "already exists" in result.output.lower()


class TestConfigCommand:
    def test_show_config(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)
        result = runner.invoke(app, ["config", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "runtime" in result.output
        assert "mech" in result.output

    def test_show_default_config(self, tmp_path: Path):
        config_path = tmp_path / "nonexistent.yaml"
        result = runner.invoke(app, ["config", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "runtime" in result.output


class TestStatusCommand:
    def test_status_empty(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        cfg = MicromechConfig(persistence={"db_path": tmp_path / "test.db"})
        cfg.save(config_path)
        result = runner.invoke(app, ["status", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "pending: 0" in result.output.lower()

    def test_status_with_requests(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        queue = PersistentQueue(db_path)
        queue.add_request(MechRequest(request_id="r1", prompt="test", tool="echo"))
        queue.add_request(MechRequest(request_id="r2", prompt="test2", tool="llm"))
        queue.close()

        config_path = tmp_path / "config.yaml"
        cfg = MicromechConfig(persistence={"db_path": db_path})
        cfg.save(config_path)

        result = runner.invoke(app, ["status", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "pending: 2" in result.output.lower()


class TestToolsCommand:
    def test_list_tools(self):
        result = runner.invoke(app, ["tools"])
        assert result.exit_code == 0
        assert "echo" in result.output.lower()


class TestTestToolCommand:
    def test_echo_tool(self):
        result = runner.invoke(app, ["test-tool", "echo", "hello world"])
        assert result.exit_code == 0
        assert "hello world" in result.output

    def test_unknown_tool(self):
        result = runner.invoke(app, ["test-tool", "nonexistent", "test"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()


class TestCleanupCommand:
    def test_cleanup_empty(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        cfg = MicromechConfig(persistence={"db_path": tmp_path / "test.db"})
        cfg.save(config_path)

        # Create DB
        queue = PersistentQueue(tmp_path / "test.db")
        queue.close()

        result = runner.invoke(app, ["cleanup", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "0 records" in result.output.lower()
