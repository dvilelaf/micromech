"""Tests for the CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

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
    @patch("micromech.cli._check_balances", return_value=(1.0, 20000.0))
    def test_init_wizard_skip_funding(self, mock_balances, tmp_path: Path):
        mock_wallet = MagicMock()
        mock_wallet.address = "0x" + "11" * 20

        config_path = tmp_path / "config.yaml"
        mock_module = MagicMock(Wallet=MagicMock(return_value=mock_wallet))
        with patch.dict("sys.modules", {"iwa.core.wallet": mock_module}):
            result = runner.invoke(app, [
                "init", "--config", str(config_path),
                "--chain", "gnosis", "--yes", "--skip-funding-check",
            ])
        # Will fail at deploy step (no iwa ServiceManager) but wizard starts
        assert "setup wizard" in result.output.lower()
        assert "wallet found" in result.output.lower()

    @patch("micromech.cli._check_balances", return_value=(1.0, 20000.0))
    def test_init_resumes_complete(self, mock_balances, tmp_path: Path):
        """Init detects already-deployed service and skips."""
        mock_wallet = MagicMock()
        mock_wallet.address = "0x" + "11" * 20

        config_path = tmp_path / "config.yaml"
        from micromech.core.config import ChainConfig
        cfg = MicromechConfig(chains={"gnosis": ChainConfig(
            chain="gnosis",
            mech_address="0x" + "33" * 20,
            marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
            factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
            staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
        )})
        cfg.save(config_path)

        mock_module = MagicMock(Wallet=MagicMock(return_value=mock_wallet))
        with patch.dict("sys.modules", {"iwa.core.wallet": mock_module}):
            result = runner.invoke(app, [
                "init", "--config", str(config_path),
                "--chain", "gnosis", "--yes", "--skip-funding-check",
            ])
        assert result.exit_code == 0
        assert "already fully deployed" in result.output.lower()


class TestConfigCommand:
    def test_show_config(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)
        result = runner.invoke(app, ["config", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "chains" in result.output
        assert "log_level" in result.output

    def test_show_default_config(self, tmp_path: Path):
        config_path = tmp_path / "nonexistent.yaml"
        result = runner.invoke(app, ["config", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "log_level" in result.output


class TestStatusCommand:
    def test_status_empty(self, tmp_path: Path, monkeypatch):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("micromech.cli.DB_PATH", db_path)
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)
        result = runner.invoke(app, ["status", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "pending: 0" in result.output.lower()

    def test_status_with_requests(self, tmp_path: Path, monkeypatch):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("micromech.cli.DB_PATH", db_path)
        queue = PersistentQueue(db_path)
        queue.add_request(MechRequest(request_id="r1", prompt="test", tool="echo"))
        queue.add_request(MechRequest(request_id="r2", prompt="test2", tool="llm"))
        queue.close()

        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

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
        assert "p_yes" in result.output

    def test_unknown_tool(self):
        result = runner.invoke(app, ["test-tool", "nonexistent", "test"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()


class TestCleanupCommand:
    def test_cleanup_empty(self, tmp_path: Path, monkeypatch):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("micromech.cli.DB_PATH", db_path)
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        queue = PersistentQueue(db_path)
        queue.close()

        result = runner.invoke(app, ["cleanup", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "0 records" in result.output.lower()


class TestRunCommand:
    @patch("micromech.runtime.server.MechServer")
    def test_run_starts_server(self, mock_server_cls, tmp_path: Path, monkeypatch):
        monkeypatch.setattr("micromech.runtime.server.DB_PATH", tmp_path / "test.db")
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_server = MagicMock()
        mock_server_cls.return_value = mock_server
        # Make asyncio.run() a no-op so the CLI returns
        mock_server.run = MagicMock(return_value=None)

        with patch("micromech.cli.asyncio"):
            result = runner.invoke(app, ["run", "--config", str(config_path), "--no-http"])

        assert result.exit_code == 0
        mock_server.shutdown.assert_called_once()


class TestWebCommand:
    @patch("uvicorn.run")
    def test_web_starts_dashboard(self, mock_uvicorn_run, tmp_path: Path, monkeypatch):
        monkeypatch.setattr("micromech.cli.DB_PATH", tmp_path / "test.db")
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        result = runner.invoke(app, ["web", "--config", str(config_path), "--port", "9999"])
        assert result.exit_code == 0
        assert "9999" in result.output
        mock_uvicorn_run.assert_called_once()


class TestFingerprintCommand:
    @patch("micromech.ipfs.metadata.fingerprint_all_builtins")
    def test_fingerprint(self, mock_fp):
        mock_fp.return_value = {"echo_tool": {"echo_tool.py": "bafkrei123"}}
        result = runner.invoke(app, ["fingerprint"])
        assert result.exit_code == 0
        assert "echo_tool" in result.output
        assert "Fingerprinted 1 tool(s)" in result.output


class TestMetadataBuildCommand:
    def test_metadata_build(self):
        result = runner.invoke(app, ["metadata-build"])
        assert result.exit_code == 0
        assert "micromech" in result.output
        assert "TOOLS_TO_PACKAGE_HASH" in result.output


class TestMetadataPushCommand:
    @patch("micromech.ipfs.client.push_json_to_ipfs")
    def test_metadata_push_success(self, mock_push):

        mock_push.return_value = ("bafkrei_cid", "f0155_hex")

        result = runner.invoke(app, ["metadata-push"])
        assert result.exit_code == 0
        assert "On-chain hash" in result.output

    @patch("micromech.ipfs.client.push_json_to_ipfs", side_effect=RuntimeError("network"))
    def test_metadata_push_failure(self, mock_push):
        result = runner.invoke(app, ["metadata-push"])
        assert result.exit_code == 0
        assert "IPFS push failed" in result.output
        assert "Use the on-chain hash" in result.output


class TestCreateServiceCommand:
    @patch("micromech.management.MechLifecycle")
    def test_create_service_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.create_service.return_value = 42
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["create-service", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "42" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_create_service_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.create_service.return_value = None
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["create-service", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Failed" in result.output


class TestDeployMechCommand:
    @patch("micromech.management.MechLifecycle")
    def test_deploy_full_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.activate.return_value = True
        mock_lc.register_agent.return_value = True
        mock_lc.deploy.return_value = "0x" + "ab" * 20
        mock_lc.create_mech.return_value = "0x" + "cd" * 20
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["deploy-mech", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Safe deployed" in result.output
        assert "Mech created" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_deploy_activation_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.activate.return_value = False
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["deploy-mech", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Activation failed" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_deploy_registration_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.activate.return_value = True
        mock_lc.register_agent.return_value = False
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["deploy-mech", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Registration failed" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_deploy_safe_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.activate.return_value = True
        mock_lc.register_agent.return_value = True
        mock_lc.deploy.return_value = None
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["deploy-mech", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Deploy failed" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_deploy_mech_creation_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.activate.return_value = True
        mock_lc.register_agent.return_value = True
        mock_lc.deploy.return_value = "0x" + "ab" * 20
        mock_lc.create_mech.return_value = None
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["deploy-mech", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Mech creation failed" in result.output


class TestStakeCommand:
    @patch("micromech.management.MechLifecycle")
    def test_stake_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.stake.return_value = True
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["stake", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Staked successfully" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_stake_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.stake.return_value = False
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["stake", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Staking failed" in result.output


class TestUnstakeCommand:
    @patch("micromech.management.MechLifecycle")
    def test_unstake_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.unstake.return_value = True
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["unstake", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Unstaked successfully" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_unstake_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.unstake.return_value = False
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["unstake", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Unstaking failed" in result.output


class TestClaimCommand:
    @patch("micromech.management.MechLifecycle")
    def test_claim_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.claim_rewards.return_value = True
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["claim", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Rewards claimed" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_claim_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.claim_rewards.return_value = False
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["claim", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 1
        assert "Claim failed" in result.output


class TestMechStatusCommand:
    @patch("micromech.management.MechLifecycle")
    def test_mech_status_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.get_status.return_value = {"service_id": 42, "is_staked": True}
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["mech-status", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "42" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_mech_status_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.get_status.return_value = None
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(app, ["mech-status", "svc-1", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Failed to get status" in result.output


class TestMetadataUpdateCommand:
    @patch("micromech.management.MechLifecycle")
    def test_metadata_update_success(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.update_metadata_onchain.return_value = "0xdeadbeef"
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(
            app,
            ["metadata-update", "svc-1", "0x1234abcd", "--config", str(config_path)],
        )
        assert result.exit_code == 0
        assert "Metadata updated" in result.output

    @patch("micromech.management.MechLifecycle")
    def test_metadata_update_failure(self, mock_lc_cls, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)

        mock_lc = MagicMock()
        mock_lc.update_metadata_onchain.return_value = None
        mock_lc_cls.return_value = mock_lc

        result = runner.invoke(
            app,
            ["metadata-update", "svc-1", "0x1234abcd", "--config", str(config_path)],
        )
        assert result.exit_code == 1
        assert "Update failed" in result.output


class TestAddToolCommand:
    def test_add_tool_creates_package(self, tmp_path: Path, monkeypatch):
        """add-tool scaffolds the correct files."""
        import micromech.cli as cli_mod

        # Create a fake directory structure so Path(__file__).parent^3 = tmp_path
        fake_cli = tmp_path / "src" / "micromech" / "cli.py"
        fake_cli.parent.mkdir(parents=True, exist_ok=True)
        fake_cli.touch()
        monkeypatch.setattr(cli_mod, "__file__", str(fake_cli))

        result = runner.invoke(app, ["add-tool", "my_test_tool"])
        assert result.exit_code == 0
        assert "Created tool package" in result.output

        tool_dir = tmp_path / "tools" / "custom" / "my_test_tool"
        assert (tool_dir / "__init__.py").exists()
        assert (tool_dir / "component.yaml").exists()
        assert (tool_dir / "my_test_tool.py").exists()

        # Verify component.yaml content
        import yaml

        spec = yaml.safe_load((tool_dir / "component.yaml").read_text())
        assert spec["name"] == "my_test_tool"
        assert spec["entry_point"] == "my_test_tool.py"

        # Verify Python file has ALLOWED_TOOLS and run()
        py_content = (tool_dir / "my_test_tool.py").read_text()
        assert 'ALLOWED_TOOLS = ["my_test_tool"]' in py_content
        assert "def run(" in py_content
        assert "ALLOWED_TOOLS" in py_content

    def test_add_tool_invalid_name(self):
        result = runner.invoke(app, ["add-tool", "Bad-Name"])
        assert result.exit_code == 1
        assert "Invalid tool name" in result.output

    def test_add_tool_already_exists(self, tmp_path: Path, monkeypatch):
        import micromech.cli as cli_mod

        fake_cli = tmp_path / "src" / "micromech" / "cli.py"
        fake_cli.parent.mkdir(parents=True, exist_ok=True)
        fake_cli.touch()
        monkeypatch.setattr(cli_mod, "__file__", str(fake_cli))

        # Create the tool dir first
        tool_dir = tmp_path / "tools" / "custom" / "existing_tool"
        tool_dir.mkdir(parents=True)

        result = runner.invoke(app, ["add-tool", "existing_tool"])
        assert result.exit_code == 1
        assert "already exists" in result.output


class TestDoctorCommand:
    def test_doctor_no_config(self, tmp_path: Path):
        config_path = tmp_path / "nonexistent.yaml"
        result = runner.invoke(app, ["doctor", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "doctor" in result.output.lower()
        assert "No config" in result.output or "warning" in result.output.lower()

    def test_doctor_with_config(self, tmp_path: Path):
        config_path = tmp_path / "config.yaml"
        MicromechConfig().save(config_path)
        result = runner.invoke(app, ["doctor", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Config loaded" in result.output
