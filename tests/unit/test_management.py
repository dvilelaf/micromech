"""Tests for the management lifecycle module."""

from unittest.mock import MagicMock, patch

from micromech.core.config import MicromechConfig
from micromech.management.lifecycle import MechLifecycle


class TestMechLifecycleInit:
    def test_creates_instance(self):
        cfg = MicromechConfig()
        lc = MechLifecycle(cfg)
        assert lc.config is cfg


class TestMechLifecycleWithMocks:
    """Test lifecycle methods with mocked iwa ServiceManager."""

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_create_service(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 42
        mock_get_mgr.return_value = mock_mgr

        cfg = MicromechConfig()
        lc = MechLifecycle(cfg)
        result = lc.create_service(agent_id=40, bond_olas=10000)
        assert result == 42
        mock_mgr.create.assert_called_once()

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_activate(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.activate("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_register_agent(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.register_agent.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.register_agent("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_deploy(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.deploy.return_value = "0x" + "ab" * 20
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.deploy("svc-1")
        assert result == "0x" + "ab" * 20

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_stake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.stake("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_unstake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.unstake("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_claim_rewards(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.claim_rewards("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_checkpoint(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.checkpoint("svc-1") is True

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_get_status(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_status = MagicMock()
        mock_status.service_id = 42
        mock_status.staking_state = "STAKED"
        mock_status.is_staked = True
        mock_status.accrued_reward_olas = 1.5
        mock_status.mech_requests_this_epoch = 10
        mock_status.required_mech_requests = 20
        mock_mgr.get_staking_status.return_value = mock_status
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        status = lc.get_status("svc-1")
        assert status["service_id"] == 42
        assert status["is_staked"] is True
        assert status["rewards"] == 1.5


class TestMechLifecycleErrorHandling:
    """Test that failures return None/False instead of crashing."""

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_create_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.create_service() is None

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_activate_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.activate("svc") is False

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_stake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.stake("svc") is False

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_unstake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.unstake("svc") is False

    @patch("micromech.management.lifecycle._get_service_manager")
    def test_claim_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.claim_rewards("svc") is False
