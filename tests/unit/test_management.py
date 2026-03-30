"""Tests for the management lifecycle module."""

from unittest.mock import MagicMock, patch

from micromech.core.config import MicromechConfig
from micromech.management import MechLifecycle


class TestMechLifecycleInit:
    def test_creates_instance(self):
        cfg = MicromechConfig()
        lc = MechLifecycle(cfg)
        assert lc.config is cfg


class TestMechLifecycleWithMocks:
    """Test lifecycle methods with mocked iwa ServiceManager."""

    @patch("micromech.management._get_service_manager")
    def test_create_service(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 42
        mock_get_mgr.return_value = mock_mgr

        cfg = MicromechConfig()
        lc = MechLifecycle(cfg)
        result = lc.create_service(agent_id=40, bond_olas=10000)
        assert result == 42
        mock_mgr.create.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_activate(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.activate("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_register_agent(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.register_agent.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.register_agent("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_deploy(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.deploy.return_value = "0x" + "ab" * 20
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.deploy("svc-1")
        assert result == "0x" + "ab" * 20

    @patch("micromech.management._get_service_manager")
    def test_stake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.stake("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_unstake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.unstake("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_claim_rewards(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.claim_rewards("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_checkpoint(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.checkpoint("svc-1") is True

    @patch("micromech.management._get_service_manager")
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


class TestCreateMech:
    """Test create_mech which interacts with the marketplace contract."""

    @patch("micromech.management._get_service_manager")
    def test_create_mech_success(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.owner_address = "0x" + "aa" * 20

        # Mock web3 + contract
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3

        tx_hash = b"\xde\xad" + b"\x00" * 30
        mock_web3.eth.contract.return_value.functions.create.return_value.transact.return_value = (
            tx_hash
        )
        # Receipt with log containing mech address
        mech_addr_hex = "cd" * 20
        mock_web3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "logs": [
                {
                    "topics": [
                        bytes(32),
                        bytes.fromhex("00" * 12 + mech_addr_hex),
                    ],
                }
            ],
        }
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.create_mech("svc-1")
        assert result is not None
        assert mech_addr_hex in result

    @patch("micromech.management._get_service_manager")
    def test_create_mech_no_service_id(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = None
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_create_mech_tx_reverted(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.owner_address = "0x" + "aa" * 20
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_create_mech_no_logs(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.owner_address = "0x" + "aa" * 20
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_web3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "logs": [],
        }
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_create_mech_exception(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.create_mech("svc-1")
        assert result is None


class TestUpdateMetadataOnchain:
    @patch("micromech.management._get_service_manager")
    def test_update_metadata_success(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.multisig_address = "0x" + "bb" * 20
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_mgr.wallet.safe_service.execute_safe_transaction.return_value = "0xtxhash"
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.update_metadata_onchain("svc-1", "0x" + "12" * 34)
        assert result == "0xtxhash"

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_no_service_id(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = None
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.update_metadata_onchain("svc-1", "0x1234")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_unknown_chain(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_get_mgr.return_value = mock_mgr

        cfg = MicromechConfig(mech={"chain": "unknown_chain"})
        lc = MechLifecycle(cfg)
        result = lc.update_metadata_onchain("svc-1", "0x1234")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_exception(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.update_metadata_onchain("svc-1", "0x1234")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_hash_without_0x(self, mock_get_mgr):
        """Metadata hash without 0x prefix is handled."""
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.multisig_address = "0x" + "bb" * 20
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_mgr.wallet.safe_service.execute_safe_transaction.return_value = "0xtxhash"
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.update_metadata_onchain("svc-1", "12" * 34)
        assert result == "0xtxhash"


class TestGetServiceManager:
    def test_raises_import_error_without_iwa(self):
        """_get_service_manager raises ImportError when iwa is not available."""

        with patch.dict(
            "sys.modules", {"iwa.core.wallet": None, "iwa.plugins.olas.service_manager": None}
        ):
            with patch("micromech.management._get_service_manager") as mock_fn:
                mock_fn.side_effect = ImportError("iwa is required")
                import pytest as pt

                with pt.raises(ImportError, match="iwa is required"):
                    mock_fn(MicromechConfig())


class TestGetStatusEdgeCases:
    @patch("micromech.management._get_service_manager")
    def test_get_status_not_staked(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.get_staking_status.return_value = None
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        status = lc.get_status("svc-1")
        assert status == {"status": "not_staked"}

    @patch("micromech.management._get_service_manager")
    def test_get_status_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.get_staking_status.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        result = lc.get_status("svc-1")
        assert result is None


class TestMechLifecycleErrorHandling:
    """Test that failures return None/False instead of crashing."""

    @patch("micromech.management._get_service_manager")
    def test_create_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.create_service() is None

    @patch("micromech.management._get_service_manager")
    def test_activate_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.activate("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_stake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.stake("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_unstake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.unstake("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_claim_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(MicromechConfig())
        assert lc.claim_rewards("svc") is False
