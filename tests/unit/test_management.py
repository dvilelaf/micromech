"""Tests for the management lifecycle module."""

from unittest.mock import MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS
from micromech.management import MechLifecycle
from tests.conftest import make_test_config

CHAIN_NAME = "gnosis"
MARKETPLACE = CHAIN_DEFAULTS["gnosis"]["marketplace"]


class TestMechLifecycleInit:
    def test_creates_instance(self):
        cfg = make_test_config()
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)
        assert lc.config is cfg

    def test_unknown_chain_raises(self):
        cfg = make_test_config()
        with pytest.raises(ValueError, match="not found in config"):
            MechLifecycle(cfg, chain_name="unknown_chain")


class TestMechLifecycleWithMocks:
    """Test lifecycle methods with mocked iwa ServiceManager."""

    @patch("micromech.management._get_service_manager")
    def test_create_service(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 42
        mock_get_mgr.return_value = mock_mgr

        cfg = make_test_config()
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)
        result = lc.create_service(agent_id=40, bond_olas=10000)
        assert result == 42
        mock_mgr.create.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_activate(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.activate("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_register_agent(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.register_agent.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.register_agent("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_deploy(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.deploy.return_value = "0x" + "ab" * 20
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.deploy("svc-1")
        assert result == "0x" + "ab" * 20

    @patch("micromech.management._get_service_manager")
    def test_stake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.stake("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_unstake(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.unstake("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_claim_rewards(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.claim_rewards("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_checkpoint(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.return_value = True
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.checkpoint("svc-1") is True

    @patch("micromech.management._get_service_manager")
    def test_checkpoint_false_still_invalidates_cache(self, mock_get_mgr):
        """Bug: when checkpoint() returns False (another instance already called it on
        the same staking contract), the staking status cache must still be invalidated.

        Without this fix, services B and C (on the same staking contract as A) see stale
        epoch data for up to CacheTTL.STAKING_STATUS (60s) after the checkpoint TX, which
        delays their traders from sending requests in the new epoch.
        """
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.return_value = False  # Another instance beat us to it
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.management.response_cache") as mock_cache:
            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.checkpoint("gnosis:101")

        assert result is False
        mock_cache.invalidate.assert_called_once_with("staking_status:gnosis:101")

    @patch("micromech.management._get_service_manager")
    def test_checkpoint_success_also_invalidates_cache(self, mock_get_mgr):
        """When checkpoint() succeeds (this instance called it), iwa invalidates its own
        cache key. Verify our explicit invalidation also fires so the guarantee holds
        regardless of iwa's internal implementation details.
        """
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.return_value = True
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.management.response_cache") as mock_cache:
            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.checkpoint("gnosis:100")

        assert result is True
        mock_cache.invalidate.assert_called_once_with("staking_status:gnosis:100")

    @patch("micromech.management._get_service_manager")
    def test_checkpoint_exception_still_invalidates_cache(self, mock_get_mgr):
        """Even when call_checkpoint() raises, the finally block must still invalidate
        the cache — the epoch state on-chain may have changed and we want fresh data
        on the next status fetch.
        """
        mock_mgr = MagicMock()
        mock_mgr.call_checkpoint.side_effect = RuntimeError("RPC timeout")
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.management.response_cache") as mock_cache:
            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.checkpoint("gnosis:102")

        assert result is False  # exception path returns False
        mock_cache.invalidate.assert_called_once_with("staking_status:gnosis:102")

    @patch("micromech.management._get_service_manager")
    def test_get_status(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_status = MagicMock()
        mock_status.staking_state = "STAKED"
        mock_status.is_staked = True
        mock_status.accrued_reward_olas = 1.5
        mock_status.mech_requests_this_epoch = 10
        mock_status.required_mech_requests = 20
        mock_mgr.get_staking_status.return_value = mock_status
        mock_get_mgr.return_value = mock_mgr

        cfg = make_test_config()
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)
        status = lc.get_status("svc-1")
        assert status["service_id"] == 42
        assert status["is_staked"] is True
        assert status["rewards"] == 1.5


class TestCreateMech:
    """Test create_mech which interacts with the marketplace contract."""

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_create_mech_success(self, mock_get_mgr, mock_get_wallet):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20

        # Mock web3 and chain_interface via get_wallet()
        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_ci = mock_wallet.chain_interfaces.get.return_value
        mock_ci.web3 = mock_web3
        mock_ci.estimate_gas.return_value = 3_000_000  # > GAS_FLOOR_CREATE2 (2M)
        mock_get_wallet.return_value = mock_wallet

        # Receipt with log containing mech address — returned by sign_and_send
        mech_addr_hex = "cd" * 20
        mock_wallet.transaction_service.sign_and_send.return_value = (
            True,
            {
                "status": 1,
                "logs": [
                    {
                        "address": MARKETPLACE,
                        "topics": [
                            bytes(32),  # any event topic
                            bytes.fromhex("00" * 12 + mech_addr_hex),
                        ],
                    }
                ],
            },
        )
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.create_mech("svc-1")
        assert result is not None
        assert mech_addr_hex in result.lower()
        mock_ci.estimate_gas.assert_called_once()

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_create_mech_gas_floor_applied(self, mock_get_mgr, mock_get_wallet):
        """When estimate_gas returns < GAS_FLOOR_CREATE2, the floor (2M) is used."""
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20

        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_ci = mock_wallet.chain_interfaces.get.return_value
        mock_ci.web3 = mock_web3
        mock_ci.estimate_gas.return_value = 500_000  # below GAS_FLOOR_CREATE2 (2M)
        mock_get_wallet.return_value = mock_wallet

        mech_addr_hex = "cd" * 20
        mock_wallet.transaction_service.sign_and_send.return_value = (
            True,
            {
                "status": 1,
                "logs": [
                    {
                        "address": MARKETPLACE,
                        "topics": [
                            bytes(32),
                            bytes.fromhex("00" * 12 + mech_addr_hex),
                        ],
                    }
                ],
            },
        )
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.create_mech("svc-1")
        assert result is not None
        # Verify build_transaction was called with gas = 2_000_000 (the floor)
        fn_call = mock_ci.web3.eth.contract.return_value.functions.create.return_value
        call_kwargs = fn_call.build_transaction.call_args[0][0]
        assert call_kwargs["gas"] == 2_000_000

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_create_mech_no_service_id(self, mock_get_mgr, mock_get_wallet):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = None
        mock_web3 = MagicMock()
        mock_get_wallet.return_value.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
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

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
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

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.management._get_service_manager")
    def test_create_mech_exception(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
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

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.update_metadata_onchain("svc-1", "0x" + "12" * 34)
        assert result == "0xtxhash"

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_no_service_id(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.service.service_id = None
        mock_web3 = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.update_metadata_onchain("svc-1", "0x1234")
        assert result is None

    def test_update_metadata_unknown_chain(self):
        """Creating MechLifecycle with unknown chain raises ValueError."""
        cfg = make_test_config()
        with pytest.raises(ValueError, match="not found in config"):
            MechLifecycle(cfg, chain_name="unknown_chain")

    @patch("micromech.management._get_service_manager")
    def test_update_metadata_exception(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.wallet.chain_interfaces.get.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
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

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
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

                with pytest.raises(ImportError, match="iwa is required"):
                    mock_fn(make_test_config())


class TestGetStatusEdgeCases:
    @patch("micromech.management._get_service_manager")
    def test_get_status_not_staked(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.get_staking_status.return_value = None
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        status = lc.get_status("svc-1")
        assert status == {"status": "not_staked", "chain": CHAIN_NAME}

    @patch("micromech.management._get_service_manager")
    def test_get_status_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.get_staking_status.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.get_status("svc-1")
        assert result is None


class TestMechLifecycleErrorHandling:
    """Test that failures return None/False instead of crashing."""

    @patch("micromech.management._get_service_manager")
    def test_create_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.create.side_effect = RuntimeError("rpc error")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.create_service() is None

    @patch("micromech.management._get_service_manager")
    def test_activate_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.activate_registration.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.activate("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_stake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.stake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.stake("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_unstake_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.unstake.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.unstake("svc") is False

    @patch("micromech.management._get_service_manager")
    def test_claim_failure(self, mock_get_mgr):
        mock_mgr = MagicMock()
        mock_mgr.claim_rewards.side_effect = RuntimeError("fail")
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        assert lc.claim_rewards("svc") is False


class TestFullDeploy:
    """Tests for full_deploy with resume support."""

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_from_scratch(self, mock_get_mgr, mock_svc_info):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 99
        mock_mgr.spin_up.return_value = True
        mock_mgr.service.multisig_address = "0x" + "aa" * 20
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        cfg = make_test_config()
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)
        with patch.object(lc, "create_mech", return_value="0x" + "bb" * 20):
            progress = []
            result = lc.full_deploy(on_progress=lambda s, t, m, ok=True: progress.append((s, m)))

        assert result["service_id"] == 99
        assert result["multisig_address"] == "0x" + "aa" * 20
        assert result["mech_address"] == "0x" + "bb" * 20
        assert result["staked"] is True
        assert len(progress) >= 6
        mock_mgr.spin_up.assert_called_once()

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_no_mech_runs_full_flow(self, mock_get_mgr, mock_svc_info):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 42
        mock_mgr.spin_up.return_value = True
        mock_mgr.service.multisig_address = "0x" + "cc" * 20
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        cfg = make_test_config()
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)

        with patch.object(lc, "create_mech", return_value="0x" + "dd" * 20):
            result = lc.full_deploy()

        assert result["mech_address"] == "0x" + "dd" * 20
        assert result["service_id"] == 42
        mock_mgr.create.assert_called_once()
        mock_mgr.spin_up.assert_called_once()

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_already_complete(self, mock_get_mgr, mock_svc_info):
        mock_mgr = MagicMock()
        mock_mgr.stake.return_value = True
        mock_get_mgr.return_value = mock_mgr

        from micromech.core.config import ChainConfig

        cfg = MicromechConfig(
            chains={
                "gnosis": ChainConfig(
                    chain="gnosis",
                    mech_address="0x" + "dd" * 20,
                    marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
                    factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
                    staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
                )
            }
        )
        lc = MechLifecycle(cfg, chain_name=CHAIN_NAME)
        result = lc.full_deploy()

        assert result["staked"] is True
        mock_mgr.create.assert_not_called()

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_create_failure_raises(self, mock_get_mgr, mock_svc_info):
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = None
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        with pytest.raises(RuntimeError, match="Service creation failed"):
            lc.full_deploy()


class TestRollbackDeploy:
    """Tests for rollback_deploy and _cleanup_after_rollback."""

    def _make_mgr(self, state):
        """Build a mock ServiceManager with the given on-chain state."""
        from iwa.plugins.olas.contracts.service import ServiceState  # noqa: F401

        mock_mgr = MagicMock()
        mock_mgr.wind_down.return_value = True
        mock_mgr.registry.get_service.return_value = {"state": state}
        return mock_mgr

    @patch("micromech.management._get_service_manager")
    def test_rollback_calls_wind_down(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.ACTIVE_REGISTRATION)
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.core.bridge.get_wallet") as mock_get_wallet:
            mock_wallet = MagicMock()
            mock_wallet.master_account.address = "0x" + "aa" * 20
            mock_get_wallet.return_value = mock_wallet

            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.rollback_deploy("gnosis:42")

        assert result is True
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_rollback_terminates_active_registration(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.ACTIVE_REGISTRATION)
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.core.bridge.get_wallet") as mock_get_wallet:
            mock_wallet = MagicMock()
            mock_wallet.master_account.address = "0x" + "aa" * 20
            mock_get_wallet.return_value = mock_wallet

            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.rollback_deploy("gnosis:42")

        assert result is True
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_rollback_terminates_and_unbonds_finished_registration(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.FINISHED_REGISTRATION)
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.core.bridge.get_wallet") as mock_get_wallet:
            mock_wallet = MagicMock()
            mock_wallet.master_account.address = "0x" + "aa" * 20
            mock_get_wallet.return_value = mock_wallet

            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.rollback_deploy("gnosis:42")

        assert result is True
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_rollback_skips_terminate_when_pre_registration(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.PRE_REGISTRATION)
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.core.bridge.get_wallet") as mock_get_wallet:
            mock_wallet = MagicMock()
            mock_wallet.master_account.address = "0x" + "aa" * 20
            mock_get_wallet.return_value = mock_wallet

            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.rollback_deploy("gnosis:42")

        assert result is True
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_rollback_skips_terminate_when_terminated_bonded(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.TERMINATED_BONDED)
        mock_get_mgr.return_value = mock_mgr

        with patch("micromech.core.bridge.get_wallet") as mock_get_wallet:
            mock_wallet = MagicMock()
            mock_wallet.master_account.address = "0x" + "aa" * 20
            mock_get_wallet.return_value = mock_wallet

            lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
            result = lc.rollback_deploy("gnosis:42")

        assert result is True
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_called_once()

    @patch("micromech.management._get_service_manager")
    def test_rollback_returns_false_on_wind_down_failure(self, mock_get_mgr):
        from iwa.plugins.olas.contracts.service import ServiceState

        mock_mgr = self._make_mgr(ServiceState.ACTIVE_REGISTRATION)
        mock_mgr.wind_down.return_value = False
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        result = lc.rollback_deploy("gnosis:42")

        assert result is False
        mock_mgr.wind_down.assert_called_once()
        mock_mgr.drain_service.assert_not_called()

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_triggers_rollback_on_spinup_failure(self, mock_get_mgr, mock_svc_info):
        """Failing at spin_up triggers rollback with the correct service_key."""
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = 77
        mock_mgr.spin_up.return_value = False  # spin_up fails
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        with patch.object(lc, "rollback_deploy") as mock_rollback:
            mock_rollback.return_value = True
            with pytest.raises(RuntimeError, match="Service spin-up failed"):
                lc.full_deploy()

        mock_rollback.assert_called_once()
        called_key = mock_rollback.call_args[0][0]
        assert called_key == "gnosis:77"

    @patch("micromech.core.bridge.get_service_info", return_value={})
    @patch("micromech.management._get_service_manager")
    def test_full_deploy_no_rollback_on_step1_failure(self, mock_get_mgr, mock_svc_info):
        """Failing at create_service (step 1) does NOT trigger rollback (no service_key yet)."""
        mock_mgr = MagicMock()
        mock_mgr.create.return_value = None  # step 1 fails
        mock_get_mgr.return_value = mock_mgr

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        with patch.object(lc, "rollback_deploy") as mock_rollback:
            mock_rollback.return_value = True
            with pytest.raises(RuntimeError, match="Service creation failed"):
                lc.full_deploy()

        # rollback IS called but with service_key=None (nothing to do)
        mock_rollback.assert_called_once()
        called_key = mock_rollback.call_args[0][0]
        assert called_key is None

    @patch("micromech.core.bridge.get_wallet")
    def test_cleanup_removes_empty_agent_key(self, mock_get_wallet):
        """Agent key with zero balance is removed after rollback."""
        agent_addr = "0x" + "bb" * 20
        master_addr = "0x" + "cc" * 20

        mock_agent = MagicMock()
        mock_agent.tag = "micromech_agent"
        mock_agent.address = agent_addr

        mock_master = MagicMock()
        mock_master.address = master_addr

        mock_wallet = MagicMock()
        mock_wallet.master_account = mock_master
        mock_wallet.key_storage.accounts = {agent_addr: mock_agent}
        mock_wallet.get_native_balance_eth.return_value = 0.0  # empty
        mock_get_wallet.return_value = mock_wallet

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        mock_mgr = MagicMock()

        with patch("iwa.core.models.Config"):
            lc._cleanup_after_rollback("gnosis:42", mock_mgr)

        mock_wallet.key_storage.remove_account.assert_called_once_with(agent_addr)

    @patch("micromech.core.bridge.get_wallet")
    def test_cleanup_keeps_agent_key_with_funds(self, mock_get_wallet):
        """Agent key with non-zero balance is NOT removed."""
        agent_addr = "0x" + "bb" * 20
        master_addr = "0x" + "cc" * 20

        mock_agent = MagicMock()
        mock_agent.tag = "micromech_agent"
        mock_agent.address = agent_addr

        mock_master = MagicMock()
        mock_master.address = master_addr

        mock_wallet = MagicMock()
        mock_wallet.master_account = mock_master
        mock_wallet.key_storage.accounts = {agent_addr: mock_agent}
        mock_wallet.get_native_balance_eth.return_value = 0.5  # has funds
        mock_get_wallet.return_value = mock_wallet

        lc = MechLifecycle(make_test_config(), chain_name=CHAIN_NAME)
        mock_mgr = MagicMock()

        with patch("iwa.core.models.Config"):
            lc._cleanup_after_rollback("gnosis:42", mock_mgr)

        mock_wallet.key_storage.remove_account.assert_not_called()
