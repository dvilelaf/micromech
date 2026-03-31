"""Mech lifecycle management via iwa's ServiceManager.

Wraps iwa's ServiceManager to provide the full mech lifecycle:
create → activate → register → deploy → create_mech → stake → run → claim → unstake

Each MechLifecycle targets a specific chain via ChainConfig.
"""

from typing import Any, Optional

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig


def _get_service_manager(config: MicromechConfig, service_key: Optional[str] = None) -> Any:
    """Get an iwa ServiceManager instance."""
    try:
        from iwa.core.wallet import Wallet
        from iwa.plugins.olas.service_manager import ServiceManager

        wallet = Wallet()
        return ServiceManager(wallet, service_key=service_key)
    except ImportError as e:
        msg = (
            "iwa is required for management operations. Install with: pip install micromech[chain]"
        )
        raise ImportError(msg) from e


class MechLifecycle:
    """Full mech lifecycle management for a specific chain.

    Wraps iwa's ServiceManager with mech-specific operations.
    Each method is idempotent where possible.
    """

    def __init__(self, config: MicromechConfig, chain_name: str):
        self.config = config
        self.chain_name = chain_name
        if chain_name not in config.chains:
            msg = f"Chain '{chain_name}' not found in config. Available: {list(config.chains)}"
            raise ValueError(msg)
        self.chain_config: ChainConfig = config.chains[chain_name]

    def create_service(
        self,
        agent_id: int = 40,
        num_agents: int = 1,
        bond_olas: int = 10000,
        threshold: int = 1,
    ) -> Optional[int]:
        """Create a new service on-chain.

        Returns the service_id or None on failure.
        """
        mgr = _get_service_manager(self.config)
        try:
            service_id = mgr.create(
                agent_id=agent_id,
                num_agents=num_agents,
                bond_olas=bond_olas,
                threshold=threshold,
            )
            logger.info("Service created on {}: {}", self.chain_name, service_id)
            return service_id
        except Exception as e:
            logger.error("Failed to create service on {}: {}", self.chain_name, e)
            return None

    def activate(self, service_key: str) -> bool:
        """Activate service registration (deposits OLAS)."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.activate_registration()
            logger.info("Service activated on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to activate on {}: {}", self.chain_name, e)
            return False

    def register_agent(self, service_key: str) -> bool:
        """Register agent instance (deposits OLAS bond)."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.register_agent()
            logger.info("Agent registered on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to register agent on {}: {}", self.chain_name, e)
            return False

    def deploy(self, service_key: str) -> Optional[str]:
        """Deploy the service (creates Gnosis Safe multisig).

        Returns the multisig address or None.
        """
        mgr = _get_service_manager(self.config, service_key)
        try:
            multisig = mgr.deploy()
            logger.info("Service deployed on {}, multisig: {}", self.chain_name, multisig)
            return multisig
        except Exception as e:
            logger.error("Failed to deploy on {}: {}", self.chain_name, e)
            return None

    def create_mech(
        self,
        service_key: str,
        factory_address: Optional[str] = None,
        delivery_rate: Optional[int] = None,
    ) -> Optional[str]:
        """Create a mech on the marketplace. Delegates to iwa's MechSupplyMixin."""
        mgr = _get_service_manager(self.config, service_key)
        factory = factory_address or self.chain_config.factory_address
        rate = delivery_rate or self.chain_config.delivery_rate
        try:
            from iwa.plugins.olas.service_manager.mech import MechSupplyMixin

            # Attach mixin method to the manager instance
            return MechSupplyMixin.create_mech_on_marketplace(
                mgr,
                chain_name=self.chain_name,
                factory_address=factory,
                delivery_rate=rate,
                marketplace_address=self.chain_config.marketplace_address,
            )
        except ImportError:
            logger.error("iwa MechSupplyMixin not available")
            return None
        except Exception as e:
            logger.error("Failed to create mech on {}: {}", self.chain_name, e)
            return None

    def stake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Stake the service in a supply staking contract."""
        contract = staking_contract or self.chain_config.staking_address
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.stake(staking_contract=contract)
            logger.info("Staked on {} in {}: {}", self.chain_name, contract[:16], result)
            return result
        except Exception as e:
            logger.error("Failed to stake on {}: {}", self.chain_name, e)
            return False

    def unstake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Unstake the service."""
        contract = staking_contract or self.chain_config.staking_address
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.unstake(staking_contract=contract)
            logger.info("Unstaked on {} from {}: {}", self.chain_name, contract[:16], result)
            return result
        except Exception as e:
            logger.error("Failed to unstake on {}: {}", self.chain_name, e)
            return False

    def claim_rewards(self, service_key: str) -> bool:
        """Claim staking rewards."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.claim_rewards()
            logger.info("Rewards claimed on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to claim rewards on {}: {}", self.chain_name, e)
            return False

    def get_status(self, service_key: str) -> Optional[dict]:
        """Get comprehensive service/staking status."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            staking = mgr.get_staking_status(force_refresh=True)
            if staking:
                return {
                    "chain": self.chain_name,
                    "service_id": staking.service_id,
                    "staking_state": staking.staking_state,
                    "is_staked": staking.is_staked,
                    "rewards": staking.accrued_reward_olas,
                    "requests_this_epoch": getattr(staking, "mech_requests_this_epoch", 0),
                    "required_requests": getattr(staking, "required_mech_requests", 0),
                }
            return {"chain": self.chain_name, "status": "not_staked"}
        except Exception as e:
            logger.error("Failed to get status on {}: {}", self.chain_name, e)
            return None

    def checkpoint(self, service_key: str) -> bool:
        """Call checkpoint on the staking contract."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.call_checkpoint()
            logger.info("Checkpoint on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to checkpoint on {}: {}", self.chain_name, e)
            return False

    def update_metadata_onchain(
        self,
        service_key: str,
        metadata_hash: str,
    ) -> Optional[str]:
        """Update mech metadata hash on-chain. Delegates to iwa's MechSupplyMixin."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            from iwa.plugins.olas.service_manager.mech import MechSupplyMixin

            return MechSupplyMixin.update_mech_metadata(
                mgr,
                chain_name=self.chain_name,
                metadata_hash=metadata_hash,
            )
        except ImportError:
            logger.error("iwa MechSupplyMixin not available")
            return None
        except Exception as e:
            logger.error("Failed to update metadata on {}: {}", self.chain_name, e)
            return None
