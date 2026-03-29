"""Mech lifecycle management via iwa's ServiceManager.

Wraps iwa's ServiceManager to provide the full mech lifecycle:
create → activate → register → deploy → create_mech → stake → run → claim → unstake
"""

from typing import Any, Optional

from loguru import logger

from micromech.core.config import MicromechConfig


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
    """Full mech lifecycle management.

    Wraps iwa's ServiceManager with mech-specific operations.
    Each method is idempotent where possible.
    """

    def __init__(self, config: MicromechConfig):
        self.config = config

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
            logger.info("Service created: {}", service_id)
            return service_id
        except Exception as e:
            logger.error("Failed to create service: {}", e)
            return None

    def activate(self, service_key: str) -> bool:
        """Activate service registration (deposits OLAS)."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.activate_registration()
            logger.info("Service activated: {}", result)
            return result
        except Exception as e:
            logger.error("Failed to activate: {}", e)
            return False

    def register_agent(self, service_key: str) -> bool:
        """Register agent instance (deposits OLAS bond)."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.register_agent()
            logger.info("Agent registered: {}", result)
            return result
        except Exception as e:
            logger.error("Failed to register agent: {}", e)
            return False

    def deploy(self, service_key: str) -> Optional[str]:
        """Deploy the service (creates Gnosis Safe multisig).

        Returns the multisig address or None.
        """
        mgr = _get_service_manager(self.config, service_key)
        try:
            multisig = mgr.deploy()
            logger.info("Service deployed, multisig: {}", multisig)
            return multisig
        except Exception as e:
            logger.error("Failed to deploy: {}", e)
            return None

    def create_mech(
        self,
        service_key: str,
        factory_address: Optional[str] = None,
        delivery_rate: Optional[int] = None,
    ) -> Optional[str]:
        """Create a mech on the marketplace.

        Calls marketplace.create(serviceId, factory, deliveryRate).
        Returns the mech contract address or None.
        """
        from micromech.core.constants import (
            DEFAULT_DELIVERY_RATE,
            MECH_FACTORY_FIXED_PRICE_NATIVE,
            MECH_MARKETPLACE_ADDRESS,
        )

        mgr = _get_service_manager(self.config, service_key)
        factory = factory_address or MECH_FACTORY_FIXED_PRICE_NATIVE
        rate = delivery_rate or DEFAULT_DELIVERY_RATE

        try:
            bridge = mgr.wallet
            web3 = bridge.chain_interfaces.get(self.config.mech.chain).web3
            from micromech.runtime.contracts import load_marketplace_abi

            marketplace = web3.eth.contract(
                address=web3.to_checksum_address(MECH_MARKETPLACE_ADDRESS),
                abi=load_marketplace_abi(),
            )

            service_id = mgr.service.service_id if mgr.service else None
            if not service_id:
                logger.error("No service ID found")
                return None

            # marketplace.create(serviceId, factory, deliveryRate)
            # This must be called from the service owner
            tx = marketplace.functions.create(
                service_id,
                web3.to_checksum_address(factory),
                rate,
            ).transact(
                {
                    "from": web3.to_checksum_address(str(mgr.service.owner_address)),
                    "gas": 10_000_000,
                }
            )
            receipt = web3.eth.wait_for_transaction_receipt(tx)
            if receipt["status"] != 1:
                logger.error("Mech creation TX reverted")
                return None

            # Extract mech address from CreateMech event
            logs = receipt.get("logs", [])
            for log in logs:
                if len(log.get("topics", [])) >= 2:
                    mech_addr = "0x" + log["topics"][1].hex()[-40:]
                    logger.info("Mech created: {}", mech_addr)
                    return mech_addr

            logger.warning("Mech created but address not found in logs")
            return None
        except Exception as e:
            logger.error("Failed to create mech: {}", e)
            return None

    def stake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Stake the service in a supply staking contract."""
        from micromech.core.constants import SUPPLY_STAKING_ALPHA

        contract = staking_contract or SUPPLY_STAKING_ALPHA
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.stake(staking_contract=contract)
            logger.info("Staked in {}: {}", contract, result)
            return result
        except Exception as e:
            logger.error("Failed to stake: {}", e)
            return False

    def unstake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Unstake the service."""
        from micromech.core.constants import SUPPLY_STAKING_ALPHA

        contract = staking_contract or SUPPLY_STAKING_ALPHA
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.unstake(staking_contract=contract)
            logger.info("Unstaked from {}: {}", contract, result)
            return result
        except Exception as e:
            logger.error("Failed to unstake: {}", e)
            return False

    def claim_rewards(self, service_key: str) -> bool:
        """Claim staking rewards."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.claim_rewards()
            logger.info("Rewards claimed: {}", result)
            return result
        except Exception as e:
            logger.error("Failed to claim rewards: {}", e)
            return False

    def get_status(self, service_key: str) -> Optional[dict]:
        """Get comprehensive service/staking status."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            staking = mgr.get_staking_status(force_refresh=True)
            if staking:
                return {
                    "service_id": staking.service_id,
                    "staking_state": staking.staking_state,
                    "is_staked": staking.is_staked,
                    "rewards": staking.accrued_reward_olas,
                    "requests_this_epoch": getattr(staking, "mech_requests_this_epoch", 0),
                    "required_requests": getattr(staking, "required_mech_requests", 0),
                }
            return {"status": "not_staked"}
        except Exception as e:
            logger.error("Failed to get status: {}", e)
            return None

    def checkpoint(self, service_key: str) -> bool:
        """Call checkpoint on the staking contract."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.call_checkpoint()
            logger.info("Checkpoint: {}", result)
            return result
        except Exception as e:
            logger.error("Failed to checkpoint: {}", e)
            return False

    def update_metadata_onchain(
        self,
        service_key: str,
        metadata_hash: str,
    ) -> Optional[str]:
        """Update mech metadata hash on-chain via changeHash().

        Args:
            service_key: The service key in iwa config.
            metadata_hash: The 0x-prefixed hash from metadata-push.

        Returns tx hash or None.
        """
        from micromech.runtime.contracts import (
            COMPLEMENTARY_SERVICE_METADATA_ABI,
            COMPLEMENTARY_SERVICE_METADATA_ADDRESS,
        )

        mgr = _get_service_manager(self.config, service_key)
        try:
            chain = self.config.mech.chain
            contract_addr = COMPLEMENTARY_SERVICE_METADATA_ADDRESS.get(chain)
            if not contract_addr:
                logger.error("No metadata contract for chain {}", chain)
                return None

            web3 = mgr.wallet.chain_interfaces.get(chain).web3
            contract = web3.eth.contract(
                address=web3.to_checksum_address(contract_addr),
                abi=COMPLEMENTARY_SERVICE_METADATA_ABI,
            )

            service_id = mgr.service.service_id if mgr.service else None
            if not service_id:
                logger.error("No service ID")
                return None

            hash_bytes = (
                bytes.fromhex(metadata_hash[2:])
                if metadata_hash.startswith("0x")
                else bytes.fromhex(metadata_hash)
            )

            # Execute via Safe
            tx_hash = mgr.wallet.safe_service.execute_safe_transaction(
                safe_address_or_tag=str(mgr.service.multisig_address),
                to=contract_addr,
                value=0,
                chain_name=chain,
                data=contract.functions.changeHash(service_id, hash_bytes).build_transaction(
                    {"from": str(mgr.service.multisig_address)}
                )["data"],
            )
            logger.info("Metadata updated on-chain: {}", tx_hash)
            return tx_hash
        except Exception as e:
            logger.error("Failed to update metadata: {}", e)
            return None
