"""Mech lifecycle management via iwa's ServiceManager.

Wraps iwa's ServiceManager to provide the full mech lifecycle:
create → activate → register → deploy → create_mech → stake → run → claim → unstake

Each MechLifecycle targets a specific chain via ChainConfig.
"""

from typing import Any, Callable, Optional

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig


def _get_service_manager(
    config: MicromechConfig,
    service_key: Optional[str] = None,
    chain_name: Optional[str] = None,
) -> Any:
    """Get an iwa ServiceManager instance.

    Args:
        service_key: "chain:id" string for existing services.
        chain_name: Target chain. When creating a new service (no service_key),
                    this ensures contracts are initialized for the right chain.
    """
    try:
        from iwa.plugins.olas.service_manager import ServiceManager

        from micromech.core.bridge import get_wallet

        wallet = get_wallet()
        mgr = ServiceManager(wallet, service_key=service_key)
        # Ensure contracts are initialized for the target chain
        # (ServiceManager defaults to gnosis if no service_key is given)
        if chain_name and not service_key:
            mgr._init_contracts(chain_name)
        return mgr
    except ImportError as e:
        msg = (
            "iwa is required for management operations. "
            "Install with: pip install micromech[chain]"
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
        bond_olas: int = 10000,
    ) -> Optional[int]:
        """Create a new service on-chain.

        Returns the service_id or None on failure.
        """
        from web3 import Web3

        mgr = _get_service_manager(self.config, chain_name=self.chain_name)
        try:
            bond_wei = Web3.to_wei(bond_olas, "ether")
            service_id = mgr.create(
                chain_name=self.chain_name,
                agent_ids=[agent_id],
                bond_amount_wei=bond_wei,
                token_address_or_tag="OLAS",
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
            # Call create on marketplace directly (iwa 0.6.0 compat)
            from micromech.core.bridge import get_wallet

            wallet = get_wallet()
            ci = wallet.chain_interfaces.get(self.chain_name)
            web3 = ci.web3

            import json

            from iwa.core.types import EthereumAddress
            from iwa.plugins.olas.contracts.base import OLAS_ABI_PATH

            abi = json.loads((OLAS_ABI_PATH / "mech_marketplace.json").read_text())
            marketplace = web3.eth.contract(
                address=EthereumAddress(self.chain_config.marketplace_address),
                abi=abi,
            )
            service_id = mgr.service.service_id
            owner = mgr.service.service_owner_eoa_address

            # Third arg is bytes: ABI-encode the delivery rate
            from eth_abi import encode
            payload = encode(["uint256"], [rate])

            tx = marketplace.functions.create(
                service_id,
                EthereumAddress(factory),
                payload,
            ).transact({"from": EthereumAddress(owner), "gas": 10_000_000})

            receipt = web3.eth.wait_for_transaction_receipt(
                tx, timeout=120,
            )
            if receipt["status"] != 1:
                logger.error("Mech creation TX reverted")
                return None

            # Extract mech address from CreateMech event emitted by marketplace.
            # Match by marketplace address (not topic hash — marketplace is a
            # proxy whose implementation can change the event signature).
            # CreateMech has indexed `mech` address as topics[1].
            mkt = self.chain_config.marketplace_address.lower()
            for log_entry in receipt.get("logs", []):
                topics = log_entry.get("topics", [])
                log_addr = (log_entry.get("address") or "").lower()
                if log_addr == mkt and len(topics) >= 2:
                    raw = topics[1].hex() if isinstance(topics[1], bytes) else str(topics[1])
                    mech_addr = "0x" + raw[-40:]
                    logger.info("Mech created on {}: {}", self.chain_name, mech_addr)
                    return mech_addr

            logger.warning("Mech created but address not found in logs")
            return None
        except ImportError:
            logger.error("iwa MechSupplyMixin not available")
            return None
        except Exception as e:
            logger.error("Failed to create mech on {}: {}", self.chain_name, e)
            return None

    def _get_staking_contract(self, address: Optional[str] = None) -> Any:
        """Create a StakingContract instance from address string."""
        from iwa.plugins.olas.contracts.staking import StakingContract
        addr = address or self.chain_config.staking_address
        return StakingContract(addr, chain_name=self.chain_name)

    def stake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Stake the service in a supply staking contract."""
        contract = self._get_staking_contract(staking_contract)
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.stake(staking_contract=contract)
            logger.info("Staked on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to stake on {}: {}", self.chain_name, e)
            return False

    def unstake(self, service_key: str, staking_contract: Optional[str] = None) -> bool:
        """Unstake the service."""
        contract = self._get_staking_contract(staking_contract)
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = mgr.unstake(staking_contract=contract)
            logger.info("Unstaked on {}: {}", self.chain_name, result)
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

    def full_deploy(
        self,
        agent_id: int = 40,
        bond_olas: int = 10000,
        on_progress: Optional[Callable[[int, int, str, bool], None]] = None,
    ) -> dict[str, Any]:
        """Run the complete lifecycle, resuming from where a previous attempt left off.

        Respects detect_setup_state() so partial deploys can be continued.

        Args:
            on_progress: Optional callback(step: int, total: int, message: str, success: bool).

        Returns dict with keys: service_id, service_key, multisig_address, mech_address, staked.
        Raises RuntimeError on any step failure.
        """
        result: dict[str, Any] = {}
        total = 6

        def _progress(step: int, msg: str, success: bool = True) -> None:
            if on_progress:
                on_progress(step, total, msg, success)

        # Determine starting point from existing chain config state.
        # States: needs_create → needs_deploy → needs_mech → complete.
        # Steps 2-4 (activate, register, deploy Safe) are an atomic unit:
        # activate/register are idempotent, so re-running them is safe if
        # deploy failed on a previous attempt.
        state = self.chain_config.detect_setup_state()
        needs_service = state == "needs_create"
        needs_deploy = state in ("needs_create", "needs_deploy")
        needs_mech = state in ("needs_create", "needs_deploy", "needs_mech")

        # Populate result with already-known values
        if self.chain_config.service_id:
            result["service_id"] = self.chain_config.service_id
        if self.chain_config.service_key:
            result["service_key"] = self.chain_config.service_key
        if self.chain_config.multisig_address:
            result["multisig_address"] = self.chain_config.multisig_address
        if self.chain_config.mech_address:
            result["mech_address"] = self.chain_config.mech_address

        if state == "complete":
            _progress(6, "Already fully deployed", True)
            result["staked"] = True
            return result

        # Step 1: Create service (skip if already done)
        if needs_service:
            _progress(1, "Creating service...")
            service_id = self.create_service(
                agent_id=agent_id, bond_olas=bond_olas,
            )
            if not service_id:
                _progress(1, "Failed to create service", False)
                msg = "Service creation failed"
                raise RuntimeError(msg)
            result["service_id"] = service_id
            self.chain_config.service_id = service_id
            _progress(1, f"Service created: #{service_id}")

            service_key = f"{self.chain_name}:{service_id}"
            result["service_key"] = service_key
            self.chain_config.service_key = service_key
        else:
            _progress(1, f"Service exists: #{result.get('service_id')}")

        service_key = result.get("service_key", "")

        # Steps 2-4: activate → register → deploy Safe (atomic unit, idempotent)
        if needs_deploy:
            _progress(2, "Activating registration...")
            if not self.activate(service_key):
                _progress(2, "Failed to activate registration", False)
                msg = "Activation failed"
                raise RuntimeError(msg)
            _progress(2, "Registration activated")

            _progress(3, "Registering agent...")
            if not self.register_agent(service_key):
                _progress(3, "Failed to register agent", False)
                msg = "Agent registration failed"
                raise RuntimeError(msg)
            _progress(3, "Agent registered")

            _progress(4, "Deploying Safe multisig...")
            multisig = self.deploy(service_key)
            if not multisig:
                _progress(4, "Failed to deploy Safe", False)
                msg = "Safe deployment failed"
                raise RuntimeError(msg)
            result["multisig_address"] = multisig
            self.chain_config.multisig_address = multisig
            _progress(4, f"Safe deployed: {multisig[:16]}...")
        else:
            _progress(2, "Already activated")
            _progress(3, "Already registered")
            _progress(4, f"Safe exists: {result.get('multisig_address', '')[:16]}...")

        # Step 5: Create mech on marketplace (skip if already done)
        if needs_mech:
            _progress(5, "Creating mech on marketplace...")
            mech_addr = self.create_mech(service_key)
            if not mech_addr:
                _progress(5, "Failed to create mech", False)
                msg = "Mech creation failed"
                raise RuntimeError(msg)
            result["mech_address"] = mech_addr
            self.chain_config.mech_address = mech_addr
            _progress(5, f"Mech created: {mech_addr[:16]}...")
        else:
            _progress(5, f"Mech exists: {result.get('mech_address', '')[:16]}...")

        # Step 6: Stake
        _progress(6, "Staking service...")
        staked = self.stake(service_key)
        result["staked"] = staked
        if staked:
            _progress(6, "Service staked successfully")
        else:
            _progress(6, "Staking failed (non-fatal)", False)

        return result

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
