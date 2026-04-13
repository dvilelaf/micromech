"""Mech lifecycle management via iwa's ServiceManager.

Wraps iwa's ServiceManager to provide the full mech lifecycle:
create → activate → register → deploy → create_mech → stake → run → claim → unstake

Each MechLifecycle targets a specific chain via ChainConfig.
"""

import time
from typing import Any, Callable, Optional

from loguru import logger

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def _with_retries(fn: Callable, label: str, retries: int = MAX_RETRIES) -> Any:
    """Retry a lifecycle step with exponential backoff.

    Handles transient errors like 'nonce too low' on Anvil forks
    where rapid TXs can race each other. Also retries when fn()
    returns a falsy value (iwa methods return False on failure
    instead of raising).
    """
    for attempt in range(1, retries + 1):
        try:
            result = fn()
            if result or result == 0:  # truthy, or zero (valid service_id)
                return result
            # Falsy return (False, None) = soft failure
            if attempt == retries:
                return result
            delay = RETRY_DELAY * attempt
            logger.warning(
                "{} returned {} (attempt {}/{}). Retrying in {}s...",
                label,
                result,
                attempt,
                retries,
                delay,
            )
            time.sleep(delay)
        except Exception as e:
            if attempt == retries:
                raise
            delay = RETRY_DELAY * attempt
            logger.warning(
                "{} failed (attempt {}/{}): {}. Retrying in {}s...",
                label,
                attempt,
                retries,
                e,
                delay,
            )
            time.sleep(delay)


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
        bond_olas: int = 5000,
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
            result = _with_retries(
                mgr.activate_registration,
                f"Activate on {self.chain_name}",
            )
            logger.info("Service activated on {}: {}", self.chain_name, result)
            return result
        except Exception as e:
            logger.error("Failed to activate on {}: {}", self.chain_name, e)
            return False

    def register_agent(self, service_key: str) -> bool:
        """Register agent instance (deposits OLAS bond)."""
        mgr = _get_service_manager(self.config, service_key)
        try:
            result = _with_retries(
                mgr.register_agent,
                f"Register agent on {self.chain_name}",
            )
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
            multisig = _with_retries(
                mgr.deploy,
                f"Deploy Safe on {self.chain_name}",
            )
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
        """Create a mech on the marketplace.

        marketplace.create() must be called by the SERVICE OWNER EOA (the
        account that created the service on ServiceRegistry). Calling from the
        Safe multisig is rejected with UnauthorizedAccount. Uses iwa's
        TransactionService.sign_and_send() which uses eth_sendRawTransaction
        (public RPCs reject eth_sendTransaction with -32002).
        """
        mgr = _get_service_manager(self.config, service_key)
        factory = factory_address or self.chain_config.factory_address
        rate = delivery_rate if delivery_rate is not None else self.chain_config.delivery_rate
        try:
            import json

            from eth_abi import encode
            from iwa.core.types import EthereumAddress
            from iwa.plugins.olas.contracts.base import OLAS_ABI_PATH

            from micromech.core.bridge import get_wallet

            wallet = get_wallet()
            ci = wallet.chain_interfaces.get(self.chain_name)
            web3 = ci.web3

            abi = json.loads((OLAS_ABI_PATH / "mech_marketplace.json").read_text())
            marketplace_addr = EthereumAddress(self.chain_config.marketplace_address)
            marketplace = web3.eth.contract(address=marketplace_addr, abi=abi)

            service_id = mgr.service.service_id
            # marketplace.create() must be called by the service owner EOA,
            # not the multisig. UnauthorizedAccount is raised if wrong caller.
            owner = str(mgr.service.service_owner_eoa_address)

            # Third arg is bytes: ABI-encode the delivery rate
            payload = encode(["uint256"], [rate])

            # Build and sign+send as owner EOA via iwa (eth_sendRawTransaction)
            tx = marketplace.functions.create(
                service_id,
                EthereumAddress(factory),
                payload,
            ).build_transaction({"from": EthereumAddress(owner), "gas": 5_000_000})

            success, receipt = wallet.transaction_service.sign_and_send(
                transaction=tx,
                signer_address_or_tag=owner,
                chain_name=self.chain_name,
            )
            if not success or not receipt:
                logger.error("Mech creation TX failed")
                return None
            if receipt.get("status") != 1:
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
                    if len(raw) < 40:
                        logger.warning("Mech log topic too short ({} chars), skipping", len(raw))
                        continue
                    mech_addr = EthereumAddress("0x" + raw[-40:])
                    logger.info("Mech created on {}: {}", self.chain_name, mech_addr)
                    return mech_addr

            logger.warning("Mech created but address not found in logs")
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
                    "service_id": getattr(mgr.service, "service_id", None),
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

    def rollback_deploy(
        self,
        service_key: Optional[str],
        on_progress: Optional[Callable] = None,
    ) -> bool:
        """Rollback a failed deployment, recovering locked funds.

        Args:
            service_key: "chain:id" string (e.g. "gnosis:3085"). If None, nothing to rollback.
            on_progress: Optional callback(step, total, message, success). Step is "rollback".

        Returns True on success, False on failure. Never raises.
        """

        def _rb(msg: str, success: bool = True) -> None:
            if on_progress:
                on_progress("rollback", 0, msg, success)

        if not service_key:
            logger.info("Rollback skipped: no service_key (service was never created)")
            _rb("No service to rollback", True)
            return True

        logger.info("Starting rollback for {}", service_key)
        _rb(f"Starting rollback for {service_key}...")

        try:
            mgr = _get_service_manager(self.config, service_key)

            service_id = int(service_key.split(":")[-1])
            svc_data = mgr.registry.get_service(service_id)
            state = svc_data["state"]
            logger.info("Rollback: service {} on-chain state = {}", service_key, state)
            _rb(f"Recovering funds (service state: {state.name})...")

            ok = mgr.wind_down()
            if not ok:
                logger.error("Rollback: wind_down() returned False for {}", service_key)
                if on_progress:
                    on_progress(
                        "rollback_failed", 0,
                        f"Failed to wind down service. Run: python scripts/recover_service.py --service '{service_key}' to recover manually.",
                        False,
                    )
                return False
            logger.info("Rollback: wind_down succeeded for {}", service_key)

            # Drain remaining funds to master
            from micromech.core.bridge import get_wallet

            wallet = get_wallet()
            master = wallet.master_account.address
            _rb("Draining remaining funds to master wallet...")
            logger.info("Rollback: draining service {} to {}", service_key, master)
            drained = mgr.drain_service(target_address=master, claim_rewards=False)
            if drained:
                logger.info("Rollback: drain completed for {} — {}", service_key, list(drained.keys()))
            else:
                logger.info("Rollback: drain returned empty for {} (accounts may already be empty)", service_key)

            # Cleanup config and agent key
            self._cleanup_after_rollback(service_key, mgr)
            if on_progress:
                on_progress("rollback_done", 0, "Funds recovered to master wallet.", True)
            logger.info("Rollback completed successfully for {}", service_key)
            return True

        except Exception as e:
            logger.error("Rollback failed for {}: {}", service_key, e)
            if on_progress:
                on_progress(
                    "rollback_failed",
                    0,
                    f"Automatic recovery failed — check logs. Run: python scripts/recover_service.py --service '{service_key}' to recover manually.",
                    False,
                )
            return False

    def _cleanup_after_rollback(self, service_key: str, mgr: Any) -> None:
        """Remove service from iwa config and remove empty agent key from wallet.

        Never raises.
        """
        try:
            from iwa.core.models import Config

            cfg = Config()
            olas_cfg = cfg.plugins.get("olas")
            removed = False
            if olas_cfg is not None:
                # OlasConfig instance (Pydantic model)
                if hasattr(olas_cfg, "remove_service"):
                    removed = olas_cfg.remove_service(service_key)
                # Raw CommentedMap (when Config reloads from disk without plugin registry)
                elif hasattr(olas_cfg, "get"):
                    services = olas_cfg.get("services") or {}
                    if service_key in services:
                        del services[service_key]
                        removed = True
            if removed:
                cfg.save_config()
                logger.info("Rollback cleanup: removed service {} from iwa config", service_key)
            else:
                logger.warning(
                    "Rollback cleanup: service {} not found in iwa config", service_key
                )
        except Exception as e:
            logger.error("Rollback cleanup: failed to remove service from config: {}", e)

        try:
            from micromech.core.bridge import get_wallet

            wallet = get_wallet()
            master_addr = str(wallet.master_account.address).lower()

            for account in list(wallet.key_storage.accounts.values()):
                tag = getattr(account, "tag", "") or ""
                addr = str(account.address)
                if not tag.endswith("_agent"):
                    continue
                if addr.lower() == master_addr:
                    continue
                balance = wallet.get_native_balance_eth(addr, self.chain_name)
                if balance >= 0.0001:
                    logger.warning(
                        "Rollback cleanup: skipping agent key {} — balance {} >= 0.0001",
                        addr,
                        balance,
                    )
                    continue
                logger.info(
                    "Rollback cleanup: removing empty agent key {} (tag={}, balance={})",
                    addr,
                    tag,
                    balance,
                )
                wallet.key_storage.remove_account(addr)
        except Exception as e:
            logger.error("Rollback cleanup: failed to remove agent key: {}", e)

    def full_deploy(
        self,
        agent_id: int = 40,
        bond_olas: int = 5000,
        on_progress: Optional[Callable[[int | str, int, str, bool], None]] = None,
    ) -> dict[str, Any]:
        """Run the complete lifecycle, resuming from where a previous attempt left off.

        Respects detect_setup_state() so partial deploys can be continued.

        Args:
            on_progress: Optional callback(step: int | str, total: int, message: str, success: bool).
                         step is an int for normal steps, "rollback" during automatic rollback.

        Returns dict with keys: service_id, service_key, multisig_address, mech_address, staked.
        Raises RuntimeError on any step failure.
        """
        result: dict[str, Any] = {}
        total = 7

        def _progress(step: int, msg: str, success: bool = True) -> None:
            if on_progress:
                on_progress(step, total, msg, success)

        def _rb_progress(step: Any, total: int, msg: str, success: bool = True) -> None:
            if on_progress:
                on_progress(step, total, msg, success)

        # Check if already complete (mech_address set)
        from micromech.core.bridge import get_service_info

        svc_info = get_service_info(self.chain_name)

        if self.chain_config.mech_address:
            result["mech_address"] = self.chain_config.mech_address
        if svc_info.get("service_id"):
            result["service_id"] = svc_info["service_id"]
        if svc_info.get("service_key"):
            result["service_key"] = svc_info["service_key"]
        if svc_info.get("multisig_address"):
            result["multisig_address"] = svc_info["multisig_address"]

        state = self.chain_config.detect_setup_state()
        if state == "complete":
            _progress(6, "Already fully deployed", True)
            result["staked"] = True
            return result

        # Resume logic: check what iwa already has for this chain
        has_service = bool(svc_info.get("service_id"))
        has_multisig = bool(svc_info.get("multisig_address"))

        service_key: Optional[str] = None  # tracked for rollback

        try:
            # Step 1: Create service (skip if iwa already has one)
            if has_service:
                service_id = svc_info["service_id"]
                result["service_id"] = service_id
                service_key = svc_info["service_key"]
                result["service_key"] = service_key
                _progress(1, f"Service exists: #{service_id}")
            else:
                _progress(1, "Creating service...")
                service_id = self.create_service(
                    agent_id=agent_id,
                    bond_olas=bond_olas,
                )
                if not service_id:
                    _progress(1, "Failed to create service", False)
                    msg = "Service creation failed"
                    raise RuntimeError(msg)
                result["service_id"] = service_id
                service_key = f"{self.chain_name}:{service_id}"
                result["service_key"] = service_key
                _progress(1, f"Service created: #{service_id}")

            # Steps 2-4: activate → register → deploy Safe via iwa spin_up (skip if multisig exists)
            if has_multisig:
                result["multisig_address"] = svc_info["multisig_address"]
                _progress(2, "Already activated")
                _progress(3, "Already registered")
                _progress(4, f"Safe exists: {svc_info['multisig_address'][:16]}...")
            else:
                _progress(2, "Activating, registering and deploying Safe...")
                mgr = _get_service_manager(self.config, service_key)
                ok = mgr.spin_up()
                if not ok:
                    _progress(2, "Failed to spin up service", False)
                    msg = "Service spin-up failed (activate/register/deploy)"
                    raise RuntimeError(msg)
                _progress(2, "Registration activated")
                _progress(3, "Agent registered")

                multisig = str(mgr.service.multisig_address) if mgr.service.multisig_address else None
                if not multisig:
                    _progress(4, "Failed to deploy Safe", False)
                    msg = "Safe deployment failed"
                    raise RuntimeError(msg)
                result["multisig_address"] = multisig
                _progress(4, f"Safe deployed: {multisig[:16]}...")

            # Step 5: Create mech on marketplace
            _progress(5, "Creating mech on marketplace...")
            mech_addr = self.create_mech(service_key)
            if not mech_addr:
                _progress(5, "Failed to create mech", False)
                msg = "Mech creation failed"
                raise RuntimeError(msg)
            result["mech_address"] = mech_addr
            _progress(5, f"Mech created: {mech_addr[:16]}...")

        except RuntimeError:
            self.rollback_deploy(service_key, _rb_progress)
            raise

        # Steps 6-7 are non-fatal — outside the rollback try block
        self.chain_config.mech_address = result.get("mech_address")

        # Step 6: Stake
        _progress(6, "Staking service...")
        staked = self.stake(service_key)
        result["staked"] = staked
        if staked:
            _progress(6, "Service staked successfully")
        else:
            _progress(6, "Staking failed (non-fatal)", False)

        # Step 7: Publish tool metadata
        _progress(7, "Publishing tool metadata...")
        try:
            from micromech.metadata_manager import MetadataManager

            mm = MetadataManager(self.config)
            publish_result = mm.publish_sync(
                service_key=result.get("service_key", ""),
                chain_name=self.chain_name,
                on_progress=lambda step, msg: _progress(7, msg),
            )
            if publish_result.success:
                result["metadata_cid"] = publish_result.ipfs_cid
                _progress(7, f"Metadata published: {publish_result.ipfs_cid[:24]}...")
            else:
                _progress(7, f"Metadata: {publish_result.error}", False)
        except Exception as e:
            _progress(7, f"Metadata publish failed (non-fatal): {e}", False)

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
