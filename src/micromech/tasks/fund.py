"""Auto-fund task.

Checks the agent EOA balance on each enabled chain and transfers
native tokens from the master wallet when below threshold.

The agent EOA pays gas for all Safe transactions (checkpoint, claim,
stake, payment withdraw, etc.) — the Safe itself does not need a
native balance for mech operations.
"""

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService


async def fund_task(
    bridges: dict[str, "IwaBridge"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check agent EOA balance and fund from master wallet if needed."""
    logger.debug("Running fund task...")

    if not config.fund_enabled:
        return

    for chain_name in config.enabled_chains:
        try:
            bridge = bridges.get(chain_name)
            if not bridge:
                logger.debug(f"No bridge for {chain_name}, skipping fund")
                continue

            # Resolve agent address from iwa service info
            from micromech.core.bridge import get_service_info

            svc_info = await asyncio.to_thread(get_service_info, chain_name)
            agent_address = svc_info.get("agent_address")
            if not agent_address:
                logger.debug(f"[{chain_name}] No agent_address in service info, skipping fund")
                continue

            from micromech.core.bridge import get_wallet
            from iwa.core.types import EthereumAddress

            wallet = get_wallet()
            agent_tag = (
                wallet.key_storage.get_tag_by_address(EthereumAddress(agent_address))
                or agent_address
            )
            _raw_native = wallet.get_native_balance_eth(agent_address, chain_name)
            native = float(_raw_native) if _raw_native is not None else 0.0

            if native >= config.fund_threshold_native:
                continue

            logger.warning(
                f"[{chain_name}] Low agent balance ({agent_tag}): {native:.4f} "
                f"(threshold: {config.fund_threshold_native})"
            )

            amount = config.fund_target_native - native
            if amount <= 0:
                continue

            master_address = str(wallet.master_account.address)
            _raw_master = wallet.get_native_balance_eth(master_address, chain_name)
            master_native = float(_raw_master) if _raw_master is not None else 0.0
            if master_native < amount:
                logger.warning(
                    f"[{chain_name}] Master balance too low: "
                    f"{master_native:.4f} < {amount:.4f} needed"
                )
                await notification_service.send(
                    "Auto-Fund: Insufficient Master Balance",
                    f"Chain: {chain_name}\n"
                    f"Master balance: {master_native:.4f} native\n"
                    f"Needed: {amount:.4f} native",
                    level="warning",
                )
                continue

            amount_wei = int(Decimal(str(amount)) * Decimal(10**18))

            try:
                tx_hash = await asyncio.to_thread(
                    bridge.wallet.send,
                    from_address_or_tag="master",
                    to_address_or_tag=agent_address,
                    amount_wei=amount_wei,
                    chain_name=chain_name,
                )

                if tx_hash:
                    logger.info(
                        f"[{chain_name}] Funded {agent_tag}: {amount:.4f} native (tx: {tx_hash})"
                    )
                    await notification_service.send(
                        "Auto-Fund Agent",
                        f"Chain: {chain_name}\n"
                        f"Agent: {agent_tag}\n"
                        f"Amount: {amount:.4f} xDAI",
                    )
                else:
                    logger.error(f"[{chain_name}] Fund transfer returned no tx hash")
                    await notification_service.send(
                        "Auto-Fund Failed",
                        f"Chain: {chain_name}\n"
                        f"Amount: {amount:.4f} native\n"
                        f"Transfer returned no transaction hash.",
                        level="warning",
                    )

            except Exception as e:
                logger.error(f"[{chain_name}] Fund transfer failed for {agent_tag}: {e}")
                await notification_service.send(
                    "Auto-Fund Failed",
                    f"Chain: {chain_name}\nAgent: {agent_tag}\nBalance: {native:.4f} xDAI\nError: {type(e).__name__}",
                    level="warning",
                )

        except Exception as e:
            logger.error(f"Error in fund task for {chain_name}: {e}")
