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

    from micromech.core.bridge import get_wallet

    wallet = get_wallet()

    for chain_name, _chain_config in config.enabled_chains.items():
        try:
            bridge = bridges.get(chain_name)
            if not bridge:
                logger.debug(
                    f"No bridge for {chain_name}, skipping fund"
                )
                continue

            # Get the agent EOA address (tagged "mech" by convention)
            agent_tag = _chain_config.account_tag  # default: "mech"
            try:
                agent_address = str(
                    wallet.account_service.get_address_by_tag(agent_tag)
                )
            except Exception:
                logger.debug(
                    f"[{chain_name}] Could not resolve agent tag "
                    f"'{agent_tag}', skipping fund"
                )
                continue

            native = wallet.get_native_balance_eth(agent_address, chain_name)

            if native >= config.fund_threshold_native:
                continue

            logger.warning(
                f"[{chain_name}] Low agent balance: {native:.4f} "
                f"(threshold: {config.fund_threshold_native})"
            )

            amount = config.fund_target_native - native
            if amount <= 0:
                continue

            # Check master has enough funds
            master_address = str(wallet.master_account.address)
            master_native = wallet.get_native_balance_eth(
                master_address, chain_name
            )
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
                        f"[{chain_name}] Funded agent EOA: "
                        f"{amount:.4f} native (tx: {tx_hash})"
                    )
                    await notification_service.send(
                        "Auto-Fund Agent",
                        f"Chain: {chain_name}\n"
                        f"Agent: {agent_address}\n"
                        f"Amount: {amount:.4f} native\n"
                        f"Tx: {tx_hash}",
                    )
                else:
                    logger.error(
                        f"[{chain_name}] Fund transfer returned no tx hash"
                    )
                    await notification_service.send(
                        "Auto-Fund Failed",
                        f"Chain: {chain_name}\n"
                        f"Amount: {amount:.4f} native\n"
                        f"Transfer returned no transaction hash.",
                        level="warning",
                    )

            except Exception as e:
                logger.error(f"[{chain_name}] Fund transfer failed: {e}")
                await notification_service.send(
                    "Auto-Fund Failed",
                    f"Chain: {chain_name}\n"
                    f"Agent balance: {native:.4f} native\n"
                    f"Error: {e}",
                    level="warning",
                )

        except Exception as e:
            logger.error(f"Error in fund task for {chain_name}: {e}")
