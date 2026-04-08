"""Auto-fund task.

Checks the Safe multisig balance on each enabled chain and transfers
native tokens from the master wallet when below threshold.
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
    """Check Safe balances and fund from master wallet if needed."""
    logger.debug("Running fund task...")

    if not config.fund_enabled:
        return

    from micromech.core.bridge import check_safe_balance, get_service_info

    for chain_name in config.enabled_chains:
        try:
            native = await asyncio.to_thread(check_safe_balance, chain_name)

            if native is None:
                logger.warning(f"Could not check Safe balance on {chain_name}, skipping fund")
                continue

            if native >= config.fund_threshold_native:
                continue

            logger.warning(
                f"Low Safe balance on {chain_name}: {native:.4f} "
                f"(threshold: {config.fund_threshold_native})"
            )

            bridge = bridges.get(chain_name)
            if not bridge:
                await notification_service.send(
                    "Auto-Fund: No Bridge",
                    f"Chain: {chain_name}\n"
                    f"Safe balance: {native:.4f} native\n"
                    f"No bridge available for auto-transfer.",
                    level="warning",
                )
                continue

            svc_info = await asyncio.to_thread(get_service_info, chain_name)
            multisig = svc_info.get("multisig_address")
            if not multisig:
                await notification_service.send(
                    "Auto-Fund: No Multisig",
                    f"Chain: {chain_name}\n"
                    f"Safe balance: {native:.4f} native\n"
                    f"No multisig address configured.",
                    level="warning",
                )
                continue

            amount = config.fund_target_native - native
            if amount <= 0:
                continue

            # Check master has enough funds
            from micromech.core.bridge import check_balances

            master_native, _ = await asyncio.to_thread(check_balances, chain_name)
            if master_native < amount:
                logger.warning(
                    f"Master balance too low on {chain_name}: "
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
                    to_address_or_tag=multisig,
                    amount_wei=amount_wei,
                    chain_name=chain_name,
                )

                if tx_hash:
                    logger.info(f"Funded Safe on {chain_name}: {amount:.4f} native (tx: {tx_hash})")
                    await notification_service.send(
                        "Auto-Fund Safe",
                        f"Chain: {chain_name}\nAmount: {amount:.4f} native\nTx: {tx_hash}",
                    )
                else:
                    logger.error(f"Fund transfer returned no tx hash on {chain_name}")
                    await notification_service.send(
                        "Auto-Fund Failed",
                        f"Chain: {chain_name}\n"
                        f"Amount: {amount:.4f} native\n"
                        f"Transfer returned no transaction hash.",
                        level="warning",
                    )

            except Exception as e:
                logger.error(f"Fund transfer failed on {chain_name}: {e}")
                await notification_service.send(
                    "Auto-Fund Failed",
                    f"Chain: {chain_name}\nSafe balance: {native:.4f} native\nError: {e}",
                    level="warning",
                )

        except Exception as e:
            logger.error(f"Error in fund task for {chain_name}: {e}")
