"""Auto-fund task.

Checks balances on each enabled chain and alerts when low.
Actual transfer logic is a TODO (requires iwa wallet.transfer integration).
"""

import asyncio
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
    """Check balances and fund if needed."""
    logger.debug("Running fund task...")

    if not config.fund_enabled:
        return

    from micromech.core.bridge import check_balances

    for chain_name in config.enabled_chains:
        try:
            native, olas = await asyncio.to_thread(check_balances, chain_name)

            if native < config.fund_threshold_native:
                logger.warning(
                    f"Low native balance on {chain_name}: {native:.4f} "
                    f"(threshold: {config.fund_threshold_native})"
                )

                # TODO: Implement actual transfer from master wallet
                # bridge = bridges.get(chain_name)
                # if bridge:
                #     amount = config.fund_target_native - native
                #     bridge.wallet.transfer_service.transfer(...)

                await notification_service.send(
                    "Fund Required",
                    f"Chain: {chain_name}\n"
                    f"Native balance: {native:.4f}\n"
                    f"Threshold: {config.fund_threshold_native}\n"
                    f"Auto-transfer not yet implemented.",
                    level="warning",
                )

        except Exception as e:
            logger.error(f"Error in fund task for {chain_name}: {e}")
