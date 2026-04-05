"""Low balance alert task - notifies when balances are critically low."""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService


async def low_balance_alert_task(
    lifecycles: dict[str, "MechLifecycle"],
    bridges: dict[str, "IwaBridge"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check and alert on low balances and eviction status."""
    if not config.tasks.low_balance_alert_enabled:
        return
    logger.debug("Running low balance alert check...")

    tasks_config = config.tasks

    from micromech.core.bridge import check_balances

    for chain_name, lifecycle in lifecycles.items():
        try:
            # Check balances
            native, olas = await asyncio.to_thread(check_balances, chain_name)

            if native < tasks_config.fund_threshold_native:
                await notification_service.send(
                    "Low Balance Alert",
                    f"Chain: {chain_name}\n"
                    f"Native balance: {native:.4f}\n"
                    f"Threshold: {tasks_config.fund_threshold_native}",
                    level="warning",
                )

            # Check staking state for eviction
            chain_config = lifecycle.chain_config
            if chain_config.service_key:
                status = await asyncio.to_thread(
                    lifecycle.get_status, chain_config.service_key
                )
                if status and status.get("staking_state") == "EVICTED":
                    await notification_service.send(
                        "Eviction Alert",
                        f"Chain: {chain_name}\n"
                        f"Service ID: {chain_config.service_id}\n"
                        f"Status: EVICTED\n"
                        f"Action required: Rejoin staking manually",
                        level="warning",
                    )

        except Exception as e:
            logger.error(f"Error checking balance for {chain_name}: {e}")
