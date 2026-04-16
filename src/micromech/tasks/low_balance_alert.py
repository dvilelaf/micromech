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
    if not config.low_balance_alert_enabled:
        return
    logger.debug("Running low balance alert check...")

    from micromech.core.bridge import check_balances

    for chain_name, lifecycle in lifecycles.items():
        try:
            # Check balances
            balances = await asyncio.to_thread(check_balances, chain_name)
            if balances is None:
                continue
            native, olas = balances

            if native < config.fund_threshold_native:
                await notification_service.send(
                    "Low Balance Alert",
                    f"Chain: {chain_name}\n"
                    f"Native balance: {native:.4f}\n"
                    f"Threshold: {config.fund_threshold_native}",
                    level="warning",
                )

            # Check staking state for eviction
            from micromech.core.bridge import get_service_info

            svc_info = get_service_info(chain_name)
            svc_key = svc_info.get("service_key")
            if svc_key:
                status = await asyncio.to_thread(lifecycle.get_status, svc_key)
                if status and status.get("staking_state") == "EVICTED":
                    await notification_service.send(
                        "Eviction Alert",
                        f"Chain: {chain_name}\n"
                        f"Service ID: {svc_info.get('service_id')}\n"
                        f"Status: EVICTED\n"
                        f"Action required: Rejoin staking manually",
                        level="warning",
                    )

        except Exception as e:
            logger.error(f"Error checking balance for {chain_name}: {e}")
