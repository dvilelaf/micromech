"""Failed deliveries alert task — notifies when too many deliveries have failed."""

import asyncio
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.config import MicromechConfig
    from micromech.core.persistence import PersistentQueue
    from micromech.tasks.notifications import NotificationService


async def failed_deliveries_alert_task(
    queue: Optional["PersistentQueue"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Alert when the number of failed deliveries in the last interval exceeds threshold."""
    if not config.failed_deliveries_alert_enabled:
        return

    if queue is None:
        logger.warning("failed_deliveries_alert_task: queue is None, skipping")
        return

    logger.debug("Checking delivery issue summary...")

    try:
        summary = await asyncio.to_thread(
            queue.failure_summary,
            hours=config.failed_deliveries_alert_interval_hours,
        )
        actionable = summary.get("actionable", 0)
        timed_out = summary.get("timed_out", 0)
        other = summary.get("other", 0)
        already_final = summary.get("already_final", 0)

        if actionable >= config.failed_deliveries_alert_threshold:
            logger.warning(
                "Delivery issues alert: {} actionable failure(s) in last {}h (threshold: {})",
                actionable,
                config.failed_deliveries_alert_interval_hours,
                config.failed_deliveries_alert_threshold,
            )
            ignored = (
                f"\nIgnored {already_final} request(s) already final on-chain."
                if already_final
                else ""
            )
            await notification_service.send(
                "Delivery Issues Alert",
                f"{actionable} actionable request issue(s) in the last "
                f"{config.failed_deliveries_alert_interval_hours}h "
                f"(threshold: {config.failed_deliveries_alert_threshold}).\n\n"
                f"Breakdown: {timed_out} on-chain timeout(s), "
                f"{other} other failure(s)."
                f"{ignored}\n\n"
                "Check the dashboard for details.",
                level="warning",
            )
        else:
            logger.debug(
                "Delivery issues OK: {} actionable in last {}h (threshold: {})",
                actionable,
                config.failed_deliveries_alert_interval_hours,
                config.failed_deliveries_alert_threshold,
            )
    except Exception as e:
        logger.error("failed_deliveries_alert_task error: {}", e)
