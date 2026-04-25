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

    logger.debug("Checking failed deliveries count...")

    try:
        stats = await asyncio.to_thread(
            queue.count_by_status,
            hours=config.failed_deliveries_alert_interval_hours,
        )
        failed = stats.get("failed", 0)

        if failed >= config.failed_deliveries_alert_threshold:
            logger.warning(
                "Failed deliveries alert: {} failed in last {}h (threshold: {})",
                failed,
                config.failed_deliveries_alert_interval_hours,
                config.failed_deliveries_alert_threshold,
            )
            await notification_service.send(
                "Failed Deliveries Alert",
                f"{failed} requests created in the last "
                f"{config.failed_deliveries_alert_interval_hours}h ended as failed "
                f"(threshold: {config.failed_deliveries_alert_threshold}).\n\n"
                "Check the dashboard for details.",
                level="warning",
            )
        else:
            logger.debug(
                "Failed deliveries OK: {} in last {}h (threshold: {})",
                failed,
                config.failed_deliveries_alert_interval_hours,
                config.failed_deliveries_alert_threshold,
            )
    except Exception as e:
        logger.error("failed_deliveries_alert_task error: {}", e)
