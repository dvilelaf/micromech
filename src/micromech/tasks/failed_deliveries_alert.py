"""Failed deliveries alert task — notifies when too many deliveries have failed."""

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.config import MicromechConfig
    from micromech.core.persistence import PersistentQueue
    from micromech.tasks.notifications import NotificationService


async def failed_deliveries_alert_task(
    queue: "PersistentQueue",
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Alert when the number of failed deliveries exceeds the configured threshold."""
    if not config.failed_deliveries_alert_enabled:
        return

    logger.debug("Checking failed deliveries count...")

    stats = queue.count_by_status()
    failed = stats.get("failed", 0)

    if failed >= config.failed_deliveries_alert_threshold:
        logger.warning(
            "Failed deliveries alert: {} failed (threshold: {})",
            failed,
            config.failed_deliveries_alert_threshold,
        )
        await notification_service.send(
            "Failed Deliveries Alert",
            f"<b>{failed}</b> failed deliveries detected "
            f"(threshold: {config.failed_deliveries_alert_threshold}).\n\n"
            "Check the dashboard for details.",
            level="warning",
        )
    else:
        logger.debug(
            "Failed deliveries OK: {} (threshold: {})",
            failed,
            config.failed_deliveries_alert_threshold,
        )
