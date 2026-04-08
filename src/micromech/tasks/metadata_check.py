"""Metadata staleness check task.

Periodically checks if local tool metadata differs from what was last
published. Notifies the operator but does NOT auto-publish (costs gas).
"""

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.metadata_manager import MetadataManager
    from micromech.tasks.notifications import NotificationService


async def metadata_check_task(
    metadata_manager: "MetadataManager",
    notification_service: "NotificationService",
) -> None:
    """Check if tool metadata is stale and notify operator."""
    logger.debug("Running metadata staleness check...")

    try:
        status = metadata_manager.get_status()

        if not status.needs_update:
            logger.debug("Metadata is up to date")
            return

        if status.ipfs_cid is None:
            msg = "Tool metadata has never been published on-chain."
        else:
            changed = ", ".join(status.changed_packages) if status.changed_packages else "unknown"
            msg = f"Tool metadata is stale (changed: {changed})."

        logger.warning(msg)
        await notification_service.send(
            "Metadata Update Needed",
            f"{msg}\nUse the dashboard Tools tab or CLI `metadata-publish` to update.",
            level="warning",
        )

    except Exception as e:
        logger.error("Metadata check failed: {}", e)
