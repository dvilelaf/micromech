"""Watchdog task - detects when background tasks stop completing.

Runs as a pure asyncio coroutine (no thread pool) so it stays responsive
even when thread pool workers are blocked by RPC failures.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.tasks.notifications import NotificationService

# Watchdog configuration
CHECK_INTERVAL_SECONDS = 300  # Check every 5 minutes
STALE_THRESHOLD_SECONDS = 1800  # Alert after 30 minutes without task completion

# Global state (updated by tasks via record_task_success)
_last_task_success: float = 0.0
_alert_sent: bool = False


def record_task_success() -> None:
    """Record that a background task completed successfully.

    Called by scheduler listener after successful execution.
    Thread-safe (float assignment is atomic in CPython).
    """
    global _last_task_success, _alert_sent
    _last_task_success = time.monotonic()
    _alert_sent = False


async def watchdog_loop(notification_service: "NotificationService") -> None:
    """Monitor background task health and alert on stalls.

    Runs directly on the asyncio event loop (not in a thread pool),
    so it remains responsive even when all thread pool workers are blocked.
    """
    global _alert_sent

    # Initialize timestamp only if not already set (e.g. by tests)
    if _last_task_success == 0.0:
        record_task_success()

    logger.info("Watchdog started")

    while True:
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

        elapsed = time.monotonic() - _last_task_success

        if elapsed > STALE_THRESHOLD_SECONDS and not _alert_sent:
            minutes = int(elapsed // 60)
            logger.warning(f"Watchdog: no task completed in {minutes} minutes!")

            try:
                await notification_service.send(
                    "Watchdog Alert",
                    f"No background task has completed in {minutes} minutes.\n"
                    "RPC or thread pool may be stuck.\n"
                    "Check logs and consider restarting.",
                    level="warning",
                )
                _alert_sent = True
            except Exception as e:
                logger.error(f"Watchdog failed to send alert: {e}")
