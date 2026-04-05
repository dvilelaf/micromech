"""Update check task - checks DockerHub for new micromech versions."""

import time
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from loguru import logger

if TYPE_CHECKING:
    from micromech.tasks.notifications import NotificationService

DOCKERHUB_REPO = "dvilela/micromech"
DOCKERHUB_API = f"https://hub.docker.com/v2/repositories/{DOCKERHUB_REPO}/tags"

# File-based IPC with the updater sidecar
TRIGGER_PATH = Path("/app/data/.update-request")
RESULT_PATH = Path("/app/data/.update-result")

# Track which version we've already notified about (resets on restart)
_notified_version: str | None = None

# Auto-update state
_pending_version: str | None = None
_auto_update_started_at: float | None = None

AUTO_UPDATE_MAX_WAIT_HOURS = 24
AUTO_UPDATE_POLL_MINUTES = 30


def get_current_version() -> str:
    """Get current installed micromech version."""
    try:
        return version("micromech")
    except Exception:
        return "0.0.0"


def parse_version(v: str) -> tuple[int, ...]:
    """Parse version string to tuple for comparison."""
    v = v.lstrip("v").split("-")[0].split("+")[0]
    parts = []
    for part in v.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(0)
    return tuple(parts)


async def check_dockerhub_latest() -> str | None:
    """Check DockerHub for the latest micromech tag.

    Returns the latest version tag, or None if check fails.
    """
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                DOCKERHUB_API,
                params={"page_size": 10, "ordering": "last_updated"},
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()

            for result in data.get("results", []):
                tag: str = result.get("name", "")
                if tag and tag != "latest" and any(c.isdigit() for c in tag):
                    return tag

            return None
    except Exception as e:
        logger.warning(f"Failed to check DockerHub for updates: {e}")
        return None


async def _trigger_update(
    version_tag: str,
    notification_service: "NotificationService",
    forced: bool = False,
) -> None:
    """Write the update trigger file and notify."""
    global _pending_version, _auto_update_started_at

    RESULT_PATH.unlink(missing_ok=True)
    TRIGGER_PATH.write_text("update")

    qualifier = " (forced after timeout)" if forced else ""
    await notification_service.send(
        "Auto-Update Triggered",
        f"Updating to {version_tag}{qualifier}\n"
        "micromech will restart shortly.",
    )

    logger.info(f"Auto-update triggered for version {version_tag} (forced={forced})")
    _pending_version = None
    _auto_update_started_at = None


async def auto_update_poll_task(
    notification_service: "NotificationService",
) -> None:
    """Poll for safe update window. Runs periodically while update is pending."""
    global _pending_version, _auto_update_started_at

    if not _pending_version or not _auto_update_started_at:
        return

    version_tag = _pending_version
    elapsed_hours = (time.time() - _auto_update_started_at) / 3600

    # Force update after max wait
    if elapsed_hours >= AUTO_UPDATE_MAX_WAIT_HOURS:
        logger.warning(
            f"Auto-update max wait ({AUTO_UPDATE_MAX_WAIT_HOURS}h) exceeded, "
            f"forcing update to {version_tag}"
        )
        await _trigger_update(version_tag, notification_service, forced=True)
        return

    # For micromech, always safe to update (no mech request tracking needed)
    logger.info(f"Triggering auto-update to {version_tag}")
    await _trigger_update(version_tag, notification_service)


async def update_check_task(
    notification_service: "NotificationService",
    config: "object | None" = None,
) -> None:
    """Check for new micromech version and handle notification/auto-update."""
    global _notified_version, _pending_version, _auto_update_started_at

    if config is not None:
        tasks_cfg = getattr(config, "tasks", None)
        if tasks_cfg and not getattr(tasks_cfg, "update_check_enabled", True):
            return

    logger.debug("Checking for micromech updates...")

    current = get_current_version()
    latest = await check_dockerhub_latest()

    if not latest:
        logger.debug("Could not determine latest version from DockerHub")
        return

    current_tuple = parse_version(current)
    latest_tuple = parse_version(latest)

    if latest_tuple <= current_tuple:
        logger.debug(f"micromech is up to date (v{current})")
        return

    if _notified_version == latest:
        logger.debug(f"Already notified about version {latest}, skipping")
        return

    # Determine auto-update from config
    auto_update = False
    if config is not None:
        tasks_cfg = getattr(config, "tasks", None)
        if tasks_cfg:
            auto_update = getattr(tasks_cfg, "auto_update_enabled", False)

    if auto_update:
        _pending_version = latest
        _auto_update_started_at = time.time()
        _notified_version = latest

        logger.info(f"Auto-update scheduled for version {latest}")
        await notification_service.send(
            "Auto-Update Scheduled",
            f"Current: {current}\n"
            f"Target: {latest}\n"
            f"Max wait: {AUTO_UPDATE_MAX_WAIT_HOURS}h.",
        )

        # Immediate trigger (micromech has no mech request safety window)
        await _trigger_update(latest, notification_service)
    else:
        logger.info(f"New micromech version available: {latest} (current: {current})")
        await notification_service.send(
            "Update Available",
            f"Current: {current}\n"
            f"Latest: {latest}",
        )
        _notified_version = latest
