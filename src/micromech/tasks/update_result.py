"""Consume updater sidecar result markers after restarts."""

import os
import stat
from pathlib import Path

from loguru import logger

RESULT_PATH = Path("/app/data/.update-result")
MAX_RESULT_BYTES = 512


def _safe_marker(value: str) -> str:
    clean = "".join(ch for ch in value if ch.isalnum() or ch in "._-")
    return clean[:64] or "unknown"


def pop_update_result(path: Path | None = None) -> str | None:
    """Read and remove a safe updater result marker."""
    path = path or RESULT_PATH
    try:
        flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.warning("Discarded unsafe update result marker: {}", exc)
        path.unlink(missing_ok=True)
        return None

    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            logger.warning("Discarded non-regular update result marker")
            return None
        if info.st_size > MAX_RESULT_BYTES:
            logger.warning("Discarded invalid update result marker")
            return None

        data = os.read(fd, MAX_RESULT_BYTES + 1)
        if len(data) > MAX_RESULT_BYTES:
            logger.warning("Discarded oversized update result marker")
            return None
        return data.decode("utf-8", errors="replace").strip()
    except Exception as exc:
        logger.warning("Failed to consume update result marker: {}", exc)
        return None
    finally:
        os.close(fd)
        path.unlink(missing_ok=True)


async def consume_update_result(notification: object) -> None:
    """Acknowledge and notify about a pending updater result marker."""
    result = pop_update_result()
    if not result:
        return

    if result.startswith("updated:"):
        parts = result.split(":")
        if len(parts) == 3:
            old = _safe_marker(parts[1])
            new = _safe_marker(parts[2])
            await notification.send("Update Complete", f"{old} -> {new}")
        else:
            logger.warning("Discarded invalid update result marker")
    elif result.startswith("rolled_back:"):
        parts = result.split(":")
        if len(parts) == 3:
            old = _safe_marker(parts[1])
            failed = _safe_marker(parts[2])
            await notification.send(
                "Update Rolled Back",
                f"{failed} failed; restored {old}",
                level="warning",
            )
        else:
            logger.warning("Discarded invalid update result marker")
    elif result.startswith("error:"):
        error = _safe_marker(result.split(":", 1)[1])
        await notification.send("Update Failed", error, level="warning")
