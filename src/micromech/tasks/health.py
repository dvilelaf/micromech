"""Health heartbeat task."""

import httpx
from loguru import logger

from micromech.secrets import secrets


async def health_task() -> None:
    """Send heartbeat to health monitor."""
    if not secrets.health_url:
        return

    logger.debug("Sending health heartbeat...")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(secrets.health_url, timeout=10.0)
            if resp.status_code == 200:
                logger.debug("Health heartbeat sent successfully.")
            else:
                logger.warning(f"Health heartbeat failed: status {resp.status_code}")
    except Exception as e:
        logger.warning(f"Health heartbeat failed: {e}")
