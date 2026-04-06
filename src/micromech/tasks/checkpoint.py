"""Checkpoint periodic task.

Checks if any staking contract needs a checkpoint call (epoch ended + grace period).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.config import MicromechConfig
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService


async def checkpoint_task(
    lifecycles: dict[str, "MechLifecycle"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check if any staking contract needs a checkpoint call."""
    logger.debug("Running checkpoint task...")

    from micromech.core.constants import CHECKPOINT_GRACE_PERIOD_SECONDS

    from iwa.plugins.olas.contracts.staking import StakingContract

    for chain_name, lifecycle in lifecycles.items():
        chain_config = lifecycle.chain_config
        from micromech.core.bridge import get_service_info
        svc_key = get_service_info(chain_name).get("service_key")
        if not svc_key:
            logger.debug(f"No service_key for {chain_name}, skipping checkpoint")
            continue

        try:
            # Get staking status
            status = await asyncio.to_thread(
                lifecycle.get_status, svc_key
            )
            if not status or not status.get("is_staked"):
                logger.debug(f"Service not staked on {chain_name}, skipping checkpoint")
                continue

            # Check if epoch has ended by looking at the staking contract
            # get_status returns staking info; we need to check the contract directly
            contract = await asyncio.to_thread(
                StakingContract,
                chain_config.staking_address,
                chain_name=chain_name,
            )
            epoch_end = await asyncio.to_thread(contract.get_next_epoch_start)
            now = datetime.now(timezone.utc)

            if now < epoch_end:
                logger.debug(f"Epoch still active on {chain_name}, next end: {epoch_end}")
                continue

            grace = timedelta(seconds=CHECKPOINT_GRACE_PERIOD_SECONDS)
            if now < epoch_end + grace:
                logger.debug(f"Within grace period on {chain_name}, waiting...")
                continue

            logger.info(f"Checkpoint needed for {chain_name}")

            success = await asyncio.to_thread(
                lifecycle.checkpoint, svc_key
            )

            if success:
                logger.info(f"Checkpoint called successfully on {chain_name}")
                if config.checkpoint_alert_enabled:
                    await notification_service.send(
                        "Checkpoint",
                        f"Checkpoint called on {chain_name}\n"
                        f"Epoch ended: {epoch_end.strftime('%Y-%m-%d %H:%M UTC')}",
                    )
            else:
                logger.warning(f"Checkpoint not called on {chain_name} (already done or not needed)")

        except Exception as e:
            logger.error(f"Error in checkpoint task for {chain_name}: {e}")
