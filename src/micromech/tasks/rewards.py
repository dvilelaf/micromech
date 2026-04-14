"""Rewards claiming task.

Checks accrued rewards and claims when above threshold.
"""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.config import MicromechConfig
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService


async def rewards_task(
    lifecycles: dict[str, "MechLifecycle"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check and claim rewards for each enabled chain."""
    logger.debug("Running rewards task...")

    threshold = config.claim_threshold_olas
    claimed_any = False

    for chain_name, lifecycle in lifecycles.items():
        from micromech.core.bridge import get_service_info

        svc_info = await asyncio.to_thread(get_service_info, chain_name)
        svc_key = svc_info.get("service_key")
        if not svc_key:
            logger.debug(f"No service_key for {chain_name}, skipping rewards")
            continue

        try:
            status = await asyncio.to_thread(lifecycle.get_status, svc_key)
            if not status or not status.get("is_staked"):
                continue

            accrued = status.get("rewards", 0.0)
            if accrued < threshold:
                logger.debug(
                    f"Rewards on {chain_name}: {accrued:.4f} OLAS "
                    f"(below threshold {threshold})"
                )
                continue

            logger.info(f"Claiming {accrued:.4f} OLAS rewards on {chain_name}")

            success = await asyncio.to_thread(lifecycle.claim_rewards, svc_key)

            if success:
                claimed_any = True
                # Transfer OLAS from Safe to master (mirrors triton's behaviour)
                _ok, transferred = await asyncio.to_thread(
                    lifecycle.withdraw_rewards, svc_key
                )
                transfer_line = (
                    f"\nTransferred to master: {transferred:.4f} OLAS" if _ok else ""
                )
                logger.info(
                    f"Rewards claimed on {chain_name}: {accrued:.4f} OLAS"
                )
                await notification_service.send(
                    "Rewards Claimed",
                    f"Chain: {chain_name}\nAmount: {accrued:.4f} OLAS{transfer_line}",
                )
            else:
                logger.warning(f"Claim returned false on {chain_name}")

        except Exception as e:
            logger.error(f"Error in rewards task for {chain_name}: {e}")
