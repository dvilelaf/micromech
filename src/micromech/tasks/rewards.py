"""Rewards claiming task.

Checks accrued rewards and claims when above threshold.
Optionally triggers auto-sell after claiming.
"""

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService


async def rewards_task(
    lifecycles: dict[str, "MechLifecycle"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
    bridges: dict[str, "IwaBridge"] | None = None,
) -> None:
    """Check and claim rewards for each enabled chain."""
    logger.debug("Running rewards task...")

    threshold = config.claim_threshold_olas

    # Snapshot master OLAS balance before claiming (per chain)
    # so auto-sell only sells freshly claimed OLAS
    pre_claim_olas_wei: dict[str, int] = {}
    if config.auto_sell_enabled and bridges:
        from micromech.core.bridge import check_balances
        for chain_name in config.enabled_chains:
            try:
                _, olas = await asyncio.to_thread(check_balances, chain_name)
                pre_claim_olas_wei[chain_name] = int(
                    Decimal(str(olas)) * Decimal(10**18)
                )
            except Exception:
                # Don't add to dict — auto_sell will skip chains
                # without a floor rather than selling everything
                logger.warning(f"Could not snapshot OLAS balance on {chain_name}")

    claimed_any = False

    for chain_name, lifecycle in lifecycles.items():
        from micromech.core.bridge import get_service_info
        svc_info = await asyncio.to_thread(get_service_info, chain_name)
        svc_key = svc_info.get("service_key")
        if not svc_key:
            logger.debug(f"No service_key for {chain_name}, skipping rewards")
            continue

        try:
            status = await asyncio.to_thread(
                lifecycle.get_status, svc_key
            )
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

            success = await asyncio.to_thread(
                lifecycle.claim_rewards, svc_key
            )

            if success:
                claimed_any = True
                logger.info(f"Rewards claimed on {chain_name}: {accrued:.4f} OLAS")
                await notification_service.send(
                    "Rewards Claimed",
                    f"Chain: {chain_name}\nAmount: {accrued:.4f} OLAS",
                )
            else:
                logger.warning(f"Claim returned false on {chain_name}")

        except Exception as e:
            logger.error(f"Error in rewards task for {chain_name}: {e}")

    # Trigger auto-sell after claiming
    if claimed_any and config.auto_sell_enabled and bridges:
        from micromech.tasks.auto_sell import auto_sell_task

        # Pass per-chain floor so each chain's pre-existing OLAS is protected
        await auto_sell_task(
            bridges, notification_service, config,
            olas_floor_wei=pre_claim_olas_wei,
        )
