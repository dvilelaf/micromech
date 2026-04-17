"""Checkpoint periodic task with eviction detection.

Checks if any staking contract needs a checkpoint call (epoch ended + grace period).
After every epoch change (ours or someone else's), checks for ServiceInactivityWarning
and ServicesEvicted events and notifies if our service is affected.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from iwa.plugins.olas.contracts.staking import StakingContract

    from micromech.core.config import MicromechConfig
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService

# Blocks to look back when searching for checkpoint events (~1h on Gnosis at ~5s/block)
_CHECKPOINT_SEARCH_BLOCKS = 720

# Track last epoch where we already sent eviction alerts, per chain
_last_alerted_epoch: dict[str, int] = {}


async def _check_eviction_events(
    contract: "StakingContract",
    service_id: int,
    chain_name: str,
    notification_service: "NotificationService",
) -> None:
    """Check recent checkpoint events for inactivity warnings or eviction of our service.

    Queries the last _CHECKPOINT_SEARCH_BLOCKS blocks for ServiceInactivityWarning
    and ServicesEvicted events. Sends an alert if our service_id appears in either.
    De-duplicates per epoch so a single checkpoint doesn't trigger multiple alerts.
    """
    global _last_alerted_epoch

    try:
        current_block = await asyncio.to_thread(
            contract.chain_interface.with_retry,
            lambda: contract.chain_interface.web3.eth.block_number,
        )
        from_block = max(0, current_block - _CHECKPOINT_SEARCH_BLOCKS)

        events = await asyncio.to_thread(
            contract.get_checkpoint_events,
            from_block=from_block,
            to_block=current_block,
        )

        epoch = events.get("epoch")
        if epoch is None:
            return

        # De-duplicate: only alert once per epoch per chain
        last = _last_alerted_epoch.get(chain_name, 0)
        if epoch <= last:
            return
        _last_alerted_epoch[chain_name] = epoch

        warnings = events.get("inactivity_warnings", [])
        evicted = events.get("evicted_services", [])
        rewarded = events.get("rewarded_services", {})

        is_warned = service_id in warnings
        is_evicted = service_id in evicted
        got_reward = service_id in rewarded

        if not is_warned and not is_evicted and got_reward:
            logger.info(
                f"[{chain_name}] Epoch {epoch}: service {service_id} received rewards — OK"
            )
            return

        if not is_warned and not is_evicted and not got_reward:
            logger.warning(
                f"[{chain_name}] Epoch {epoch}: service {service_id} got no reward "
                f"(not warned, not evicted — may not be staked or already EVICTED)"
            )
            return

        # Build alert message
        lines = [f"⚠️ Staking Alert — {chain_name} — Epoch {epoch}"]

        if is_evicted:
            lines.append(f"🚨 EVICTED: service {service_id}")
            lines.append("Action required: unstake and re-stake the service.")
            logger.error(
                f"[{chain_name}] Epoch {epoch}: service {service_id} was EVICTED"
            )
        elif is_warned:
            lines.append(f"⚠️ Inactivity warning: service {service_id}")
            lines.append(
                "Delivery batch size is 1 — check that deliveries are reaching "
                "the staking contract's liveness threshold."
            )
            logger.warning(
                f"[{chain_name}] Epoch {epoch}: service {service_id} received "
                "an inactivity warning"
            )

        await notification_service.send(
            "Staking Alert",
            "\n".join(lines),
        )

    except Exception as e:
        logger.error(f"[{chain_name}] Error checking eviction events: {e}")


async def checkpoint_task(
    lifecycles: dict[str, "MechLifecycle"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check if any staking contract needs a checkpoint call."""
    logger.debug("Running checkpoint task...")

    from iwa.plugins.olas.contracts.staking import StakingContract

    from micromech.core.constants import CHECKPOINT_GRACE_PERIOD_SECONDS

    for chain_name, lifecycle in lifecycles.items():
        chain_config = lifecycle.chain_config
        from micromech.core.bridge import get_service_info

        svc_info = get_service_info(chain_name)
        svc_key = svc_info.get("service_key")
        service_id = svc_info.get("service_id")
        if not svc_key:
            logger.debug(f"No service_key for {chain_name}, skipping checkpoint")
            continue

        try:
            # Get staking status
            status = await asyncio.to_thread(lifecycle.get_status, svc_key)
            if not status or not status.get("is_staked"):
                logger.debug(f"Service not staked on {chain_name}, skipping checkpoint")
                continue

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

            success = await asyncio.to_thread(lifecycle.checkpoint, svc_key)

            if success:
                logger.info(f"Checkpoint called successfully on {chain_name}")
                if config.checkpoint_alert_enabled:
                    await notification_service.send(
                        "Checkpoint",
                        f"Checkpoint called on {chain_name}\n"
                        f"Epoch ended: {epoch_end.strftime('%Y-%m-%d %H:%M UTC')}",
                    )
            else:
                logger.warning(
                    f"Checkpoint not called on {chain_name} (already done or not needed)"
                )

            # Always check eviction events after an epoch boundary, regardless
            # of whether we called checkpoint or someone else did
            if service_id:
                await _check_eviction_events(
                    contract=contract,
                    service_id=int(service_id),
                    chain_name=chain_name,
                    notification_service=notification_service,
                )

        except Exception as e:
            logger.error(f"Error in checkpoint task for {chain_name}: {e}")
