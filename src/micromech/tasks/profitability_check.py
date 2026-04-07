"""Daily profitability check task.

Compares delivery fee revenue against estimated gas costs.
Alerts when the mech is operating at a loss.
"""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.core.persistence import PersistentQueue
    from micromech.management import MechLifecycle
    from micromech.tasks.notifications import NotificationService

# Conservative gas estimate per delivery transaction (in native token units).
# A deliverToMarketplace call typically costs ~200k-500k gas.
# At ~3 gwei gas price on Gnosis, that's ~0.001 xDAI per delivery.
ESTIMATED_GAS_COST_PER_DELIVERY = 0.001


async def profitability_check_task(
    queue: "PersistentQueue",
    lifecycles: dict[str, "MechLifecycle"],
    bridges: dict[str, "IwaBridge"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Check if the mech is profitable over the last 24 hours.

    Revenue = delivery fees + staking rewards estimate.
    Costs = estimated gas per delivery + checkpoint/claim gas.
    """
    logger.debug("Running profitability check...")

    for chain_name, chain_config in config.enabled_chains.items():
        try:
            # Count deliveries in last 24h
            deliveries_24h = await asyncio.to_thread(
                queue.count_delivered_since, 24, chain_name,
            )

            # Revenue from delivery fees (native token)
            delivery_rate_native = chain_config.delivery_rate / 10**18
            delivery_revenue = deliveries_24h * delivery_rate_native

            # Accrued staking rewards (unclaimed, not necessarily daily)
            accrued_olas = 0.0
            is_staked = False
            lifecycle = lifecycles.get(chain_name)
            if lifecycle:
                from micromech.core.bridge import get_service_info
                svc_info = await asyncio.to_thread(get_service_info, chain_name)
                svc_key = svc_info.get("service_key")
                if svc_key:
                    try:
                        status = await asyncio.to_thread(
                            lifecycle.get_status, svc_key,
                        )
                        if status and status.get("is_staked"):
                            is_staked = True
                            accrued_olas = status.get("rewards", 0.0)
                    except Exception:
                        pass

            # Estimated gas costs
            gas_cost = deliveries_24h * ESTIMATED_GAS_COST_PER_DELIVERY
            # Add overhead for checkpoint + claim transactions (~0.002 each, ~6 per day)
            gas_cost += 6 * 0.002

            # Net profit in native token (excluding OLAS value)
            net_native = delivery_revenue - gas_cost

            # Alert if: native fees don't cover gas AND no staking rewards accruing
            if net_native < 0 and not is_staked:
                logger.warning(
                    f"Unprofitable on {chain_name}: "
                    f"revenue={delivery_revenue:.4f}, "
                    f"gas_cost={gas_cost:.4f}, "
                    f"deliveries={deliveries_24h}"
                )
                await notification_service.send(
                    "Unprofitable Operation",
                    f"Chain: {chain_name}\n"
                    f"Deliveries (24h): {deliveries_24h}\n"
                    f"Fee revenue: {delivery_revenue:.4f} native\n"
                    f"Gas cost (est.): {gas_cost:.4f} native\n"
                    f"Net: {net_native:.4f} native\n"
                    f"Not staked — no OLAS rewards accruing.\n"
                    f"Review delivery rate and demand volume.",
                    level="warning",
                )
            else:
                logger.info(
                    f"Profitability OK on {chain_name}: "
                    f"deliveries={deliveries_24h}, "
                    f"net_native={net_native:.4f}, "
                    f"accrued_olas={accrued_olas:.4f}, "
                    f"staked={is_staked}"
                )

        except Exception as e:
            logger.error(f"Profitability check error on {chain_name}: {e}")
