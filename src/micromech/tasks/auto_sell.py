"""Auto-sell task.

Sells OLAS rewards to native token for profit-taking.
Uses iwa's wallet.swap() which integrates with CoW Protocol.

Currently supported chains: Gnosis (CoW Protocol).
Other chains are skipped with a log message.
"""

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService

# CoW Protocol is only available on these chains
COW_SUPPORTED_CHAINS = {"gnosis"}

# Native wrapped token name per chain (CoW swaps to wrapped, then unwrap)
WRAPPED_NATIVE = {
    "gnosis": "wxdai",
}


async def auto_sell_task(
    bridges: dict[str, "IwaBridge"],
    notification_service: "NotificationService",
    config: "MicromechConfig",
    olas_floor_wei: dict[str, int] | None = None,
) -> None:
    """Sell OLAS above floor to native token on supported chains.

    Args:
        bridges: IwaBridge instances per chain.
        notification_service: For sending notifications.
        config: MicromechConfig with auto_sell settings.
        olas_floor_wei: Per-chain OLAS balance (in wei) to protect from selling.
            Typically the master balance snapshot taken before claiming,
            so only freshly claimed OLAS gets sold.
    """
    if not config.auto_sell_enabled:
        return

    logger.debug("Running auto-sell task...")

    from micromech.core.bridge import check_balances

    min_olas = config.auto_sell_min_olas

    for chain_name in config.enabled_chains:
        if chain_name not in COW_SUPPORTED_CHAINS:
            logger.debug(f"Auto-sell not supported on {chain_name}, skipping")
            continue

        bridge = bridges.get(chain_name)
        if not bridge:
            continue

        try:
            _, olas_balance = await asyncio.to_thread(check_balances, chain_name)

            # Convert per-chain floor from wei to whole units.
            # If floor dict was provided but chain is missing, skip
            # (balance snapshot failed — don't risk selling pre-existing OLAS)
            floors = olas_floor_wei or {}
            if olas_floor_wei is not None and chain_name not in floors:
                logger.warning(f"No OLAS floor for {chain_name}, skipping auto-sell")
                continue
            chain_floor_wei = floors.get(chain_name, 0)
            olas_floor = float(Decimal(str(chain_floor_wei)) / Decimal(10**18))
            sellable = olas_balance - olas_floor

            if sellable < min_olas:
                logger.debug(
                    f"Auto-sell on {chain_name}: {sellable:.4f} OLAS sellable "
                    f"(min: {min_olas}), skipping"
                )
                continue

            sell_wei = int(Decimal(str(sellable)) * Decimal(10**18))
            buy_token = WRAPPED_NATIVE.get(chain_name, "wxdai")

            logger.info(f"Auto-sell on {chain_name}: selling {sellable:.4f} OLAS → {buy_token}")

            success = await bridge.wallet.swap(
                account_address_or_tag="master",
                amount_wei=sell_wei,
                sell_token_name="olas",
                buy_token_name=buy_token,
                chain_name=chain_name,
            )

            if success:
                logger.info(f"Auto-sell completed on {chain_name}: {sellable:.4f} OLAS")
                await notification_service.send(
                    "Auto-Sell",
                    f"Chain: {chain_name}\nSold: {sellable:.4f} OLAS → {buy_token}",
                )
            else:
                logger.warning(f"Auto-sell swap returned False on {chain_name}")
                await notification_service.send(
                    "Auto-Sell Failed",
                    f"Chain: {chain_name}\nAmount: {sellable:.4f} OLAS\nSwap order was not filled.",
                    level="warning",
                )

        except Exception as e:
            logger.error(f"Auto-sell error on {chain_name}: {e}")
            await notification_service.send(
                "Auto-Sell Error",
                f"Chain: {chain_name}\nError: {e}",
                level="warning",
            )
