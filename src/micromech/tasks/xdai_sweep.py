"""xDAI sweep task.

When the master wallet accumulates more xDAI than a configured threshold,
sends a fixed amount to an address resolved by tag (wallet or iwa whitelist).

This lets excess xDAI earnings flow out automatically without manual transfers.
If xdai_sweep_tag is empty, the task is a no-op.
"""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService


async def xdai_sweep_task(
    bridges: "dict[str, IwaBridge]",
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Sweep excess xDAI from master to a configured destination address."""
    tag = config.xdai_sweep_tag
    if not tag:
        logger.debug("xDAI sweep: no tag configured, skipping")
        return

    from micromech.core.bridge import get_wallet

    wallet = get_wallet()

    # Resolve tag → address: try wallet key storage first, then iwa whitelist
    try:
        dest_addr = wallet.account_service.get_address_by_tag(tag)
        if not dest_addr:
            logger.error("xDAI sweep tag '{}' not found in wallet", tag)
            return
    except Exception:
        try:
            from iwa.core.models import Config

            dest_addr = Config().core.whitelist.get(tag)
        except Exception:
            dest_addr = None
        if not dest_addr:
            logger.error("xDAI sweep tag '{}' not found in wallet or iwa whitelist", tag)
            return

    dest_addr = str(dest_addr)
    master_address = str(wallet.master_account.address)

    # xDAI is Gnosis-only
    bridge = bridges.get("gnosis")
    if not bridge:
        logger.debug("xDAI sweep: no gnosis bridge available, skipping")
        return

    try:
        balance = await asyncio.to_thread(wallet.get_native_balance_eth, master_address, "gnosis")
        logger.debug("xDAI sweep: master balance = {:.6f} xDAI", balance)

        if balance <= config.xdai_sweep_threshold_xdai:
            logger.debug(
                "xDAI sweep: {:.6f} <= threshold {:.4f} — skipping",
                balance,
                config.xdai_sweep_threshold_xdai,
            )
            return

        sweep_amount = config.xdai_sweep_amount_xdai
        amount_wei = int(sweep_amount * 1e18)

        logger.info(
            "xDAI sweep: sending {:.4f} xDAI to {} ({})",
            sweep_amount,
            tag,
            dest_addr,
        )

        tx_hash = await asyncio.to_thread(
            bridge.wallet.send,
            from_address_or_tag="master",
            to_address_or_tag=dest_addr,
            amount_wei=amount_wei,
            chain_name="gnosis",
        )

        logger.info("xDAI sweep complete. TX: {}", tx_hash)
        explorer_url = f"https://gnosisscan.io/address/{dest_addr}"
        logger.info("[xDAI Sweep] Amount: {:.4f} xDAI → {} ({})", sweep_amount, tag, dest_addr)
        await notification_service.notify(
            f"<b>xDAI Sweep</b>\n"
            f"Amount: {sweep_amount:.4f} xDAI\n"
            f"To: <a href=\"{explorer_url}\">{tag}</a>"
        )

    except Exception as e:
        logger.error("xDAI sweep task error: {}", e)
