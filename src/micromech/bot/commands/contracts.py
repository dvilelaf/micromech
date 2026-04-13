"""Contracts command — show staking contract info per chain."""

import asyncio
from datetime import datetime, timezone

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html, format_address
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig

EXPLORER_URLS = {
    "gnosis": "https://gnosisscan.io/address/",
    "base": "https://basescan.org/address/",
    "ethereum": "https://etherscan.io/address/",
}


def _explorer_link(chain: str, address: str, label: str) -> str:
    base = EXPLORER_URLS.get(chain, EXPLORER_URLS["gnosis"])
    return f'<a href="{base}{address}">{escape_html(label)}</a>'


def _format_epoch_countdown(epoch_end: datetime) -> str:
    now = datetime.now(timezone.utc)
    seconds = int((epoch_end - now).total_seconds())
    if seconds >= 0:
        h, m = seconds // 3600, (seconds % 3600) // 60
        return f"{h}h {m}m"
    abs_s = abs(seconds)
    h, m = abs_s // 3600, (abs_s % 3600) // 60
    return f"⚠️ overdue {h}h {m}m"


@authorized_only
@rate_limited
async def contracts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /contracts command — show staking contract info per chain."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    wait_msg = await update.message.reply_text("Fetching contract data...")

    blocks = []
    for chain_name, chain_config in enabled.items():
        staking_address = chain_config.staking_address
        if not staking_address:
            blocks.append(f"{bold(chain_name.upper())}\nNo staking contract configured")
            continue

        try:

            def _fetch(addr: str = staking_address, chain: str = chain_name):
                from iwa.plugins.olas.contracts.staking import StakingContract

                contract = StakingContract(address=addr, chain_name=chain)
                staked_count = len(contract.get_service_ids())
                max_services = contract.max_num_services
                balance_olas = contract.balance / 1e18
                min_stake_olas = (contract.min_staking_deposit) / 1e18
                epoch_end = contract.get_next_epoch_start()
                countdown = _format_epoch_countdown(epoch_end)
                return staked_count, max_services, balance_olas, min_stake_olas, countdown

            staked, max_s, balance, min_stake, countdown = await asyncio.to_thread(_fetch)

            link = _explorer_link(chain_name, staking_address, format_address(staking_address))
            lines = [bold(chain_name.upper())]
            lines.append(f"Address: {link}")
            lines.append(f"Slots: {code(f'{staked}/{max_s}')}")
            lines.append(f"Min stake: {code(f'{min_stake:.0f} OLAS')}")
            lines.append(f"Balance: {code(f'{balance:.2f} OLAS')}")
            lines.append(f"Epoch ends: {code(countdown)}")
            blocks.append("\n".join(lines))
        except Exception as e:
            logger.error(f"Contracts error for {chain_name}: {e}")
            blocks.append(f"{bold(chain_name.upper())}\nError: {escape_html(str(e))}")

    text = "\n\n".join(blocks) if blocks else "No contract data available."
    await wait_msg.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)
