"""Wallet command handler — show addresses and balances per chain."""

import asyncio

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html, format_address, format_balance
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.bridge import check_balances, get_wallet
from micromech.core.config import MicromechConfig

EXPLORER_URLS = {
    "gnosis": "https://gnosisscan.io/address/",
    "base": "https://basescan.org/address/",
    "ethereum": "https://etherscan.io/address/",
}


def _explorer_link(chain: str, address: str, label: str) -> str:
    """Build an HTML link to a block explorer."""
    base = EXPLORER_URLS.get(chain, EXPLORER_URLS["gnosis"])
    return f'<a href="{base}{address}">{escape_html(label)}</a>'


def _format_chain_wallet(chain_name: str, chain_config: "ChainConfig") -> str:
    """Format wallet info for a single chain."""
    from micromech.core.bridge import get_service_info
    svc_info = get_service_info(chain_name)
    multisig = svc_info.get("multisig_address")
    lines = [bold(chain_name.upper())]

    if multisig:
        link = _explorer_link(chain_name, multisig, format_address(multisig))
        lines.append(f"Multisig: {link}")

    if chain_config.mech_address:
        addr = chain_config.mech_address
        link = _explorer_link(chain_name, addr, format_address(addr))
        lines.append(f"Mech: {link}")

    if not multisig and not chain_config.mech_address:
        lines.append("Not deployed")

    return "\n".join(lines)


@authorized_only
@rate_limited
async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /wallet command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    status_msg = await update.message.reply_text("Fetching wallet info...")

    blocks = []

    # Master wallet
    try:
        wallet = await asyncio.to_thread(get_wallet)
        master_addr = wallet.master_account.address
        first_chain = next(iter(enabled))
        native, olas = await asyncio.to_thread(check_balances, first_chain)
        link = _explorer_link(first_chain, master_addr, format_address(master_addr))
        blocks.append(
            f"{bold('Master')}\n"
            f"Address: {link}\n"
            f"Balance: {code(format_balance(native, 'xDAI'))} | "
            f"{code(format_balance(olas, 'OLAS'))}"
        )
    except Exception as e:
        logger.warning(f"Failed to get master wallet: {e}")
        blocks.append(f"{bold('Master')}\nUnavailable")

    # Per-chain addresses
    from micromech.core.bridge import get_service_info
    for chain_name, chain_config in enabled.items():
        svc_info = get_service_info(chain_name)
        multisig = svc_info.get("multisig_address")
        lines = [bold(chain_name.upper())]

        if multisig:
            link = _explorer_link(chain_name, multisig, format_address(multisig))
            lines.append(f"Multisig: {link}")

        if chain_config.mech_address:
            addr = chain_config.mech_address
            link = _explorer_link(chain_name, addr, format_address(addr))
            lines.append(f"Mech: {link}")

        if svc_info.get("service_id"):
            lines.append(f"Service ID: {code(str(svc_info['service_id']))}")

        if not multisig and not chain_config.mech_address:
            lines.append("Not deployed")

        blocks.append("\n".join(lines))

    text = "\n\n".join(blocks)
    await status_msg.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)
