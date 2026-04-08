"""Claim command handler — claim staking rewards per chain."""

import asyncio

from loguru import logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, escape_html
from micromech.bot.security import authorized_only
from micromech.core.config import MicromechConfig

ACTION_CLAIM = "claim"


def _build_chain_keyboard(chains: dict, action: str) -> InlineKeyboardMarkup:
    """Build inline keyboard with one button per enabled chain + All."""
    buttons = []
    if len(chains) > 1:
        buttons.append([InlineKeyboardButton("All Chains", callback_data=f"{action}:all")])
    for chain_name in chains:
        buttons.append(
            [InlineKeyboardButton(chain_name.upper(), callback_data=f"{action}:{chain_name}")]
        )
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"{action}:cancel")])
    return InlineKeyboardMarkup(buttons)


@authorized_only
async def claim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /claim command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains
    from micromech.core.bridge import get_service_info

    staked = {k: v for k, v in enabled.items() if get_service_info(k).get("service_key")}

    if not staked:
        await update.message.reply_text("No staked services to claim from.")
        return

    if len(staked) == 1:
        chain_name = next(iter(staked))
        await _claim_chain(update, context, chain_name)
        return

    keyboard = _build_chain_keyboard(staked, ACTION_CLAIM)
    await update.message.reply_text(
        bold("Select chain to claim rewards:"),
        reply_markup=keyboard,
        parse_mode="HTML",
    )


async def handle_claim_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle claim callback."""
    query = update.callback_query
    if not query:
        return

    if payload == "cancel":
        await query.delete_message()
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    lifecycles = context.bot_data.get("lifecycles", {})

    if payload == "all":
        await query.answer("Claiming all chains...")
        await query.edit_message_text("Claiming rewards for all chains...")
        results = []
        from micromech.core.bridge import get_service_info

        for chain_name, chain_config in enabled.items():
            svc_key = get_service_info(chain_name).get("service_key")
            if not svc_key:
                continue
            lifecycle = lifecycles.get(chain_name)
            if not lifecycle:
                results.append(f"{bold(chain_name.upper())}: Lifecycle not available")
                continue
            try:
                success = await asyncio.to_thread(lifecycle.claim_rewards, svc_key)
                status = "Claimed" if success else "Nothing to claim"
                results.append(f"{bold(chain_name.upper())}: {status}")
            except Exception as e:
                results.append(f"{bold(chain_name.upper())}: Error - {escape_html(str(e))}")
        text = f"{bold('Claim Report')}\n\n" + "\n".join(results)
        await query.edit_message_text(text, parse_mode="HTML")
        return

    # Single chain
    chain_name = payload
    from micromech.core.bridge import get_service_info

    svc_key = get_service_info(chain_name).get("service_key")
    if chain_name not in enabled or not svc_key:
        await query.answer("Chain not found or not staked")
        return

    await query.answer("Claiming...")
    await query.edit_message_text(
        f"Claiming rewards for {bold(chain_name.upper())}...", parse_mode="HTML"
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await query.edit_message_text("Lifecycle not available for this chain.")
        return
    try:
        success = await asyncio.to_thread(lifecycle.claim_rewards, svc_key)
        msg = "Rewards claimed" if success else "Nothing to claim"
        await query.edit_message_text(f"{bold(chain_name.upper())}: {msg}", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Claim error for {chain_name}: {e}")
        await query.edit_message_text(f"Claim failed: {escape_html(str(e))}", parse_mode="HTML")


async def _claim_chain(update: Update, context: ContextTypes.DEFAULT_TYPE, chain_name: str) -> None:
    """Claim rewards for a single chain (no selection menu)."""
    if not update.message:
        return
    config: MicromechConfig = context.bot_data["config"]
    lifecycles = context.bot_data.get("lifecycles", {})
    config.enabled_chains[chain_name]

    status_msg = await update.message.reply_text(
        f"Claiming rewards for {bold(chain_name.upper())}...", parse_mode="HTML"
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await status_msg.edit_text("Lifecycle not available for this chain.")
        return
    try:
        from micromech.core.bridge import get_service_info

        svc_key = get_service_info(chain_name).get("service_key", "")
        success = await asyncio.to_thread(lifecycle.claim_rewards, svc_key)
        msg = "Rewards claimed" if success else "Nothing to claim"
        await status_msg.edit_text(f"{bold(chain_name.upper())}: {msg}", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Claim error for {chain_name}: {e}")
        await status_msg.edit_text(f"Claim failed: {escape_html(str(e))}", parse_mode="HTML")
