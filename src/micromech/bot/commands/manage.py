"""Manage command handler — stake/unstake per chain."""

import asyncio

from loguru import logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html
from micromech.bot.security import authorized_only
from micromech.core.config import MicromechConfig
from micromech.management import MechLifecycle

ACTION_MANAGE = "manage"
ACTION_MANAGE_CONFIRM = "mgcfm"


def _build_chain_keyboard(chains: dict) -> InlineKeyboardMarkup:
    """Build chain selection keyboard for manage."""
    buttons = []
    for chain_name in chains:
        buttons.append(
            [InlineKeyboardButton(
                chain_name.upper(),
                callback_data=f"{ACTION_MANAGE}:{chain_name}",
            )]
        )
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"{ACTION_MANAGE}:cancel")])
    return InlineKeyboardMarkup(buttons)


def _build_actions_keyboard(chain_name: str, status: dict) -> InlineKeyboardMarkup:
    """Build action keyboard based on staking state."""
    buttons = []
    state = status.get("staking_state", "unknown")
    is_staked = status.get("is_staked", False)

    if is_staked and state == "STAKED":
        buttons.append(
            [InlineKeyboardButton(
                "Unstake", callback_data=f"{ACTION_MANAGE}:{chain_name}:unstake"
            )]
        )
    elif state == "EVICTED":
        buttons.append(
            [InlineKeyboardButton(
                "Restake", callback_data=f"{ACTION_MANAGE}:{chain_name}:restake"
            )]
        )
    elif state in ("NOT_STAKED", "not_staked"):
        buttons.append(
            [InlineKeyboardButton(
                "Stake", callback_data=f"{ACTION_MANAGE}:{chain_name}:stake"
            )]
        )

    buttons.append(
        [InlineKeyboardButton("Back", callback_data=f"{ACTION_MANAGE}:back")]
    )
    return InlineKeyboardMarkup(buttons)


@authorized_only
async def manage_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /manage command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if not enabled:
        await update.message.reply_text("No chains enabled.")
        return

    keyboard = _build_chain_keyboard(enabled)
    await update.message.reply_text(
        bold("Manage Mech — select chain:"),
        reply_markup=keyboard,
        parse_mode="HTML",
    )


async def handle_manage_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle manage callbacks."""
    query = update.callback_query
    if not query:
        return

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    if payload == "cancel":
        await query.delete_message()
        return

    if payload == "back":
        keyboard = _build_chain_keyboard(enabled)
        await query.answer()
        await query.edit_message_text(
            bold("Manage Mech — select chain:"),
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        return

    # Check for action: "chain:action"
    if ":" in payload:
        chain_name, action = payload.split(":", 1)
        if chain_name in enabled:
            if action in ("unstake", "restake"):
                # Destructive — confirm
                if context.user_data is not None:
                    context.user_data["manage_chain"] = chain_name
                    context.user_data["manage_action"] = action
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "Confirm", callback_data=f"{ACTION_MANAGE_CONFIRM}:yes"
                        ),
                        InlineKeyboardButton(
                            "Cancel", callback_data=f"{ACTION_MANAGE_CONFIRM}:no"
                        ),
                    ]
                ])
                label = "Unstake" if action == "unstake" else "Restake"
                await query.answer()
                await query.edit_message_text(
                    f"{label} {bold(chain_name.upper())}?",
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )
                return

            if action == "stake":
                await _execute_action(query, config, chain_name, "stake")
                return

    # Chain selection — show status + actions
    chain_name = payload
    if chain_name not in enabled:
        await query.answer("Chain not found")
        return

    chain_config = enabled[chain_name]
    if not chain_config.service_key:
        await query.answer()
        await query.edit_message_text(
            f"{bold(chain_name.upper())}: Not deployed", parse_mode="HTML"
        )
        return

    await query.answer("Fetching status...")

    try:
        lifecycle = MechLifecycle(config, chain_name)
        status = await asyncio.to_thread(lifecycle.get_status, chain_config.service_key)
        if not status:
            await query.edit_message_text(
                f"{bold(chain_name.upper())}: Could not fetch status", parse_mode="HTML"
            )
            return

        state = status.get("staking_state", "unknown")
        keyboard = _build_actions_keyboard(chain_name, status)
        await query.edit_message_text(
            f"{bold(chain_name.upper())} ({code(state)})",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Manage error for {chain_name}: {e}")
        await query.edit_message_text(
            f"Error: {escape_html(str(e))}", parse_mode="HTML"
        )


async def handle_manage_confirm_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle confirmation for destructive manage actions."""
    query = update.callback_query
    if not query:
        return

    config: MicromechConfig = context.bot_data["config"]

    if payload == "no" or not context.user_data:
        await query.answer("Cancelled")
        enabled = config.enabled_chains
        keyboard = _build_chain_keyboard(enabled)
        await query.edit_message_text(
            bold("Manage Mech — select chain:"),
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        return

    chain_name = context.user_data.get("manage_chain")
    action = context.user_data.get("manage_action")
    context.user_data.pop("manage_chain", None)
    context.user_data.pop("manage_action", None)

    if not chain_name or not action:
        await query.answer("Session expired")
        return

    await _execute_action(query, config, chain_name, action)


async def _execute_action(query, config: MicromechConfig, chain_name: str, action: str) -> None:
    """Execute a manage action (stake/unstake/restake)."""
    enabled = config.enabled_chains
    chain_config = enabled.get(chain_name)
    if not chain_config or not chain_config.service_key:
        await query.answer("Chain not configured")
        return

    label = action.capitalize()
    await query.answer(f"{label}...")
    await query.edit_message_text(
        f"{label} {bold(chain_name.upper())}...", parse_mode="HTML"
    )

    try:
        lifecycle = MechLifecycle(config, chain_name)
        service_key = chain_config.service_key

        if action == "stake":
            success = await asyncio.to_thread(lifecycle.stake, service_key)
        elif action == "unstake":
            success = await asyncio.to_thread(lifecycle.unstake, service_key)
        elif action == "restake":
            unstaked = await asyncio.to_thread(lifecycle.unstake, service_key)
            if not unstaked:
                await query.edit_message_text(
                    f"Unstake failed for {bold(chain_name.upper())}", parse_mode="HTML"
                )
                return
            success = await asyncio.to_thread(lifecycle.stake, service_key)
        else:
            await query.edit_message_text("Unknown action")
            return

        if success:
            await query.edit_message_text(
                f"{label} completed for {bold(chain_name.upper())}", parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                f"{label} failed for {bold(chain_name.upper())}", parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Manage {action} error for {chain_name}: {e}")
        await query.edit_message_text(
            f"{label} failed: {escape_html(str(e))}", parse_mode="HTML"
        )
