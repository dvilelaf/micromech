"""Checkpoint command handler — call staking checkpoint per chain (MarkdownV2)."""

import asyncio

from loguru import logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold_md, user_error
from micromech.bot.security import authorized_only
from micromech.core.config import MicromechConfig

ACTION_CHECKPOINT = "checkpoint"


def _build_chain_keyboard(chains: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard with one button per chain + All."""
    buttons = []
    if len(chains) > 1:
        buttons.append(
            [InlineKeyboardButton("All Chains", callback_data=f"{ACTION_CHECKPOINT}:all")]
        )
    for chain_name in chains:
        buttons.append(
            [
                InlineKeyboardButton(
                    chain_name.upper(),
                    callback_data=f"{ACTION_CHECKPOINT}:{chain_name}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"{ACTION_CHECKPOINT}:cancel")])
    return InlineKeyboardMarkup(buttons)


@authorized_only
async def checkpoint_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /checkpoint command."""
    if not update.message:
        return
    from micromech.core.bridge import get_service_info

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

    staked = {k: v for k, v in enabled.items() if get_service_info(k).get("service_key")}

    if not staked:
        await update.message.reply_text("No staked services.")
        return

    if len(staked) == 1:
        chain_name = next(iter(staked))
        await _checkpoint_chain(update, context, chain_name)
        return

    keyboard = _build_chain_keyboard(staked)
    await update.message.reply_text(
        bold_md("Select chain to checkpoint:"),
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def handle_checkpoint_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle checkpoint callback."""
    query = update.callback_query
    if not query:
        return

    if payload == "cancel":
        await query.delete_message()
        return

    from micromech.core.bridge import get_service_info

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains
    lifecycles = context.bot_data.get("lifecycles", {})

    if payload == "all":
        await query.answer("Checkpointing all chains...")
        await query.edit_message_text("Calling checkpoint for all chains...")
        called = []
        skipped = []

        for chain_name, _chain_config in enabled.items():
            svc_key = get_service_info(chain_name).get("service_key")
            if not svc_key:
                continue
            lifecycle = lifecycles.get(chain_name)
            if not lifecycle:
                skipped.append(chain_name.upper())
                continue
            try:
                success = await asyncio.to_thread(lifecycle.checkpoint, svc_key)
                if success:
                    called.append(chain_name.upper())
                else:
                    skipped.append(chain_name.upper())
            except Exception as e:
                # R3-L3: ASYMMETRIC with the single-chain path, which uses
                # user_error(). Here we silently skip rather than render the
                # categorized error, because the all-chains UX is a short
                # summary ("called: X / not needed: Y") — propagating even a
                # categorized error line per chain would clutter it. Do NOT
                # replace with `user_error(...)` without re-thinking the UX.
                logger.warning("checkpoint failed for {}: {}", chain_name, e)
                skipped.append(chain_name.upper())

        lines = []
        if called:
            lines.append(f"Checkpoint called: {bold_md(', '.join(called))}")
        if skipped:
            lines.append(f"Not needed: {', '.join(skipped)}")
        if not lines:
            lines.append("No chains to checkpoint.")
        await query.edit_message_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Single chain
    chain_name = payload
    svc_key = get_service_info(chain_name).get("service_key")
    if chain_name not in enabled or not svc_key:
        await query.answer("Chain not found or not staked")
        return

    await query.answer("Checkpointing...")
    await query.edit_message_text(
        f"Calling checkpoint for {bold_md(chain_name.upper())}\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await query.edit_message_text("Lifecycle not available for this chain.")
        return
    try:
        success = await asyncio.to_thread(lifecycle.checkpoint, svc_key)
        if success:
            await query.edit_message_text(
                f"Checkpoint called for {bold_md(chain_name.upper())}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            await query.edit_message_text(
                f"Checkpoint not needed for {bold_md(chain_name.upper())}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
    except Exception as e:
        # R2-H1: user_error logs full exception + returns categorized message
        # without leaking RPC URLs that may contain API keys.
        await query.edit_message_text(
            user_error(f"checkpoint {chain_name}", e),
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def _checkpoint_chain(
    update: Update, context: ContextTypes.DEFAULT_TYPE, chain_name: str
) -> None:
    """Checkpoint a single chain (no selection menu)."""
    if not update.message:
        return
    from micromech.core.bridge import get_service_info

    lifecycles = context.bot_data.get("lifecycles", {})

    status_msg = await update.message.reply_text(
        f"Calling checkpoint for {bold_md(chain_name.upper())}\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await status_msg.edit_text("Lifecycle not available for this chain.")
        return
    try:
        svc_key = get_service_info(chain_name).get("service_key", "")
        success = await asyncio.to_thread(lifecycle.checkpoint, svc_key)
        if success:
            await status_msg.edit_text(
                f"Checkpoint called for {bold_md(chain_name.upper())}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            await status_msg.edit_text(
                f"Checkpoint not needed for {bold_md(chain_name.upper())}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
    except Exception as e:
        # R2-H1: see comment in handle_checkpoint_callback above.
        await status_msg.edit_text(
            user_error(f"checkpoint {chain_name}", e),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
