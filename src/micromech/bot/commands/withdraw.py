"""Withdraw command — manually trigger mech payment withdrawal (MarkdownV2)."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold_md, code_md, user_error
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig

ACTION_WITHDRAW = "withdraw"


def _build_chain_keyboard(chains: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard with one button per chain + All + Cancel."""
    buttons = []
    if len(chains) > 1:
        buttons.append(
            [
                InlineKeyboardButton(
                    "All Chains",
                    callback_data=f"{ACTION_WITHDRAW}:all",
                )
            ]
        )
    for chain_name in chains:
        buttons.append(
            [
                InlineKeyboardButton(
                    chain_name.upper(),
                    callback_data=f"{ACTION_WITHDRAW}:{chain_name}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"{ACTION_WITHDRAW}:cancel")])
    return InlineKeyboardMarkup(buttons)


async def _get_withdraw_preview(bridge, chain_name: str, chain_config, safe_reserve_xdai: float):
    """Return shared withdraw preview for Telegram UI."""
    from micromech.tasks.payment_withdraw import preview_payment_withdraw

    return await preview_payment_withdraw(
        bridge,
        chain_name,
        chain_config,
        safe_reserve_xdai=safe_reserve_xdai,
    )


def _format_available_balance(pending: float | None, mech_wei: int | None) -> str:
    """Format pending and stranded mech balance for confirmation messages."""
    parts = []
    if pending is not None:
        parts.append(f"Pending: {code_md(f'{pending:.6f} xDAI')}")
    if mech_wei is not None:
        parts.append(f"Mech: {code_md(f'{mech_wei / 1e18:.6f} xDAI')}")
    return "\n".join(parts)


def _append_safe_balance_line(parts: str, safe_excess_wei: int | None) -> str:
    """Append Safe excess balance to a confirmation message."""
    if safe_excess_wei is None or safe_excess_wei <= 0:
        return parts
    safe_line = f"Safe excess: {code_md(f'{safe_excess_wei / 1e18:.6f} xDAI')}"
    return f"{parts}\n{safe_line}" if parts else safe_line


async def _run_withdraw(
    bridge, chain_name: str, chain_config, safe_reserve_xdai: float = 0.0
) -> tuple[bool, str]:
    """Run the shared withdrawal pipeline for one chain."""
    from micromech.tasks.payment_withdraw import execute_payment_withdraw

    try:
        result = await execute_payment_withdraw(
            bridge,
            chain_name,
            chain_config,
            threshold_xdai=0.0,
            safe_reserve_xdai=safe_reserve_xdai,
            sweep_existing_safe_excess=True,
        )
    except Exception as e:
        return False, user_error(f"withdraw {chain_name}", e)

    if result.status == "no_funds":
        return True, f"{bold_md(chain_name.upper())}: No pending payments"

    if result.status == "swept_safe":
        return True, (
            f"{bold_md(chain_name.upper())}: "
            f"Swept {code_md(f'{result.transferred_to_master_wei / 1e18:.6f} xDAI')} "
            "from Safe to master"
        )

    if result.status == "drained_to_safe":
        return True, (
            f"{bold_md(chain_name.upper())}: "
            f"Drained {code_md(f'{result.mech_withdrawn_wei / 1e18:.6f} xDAI')} to Safe; "
            "nothing transferred to master because Safe excess is at or below reserve"
        )

    if result.status == "lock_busy":
        return True, f"{bold_md(chain_name.upper())}: Safe is busy, try again shortly"

    if result.transfer_error is not None:
        failed_amount = code_md(f"{result.attempted_transfer_to_master_wei / 1e18:.6f} xDAI")
        if result.mech_withdrawn_wei <= 0:
            return True, (
                f"{bold_md(chain_name.upper())}: "
                f"Safe sweep failed; {failed_amount} remains in Safe. Check local logs."
            )
        return True, (
            f"{bold_md(chain_name.upper())}: "
            f"{code_md(f'{result.mech_withdrawn_wei / 1e18:.6f} xDAI')} drained to Safe "
            f"but {failed_amount} could not be transferred to master. Check local logs."
        )

    return True, (
        f"{bold_md(chain_name.upper())}: "
        f"Withdrawn {code_md(f'{result.transferred_to_master_wei / 1e18:.6f} xDAI')} to master"
    )


def _chains_with_mech(config: MicromechConfig, bridges: dict) -> dict:
    """Return enabled chains that have a mech address and a safe_service."""
    return {
        k: v
        for k, v in config.enabled_chains.items()
        if v.mech_address
        and k in bridges
        and hasattr(bridges[k], "wallet")
        and hasattr(bridges[k].wallet, "safe_service")
    }


@authorized_only
@rate_limited
async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /withdraw command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    bridges: dict = context.bot_data.get("bridges", {})
    chains = _chains_with_mech(config, bridges)

    if not chains:
        await update.message.reply_text("No chains with mech payment configured.")
        return

    if len(chains) == 1:
        chain_name = next(iter(chains))
        await _show_balance_and_confirm(update, context, chain_name)
        return

    keyboard = _build_chain_keyboard(chains)
    await update.message.reply_text(
        bold_md("Select chain to withdraw payment:"),
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def _show_balance_and_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE, chain_name: str
) -> None:
    """Show pending balance with Confirm/Cancel keyboard."""
    config: MicromechConfig = context.bot_data["config"]
    bridges: dict = context.bot_data.get("bridges", {})
    bridge = bridges.get(chain_name)
    chain_config = config.enabled_chains.get(chain_name)

    status_msg = await update.message.reply_text(
        f"Checking pending balance for {bold_md(chain_name.upper())}\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        preview = await _get_withdraw_preview(
            bridge,
            chain_name,
            chain_config,
            config.payment_withdraw_safe_reserve_xdai,
        )
    except Exception:
        preview = None

    pending = preview.pending_xdai if preview else None
    mech_wei = preview.mech_balance_wei if preview else None
    safe_excess_wei = preview.safe_excess_wei if preview else None

    if pending is None and mech_wei is None and (safe_excess_wei or 0) <= 0:
        await status_msg.edit_text(
            "Could not retrieve withdraw balances\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if pending is None and (mech_wei or 0) <= 0 and (safe_excess_wei or 0) <= 0:
        await status_msg.edit_text(
            "Could not retrieve pending balance\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if (pending or 0.0) <= 0 and (mech_wei or 0) <= 0 and (safe_excess_wei or 0) <= 0:
        await status_msg.edit_text(
            f"{bold_md(chain_name.upper())}: No pending payments\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Withdraw",
                    callback_data=f"{ACTION_WITHDRAW}:confirm:{chain_name}",
                )
            ],
            [InlineKeyboardButton("Cancel", callback_data=f"{ACTION_WITHDRAW}:cancel")],
        ]
    )
    await status_msg.edit_text(
        f"{bold_md(chain_name.upper())}: "
        f"withdrawable balance\n"
        f"{_append_safe_balance_line(_format_available_balance(pending, mech_wei), safe_excess_wei)}\n"
        f"Withdraw to master?",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def handle_withdraw_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle withdraw callbacks."""
    query = update.callback_query
    if not query:
        return

    if payload == "cancel":
        await query.delete_message()
        return

    config: MicromechConfig = context.bot_data["config"]
    bridges: dict = context.bot_data.get("bridges", {})
    withdraw_inflight: set = context.bot_data.setdefault("withdraw_inflight", set())

    # Chain picker selected → show balance + confirm button
    if not payload.startswith("confirm:") and payload != "all":
        chain_name = payload
        chain_config = config.enabled_chains.get(chain_name)
        bridge = bridges.get(chain_name)
        if not chain_config or not bridge:
            await query.answer("Chain not found")
            return

        await query.answer()
        await query.edit_message_text(
            f"Checking balance for {bold_md(chain_name.upper())}\\.\\.\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        try:
            preview = await _get_withdraw_preview(
                bridge,
                chain_name,
                chain_config,
                config.payment_withdraw_safe_reserve_xdai,
            )
        except Exception:
            preview = None

        pending = preview.pending_xdai if preview else None
        mech_wei = preview.mech_balance_wei if preview else None
        safe_excess_wei = preview.safe_excess_wei if preview else None

        if (
            (pending is None and mech_wei is None and (safe_excess_wei or 0) <= 0)
            or (pending is None and (mech_wei or 0) <= 0 and (safe_excess_wei or 0) <= 0)
            or ((pending or 0.0) <= 0 and (mech_wei or 0) <= 0 and (safe_excess_wei or 0) <= 0)
        ):
            msg = (
                "No pending payments\\."
                if pending is not None and (pending or 0.0) <= 0
                else "Could not retrieve balance\\."
            )
            await query.edit_message_text(
                f"{bold_md(chain_name.upper())}: {msg}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Withdraw",
                        callback_data=(f"{ACTION_WITHDRAW}:confirm:{chain_name}"),
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Cancel",
                        callback_data=f"{ACTION_WITHDRAW}:cancel",
                    )
                ],
            ]
        )
        await query.edit_message_text(
            f"{bold_md(chain_name.upper())}: "
            f"withdrawable balance\n"
            f"{_append_safe_balance_line(_format_available_balance(pending, mech_wei), safe_excess_wei)}\n"
            f"Withdraw to master?",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Confirm single chain
    if payload.startswith("confirm:"):
        chain_name = payload[len("confirm:") :]
        chain_config = config.enabled_chains.get(chain_name)
        bridge = bridges.get(chain_name)
        if not chain_config or not bridge:
            await query.answer("Chain not found")
            return

        if chain_name in withdraw_inflight:
            await query.answer("Withdrawal already in progress")
            return

        withdraw_inflight.add(chain_name)
        await query.answer("Withdrawing...")
        await query.edit_message_text(
            f"Withdrawing payment for {bold_md(chain_name.upper())}\\.\\.\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        try:
            _ok, msg = await _run_withdraw(
                bridge,
                chain_name,
                chain_config,
                config.payment_withdraw_safe_reserve_xdai,
            )
            await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            await query.edit_message_text(
                user_error(f"withdraw {chain_name}", e),
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        finally:
            withdraw_inflight.discard(chain_name)
        return

    # All chains
    chains = _chains_with_mech(config, bridges)
    await query.answer("Withdrawing all chains...")
    await query.edit_message_text(
        "Withdrawing payments for all chains\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    results = []
    for chain_name, chain_config in chains.items():
        if chain_name in withdraw_inflight:
            results.append(f"{bold_md(chain_name.upper())}: Already in progress")
            continue
        withdraw_inflight.add(chain_name)
        try:
            _ok, msg = await _run_withdraw(
                bridges[chain_name],
                chain_name,
                chain_config,
                config.payment_withdraw_safe_reserve_xdai,
            )
            results.append(msg)
        except Exception as e:
            results.append(user_error(f"withdraw {chain_name}", e))
        finally:
            withdraw_inflight.discard(chain_name)

    await query.edit_message_text(
        bold_md("Withdraw Report") + "\n" + "\n".join(results),
        parse_mode=ParseMode.MARKDOWN_V2,
    )
