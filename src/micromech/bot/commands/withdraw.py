"""Withdraw command — manually trigger mech payment withdrawal (MarkdownV2)."""

import asyncio

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
    buttons.append(
        [InlineKeyboardButton("Cancel", callback_data=f"{ACTION_WITHDRAW}:cancel")]
    )
    return InlineKeyboardMarkup(buttons)


async def _get_pending_balance(
    bridge, chain_name: str, chain_config
) -> float | None:
    """Return pending mech balance in xDAI, or None on error."""
    from micromech.core.marketplace import (
        get_balance_tracker_address,
        get_pending_balance,
    )

    try:
        bt_address = await asyncio.to_thread(
            get_balance_tracker_address,
            bridge,
            chain_name,
            chain_config.mech_address,
            chain_config.marketplace_address,
        )
        if not bt_address:
            return None
        return await asyncio.to_thread(
            get_pending_balance,
            bridge,
            bt_address,
            chain_config.mech_address,
        )
    except Exception:
        return None


async def _run_withdraw(
    bridge, chain_name: str, chain_config
) -> tuple[bool, str]:
    """Run the full withdrawal pipeline for one chain.

    Returns (success, result_message_md).
    """
    from micromech.core.bridge import get_service_info
    from micromech.core.marketplace import get_balance_tracker_address
    from micromech.tasks.payment_withdraw import (
        _drain_mech_to_safe,
        _transfer_to_master,
        _withdraw,
    )

    svc_info = await asyncio.to_thread(get_service_info, chain_name)
    multisig = svc_info.get("multisig_address")
    if not multisig:
        return False, f"{bold_md(chain_name.upper())}: No multisig address"

    bt_address = await asyncio.to_thread(
        get_balance_tracker_address,
        bridge,
        chain_name,
        chain_config.mech_address,
        chain_config.marketplace_address,
    )
    if not bt_address:
        return False, (
            f"{bold_md(chain_name.upper())}: Could not resolve balance tracker"
        )

    # Step 1: processPaymentByMultisig → xDAI to mech contract
    await asyncio.to_thread(
        _withdraw,
        bridge,
        chain_name,
        bt_address,
        chain_config.mech_address,
        multisig,
    )

    # Step 2: mech.exec → drain mech to Safe (read exact wei first)
    web3 = bridge.web3
    mech_wei = await asyncio.to_thread(
        bridge.with_retry,
        lambda: web3.eth.get_balance(
            web3.to_checksum_address(chain_config.mech_address)
        ),
    )
    await asyncio.to_thread(
        _drain_mech_to_safe,
        bridge,
        chain_name,
        chain_config.mech_address,
        multisig,
        mech_wei,
    )

    # Step 3: Safe → master (funds are in Safe if this fails)
    amount = mech_wei / 1e18
    try:
        await asyncio.to_thread(
            _transfer_to_master, bridge, chain_name, multisig, mech_wei
        )
    except Exception as e:
        return True, (
            f"{bold_md(chain_name.upper())}: "
            f"{code_md(f'{amount:.6f} xDAI')} drained to Safe "
            f"but transfer to master failed: {e}"
        )

    return True, (
        f"{bold_md(chain_name.upper())}: "
        f"Withdrawn {code_md(f'{amount:.6f} xDAI')} to master"
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
async def withdraw_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle /withdraw command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    bridges: dict = context.bot_data.get("bridges", {})
    chains = _chains_with_mech(config, bridges)

    if not chains:
        await update.message.reply_text(
            "No chains with mech payment configured."
        )
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

    balance = await _get_pending_balance(bridge, chain_name, chain_config)
    if balance is None:
        await status_msg.edit_text(
            "Could not retrieve pending balance\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if balance <= 0:
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
            [
                InlineKeyboardButton(
                    "Cancel", callback_data=f"{ACTION_WITHDRAW}:cancel"
                )
            ],
        ]
    )
    await status_msg.edit_text(
        f"{bold_md(chain_name.upper())}: "
        f"{code_md(f'{balance:.6f} xDAI')} pending\n"
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
    withdraw_inflight: set = context.bot_data.setdefault(
        "withdraw_inflight", set()
    )

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
        balance = await _get_pending_balance(bridge, chain_name, chain_config)
        if balance is None or balance <= 0:
            msg = (
                "No pending payments\\."
                if (balance is not None and balance <= 0)
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
                        callback_data=(
                            f"{ACTION_WITHDRAW}:confirm:{chain_name}"
                        ),
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
            f"{code_md(f'{balance:.6f} xDAI')} pending\n"
            f"Withdraw to master?",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Confirm single chain
    if payload.startswith("confirm:"):
        chain_name = payload[len("confirm:"):]
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
            _ok, msg = await _run_withdraw(bridge, chain_name, chain_config)
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN_V2
            )
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
            results.append(
                f"{bold_md(chain_name.upper())}: Already in progress"
            )
            continue
        withdraw_inflight.add(chain_name)
        try:
            _ok, msg = await _run_withdraw(
                bridges[chain_name], chain_name, chain_config
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
