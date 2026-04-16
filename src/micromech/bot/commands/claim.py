"""Claim command handler — claim staking rewards per chain (MarkdownV2)."""

import asyncio
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import (
    bold_md,
    code_md,
    escape_md,
    format_currency,
    format_token,
    split_md_blocks,
    user_error,
)
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.bridge import get_olas_price_eur
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


def _format_claim_result(
    chain_name: str,
    claimed,
    olas_price: Optional[float],
) -> str:
    """Render a claim outcome line for a single chain (MarkdownV2).

    Handles both legacy bool return and new float (OLAS amount) return from
    lifecycle.claim_rewards.
    """
    prefix = bold_md(chain_name.upper())
    if isinstance(claimed, bool):
        msg = "Claimed" if claimed else "Nothing to claim"
        return f"{prefix}: {escape_md(msg)}"
    amount = float(claimed or 0)
    if amount <= 0:
        return f"{prefix}: Nothing to claim"
    reward_str = code_md(format_token(amount, "OLAS"))
    if olas_price:
        eur_str = escape_md(format_currency(amount * olas_price))
        return f"{prefix}: Claimed {reward_str} \\(\\~{eur_str}\\)"
    return f"{prefix}: Claimed {reward_str}"


async def _run_claim(
    claim_inflight: set,
    lifecycle,
    chain: str,
    svc_key: str,
    olas_price: Optional[float],
) -> tuple[str, float]:
    """Run a claim under an in-flight guard. Returns (line, amount_claimed).

    R3-H1: use a set of (chain, svc_key) keys for mutual exclusion instead of
    asyncio.Lock + reject-if-locked. Rationale:

    * Lock-based reject-if-locked had a subtle TOCTOU failure mode: the
      relationship between ``lock.release()`` (at ``async with`` exit),
      ``claim_locks.pop(key)`` (in ``finally``), and a concurrent caller's
      ``setdefault(key, asyncio.Lock())`` is easy to reason about today but
      fragile — any future ``await`` inserted between release and pop would
      allow two coroutines to acquire different Lock objects for the same key.

    * A simple ``key in claim_inflight`` / ``claim_inflight.add(key)`` check
      has no such subtlety: entries are added before any await and discarded
      in ``finally``, and the check is atomic (no await between ``in`` and
      ``add``).

    R3-L1: the reject path is now INSIDE the try, so the finally always runs
    (even though it's a no-op on the reject path, since we never added).

    R3-M4: reject semantics — double-tap returns "Claim already in progress"
    instead of queueing.
    """
    key = (chain, svc_key)
    if key in claim_inflight:
        return (
            f"{bold_md(chain.upper())}: Claim already in progress",
            0.0,
        )
    claim_inflight.add(key)
    try:
        try:
            claimed = await asyncio.to_thread(lifecycle.claim_rewards, svc_key)
        except Exception as e:
            return (
                f"{bold_md(chain.upper())}: {user_error(f'claim {chain}', e)}",
                0.0,
            )
        line = _format_claim_result(chain, claimed, olas_price)
        amount = float(claimed) if not isinstance(claimed, bool) and claimed else 0.0
        return (line, amount)
    finally:
        claim_inflight.discard(key)


@authorized_only
@rate_limited
async def claim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /claim command."""
    if not update.message:
        return
    # Import locally so tests can patch `micromech.core.bridge.get_service_info`.
    from micromech.core.bridge import get_service_info

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains

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
        bold_md("Select chain to claim rewards:"),
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2,
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

    # Local import so tests can patch `micromech.core.bridge.get_service_info`.
    from micromech.core.bridge import get_service_info

    config: MicromechConfig = context.bot_data["config"]
    enabled = config.enabled_chains
    lifecycles = context.bot_data.get("lifecycles", {})
    # R2-M2: claim locks live in bot_data (per-Application state), not module state.
    claim_inflight = context.bot_data.setdefault("claim_inflight", set())

    if payload == "all":
        await query.answer("Claiming all chains...")
        await query.edit_message_text("Claiming rewards for all chains...")

        olas_price = await asyncio.to_thread(get_olas_price_eur)

        # Build parallel claim tasks for each staked chain with a lifecycle.
        tasks = []
        labels = []
        for chain_name in enabled:
            svc_key = get_service_info(chain_name).get("service_key")
            if not svc_key:
                continue
            lifecycle = lifecycles.get(chain_name)
            if not lifecycle:
                labels.append(f"{bold_md(chain_name.upper())}: Lifecycle not available")
                tasks.append(None)
                continue
            tasks.append(_run_claim(claim_inflight, lifecycle, chain_name, svc_key, olas_price))
            labels.append(None)

        # Execute real tasks in parallel; preserve order with placeholders.
        real_tasks = [t for t in tasks if t is not None]
        real_results = await asyncio.gather(*real_tasks, return_exceptions=True)

        results: list[str] = []
        total_claimed = 0.0
        claimed_count = 0
        idx = 0
        for placeholder_label, task in zip(labels, tasks):
            if placeholder_label is not None:
                results.append(placeholder_label)
                continue
            r = real_results[idx]
            idx += 1
            if isinstance(r, Exception):
                # R2-L1: route to user_error for consistent log + categorized message.
                results.append(user_error("claim", r))
                continue
            line, amount = r
            results.append(line)
            total_claimed += amount
            if amount > 0:
                claimed_count += 1

        if claimed_count > 0 and total_claimed > 0:
            summary = f"{claimed_count} claimed \\({code_md(format_token(total_claimed, 'OLAS'))}"
            if olas_price:
                eur = total_claimed * olas_price
                summary += f" \\~{escape_md(format_currency(eur))}"
            summary += "\\)"
            results.append(f"\n_{summary}_")

        header = bold_md("Claim Report") + "\n"
        messages = split_md_blocks(results, header=header, separator="\n")
        await query.edit_message_text(messages[0], parse_mode=ParseMode.MARKDOWN_V2)
        return

    # Single chain
    chain_name = payload
    svc_key = get_service_info(chain_name).get("service_key")
    if chain_name not in enabled or not svc_key:
        await query.answer("Chain not found or not staked")
        return

    await query.answer("Claiming...")
    await query.edit_message_text(
        f"Claiming rewards for {bold_md(chain_name.upper())}\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await query.edit_message_text("Lifecycle not available for this chain.")
        return

    olas_price = await asyncio.to_thread(get_olas_price_eur)
    line, _ = await _run_claim(claim_inflight, lifecycle, chain_name, svc_key, olas_price)
    await query.edit_message_text(line, parse_mode=ParseMode.MARKDOWN_V2)


async def _claim_chain(update: Update, context: ContextTypes.DEFAULT_TYPE, chain_name: str) -> None:
    """Claim rewards for a single chain (no selection menu)."""
    if not update.message:
        return
    from micromech.core.bridge import get_service_info

    lifecycles = context.bot_data.get("lifecycles", {})
    claim_inflight = context.bot_data.setdefault("claim_inflight", set())

    status_msg = await update.message.reply_text(
        f"Claiming rewards for {bold_md(chain_name.upper())}\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    lifecycle = lifecycles.get(chain_name)
    if not lifecycle:
        await status_msg.edit_text("Lifecycle not available for this chain.")
        return

    svc_key = get_service_info(chain_name).get("service_key", "")
    olas_price = await asyncio.to_thread(get_olas_price_eur)
    line, _ = await _run_claim(claim_inflight, lifecycle, chain_name, svc_key, olas_price)
    await status_msg.edit_text(line, parse_mode=ParseMode.MARKDOWN_V2)
