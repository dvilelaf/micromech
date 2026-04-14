"""Settings command handler — toggle features and edit numeric values."""

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html
from micromech.bot.security import authorized_only
from micromech.core.config import MicromechConfig

ACTION_SETTINGS = "settings"

# (callback_key, config_attr, display_label)
_TOGGLES = [
    ("checkpoint_alert", "checkpoint_alert_enabled", "Checkpoint alerts"),
    ("low_balance_alert", "low_balance_alert_enabled", "Low balance alerts"),
    ("fund", "fund_enabled", "Auto-fund"),
    ("payment_withdraw", "payment_withdraw_enabled", "Payment withdraw"),
    ("auto_update", "auto_update_enabled", "Auto-update"),
    ("update_check", "update_check_enabled", "Update check"),
]

_TOGGLE_MAP: dict[str, tuple[str, str]] = {key: (attr, label) for key, attr, label in _TOGGLES}

# Editable numeric settings
EDITABLE_SETTINGS: dict[str, dict[str, Any]] = {
    "claim_thr": {
        "attr": "claim_threshold_olas",
        "label": "Claim threshold",
        "type": float,
        "min": 0.1,
        "max": 500.0,
        "unit": "OLAS",
        "group": "rewards",
    },
    "fund_thr": {
        "attr": "fund_threshold_native",
        "label": "Fund threshold",
        "type": float,
        "min": 0.001,
        "max": 10.0,
        "unit": "native",
        "group": "fund",
    },
    "fund_tgt": {
        "attr": "fund_target_native",
        "label": "Fund target",
        "type": float,
        "min": 0.01,
        "max": 50.0,
        "unit": "native",
        "group": "fund",
    },
    "withdraw_thr": {
        "attr": "payment_withdraw_threshold_xdai",
        "label": "Withdraw threshold",
        "type": float,
        "min": 0.001,
        "max": 10.0,
        "unit": "xDAI",
        "group": "fund",
    },
}


def _get_value(config: MicromechConfig, attr: str) -> bool:
    """Get a toggle value from config."""
    return getattr(config, attr)


def _validate_setting_input(setting: dict[str, Any], raw: str) -> tuple[Any, str | None]:
    """Validate and convert raw input for a setting.

    Returns (value, error_message). error_message is None on success.
    """
    try:
        value = setting["type"](raw)
    except ValueError:
        type_name = "integer" if setting["type"] is int else "number"
        return None, f"Please enter a valid {type_name}:"

    if value < setting["min"] or value > setting["max"]:
        return None, f"Value must be between {setting['min']} and {setting['max']}. Try again:"

    return value, None


def _validate_fund_thresholds(attr: str, new_value: float, config: MicromechConfig) -> str | None:
    """Validate that fund_target_native >= fund_threshold_native."""
    if attr == "fund_threshold_native":
        if new_value > config.fund_target_native:
            return (
                f"Threshold ({new_value}) must be <= target ({config.fund_target_native}). "
                "Increase target first."
            )
    if attr == "fund_target_native":
        if new_value < config.fund_threshold_native:
            return (
                f"Target ({new_value}) must be >= threshold ({config.fund_threshold_native}). "
                "Decrease threshold first."
            )
    return None


def _format_settings(config: MicromechConfig) -> str:
    """Format current settings status."""
    lines = [bold("Settings"), ""]

    for key, attr, label in _TOGGLES:
        enabled = _get_value(config, attr)
        status = "Enabled" if enabled else "Disabled"
        lines.append(f"{escape_html(label)}: {code(status)}")

    return "\n".join(lines)


def _format_edit_status(config: MicromechConfig) -> str:
    """Format current values for edit page."""
    lines = [bold("Edit Settings"), ""]

    lines.append(escape_html("Rewards"))
    for setting in EDITABLE_SETTINGS.values():
        if setting["group"] != "rewards":
            continue
        current = getattr(config, setting["attr"])
        unit = setting.get("unit", "")
        lines.append(f"  {escape_html(setting['label'])}: {code(f'{current} {unit}'.strip())}")
    lines.append("")

    if config.fund_enabled or config.payment_withdraw_enabled:
        lines.append(escape_html("Funding"))
        for setting in EDITABLE_SETTINGS.values():
            if setting["group"] != "fund":
                continue
            current = getattr(config, setting["attr"])
            unit = setting.get("unit", "")
            lines.append(f"  {escape_html(setting['label'])}: {code(f'{current} {unit}'.strip())}")

    return "\n".join(lines)


def _build_settings_keyboard(config: MicromechConfig) -> InlineKeyboardMarkup:
    """Build toggle keyboard."""
    rows = []
    for key, attr, label in _TOGGLES:
        enabled = _get_value(config, attr)
        prefix = "Disable" if enabled else "Enable"
        value = "off" if enabled else "on"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{prefix} {label}",
                    callback_data=f"{ACTION_SETTINGS}:{key}:{value}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("Edit values ›", callback_data=f"{ACTION_SETTINGS}:values")])
    rows.append(
        [
            InlineKeyboardButton(
                "Close",
                callback_data=f"{ACTION_SETTINGS}:cancel",
            ),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _build_edit_keyboard(config: MicromechConfig) -> InlineKeyboardMarkup:
    """Build keyboard for edit values page."""
    rows: list[list[InlineKeyboardButton]] = []
    for key, setting in EDITABLE_SETTINGS.items():
        current = getattr(config, setting["attr"])
        unit = setting.get("unit", "")
        display = f"{current} {unit}".strip()
        rows.append(
            [
                InlineKeyboardButton(
                    f"{setting['label']}: {display}",
                    callback_data=f"{ACTION_SETTINGS}:edit:{key}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("‹ Back", callback_data=f"{ACTION_SETTINGS}:back")])
    return InlineKeyboardMarkup(rows)


def _clear_settings_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear settings editing state."""
    if context.user_data:
        context.user_data.pop("settings_editing", None)


@authorized_only
async def settings_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle /settings command."""
    if not update.message:
        return

    _clear_settings_state(context)
    config: MicromechConfig = context.bot_data["config"]
    text = _format_settings(config)
    keyboard = _build_settings_keyboard(config)

    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


async def handle_settings_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    payload: str,
) -> None:
    """Handle settings callback."""
    query = update.callback_query
    if not query:
        return

    config: MicromechConfig = context.bot_data["config"]

    if payload == "cancel":
        _clear_settings_state(context)
        await query.delete_message()
        return

    if payload == "values":
        text = _format_edit_status(config)
        keyboard = _build_edit_keyboard(config)
        await query.answer()
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        return

    if payload == "back":
        _clear_settings_state(context)
        text = _format_settings(config)
        keyboard = _build_settings_keyboard(config)
        await query.answer()
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        return

    if payload.startswith("edit:"):
        key = payload[5:]
        if key not in EDITABLE_SETTINGS:
            await query.answer("Unknown setting")
            return
        setting = EDITABLE_SETTINGS[key]
        current = getattr(config, setting["attr"])
        unit = setting.get("unit", "")
        prompt = (
            f"Enter new value for {setting['label']}\nCurrent: {current} {unit}".strip()
            + f", Range: {setting['min']}-{setting['max']}"
        )
        if context.user_data is None:
            await query.answer("Session error")
            return
        context.user_data["settings_editing"] = key
        await query.answer()
        await query.edit_message_text(escape_html(prompt), parse_mode=ParseMode.HTML)
        return

    # Toggle: "key:on" or "key:off"
    parts = payload.split(":")
    if len(parts) != 2:
        await query.answer("Invalid request")
        return

    key, value = parts
    if key not in _TOGGLE_MAP:
        await query.answer("Unknown setting")
        return

    attr, label = _TOGGLE_MAP[key]
    new_value = value == "on"
    setattr(config, attr, new_value)
    status = "enabled" if new_value else "disabled"
    await query.answer(f"{label} {status}")
    config.save()

    text = _format_settings(config)
    keyboard = _build_settings_keyboard(config)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


async def handle_settings_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle text input for editing setting values."""
    if not context.user_data or not context.user_data.get("settings_editing"):
        return

    if not update.message or not update.message.text:
        return

    key = context.user_data["settings_editing"]
    if key not in EDITABLE_SETTINGS:
        context.user_data.pop("settings_editing", None)
        return

    setting = EDITABLE_SETTINGS[key]
    raw = update.message.text.strip()

    new_value, error = _validate_setting_input(setting, raw)
    if error:
        await update.message.reply_text(error)
        return

    config: MicromechConfig = context.bot_data["config"]

    fund_error = _validate_fund_thresholds(setting["attr"], new_value, config)
    if fund_error:
        await update.message.reply_text(f"Invalid: {fund_error}")
        context.user_data.pop("settings_editing", None)
        return

    setattr(config, setting["attr"], new_value)
    config.save()
    context.user_data.pop("settings_editing", None)

    unit = setting.get("unit", "")
    display = f"{new_value} {unit}".strip()
    text = _format_edit_status(config)
    keyboard = _build_edit_keyboard(config)

    await update.message.reply_text(
        f"{bold('Saved')}: {escape_html(setting['label'])} → {code(display)}\n\n{text}",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
