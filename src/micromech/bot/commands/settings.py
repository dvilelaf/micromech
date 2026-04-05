"""Settings command handler — toggle features with inline buttons."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from micromech.bot.formatting import bold, code, escape_html
from micromech.bot.security import authorized_only
from micromech.core.config import MicromechConfig

ACTION_SETTINGS = "settings"

# (callback_key, config_section, config_attr, display_label)
_TOGGLES = [
    ("checkpoint_alert", "tasks", "checkpoint_alert_enabled", "Checkpoint alerts"),
    ("low_balance_alert", "tasks", "low_balance_alert_enabled", "Low balance alerts"),
    ("fund", "tasks", "fund_enabled", "Auto-fund"),
    ("auto_sell", "tasks", "auto_sell_enabled", "Auto-sell"),
    ("auto_update", "tasks", "auto_update_enabled", "Auto-update"),
    ("update_check", "tasks", "update_check_enabled", "Update check"),
]

_TOGGLE_MAP: dict[str, tuple[str, str, str]] = {
    key: (section, attr, label) for key, section, attr, label in _TOGGLES
}


def _get_value(config: MicromechConfig, section: str, attr: str) -> bool:
    """Get a toggle value from config."""
    section_obj = getattr(config, section)
    return getattr(section_obj, attr)


def _set_value(config: MicromechConfig, section: str, attr: str, value: bool) -> None:
    """Set a toggle value on config."""
    section_obj = getattr(config, section)
    setattr(section_obj, attr, value)


def _format_settings(config: MicromechConfig) -> str:
    """Format current settings status."""
    lines = [bold("Settings"), ""]

    for key, section, attr, label in _TOGGLES:
        enabled = _get_value(config, section, attr)
        status = "Enabled" if enabled else "Disabled"
        lines.append(f"{escape_html(label)}: {code(status)}")

    return "\n".join(lines)


def _build_settings_keyboard(config: MicromechConfig) -> InlineKeyboardMarkup:
    """Build toggle keyboard."""
    rows = []
    for key, section, attr, label in _TOGGLES:
        enabled = _get_value(config, section, attr)
        prefix = "Disable" if enabled else "Enable"
        value = "off" if enabled else "on"
        rows.append(
            [InlineKeyboardButton(
                f"{prefix} {label}",
                callback_data=f"{ACTION_SETTINGS}:{key}:{value}",
            )]
        )
    rows.append([InlineKeyboardButton("Close", callback_data=f"{ACTION_SETTINGS}:cancel")])
    return InlineKeyboardMarkup(rows)


@authorized_only
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /settings command."""
    if not update.message:
        return

    config: MicromechConfig = context.bot_data["config"]
    text = _format_settings(config)
    keyboard = _build_settings_keyboard(config)

    await update.message.reply_text(
        text, reply_markup=keyboard, parse_mode=ParseMode.HTML
    )


async def handle_settings_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
) -> None:
    """Handle settings callback."""
    query = update.callback_query
    if not query:
        return

    if payload == "cancel":
        await query.delete_message()
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

    section, attr, label = _TOGGLE_MAP[key]
    config: MicromechConfig = context.bot_data["config"]
    new_value = value == "on"
    _set_value(config, section, attr, new_value)

    status = "enabled" if new_value else "disabled"
    await query.answer(f"{label} {status}")

    # Save config
    config.save()

    # Refresh display
    text = _format_settings(config)
    keyboard = _build_settings_keyboard(config)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
