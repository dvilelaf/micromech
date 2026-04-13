"""Telegram Bot Application."""

from typing import Any, Optional

from loguru import logger
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from micromech.bot.commands.checkpoint import checkpoint_command, handle_checkpoint_callback
from micromech.bot.commands.claim import claim_command, handle_claim_callback
from micromech.bot.commands.contracts import contracts_command
from micromech.bot.commands.info import info_command
from micromech.bot.commands.last_rewards import last_rewards_command
from micromech.bot.commands.logs import logs_command
from micromech.bot.commands.manage import (
    handle_manage_callback,
    handle_manage_confirm_callback,
    manage_command,
)
from micromech.bot.commands.queue_cmd import queue_command
from micromech.bot.commands.restart import restart_command
from micromech.bot.commands.schedule import schedule_command
from micromech.bot.commands.sell import sell_command
from micromech.bot.commands.settings import (
    handle_settings_callback,
    handle_settings_text,
    settings_command,
)
from micromech.bot.commands.status import status_command
from micromech.bot.commands.update import update_command
from micromech.bot.commands.wallet import wallet_command
from micromech.bot.formatting import bold
from micromech.bot.security import authorized_only, rate_limited
from micromech.core.config import MicromechConfig
from micromech.core.persistence import PersistentQueue
from micromech.management import MechLifecycle
from micromech.runtime.manager import RuntimeManager
from micromech.runtime.metrics import MetricsCollector
from micromech.secrets import secrets

# Callback action prefixes
ACTION_STATUS = "status"
ACTION_CLAIM = "claim"
ACTION_CHECKPOINT = "checkpoint"
ACTION_WALLET = "wallet"
ACTION_MANAGE = "manage"
ACTION_MANAGE_CONFIRM = "mgcfm"
ACTION_SETTINGS = "settings"

# Bot command menu (shown in the blue button)
_BOT_COMMANDS = [
    BotCommand("status", "Mech status per chain"),
    BotCommand("wallet", "Wallet addresses and balances"),
    BotCommand("claim", "Claim staking rewards"),
    BotCommand("checkpoint", "Call staking checkpoint"),
    BotCommand("manage", "Stake/unstake per chain"),
    BotCommand("contracts", "Staking contract info"),
    BotCommand("schedule", "Next epoch checkpoint"),
    BotCommand("last_rewards", "Accrued rewards this epoch"),
    BotCommand("sell", "Run auto-sell manually"),
    BotCommand("queue", "Request queue status"),
    BotCommand("info", "Version and runtime info"),
    BotCommand("logs", "Download last 24h logs"),
    BotCommand("settings", "Toggle features and edit values"),
    BotCommand("update", "Check for updates"),
    BotCommand("restart", "Restart runtime"),
    BotCommand("help", "Show all commands"),
]


@authorized_only
@rate_limited
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message."""
    if not update.message:
        return
    await update.message.reply_text(
        f"Welcome to {bold('Micromech')} Telegram Bot.\nUse /status to see your mech status.",
        parse_mode="HTML",
    )


@authorized_only
@rate_limited
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send help message."""
    help_text = (
        f"{bold('Micromech Commands')}\n\n"
        "/status - Mech status per chain\n"
        "/wallet - Wallet addresses and balances\n"
        "/claim - Claim staking rewards\n"
        "/checkpoint - Call staking checkpoint\n"
        "/manage - Stake/unstake per chain\n"
        "/contracts - Staking contract info\n"
        "/schedule - Next epoch checkpoint\n"
        "/last_rewards - Accrued rewards this epoch\n"
        "/sell - Run auto-sell manually\n"
        "/queue - Request queue status\n"
        "/info - Version and runtime info\n"
        "/logs - Download last 24h logs\n"
        "/settings - Toggle features and edit values\n"
        "/update - Check for updates\n"
        "/restart - Restart runtime"
    )
    if not update.message:
        return
    await update.message.reply_text(help_text, parse_mode="HTML")


async def global_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dispatch callbacks to appropriate handlers."""
    if not update.effective_chat:
        return
    if not secrets.telegram_chat_id or update.effective_chat.id != secrets.telegram_chat_id:
        logger.warning(
            "Unauthorized callback attempt from chat_id={} user={}",
            update.effective_chat.id,
            update.effective_user.username if update.effective_user else "unknown",
        )
        return

    query = update.callback_query
    if not query or not query.data:
        return
    data = query.data

    if ":" not in data:
        await query.answer("Invalid request")
        return

    action, payload = data.split(":", 1)

    try:
        if action == ACTION_CLAIM:
            await handle_claim_callback(update, context, payload)
        elif action == ACTION_CHECKPOINT:
            await handle_checkpoint_callback(update, context, payload)
        elif action == ACTION_SETTINGS:
            await handle_settings_callback(update, context, payload)
        elif action == ACTION_MANAGE:
            await handle_manage_callback(update, context, payload)
        elif action == ACTION_MANAGE_CONFIRM:
            await handle_manage_confirm_callback(update, context, payload)
        else:
            await query.answer("Unknown action")
    except Exception as e:
        logger.opt(exception=True).error("Error handling callback {}: {}", data, e)
        try:
            await query.answer("An error occurred")
        except Exception:
            pass


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors."""
    logger.opt(exception=context.error).error("Exception while handling an update")


async def _post_init(application: Application) -> None:
    """Post-init hook — register bot command menu."""
    try:
        await application.bot.set_my_commands(_BOT_COMMANDS)
        logger.info("Bot command menu registered ({} commands)", len(_BOT_COMMANDS))
    except Exception as e:
        logger.warning("Failed to register bot commands: {}", e)


def create_application(
    config: MicromechConfig,
    runtime_manager: Optional[RuntimeManager] = None,
    queue: Optional[PersistentQueue] = None,
    metrics: Optional[MetricsCollector] = None,
    bridges: Optional[dict[str, Any]] = None,
) -> Application:
    """Create and configure the Telegram application."""
    if not secrets.telegram_token:
        raise ValueError("Telegram token is not set")

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=10.0)
    builder = (
        Application.builder()
        .token(secrets.telegram_token.get_secret_value())
        .request(request)
        .post_init(_post_init)
    )
    app = builder.build()

    # Store services in bot_data for handler access
    app.bot_data["config"] = config
    app.bot_data["runtime_manager"] = runtime_manager
    app.bot_data["queue"] = queue
    app.bot_data["metrics"] = metrics
    app.bot_data["bridges"] = bridges or {}

    # Pre-create MechLifecycle instances for all enabled chains
    lifecycles: dict[str, MechLifecycle] = {}
    for chain_name in config.enabled_chains:
        try:
            lifecycles[chain_name] = MechLifecycle(config, chain_name)
        except Exception as e:
            logger.warning("Failed to create MechLifecycle for {}: {}", chain_name, e)
    app.bot_data["lifecycles"] = lifecycles

    # Commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("wallet", wallet_command))
    app.add_handler(CommandHandler("claim", claim_command))
    app.add_handler(CommandHandler("checkpoint", checkpoint_command))
    app.add_handler(CommandHandler("manage", manage_command))
    app.add_handler(CommandHandler("contracts", contracts_command))
    app.add_handler(CommandHandler("schedule", schedule_command))
    app.add_handler(CommandHandler("last_rewards", last_rewards_command))
    app.add_handler(CommandHandler("sell", sell_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("logs", logs_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("update", update_command))
    app.add_handler(CommandHandler("restart", restart_command))

    # Text input for settings value editing
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_settings_text),
        group=1,
    )

    # Callbacks
    app.add_handler(CallbackQueryHandler(global_callback_handler))

    # Error handler
    app.add_error_handler(error_handler)

    # Store global reference for NotificationService lazy resolution
    import micromech.bot as _bot_pkg

    _bot_pkg._application = app

    return app
