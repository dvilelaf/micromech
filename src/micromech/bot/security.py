"""Security middleware for the Telegram bot."""

import time
from functools import wraps
from typing import Callable, Dict

from loguru import logger
from telegram import Update
from telegram.ext import ContextTypes

from micromech.secrets import secrets

# Rate limiting state
_rate_limit_cache: Dict[int, float] = {}
RATE_LIMIT_SECONDS = 2


def authorized_only(func: Callable) -> Callable:
    """Decorator to restrict commands to authorized chat only."""

    @wraps(func)
    async def wrapper(
        update: Update, context: ContextTypes.DEFAULT_TYPE, *args: object, **kwargs: object
    ) -> None:
        if not update.effective_chat:
            return

        current_chat_id = update.effective_chat.id

        if current_chat_id != secrets.telegram_chat_id:
            logger.warning(
                f"Unauthorized access attempt from chat_id={current_chat_id} "
                f"user={update.effective_user.username if update.effective_user else 'unknown'}"
            )
            return

        await func(update, context, *args, **kwargs)

    return wrapper


def rate_limited(func: Callable) -> Callable:
    """Decorator to rate limit commands per user."""

    @wraps(func)
    async def wrapper(
        update: Update, context: ContextTypes.DEFAULT_TYPE, *args: object, **kwargs: object
    ) -> None:
        if not update.effective_user:
            await func(update, context, *args, **kwargs)
            return

        user_id = update.effective_user.id
        current_time = time.time()
        last_call = _rate_limit_cache.get(user_id, 0)

        if current_time - last_call < RATE_LIMIT_SECONDS:
            logger.debug(f"Rate limited user {user_id}")
            return

        _rate_limit_cache[user_id] = current_time
        await func(update, context, *args, **kwargs)

    return wrapper
