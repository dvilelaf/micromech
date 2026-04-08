"""Telegram bot package.

_application holds a reference to the running Application instance,
set by app.py when the bot starts. Used by NotificationService to
lazily resolve the bot for task notifications.
"""

from typing import Any

_application: Any = None
