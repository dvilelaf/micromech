"""Secrets from environment variables (never stored in config.yaml)."""

from typing import Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class MicromechSecrets(BaseSettings):
    """Secrets loaded from environment or secrets.env file."""

    model_config = SettingsConfigDict(extra="ignore")

    # Wallet
    wallet_password: Optional[SecretStr] = None

    # Telegram (optional — bot only starts if both are set)
    telegram_token: Optional[SecretStr] = None
    telegram_chat_id: Optional[int] = None

    # Health monitor URL (e.g. Uptime Kuma, Healthchecks.io)
    health_url: Optional[str] = None

    # Web dashboard auth (existing MICROMECH_AUTH_TOKEN)
    micromech_auth_token: Optional[str] = None

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_token and self.telegram_chat_id)


secrets = MicromechSecrets()
