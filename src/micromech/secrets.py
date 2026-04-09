"""Secrets from environment variables (never stored in config.yaml)."""

from typing import Optional

from pydantic import SecretStr, field_validator
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

    @field_validator("health_url")
    @classmethod
    def validate_health_url(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith(("http://", "https://")):
            msg = "health_url must start with http:// or https://"
            raise ValueError(msg)
        return v

secrets = MicromechSecrets()
