"""Configuration models for micromech (all Pydantic-validated)."""

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from micromech.core.constants import (
    CHAIN_DEFAULTS,
    DEFAULT_CHAIN,
    DEFAULT_CLEANUP_DAYS,
    DEFAULT_DB_NAME,
    DEFAULT_DELIVERY_BATCH_SIZE,
    DEFAULT_DELIVERY_INTERVAL,
    DEFAULT_DELIVERY_RATE,
    DEFAULT_EVENT_LOOKBACK_BLOCKS,
    DEFAULT_EVENT_POLL_INTERVAL,
    DEFAULT_HOST,
    DEFAULT_LLM_CONTEXT_SIZE,
    DEFAULT_LLM_FILE,
    DEFAULT_LLM_MAX_TOKENS,
    DEFAULT_LLM_MODEL,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_PORT,
    DEFAULT_REQUEST_TIMEOUT,
    validate_eth_address,
)

DEFAULT_CONFIG_DIR = Path.home() / ".micromech"
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.yaml"


class RuntimeConfig(BaseModel):
    """Runtime server settings."""

    host: str = DEFAULT_HOST
    port: int = Field(default=DEFAULT_PORT, ge=1, le=65535)
    log_level: str = "INFO"
    max_concurrent: int = Field(default=DEFAULT_MAX_CONCURRENT, ge=1, le=1000)
    request_timeout: int = Field(default=DEFAULT_REQUEST_TIMEOUT, ge=10, le=3600)
    event_poll_interval: int = Field(default=DEFAULT_EVENT_POLL_INTERVAL, ge=1, le=300)
    event_lookback_blocks: int = Field(default=DEFAULT_EVENT_LOOKBACK_BLOCKS, ge=10)
    delivery_batch_size: int = Field(default=DEFAULT_DELIVERY_BATCH_SIZE, ge=1, le=100)
    delivery_interval: int = Field(default=DEFAULT_DELIVERY_INTERVAL, ge=1, le=300)

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid:
            msg = f"Invalid log level: {v}. Must be one of {valid}"
            raise ValueError(msg)
        return upper


class ChainConfig(BaseModel):
    """Per-chain mech identity and contract addresses."""

    chain: str = DEFAULT_CHAIN
    enabled: bool = True
    service_id: Optional[int] = None
    service_key: Optional[str] = None
    mech_address: Optional[str] = None
    multisig_address: Optional[str] = None
    marketplace_address: str
    factory_address: str
    staking_address: str
    delivery_rate: int = Field(default=DEFAULT_DELIVERY_RATE, ge=0)
    account_tag: str = "mech"

    @field_validator("mech_address", "multisig_address", "marketplace_address",
                     "factory_address", "staking_address")
    @classmethod
    def check_eth_address(cls, v: Optional[str]) -> Optional[str]:
        return validate_eth_address(v)


class PersistenceConfig(BaseModel):
    """Database settings."""

    db_path: Path = Field(default=DEFAULT_CONFIG_DIR / "data" / DEFAULT_DB_NAME)
    cleanup_days: int = Field(default=DEFAULT_CLEANUP_DAYS, ge=1)


class LLMConfig(BaseModel):
    """Built-in LLM tool settings."""

    model_repo: str = DEFAULT_LLM_MODEL
    model_file: str = DEFAULT_LLM_FILE
    max_tokens: int = Field(default=DEFAULT_LLM_MAX_TOKENS, ge=1, le=4096)
    context_size: int = Field(default=DEFAULT_LLM_CONTEXT_SIZE, ge=256, le=32768)
    models_dir: Path = Field(default=DEFAULT_CONFIG_DIR / "models")


class IpfsConfig(BaseModel):
    """IPFS settings."""

    gateway: str = "https://gateway.autonolas.tech/ipfs/"
    api_url: str = "http://localhost:5001"
    timeout: int = Field(default=30, ge=5, le=120)
    enabled: bool = True


class ToolConfig(BaseModel):
    """Configuration for a single tool."""

    id: str = Field(min_length=1)
    enabled: bool = True
    params: dict[str, Any] = Field(default_factory=dict)


def _default_chains() -> dict[str, ChainConfig]:
    """Default: only gnosis enabled."""
    gnosis = CHAIN_DEFAULTS["gnosis"]
    return {
        "gnosis": ChainConfig(
            chain="gnosis",
            marketplace_address=gnosis["marketplace"],
            factory_address=gnosis["factory"],
            staking_address=gnosis["staking"],
        )
    }


class MicromechConfig(BaseModel):
    """Top-level configuration."""

    version: str = "1"
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    chains: dict[str, ChainConfig] = Field(default_factory=_default_chains)
    ipfs: IpfsConfig = Field(default_factory=IpfsConfig)
    persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    tools: list[ToolConfig] = Field(
        default_factory=lambda: [
            ToolConfig(id="echo", enabled=True),
        ]
    )

    @model_validator(mode="before")
    @classmethod
    def migrate_single_chain(cls, data: Any) -> Any:
        """Auto-migrate old single-chain `mech:` config to `chains:` dict."""
        if isinstance(data, dict) and "mech" in data and "chains" not in data:
            mech = data.pop("mech")
            chain_name = mech.get("chain", DEFAULT_CHAIN)
            defaults = CHAIN_DEFAULTS.get(chain_name, {})
            mech.setdefault("marketplace_address", defaults.get("marketplace", ""))
            mech.setdefault("factory_address", defaults.get("factory", ""))
            mech.setdefault("staking_address", defaults.get("staking", ""))
            data["chains"] = {chain_name: mech}
        return data

    @property
    def enabled_chains(self) -> dict[str, ChainConfig]:
        """Get only enabled chain configs."""
        return {k: v for k, v in self.chains.items() if v.enabled}

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "MicromechConfig":
        """Load config from YAML file. Returns defaults if file doesn't exist."""
        config_path = path or DEFAULT_CONFIG_PATH
        if config_path.exists():
            data = yaml.safe_load(config_path.read_text()) or {}
            return cls.model_validate(data)
        return cls()

    def save(self, path: Optional[Path] = None) -> None:
        """Save config to YAML file."""
        config_path = path or DEFAULT_CONFIG_PATH
        config_path.parent.mkdir(parents=True, exist_ok=True)
        data = self.model_dump(mode="json")
        config_path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False)
        )
