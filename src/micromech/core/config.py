"""Configuration models for micromech (all Pydantic-validated)."""

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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
    DEFAULT_LLM_PRESET,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_PORT,
    DEFAULT_REQUEST_TIMEOUT,
    LLM_MODEL_PRESETS,
    validate_eth_address,
)

DEFAULT_CONFIG_DIR = Path("data")


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

    def detect_setup_state(self) -> str:
        """Detect how far along the setup process this chain config is.

        Returns one of: needs_create, needs_deploy, needs_mech, complete.
        """
        if not self.service_id:
            return "needs_create"
        if not self.multisig_address:
            return "needs_deploy"
        if not self.mech_address:
            return "needs_mech"
        return "complete"

    @property
    def setup_complete(self) -> bool:
        """Check if all required addresses are populated."""
        return self.detect_setup_state() == "complete"

    def apply_deploy_result(self, result: dict) -> None:
        """Update config fields from a full_deploy result dict."""
        if "service_id" in result:
            self.service_id = result["service_id"]
        if "service_key" in result:
            self.service_key = result["service_key"]
        if "multisig_address" in result:
            self.multisig_address = result["multisig_address"]
        if "mech_address" in result:
            self.mech_address = result["mech_address"]


class PersistenceConfig(BaseModel):
    """Database settings."""

    db_path: Path = Field(default=DEFAULT_CONFIG_DIR / DEFAULT_DB_NAME)
    cleanup_days: int = Field(default=DEFAULT_CLEANUP_DAYS, ge=1)


class LLMConfig(BaseModel):
    """Built-in LLM tool settings.

    Use ``model`` to select a preset (e.g. "qwen", "gemma4"), or override
    ``model_repo`` / ``model_file`` directly for a custom GGUF model.
    """

    model: str = DEFAULT_LLM_PRESET
    model_repo: str = DEFAULT_LLM_MODEL
    model_file: str = DEFAULT_LLM_FILE
    max_tokens: int = Field(default=DEFAULT_LLM_MAX_TOKENS, ge=1, le=4096)
    context_size: int = Field(default=DEFAULT_LLM_CONTEXT_SIZE, ge=256, le=32768)
    models_dir: Path = Field(default=DEFAULT_CONFIG_DIR / "models")

    @model_validator(mode="before")
    @classmethod
    def resolve_preset(cls, data: Any) -> Any:
        """If model preset is given, fill in model_repo/model_file from it."""
        if isinstance(data, dict):
            preset = data.get("model", DEFAULT_LLM_PRESET)
            if preset in LLM_MODEL_PRESETS:
                repo, fname = LLM_MODEL_PRESETS[preset]
                data.setdefault("model_repo", repo)
                data.setdefault("model_file", fname)
        return data


class IpfsConfig(BaseModel):
    """IPFS settings."""

    gateway: str = "https://gateway.autonolas.tech/ipfs/"
    api_url: str = "https://registry.autonolas.tech"
    timeout: int = Field(default=30, ge=5, le=120)
    enabled: bool = True

    @field_validator("gateway", "api_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            msg = f"URL must start with http:// or https://: {v}"
            raise ValueError(msg)
        return v


class ToolConfig(BaseModel):
    """Configuration for a single tool."""

    id: str = Field(min_length=1)
    enabled: bool = True
    params: dict[str, Any] = Field(default_factory=dict)


class TelegramConfig(BaseModel):
    """Telegram bot settings (non-secret — tokens go in secrets.env)."""

    enabled: bool = False
    rate_limit_seconds: int = Field(default=2, ge=1, le=30)


class TasksConfig(BaseModel):
    """Automated task settings."""

    enabled: bool = True

    # Checkpoint
    checkpoint_interval_minutes: int = Field(default=10, ge=1, le=120)
    checkpoint_grace_period_seconds: int = Field(default=120, ge=0, le=600)
    checkpoint_alert_enabled: bool = True

    # Rewards claiming
    claim_interval_minutes: int = Field(default=240, ge=10, le=1440)
    claim_threshold_olas: float = Field(default=1.0, ge=0)

    # Auto-fund
    fund_enabled: bool = True
    fund_interval_minutes: int = Field(default=360, ge=10, le=1440)
    fund_threshold_native: float = Field(default=0.05, ge=0)
    fund_target_native: float = Field(default=0.5, ge=0)

    # Auto-sell (OLAS -> native for gas)
    auto_sell_enabled: bool = False
    auto_sell_min_olas: float = Field(default=1.0, ge=0)
    auto_sell_runway_days: int = Field(default=20, ge=1, le=365)

    # Low balance alerts
    low_balance_alert_enabled: bool = True
    low_balance_alert_interval_hours: int = Field(default=6, ge=1, le=48)

    # Update check
    update_check_enabled: bool = True
    auto_update_enabled: bool = False
    update_channel: str = "release"

    # Health heartbeat
    health_interval_seconds: int = Field(default=55, ge=10, le=300)


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

    model_config = ConfigDict(extra="ignore")

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
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    tasks: TasksConfig = Field(default_factory=TasksConfig)

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
        """Load config via iwa's plugin system (singleton).

        Falls back to standalone YAML file if iwa is not available.
        """
        try:
            from iwa.core.models import Config
            cfg = Config().get_plugin_config("micromech")
            if cfg is not None:
                return cfg
        except ImportError:
            pass

        # Fallback: standalone YAML (no iwa)
        config_path = path or (DEFAULT_CONFIG_DIR / "micromech.yaml")
        if config_path.exists():
            data = yaml.safe_load(config_path.read_text()) or {}
            return cls.model_validate(data)
        return cls()

    def save(self, path: Optional[Path] = None) -> None:
        """Save config via iwa's plugin system.

        Falls back to standalone YAML file if iwa is not available.
        """
        try:
            from iwa.core.models import Config
            iwa_cfg = Config()
            iwa_cfg.plugins["micromech"] = self
            iwa_cfg.save_config()
            return
        except ImportError:
            pass

        # Fallback: standalone YAML (no iwa)
        config_path = path or (DEFAULT_CONFIG_DIR / "micromech.yaml")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        data = self.model_dump(mode="json")
        config_path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False)
        )


def register_plugin() -> None:
    """Register MicromechConfig with iwa's plugin system.

    Call once at startup before any MicromechConfig.load().
    Safe to call multiple times (idempotent) and when iwa
    is not installed (silently ignored).
    """
    try:
        from iwa.core.models import Config
        iwa_cfg = Config()
        if "micromech" not in iwa_cfg._plugin_models:
            iwa_cfg.register_plugin_config(
                "micromech", MicromechConfig,
            )
    except ImportError:
        pass
