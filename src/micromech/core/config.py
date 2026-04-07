"""Configuration models for micromech (all Pydantic-validated)."""

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from micromech.core.constants import (
    CHAIN_DEFAULTS,
    DEFAULT_CHAIN,
    DEFAULT_CONFIG_DIR,
    DEFAULT_DELIVERY_RATE,
    DB_PATH,
    validate_eth_address,
)


class ChainConfig(BaseModel):
    """Per-chain mech identity and contract addresses."""

    chain: str = DEFAULT_CHAIN
    enabled: bool = True
    mech_address: Optional[str] = None
    marketplace_address: str
    factory_address: str
    staking_address: str
    delivery_rate: int = Field(default=DEFAULT_DELIVERY_RATE, ge=0)
    account_tag: str = "mech"

    @field_validator("mech_address", "marketplace_address",
                     "factory_address", "staking_address")
    @classmethod
    def check_eth_address(cls, v: Optional[str]) -> Optional[str]:
        return validate_eth_address(v)

    def detect_setup_state(self) -> str:
        """Detect how far along the setup process this chain config is.

        Returns one of: needs_create, complete.
        """
        if self.mech_address:
            return "complete"
        return "needs_create"

    @property
    def setup_complete(self) -> bool:
        """Check if mech_address is populated."""
        return bool(self.mech_address)

    def apply_deploy_result(self, result: dict) -> None:
        """Update config fields from a full_deploy result dict."""
        if result.get("mech_address"):
            self.mech_address = result["mech_address"]


class MicromechConfig(BaseModel):
    """Top-level configuration — flat, no sub-models."""

    model_config = ConfigDict(extra="ignore")

    # Chains (written by setup wizard)
    chains: dict[str, ChainConfig] = Field(default_factory=dict)

    # Tasks
    checkpoint_interval_minutes: int = Field(default=10, ge=1, le=120)
    checkpoint_alert_enabled: bool = True
    claim_interval_minutes: int = Field(default=240, ge=10, le=1440)
    claim_threshold_olas: float = Field(default=1.0, ge=0)
    fund_enabled: bool = True
    fund_interval_minutes: int = Field(default=360, ge=10, le=1440)
    fund_threshold_native: float = Field(default=0.05, ge=0, le=50)
    fund_target_native: float = Field(default=0.5, ge=0, le=50)
    auto_sell_enabled: bool = False
    auto_sell_min_olas: float = Field(default=1.0, ge=0)
    low_balance_alert_enabled: bool = True
    low_balance_alert_interval_hours: int = Field(default=6, ge=1, le=48)
    update_check_enabled: bool = True
    auto_update_enabled: bool = False

    @model_validator(mode="after")
    def validate_fund_target_above_threshold(self) -> "MicromechConfig":
        if self.fund_target_native < self.fund_threshold_native:
            msg = (
                f"fund_target_native ({self.fund_target_native}) must be >= "
                f"fund_threshold_native ({self.fund_threshold_native})"
            )
            raise ValueError(msg)
        return self

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
