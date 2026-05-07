"""Configuration models for micromech (all Pydantic-validated)."""

import unicodedata
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from micromech.core.constants import (
    DEFAULT_CHAIN,
    DEFAULT_CONFIG_DIR,
    DEFAULT_DELIVERY_BATCH_SIZE,
    DEFAULT_DELIVERY_FLUSH_TIMEOUT,
    DEFAULT_DELIVERY_INTERVAL,
    DEFAULT_DELIVERY_RATE,
    validate_eth_address,
)


class FallbackMechConfig(BaseModel):
    """Named fallback priority mech config."""

    name: str
    address: str

    @field_validator("name")
    @classmethod
    def check_name(cls, v: str) -> str:
        name = v.strip()
        if not name:
            raise ValueError("fallback mech name cannot be empty")
        if len(name) > 80:
            raise ValueError("fallback mech name cannot exceed 80 characters")
        if any(
            ord(char) < 32
            or ord(char) == 127
            or unicodedata.category(char) in {"Cc", "Cf", "Zl", "Zp"}
            for char in name
        ):
            raise ValueError("fallback mech name cannot contain control characters")
        return name

    @field_validator("address")
    @classmethod
    def check_eth_address(cls, v: str) -> str:
        return validate_eth_address(v) or v


KNOWN_FALLBACK_MECHS: tuple[dict[str, str], ...] = (
    {
        "name": "service-2675",
        "address": "0x1b5891Ba18DEcE123Cc73FC8b800B03C56aCE135",
    },
    {
        "name": "valory-priority-service-2182",
        "address": "0xC05e7412439bD7e91730a6880E18d5D5873F632C",
    },
    {
        "name": "service-3066",
        "address": "0x154E5F443d4f1E0745F91283Fc3Ce69AFede3311",
    },
    {
        "name": "service-2685",
        "address": "0xAa4Eb6F0F72f28d27C40aba1cBa2A3A3E4b2d79f",
    },
    {
        "name": "valory-priority-service-2198",
        "address": "0x601024E27f1C67B28209E24272CED8A31fc8151F",
    },
)
# The legacy/direct OLAS mech (0x77af31De935740567Cf4fF1986D04B2c964A786a)
# is intentionally not listed: fallback queue scanning only supports marketplace mechs.


def known_fallback_mechs() -> list[FallbackMechConfig]:
    """Return named known fallback priority mechs."""
    return [FallbackMechConfig.model_validate(mech) for mech in KNOWN_FALLBACK_MECHS]


def _fallback_mechs_from_addresses(addresses: list[str]) -> list[FallbackMechConfig]:
    """Build named fallback mech configs from legacy address entries."""
    merged = []
    seen = set()
    for addr in addresses:
        key = addr.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(FallbackMechConfig(name=addr, address=addr))
    return merged


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

    @field_validator("mech_address", "marketplace_address", "factory_address", "staking_address")
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

    # Delivery
    delivery_interval: int = Field(default=DEFAULT_DELIVERY_INTERVAL, ge=1, le=60)
    batch_delivery_enabled: bool = False
    delivery_batch_size: int = Field(default=DEFAULT_DELIVERY_BATCH_SIZE, ge=1, le=100)
    delivery_flush_timeout_seconds: int = Field(
        default=DEFAULT_DELIVERY_FLUSH_TIMEOUT, ge=0, le=300
    )

    # Tasks
    checkpoint_interval_minutes: int = Field(default=10, ge=1, le=120)
    checkpoint_alert_enabled: bool = True
    claim_interval_minutes: int = Field(default=240, ge=10, le=1440)
    claim_threshold_eur: float = Field(default=10.0, ge=0.1)
    fund_enabled: bool = True
    fund_interval_minutes: int = Field(default=360, ge=10, le=1440)
    fund_threshold_native: float = Field(default=0.1, ge=0, le=50)
    fund_target_native: float = Field(default=1.0, ge=0, le=50)
    payment_withdraw_enabled: bool = True
    payment_withdraw_threshold_xdai: float = Field(default=30.0, ge=0)
    payment_withdraw_interval_hours: int = Field(default=6, ge=1, le=168)
    xdai_sweep_enabled: bool = True
    xdai_sweep_tag: str = ""
    xdai_sweep_threshold_xdai: float = Field(default=30.0, ge=0)
    xdai_sweep_reserve_xdai: float = Field(default=10.0, ge=0)
    xdai_sweep_interval_hours: int = Field(default=6, ge=1, le=168)
    low_balance_alert_enabled: bool = True
    low_balance_alert_interval_hours: int = Field(default=6, ge=1, le=48)
    failed_deliveries_alert_enabled: bool = True
    failed_deliveries_alert_threshold: int = Field(default=10, ge=1)
    failed_deliveries_alert_interval_hours: int = Field(default=1, ge=1, le=48)
    update_check_enabled: bool = True
    auto_update_enabled: bool = False

    # Parallel Safe TX delivery (NonceAllocator)
    parallel_nonce_enabled: bool = False
    nonce_gap_alert_threshold: int = Field(default=3, ge=0, le=20)

    # Tools
    disabled_tools: list[str] = Field(default_factory=list)
    fallback_mode_enabled: bool = False
    fallback_check_interval: int = Field(default=30, ge=5, le=300)
    fallback_ttl_seconds: int = Field(default=3600, ge=60)
    fallback_poll_delay: int = Field(default=300, ge=0, le=600)
    queue_scanner_enabled: bool = False
    queue_scanner_interval_seconds: int = Field(default=300, ge=30, le=3600)
    queue_scanner_page_size: int = Field(default=50, ge=1, le=200)
    queue_scanner_fallback_pages_per_cycle: int = Field(default=5, ge=1, le=100)
    queue_scanner_event_lookback_blocks: int = Field(default=7200, ge=100, le=100000)
    fallback_mech_addresses: list[str] = Field(default_factory=list)
    fallback_mechs: list[FallbackMechConfig] = Field(default_factory=list)

    # Metadata state (set by MetadataManager after publish)
    metadata_ipfs_cid: Optional[str] = None
    metadata_onchain_hash: Optional[str] = None
    metadata_fingerprints: Optional[dict[str, str]] = None

    @model_validator(mode="after")
    def validate_fund_target_above_threshold(self) -> "MicromechConfig":
        if self.fund_target_native < self.fund_threshold_native:
            msg = (
                f"fund_target_native ({self.fund_target_native}) must be >= "
                f"fund_threshold_native ({self.fund_threshold_native})"
            )
            raise ValueError(msg)
        return self

    @field_validator("fallback_mech_addresses")
    @classmethod
    def validate_fallback_mechs(cls, values: list[str]) -> list[str]:
        return [validate_eth_address(v) or v for v in values]

    @field_validator("fallback_mechs", mode="before")
    @classmethod
    def normalize_fallback_mechs(cls, values):
        if not values:
            return []
        normalized = []
        for value in values:
            if isinstance(value, str):
                normalized.append({"name": value, "address": value})
            else:
                normalized.append(value)
        return normalized

    @model_validator(mode="after")
    def sync_fallback_mech_fields(self) -> "MicromechConfig":
        """Keep named mechs and legacy address list in sync."""
        fields_set = self.model_fields_set
        if "fallback_mech_addresses" in fields_set and "fallback_mechs" not in fields_set:
            merged = _fallback_mechs_from_addresses(self.fallback_mech_addresses)
            self.fallback_mech_addresses = [mech.address for mech in merged]
            self.fallback_mechs = merged
            return self

        if self.fallback_mechs:
            merged = []
            seen = set()
            for mech in self.fallback_mechs:
                key = mech.address.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(mech)
            for addr in self.fallback_mech_addresses:
                key = addr.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(FallbackMechConfig(name=addr, address=addr))
            self.fallback_mechs = merged
            self.fallback_mech_addresses = [mech.address for mech in merged]
        elif self.fallback_mech_addresses:
            merged = _fallback_mechs_from_addresses(self.fallback_mech_addresses)
            self.fallback_mech_addresses = [mech.address for mech in merged]
            self.fallback_mechs = merged
        return self

    def fallback_mech_name(self, address: str) -> str:
        """Return configured display name for a fallback mech address."""
        address_lower = address.lower()
        for mech in self.fallback_mechs:
            if mech.address.lower() == address_lower:
                return mech.name
        return address

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
        config_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


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
                "micromech",
                MicromechConfig,
            )
    except ImportError:
        pass
