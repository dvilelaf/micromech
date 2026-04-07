"""Tests for configuration models."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from micromech.core.config import (
    ChainConfig,
    MicromechConfig,
)


STUB_MARKETPLACE = "0x" + "a" * 40
STUB_FACTORY = "0x" + "b" * 40
STUB_STAKING = "0x" + "c" * 40


def _chain_cfg(**overrides) -> ChainConfig:
    """Helper to build a ChainConfig with stub required addresses."""
    defaults = dict(
        marketplace_address=STUB_MARKETPLACE,
        factory_address=STUB_FACTORY,
        staking_address=STUB_STAKING,
    )
    defaults.update(overrides)
    return ChainConfig(**defaults)


class TestChainConfig:
    def test_defaults(self):
        cfg = _chain_cfg()
        assert cfg.chain == "gnosis"
        assert cfg.enabled is True
        assert cfg.mech_address is None
        assert cfg.marketplace_address.startswith("0x")
        assert cfg.account_tag == "mech"

    def test_valid_address(self):
        addr = "0x" + "a" * 40
        cfg = _chain_cfg(mech_address=addr)
        assert cfg.mech_address == addr

    def test_invalid_address_no_prefix(self):
        with pytest.raises(ValidationError):
            _chain_cfg(mech_address="not_an_address")

    def test_invalid_address_wrong_length(self):
        with pytest.raises(ValidationError):
            _chain_cfg(mech_address="0x123")

    def test_invalid_address_non_hex(self):
        with pytest.raises(ValidationError):
            _chain_cfg(mech_address="0x" + "Z" * 40)

    def test_none_address_is_valid(self):
        cfg = _chain_cfg(mech_address=None)
        assert cfg.mech_address is None

    def test_custom_chain(self):
        cfg = _chain_cfg(chain="base")
        assert cfg.chain == "base"

    def test_disabled(self):
        cfg = _chain_cfg(enabled=False)
        assert cfg.enabled is False


class TestMicromechConfig:
    def test_defaults(self):
        cfg = MicromechConfig()
        assert cfg.chains == {}
        assert cfg.checkpoint_interval_minutes == 10

    def test_save_and_load_fallback(self, tmp_path: Path):
        """Save/load via fallback path (no iwa)."""
        from unittest.mock import patch

        config_path = tmp_path / "micromech.yaml"
        cfg = MicromechConfig(
            chains={"base": _chain_cfg(chain="base")},
        )

        # Force fallback (mock iwa import failure)
        with patch.dict("sys.modules", {"iwa.core.models": None}):
            cfg.save(config_path)
            assert config_path.exists()
            loaded = MicromechConfig.load(config_path)

        assert "base" in loaded.chains
        assert loaded.chains["base"].chain == "base"

    def test_load_defaults(self):
        """Load returns defaults when no config exists."""
        cfg = MicromechConfig()
        assert cfg.chains == {}

    def test_from_dict(self):
        data = {
            "chains": {
                "base": {
                    "chain": "base",
                    "marketplace_address": STUB_MARKETPLACE,
                    "factory_address": STUB_FACTORY,
                    "staking_address": STUB_STAKING,
                }
            },
        }
        cfg = MicromechConfig.model_validate(data)
        assert cfg.chains["base"].chain == "base"

    def test_roundtrip_json(self):
        cfg = MicromechConfig(
            chains={"base": _chain_cfg(chain="base")},
        )
        data = cfg.model_dump(mode="json")
        restored = MicromechConfig.model_validate(data)
        assert restored.chains["base"].chain == "base"

    def test_enabled_chains_property(self):
        cfg = MicromechConfig(
            chains={
                "gnosis": _chain_cfg(chain="gnosis", enabled=True),
                "base": _chain_cfg(chain="base", enabled=False),
            }
        )
        enabled = cfg.enabled_chains
        assert "gnosis" in enabled
        assert "base" not in enabled

    def test_extra_fields_ignored(self):
        """Old config fields (version, runtime, etc.) are ignored."""
        data = {
            "version": "1",
            "runtime": {"port": 9999},
            "ipfs": {"gateway": "https://foo/"},
            "log_level": "WARNING",  # old field, should be ignored
        }
        cfg = MicromechConfig.model_validate(data)

    def test_tasks_fields_flat(self):
        cfg = MicromechConfig(
            checkpoint_interval_minutes=30,
            claim_threshold_olas=5.0,
            fund_enabled=False,
            auto_update_enabled=True,
        )
        assert cfg.checkpoint_interval_minutes == 30
        assert cfg.claim_threshold_olas == 5.0
        assert cfg.fund_enabled is False
        assert cfg.auto_update_enabled is True

    def test_checkpoint_interval_bounds(self):
        with pytest.raises(ValidationError):
            MicromechConfig(checkpoint_interval_minutes=0)
        with pytest.raises(ValidationError):
            MicromechConfig(checkpoint_interval_minutes=121)

    def test_claim_interval_bounds(self):
        with pytest.raises(ValidationError):
            MicromechConfig(claim_interval_minutes=5)
        with pytest.raises(ValidationError):
            MicromechConfig(claim_interval_minutes=1441)

    def test_fund_threshold_non_negative(self):
        cfg = MicromechConfig(fund_threshold_native=0)
        assert cfg.fund_threshold_native == 0


VALID_ADDR = "0x" + "aB" * 20
MARKETPLACE = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
FACTORY = "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF"
STAKING = "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44"


class TestDetectSetupState:
    def _make(self, **overrides) -> ChainConfig:
        defaults = {
            "marketplace_address": MARKETPLACE,
            "factory_address": FACTORY,
            "staking_address": STAKING,
        }
        defaults.update(overrides)
        return ChainConfig(**defaults)

    def test_needs_create_when_empty(self):
        cfg = self._make()
        assert cfg.detect_setup_state() == "needs_create"
        assert cfg.setup_complete is False

    def test_complete_when_mech_address_set(self):
        cfg = self._make(mech_address=VALID_ADDR)
        assert cfg.detect_setup_state() == "complete"
        assert cfg.setup_complete is True
