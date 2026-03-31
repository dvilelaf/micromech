"""Tests for configuration models."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from micromech.core.config import (
    ChainConfig,
    LLMConfig,
    MicromechConfig,
    PersistenceConfig,
    RuntimeConfig,
    ToolConfig,
)


class TestRuntimeConfig:
    def test_defaults(self):
        cfg = RuntimeConfig()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8000
        assert cfg.log_level == "INFO"
        assert cfg.max_concurrent == 10
        assert cfg.request_timeout == 300
        assert cfg.event_poll_interval == 15

    def test_custom_values(self):
        cfg = RuntimeConfig(port=9000, max_concurrent=50, log_level="debug")
        assert cfg.port == 9000
        assert cfg.max_concurrent == 50
        assert cfg.log_level == "DEBUG"  # normalized to uppercase

    def test_invalid_port_too_high(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(port=99999)

    def test_invalid_port_zero(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(port=0)

    def test_invalid_log_level(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(log_level="VERBOSE")

    def test_max_concurrent_must_be_positive(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(max_concurrent=0)

    def test_request_timeout_min(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(request_timeout=5)


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
        assert cfg.service_id is None
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


class TestPersistenceConfig:
    def test_defaults(self):
        cfg = PersistenceConfig()
        assert cfg.db_path.name == "micromech.db"
        assert cfg.cleanup_days == 30

    def test_custom_path(self, tmp_path: Path):
        cfg = PersistenceConfig(db_path=tmp_path / "custom.db")
        assert cfg.db_path.name == "custom.db"

    def test_cleanup_days_must_be_positive(self):
        with pytest.raises(ValidationError):
            PersistenceConfig(cleanup_days=0)


class TestLLMConfig:
    def test_defaults(self):
        cfg = LLMConfig()
        assert "Qwen" in cfg.model_repo
        assert cfg.max_tokens == 256
        assert cfg.context_size == 2048

    def test_max_tokens_bounds(self):
        with pytest.raises(ValidationError):
            LLMConfig(max_tokens=0)
        with pytest.raises(ValidationError):
            LLMConfig(max_tokens=5000)

    def test_context_size_bounds(self):
        with pytest.raises(ValidationError):
            LLMConfig(context_size=100)


class TestToolConfig:
    def test_basic(self):
        cfg = ToolConfig(id="echo")
        assert cfg.id == "echo"
        assert cfg.enabled is True
        assert cfg.params == {}

    def test_empty_id_rejected(self):
        with pytest.raises(ValidationError):
            ToolConfig(id="")

    def test_with_params(self):
        cfg = ToolConfig(id="llm", params={"temperature": 0.7})
        assert cfg.params["temperature"] == 0.7


class TestMicromechConfig:
    def test_defaults(self):
        cfg = MicromechConfig()
        assert cfg.version == "1"
        assert isinstance(cfg.runtime, RuntimeConfig)
        assert "gnosis" in cfg.chains
        assert isinstance(cfg.chains["gnosis"], ChainConfig)
        assert isinstance(cfg.persistence, PersistenceConfig)
        assert isinstance(cfg.llm, LLMConfig)
        assert len(cfg.tools) >= 1

    def test_save_and_load(self, tmp_path: Path):
        cfg = MicromechConfig(
            runtime=RuntimeConfig(port=9999),
            chains={"base": _chain_cfg(chain="base")},
        )
        config_path = tmp_path / "config.yaml"
        cfg.save(config_path)

        assert config_path.exists()

        loaded = MicromechConfig.load(config_path)
        assert loaded.runtime.port == 9999
        assert "base" in loaded.chains
        assert loaded.chains["base"].chain == "base"

    def test_load_nonexistent_returns_defaults(self, tmp_path: Path):
        cfg = MicromechConfig.load(tmp_path / "nonexistent.yaml")
        assert cfg.runtime.port == 8000

    def test_from_dict(self):
        data = {
            "runtime": {"port": 7000, "log_level": "DEBUG"},
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
        assert cfg.runtime.port == 7000
        assert cfg.chains["base"].chain == "base"

    def test_roundtrip_json(self):
        cfg = MicromechConfig()
        data = cfg.model_dump(mode="json")
        restored = MicromechConfig.model_validate(data)
        assert restored.runtime.port == cfg.runtime.port
        assert restored.chains["gnosis"].chain == cfg.chains["gnosis"].chain

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

    def test_backward_compat_mech_key_migration(self):
        """Old configs with `mech:` key auto-migrate to `chains:` dict."""
        data = {
            "mech": {
                "chain": "gnosis",
                "service_id": 42,
                "mech_address": "0x" + "d" * 40,
            }
        }
        cfg = MicromechConfig.model_validate(data)
        assert "gnosis" in cfg.chains
        assert cfg.chains["gnosis"].service_id == 42
        assert cfg.chains["gnosis"].mech_address == "0x" + "d" * 40
        # marketplace etc. filled from CHAIN_DEFAULTS
        assert cfg.chains["gnosis"].marketplace_address.startswith("0x")


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

    def test_needs_deploy_when_only_service_id(self):
        cfg = self._make(service_id=42, service_key="gnosis_42")
        assert cfg.detect_setup_state() == "needs_deploy"
        assert cfg.setup_complete is False

    def test_needs_mech_when_multisig_but_no_mech(self):
        cfg = self._make(
            service_id=42, service_key="gnosis_42",
            multisig_address=VALID_ADDR,
        )
        assert cfg.detect_setup_state() == "needs_mech"
        assert cfg.setup_complete is False

    def test_complete_when_all_set(self):
        cfg = self._make(
            service_id=42, service_key="gnosis_42",
            multisig_address=VALID_ADDR,
            mech_address=VALID_ADDR,
        )
        assert cfg.detect_setup_state() == "complete"
        assert cfg.setup_complete is True
