"""Tests for configuration models."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from micromech.core.config import (
    LLMConfig,
    MechConfig,
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


class TestMechConfig:
    def test_defaults(self):
        cfg = MechConfig()
        assert cfg.chain == "gnosis"
        assert cfg.service_id is None
        assert cfg.mech_address is None
        assert cfg.marketplace_address.startswith("0x")
        assert cfg.account_tag == "mech"

    def test_valid_address(self):
        addr = "0x" + "a" * 40
        cfg = MechConfig(mech_address=addr)
        assert cfg.mech_address == addr

    def test_invalid_address_no_prefix(self):
        with pytest.raises(ValidationError):
            MechConfig(mech_address="not_an_address")

    def test_invalid_address_wrong_length(self):
        with pytest.raises(ValidationError):
            MechConfig(mech_address="0x123")

    def test_invalid_address_non_hex(self):
        with pytest.raises(ValidationError):
            MechConfig(mech_address="0x" + "Z" * 40)

    def test_none_address_is_valid(self):
        cfg = MechConfig(mech_address=None)
        assert cfg.mech_address is None


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
        assert isinstance(cfg.mech, MechConfig)
        assert isinstance(cfg.persistence, PersistenceConfig)
        assert isinstance(cfg.llm, LLMConfig)
        assert len(cfg.tools) >= 1

    def test_save_and_load(self, tmp_path: Path):
        cfg = MicromechConfig(
            runtime=RuntimeConfig(port=9999),
            mech=MechConfig(chain="base"),
        )
        config_path = tmp_path / "config.yaml"
        cfg.save(config_path)

        assert config_path.exists()

        loaded = MicromechConfig.load(config_path)
        assert loaded.runtime.port == 9999
        assert loaded.mech.chain == "base"

    def test_load_nonexistent_returns_defaults(self, tmp_path: Path):
        cfg = MicromechConfig.load(tmp_path / "nonexistent.yaml")
        assert cfg.runtime.port == 8000

    def test_from_dict(self):
        data = {
            "runtime": {"port": 7000, "log_level": "DEBUG"},
            "mech": {"chain": "base"},
        }
        cfg = MicromechConfig.model_validate(data)
        assert cfg.runtime.port == 7000
        assert cfg.mech.chain == "base"

    def test_roundtrip_json(self):
        cfg = MicromechConfig()
        data = cfg.model_dump(mode="json")
        restored = MicromechConfig.model_validate(data)
        assert restored.runtime.port == cfg.runtime.port
        assert restored.mech.chain == cfg.mech.chain
