"""Tests for configuration models."""

import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from micromech.core.config import (
    KNOWN_FALLBACK_MECHS,
    ChainConfig,
    FallbackMechConfig,
    MicromechConfig,
    known_fallback_mechs,
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
        assert cfg.batch_delivery_enabled is False
        assert cfg.delivery_batch_size == 1
        assert cfg.delivery_flush_timeout_seconds == 60
        assert cfg.checkpoint_interval_minutes == 10
        assert cfg.queue_scanner_enabled is False
        assert cfg.queue_scanner_interval_seconds == 300
        assert cfg.queue_scanner_page_size == 50
        assert cfg.queue_scanner_fallback_pages_per_cycle == 5
        assert cfg.fallback_mech_addresses == []
        assert cfg.fallback_mechs == []

    def test_known_fallback_mech_preset(self):
        mechs = known_fallback_mechs()

        assert [mech.address for mech in mechs] == [
            mech["address"] for mech in KNOWN_FALLBACK_MECHS
        ]
        assert [mech.name for mech in mechs] == [mech["name"] for mech in KNOWN_FALLBACK_MECHS]

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
        fallback_mech = "0x" + "d" * 40
        cfg = MicromechConfig(
            chains={"base": _chain_cfg(chain="base")},
            fallback_mechs=[FallbackMechConfig(name="test mech", address=fallback_mech)],
        )
        data = cfg.model_dump(mode="json")
        restored = MicromechConfig.model_validate(data)
        assert restored.chains["base"].chain == "base"
        assert restored.fallback_mech_addresses == [fallback_mech]
        assert restored.fallback_mechs[0].name == "test mech"
        assert restored.fallback_mechs[0].address == fallback_mech

    def test_batch_delivery_config_roundtrip(self):
        cfg = MicromechConfig(
            batch_delivery_enabled=True,
            delivery_batch_size=25,
            delivery_flush_timeout_seconds=15,
        )
        restored = MicromechConfig.model_validate(cfg.model_dump(mode="json"))

        assert restored.batch_delivery_enabled is True
        assert restored.delivery_batch_size == 25
        assert restored.delivery_flush_timeout_seconds == 15

    @pytest.mark.parametrize(
        "field,value",
        [
            ("delivery_batch_size", 0),
            ("delivery_batch_size", 101),
            ("delivery_flush_timeout_seconds", -1),
            ("delivery_flush_timeout_seconds", 301),
        ],
    )
    def test_batch_delivery_config_bounds(self, field, value):
        with pytest.raises(ValidationError):
            MicromechConfig(**{field: value})

    def test_release_version_consistency(self):
        import micromech

        pyproject = Path(__file__).parents[2] / "pyproject.toml"
        version = tomllib.loads(pyproject.read_text())["project"]["version"]

        assert micromech.__version__ == version

    def test_legacy_fallback_mech_addresses_populate_named_mechs(self):
        fallback_mech = "0x" + "d" * 40
        cfg = MicromechConfig(fallback_mech_addresses=[fallback_mech, fallback_mech])

        assert cfg.fallback_mech_addresses == [fallback_mech]
        assert cfg.fallback_mechs[0].name == fallback_mech
        assert cfg.fallback_mechs[0].address == fallback_mech

    def test_explicit_empty_named_mechs_dedupes_legacy_addresses(self):
        fallback_mech = "0x" + "d" * 40
        cfg = MicromechConfig(
            fallback_mechs=[],
            fallback_mech_addresses=[fallback_mech, fallback_mech],
        )

        assert cfg.fallback_mech_addresses == [fallback_mech]
        assert [mech.address for mech in cfg.fallback_mechs] == [fallback_mech]

    def test_named_fallback_mechs_populate_legacy_addresses(self):
        fallback_mech = "0x" + "d" * 40
        cfg = MicromechConfig(fallback_mechs=[{"name": "olas priority", "address": fallback_mech}])

        assert cfg.fallback_mech_addresses == [fallback_mech]
        assert cfg.fallback_mech_name(fallback_mech) == "olas priority"

    def test_named_and_legacy_fallback_mechs_merge(self):
        named_mech = "0x" + "d" * 40
        legacy_mech = "0x" + "e" * 40
        cfg = MicromechConfig(
            fallback_mechs=[{"name": "named", "address": named_mech}],
            fallback_mech_addresses=[named_mech, legacy_mech, legacy_mech],
        )

        assert cfg.fallback_mech_addresses == [named_mech, legacy_mech]
        assert [mech.name for mech in cfg.fallback_mechs] == ["named", legacy_mech]

    def test_invalid_fallback_mech_address(self):
        with pytest.raises(ValidationError):
            MicromechConfig(fallback_mech_addresses=["not_an_address"])

    def test_invalid_named_fallback_mech_address(self):
        with pytest.raises(ValidationError):
            MicromechConfig(fallback_mechs=[{"name": "bad", "address": "not_an_address"}])

    def test_invalid_named_fallback_mech_name(self):
        fallback_mech = "0x" + "d" * 40
        with pytest.raises(ValidationError):
            MicromechConfig(fallback_mechs=[{"name": "bad\nname", "address": fallback_mech}])
        with pytest.raises(ValidationError):
            MicromechConfig(fallback_mechs=[{"name": "bad\u202ename", "address": fallback_mech}])

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
        MicromechConfig.model_validate(data)

    def test_tasks_fields_flat(self):
        cfg = MicromechConfig(
            checkpoint_interval_minutes=30,
            claim_threshold_eur=5.0,
            fund_enabled=False,
            auto_update_enabled=True,
        )
        assert cfg.checkpoint_interval_minutes == 30
        assert cfg.claim_threshold_eur == 5.0
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
