"""Shared test fixtures."""

from pathlib import Path
from unittest.mock import patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS
from micromech.core.persistence import PersistentQueue


def make_test_config(**kwargs) -> MicromechConfig:
    """Create a MicromechConfig with gnosis chain for testing."""
    gnosis = CHAIN_DEFAULTS["gnosis"]
    defaults = {
        "chains": {
            "gnosis": ChainConfig(
                chain="gnosis",
                marketplace_address=gnosis["marketplace"],
                factory_address=gnosis["factory"],
                staking_address=gnosis["staking"],
            )
        }
    }
    defaults.update(kwargs)
    return MicromechConfig(**defaults)


@pytest.fixture(autouse=True)
def _protect_real_data(tmp_path: Path):
    """CRITICAL: Prevent ALL tests from touching real wallet or config.

    Patches iwa's WALLET_PATH and CONFIG_PATH to temp directory so tests
    never read/write data/wallet.json or data/config.yaml.
    """
    fake_wallet = str(tmp_path / "wallet.json")
    fake_config = tmp_path / "config.yaml"
    with (
        patch("iwa.core.constants.WALLET_PATH", fake_wallet),
        patch("iwa.core.constants.CONFIG_PATH", fake_config),
    ):
        yield


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for test files."""
    return tmp_path


@pytest.fixture
def config(tmp_dir: Path) -> MicromechConfig:
    """Provide a test config."""
    return make_test_config()


@pytest.fixture
def queue(tmp_dir: Path) -> PersistentQueue:
    """Provide a fresh PersistentQueue."""
    q = PersistentQueue(tmp_dir / "test.db")
    yield q
    q.close()
