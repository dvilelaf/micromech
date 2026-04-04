"""Shared test fixtures."""

from pathlib import Path
from unittest.mock import patch

import pytest

from micromech.core.config import MicromechConfig
from micromech.core.persistence import PersistentQueue


@pytest.fixture(autouse=True)
def _protect_real_wallet(tmp_path: Path):
    """CRITICAL: Prevent ALL tests from touching the real wallet.

    Patches iwa's WALLET_PATH to a temp directory so KeyStorage never
    reads/writes data/wallet.json. This is autouse — applies to every test.
    """
    fake_wallet = str(tmp_path / "wallet.json")
    with patch.dict("sys.modules", {}):  # don't interfere with cached modules
        pass
    # Patch at the source (iwa.core.constants) and anywhere it's imported
    with (
        patch("iwa.core.constants.WALLET_PATH", fake_wallet),
    ):
        yield


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for test files."""
    return tmp_path


@pytest.fixture
def config(tmp_dir: Path) -> MicromechConfig:
    """Provide a test config with tmp paths."""
    return MicromechConfig(
        persistence={"db_path": tmp_dir / "test.db"},
        llm={"models_dir": tmp_dir / "models"},
    )


@pytest.fixture
def queue(tmp_dir: Path) -> PersistentQueue:
    """Provide a fresh PersistentQueue."""
    q = PersistentQueue(tmp_dir / "test.db")
    yield q
    q.close()
