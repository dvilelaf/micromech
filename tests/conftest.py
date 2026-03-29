"""Shared test fixtures."""

from pathlib import Path

import pytest

from micromech.core.config import MicromechConfig
from micromech.core.persistence import PersistentQueue


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
