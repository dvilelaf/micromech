"""Shared fixtures for integration tests."""

import pytest


@pytest.fixture(autouse=True)
def _reset_ipfs_session():
    """Reset iwa's global aiohttp session before each integration test.

    iwa.core.ipfs caches an aiohttp.ClientSession at module level.
    If a previous test closed the event loop, this cached session becomes
    orphaned and poisons all subsequent IPFS calls with 'Event loop is closed'.
    """
    try:
        import iwa.core.ipfs as ipfs_mod
        ipfs_mod._ASYNC_SESSION = None
    except (ImportError, AttributeError):
        pass
    yield
    try:
        import iwa.core.ipfs as ipfs_mod
        ipfs_mod._ASYNC_SESSION = None
    except (ImportError, AttributeError):
        pass
