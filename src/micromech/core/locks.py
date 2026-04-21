"""Per-Safe asyncio.Lock registry — shared across all Safe TX consumers.

All consumers that submit Safe TXs (DeliveryManager, payment_withdraw_task, etc.)
must acquire the per-Safe lock before submitting to prevent nonce collisions.
"""

import asyncio

_SAFE_LOCKS: dict[str, asyncio.Lock] = {}


def get_safe_lock(safe_addr: str) -> asyncio.Lock:
    """Return (or lazily create) the per-Safe asyncio.Lock for this process.

    Shared between DeliveryManager and payment_withdraw_task to prevent
    concurrent Safe TXs from different consumers colliding on nonce.
    Thread-safe: asyncio is single-threaded; no await in this function so
    two coroutines cannot interleave between the dict lookup and the insert.
    """
    if safe_addr not in _SAFE_LOCKS:
        _SAFE_LOCKS[safe_addr] = asyncio.Lock()
    return _SAFE_LOCKS[safe_addr]
