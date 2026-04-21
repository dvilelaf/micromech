"""Per-Safe asyncio.Lock registry — shared across all Safe TX consumers.

Two coordination mechanisms work together to prevent Safe TX nonce collisions:

1. **get_safe_lock (this module)** — an asyncio.Lock that serialises *consumers*:
   DeliveryManager, payment_withdraw_task, and the web /api/management/withdraw
   endpoint each call `async with get_safe_lock(multisig)` before submitting any
   Safe TX.  This prevents two consumers from running concurrently for the same
   Safe regardless of whether the NonceAllocator is enabled.

2. **NonceAllocator (iwa.core.services.safe)** — pre-assigns sequential nonces to
   *parallel delivery workers* inside a single DeliveryManager tick.  Workers held
   under the same safe_lock acquire distinct nonces so concurrent Safe TXs don't
   collide on GS026.  The allocator is only active when parallel_nonce_enabled=True.

Without (1) alone: payment_withdraw could submit a Safe TX while delivery workers
are mid-flight → nonce collision / revert.
Without (2) alone: parallel delivery workers would all attempt the same pending
nonce → all but the first would get GS026.
"""
import asyncio

_SAFE_LOCKS: dict[str, asyncio.Lock] = {}


def get_safe_lock(safe_addr: str) -> asyncio.Lock:
    """Return (or lazily create) the per-Safe asyncio.Lock for this process.

    Thread-safe: asyncio is single-threaded; no await in this function so
    two coroutines cannot interleave between the dict lookup and the insert.
    """
    key = safe_addr.lower()
    if key not in _SAFE_LOCKS:
        _SAFE_LOCKS[key] = asyncio.Lock()
    return _SAFE_LOCKS[key]
