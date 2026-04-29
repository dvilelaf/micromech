"""Low-RPC mech queue scanner.

The normal event listener is the fast path for our own requests. This scanner is
the reconciliation path: it reads mech-local undelivered queues, validates each
candidate, resolves the original MarketplaceRequest payload only for candidates
that may be actionable, and hands complete MechRequest objects back to the
server.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import (
    REQUEST_STATUS_REQUESTED_EXPIRED,
    REQUEST_STATUS_REQUESTED_PRIORITY,
    STATUS_PENDING,
)
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.ipfs.client import (
    fetch_json_from_ipfs,
    multihash_to_cid,
    normalize_to_multihash,
)
from micromech.runtime.contracts import load_marketplace_abi, load_mech_abi
from micromech.runtime.listener import EventListener
from micromech.tools.registry import ToolRegistry

ScanMode = Literal["own", "fallback"]
EnqueueFn = Callable[[MechRequest], Awaitable[None]]


@dataclass(frozen=True)
class QueueCandidate:
    """A request ID discovered in a priority mech queue."""

    request_id: bytes
    priority_mech: str
    mode: ScanMode

    @property
    def request_id_hex(self) -> str:
        return self.request_id.hex()


class MechQueueScanner:
    """Scan mech-local undelivered queues without broad marketplace log reads."""

    def __init__(
        self,
        *,
        config: MicromechConfig,
        chain_config: ChainConfig,
        bridge: Any,
        queue: PersistentQueue,
        registry: ToolRegistry,
        queued_ids: set[str],
        enqueue: EnqueueFn,
    ) -> None:
        self.config = config
        self.chain_config = chain_config
        self.bridge = bridge
        self.queue = queue
        self.registry = registry
        self.queued_ids = queued_ids
        self.enqueue = enqueue
        self._marketplace: Any | None = None
        self._mech_contracts: dict[str, Any] = {}
        self._our_payment_type: bytes | None = None
        self._event_cache: dict[tuple[str, int, int], dict[str, dict]] = {}
        self._event_lookup_bounds_for_scan: tuple[int, int] | None = None

    async def scan_once(self) -> None:
        """Scan our own queue and, when enabled, configured fallback queues."""
        if not self.bridge or not self.chain_config.mech_address:
            return

        self._event_cache = {}
        self._event_lookup_bounds_for_scan = None
        await self._scan_mech(self.chain_config.mech_address, mode="own")

        if not self.config.fallback_mode_enabled:
            return

        for mech_addr in self.config.fallback_mech_addresses:
            if mech_addr.lower() == self.chain_config.mech_address.lower():
                continue
            await self._scan_mech(mech_addr, mode="fallback")

    async def run(self) -> None:
        """Run scanner forever until cancelled by MechServer."""
        interval = self.config.queue_scanner_interval_seconds
        logger.info(
            "Mech queue scanner started for {} (interval={}s, fallback_mechs={})",
            self.chain_config.chain,
            interval,
            len(self.config.fallback_mech_addresses)
            if self.config.fallback_mode_enabled
            else 0,
        )
        while True:
            try:
                await self.scan_once()
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Mech queue scanner failed on {}: {}",
                    self.chain_config.chain,
                    e,
                )
            await asyncio.sleep(interval)

    async def _scan_mech(self, mech_addr: str, *, mode: ScanMode) -> None:
        mech_addr = self.bridge.web3.to_checksum_address(mech_addr)
        count = await asyncio.to_thread(self._undelivered_count, mech_addr)
        if count <= 0:
            return

        page_size = self.config.queue_scanner_page_size
        offsets = self._scan_offsets(mech_addr, mode=mode, count=count)
        logger.debug(
            "Queue scanner {} {} candidate(s) for {} (pages={})",
            mode,
            count,
            mech_addr,
            len(offsets),
        )
        next_offset = 0
        for offset in offsets:
            size = min(page_size, max(count - offset, 0))
            ids = await asyncio.to_thread(
                self._undelivered_ids,
                mech_addr,
                size,
                offset,
            )
            next_offset = offset + size
            if mode == "fallback":
                self._save_fallback_cursor(
                    mech_addr,
                    next_offset=next_offset,
                    count=count,
                )
            for request_id in ids:
                candidate = QueueCandidate(
                    request_id=bytes(request_id),
                    priority_mech=mech_addr,
                    mode=mode,
                )
                await self._handle_candidate(candidate)
        if mode == "fallback" and offsets:
            self._save_fallback_cursor(mech_addr, next_offset=next_offset, count=count)

    def _save_fallback_cursor(self, mech_addr: str, *, next_offset: int, count: int) -> None:
        if next_offset >= count:
            next_offset = 0
        self.queue.set_queue_scanner_cursor(
            chain=self.chain_config.chain,
            mech_address=mech_addr,
            mode="fallback",
            next_offset=next_offset,
            last_count=count,
        )

    def _scan_offsets(self, mech_addr: str, *, mode: ScanMode, count: int) -> list[int]:
        page_size = self.config.queue_scanner_page_size
        if mode == "own":
            return list(range(0, count, page_size))

        start = self.queue.get_queue_scanner_cursor(
            chain=self.chain_config.chain,
            mech_address=mech_addr,
            mode=mode,
        )
        if start >= count:
            start = 0

        max_pages = self.config.queue_scanner_fallback_pages_per_cycle
        offsets: list[int] = []
        offset = start
        while len(offsets) < max_pages and offset < count:
            offsets.append(offset)
            offset += page_size
        return offsets

    async def _handle_candidate(self, candidate: QueueCandidate) -> None:
        req_id = candidate.request_id_hex
        if req_id in self.queued_ids:
            return
        existing = self.queue.get_by_id(req_id)
        if existing and existing.request.status != STATUS_PENDING:
            return

        status = await asyncio.to_thread(self._request_status, candidate.request_id)
        if candidate.mode == "fallback":
            if status != REQUEST_STATUS_REQUESTED_EXPIRED:
                return
            if not await asyncio.to_thread(self._payment_is_compatible, candidate.request_id):
                return
        elif status not in (
            REQUEST_STATUS_REQUESTED_PRIORITY,
            REQUEST_STATUS_REQUESTED_EXPIRED,
        ):
            return

        request = await self._resolve_request_payload(candidate)
        if request is None:
            logger.debug("Queue scanner could not resolve {}", req_id[:16])
            return

        if not request.tool or not self.registry.has(request.tool):
            logger.debug(
                "Queue scanner skipping {} {}: unavailable tool '{}'",
                candidate.mode,
                req_id[:16],
                request.tool,
            )
            return

        if req_id in self.queued_ids:
            return
        existing = self.queue.get_by_id(req_id)
        if existing and existing.request.status != STATUS_PENDING:
            return

        await self.enqueue(request)

    def _marketplace_contract(self) -> Any:
        if self._marketplace is None:
            web3 = self.bridge.web3
            self._marketplace = web3.eth.contract(
                address=web3.to_checksum_address(self.chain_config.marketplace_address),
                abi=load_marketplace_abi(),
            )
        return self._marketplace

    def _mech_contract(self, mech_addr: str) -> Any:
        key = mech_addr.lower()
        if key not in self._mech_contracts:
            web3 = self.bridge.web3
            self._mech_contracts[key] = web3.eth.contract(
                address=web3.to_checksum_address(mech_addr),
                abi=load_mech_abi(),
            )
        return self._mech_contracts[key]

    def _undelivered_count(self, mech_addr: str) -> int:
        mech = self._mech_contract(mech_addr)
        return int(
            self.bridge.with_retry(
                lambda: mech.functions.numUndeliveredRequests().call()
            )
        )

    def _undelivered_ids(self, mech_addr: str, size: int, offset: int) -> list[bytes]:
        mech = self._mech_contract(mech_addr)
        return list(
            self.bridge.with_retry(
                lambda: mech.functions.getUndeliveredRequestIds(size, offset).call()
            )
        )

    def _request_status(self, request_id: bytes) -> int:
        marketplace = self._marketplace_contract()
        return int(
            self.bridge.with_retry(
                lambda: marketplace.functions.getRequestStatus(request_id).call()
            )
        )

    def _payment_is_compatible(self, request_id: bytes) -> bool:
        """Filter fallback candidates before tool execution.

        If the marketplace info call fails we fail closed: executing a tool only
        to have delivery rejected is exactly the resource burn this scanner is
        meant to avoid.
        """
        try:
            marketplace = self._marketplace_contract()
            info = self.bridge.with_retry(
                lambda: marketplace.functions.mapRequestIdInfos(request_id).call()
            )
            delivery_rate = int(info[4])
            payment_type = bytes(info[5])
            if delivery_rate < int(self.chain_config.delivery_rate):
                return False
            return payment_type == self._our_mech_payment_type()
        except Exception as e:  # noqa: BLE001
            logger.debug("Payment compatibility check failed: {}", e)
            return False

    def _our_mech_payment_type(self) -> bytes:
        if self._our_payment_type is None:
            mech = self._mech_contract(str(self.chain_config.mech_address))
            self._our_payment_type = bytes(
                self.bridge.with_retry(lambda: mech.functions.paymentType().call())
            )
        return self._our_payment_type

    async def _resolve_request_payload(
        self,
        candidate: QueueCandidate,
    ) -> MechRequest | None:
        """Find and decode the original MarketplaceRequest payload.

        Request IDs are not indexed in the marketplace event, so this performs a
        bounded, priority-mech-filtered log lookup only after the queue/status
        filters have already made the ID interesting.
        """
        event = await asyncio.to_thread(self._find_request_event, candidate)
        if event is None:
            return None

        parser = EventListener(self.config, self.chain_config, self.bridge)
        parsed = parser._parse_marketplace_event(event, candidate.priority_mech)
        for request in parsed:
            if request.request_id == candidate.request_id_hex:
                return await self._resolve_ipfs_request(request)
        return None

    def _find_request_event(self, candidate: QueueCandidate) -> dict | None:
        web3 = self.bridge.web3
        marketplace = self._marketplace_contract()
        from_block, to_block = self._get_event_lookup_bounds_for_scan()
        cache_key = (candidate.priority_mech.lower(), from_block, to_block)
        if cache_key not in self._event_cache:
            self._event_cache[cache_key] = {}
            for start in range(from_block, to_block + 1, 500):
                end = min(start + 499, to_block)
                logs = self.bridge.with_retry(
                    lambda _s=start, _e=end: marketplace.events.MarketplaceRequest.get_logs(
                        from_block=_s,
                        to_block=_e,
                        argument_filters={
                            "priorityMech": web3.to_checksum_address(candidate.priority_mech),
                        },
                    )
                )
                for event in logs:
                    ids = event.get("args", {}).get("requestIds", [])
                    for rid in ids:
                        self._event_cache[cache_key][bytes(rid).hex()] = event

        return self._event_cache[cache_key].get(candidate.request_id_hex)

    def _event_lookup_bounds(self, current_block: int) -> tuple[int, int]:
        # mapRequestIdInfos()[3] is responseTimeout on the deployed marketplace,
        # not the request block. Keep payload lookup bounded by configuration.
        lookback = self.config.queue_scanner_event_lookback_blocks
        return max(0, current_block - lookback), current_block

    def _get_event_lookup_bounds_for_scan(self) -> tuple[int, int]:
        if self._event_lookup_bounds_for_scan is None:
            current = self.bridge.with_retry(lambda: self.bridge.web3.eth.block_number)
            self._event_lookup_bounds_for_scan = self._event_lookup_bounds(int(current))
        return self._event_lookup_bounds_for_scan

    async def _resolve_ipfs_request(
        self,
        request: MechRequest,
    ) -> MechRequest:
        if request.prompt or not request.data:
            return request

        multihash = normalize_to_multihash(request.data)
        if multihash is None:
            return request
        try:
            payload = await fetch_json_from_ipfs(multihash_to_cid(multihash))
        except Exception as e:  # noqa: BLE001
            logger.debug("Queue scanner IPFS fetch failed for {}: {}", request.request_id[:16], e)
            return request.model_copy(update={"tool": "(decode_error)"})

        return request.model_copy(
            update={
                "prompt": payload.get("prompt", ""),
                "tool": payload.get("tool", ""),
                "extra_params": {
                    k: v
                    for k, v in payload.items()
                    if k not in ("prompt", "tool")
                },
            }
        )
