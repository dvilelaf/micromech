"""On-chain event listener for MechRequest events.

Polls the MechMarketplace contract for Request events and converts
them to MechRequest objects for processing.
"""

import asyncio
import json
from typing import Any, Optional

from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.models import MechRequest


class EventListener:
    """Polls on-chain MechRequest events from the marketplace contract.

    Requires iwa bridge (or raw web3) for chain access.
    Falls back to no-op if no bridge is available.
    """

    def __init__(self, config: MicromechConfig, bridge: Optional[Any] = None):
        self.config = config
        self.bridge = bridge
        self._last_block: Optional[int] = None
        self._polled_to_block: Optional[int] = None
        self._running = False
        self._marketplace_contract: Optional[Any] = None

    def _get_marketplace_contract(self) -> Any:
        """Lazy-load the marketplace contract for event filtering."""
        if self._marketplace_contract is None:
            web3 = self.bridge.web3
            from micromech.runtime.contracts import load_marketplace_abi

            abi = load_marketplace_abi()
            self._marketplace_contract = web3.eth.contract(
                address=web3.to_checksum_address(self.config.mech.marketplace_address),
                abi=abi,
            )
        return self._marketplace_contract

    async def poll_once(self) -> list[MechRequest]:
        """Poll for new requests since last block. Returns new requests.

        Does NOT advance _last_block — caller must call advance_block()
        after successfully processing all returned requests.
        """
        if self.bridge is None:
            return []

        try:
            web3 = self.bridge.web3
            current_block = self.bridge.with_retry(lambda: web3.eth.block_number)

            if self._last_block is None:
                self._last_block = max(0, current_block - self.config.runtime.event_lookback_blocks)

            if current_block <= self._last_block:
                return []

            from_block = self._last_block + 1
            to_block = current_block

            requests = await asyncio.to_thread(self._fetch_events, from_block, to_block)

            self._polled_to_block = to_block

            if requests:
                logger.info(
                    "Found {} new requests (blocks {}-{})",
                    len(requests),
                    from_block,
                    to_block,
                )
            return requests

        except Exception as e:
            logger.error("Event polling failed: {}", e)
            return []

    def advance_block(self) -> None:
        """Advance _last_block to the last polled block.

        Call this ONLY after all polled requests have been successfully processed.
        """
        if self._polled_to_block is not None:
            self._last_block = self._polled_to_block
            self._polled_to_block = None

    def _fetch_events(self, from_block: int, to_block: int) -> list[MechRequest]:
        """Fetch Request events from marketplace contract (sync, runs in thread)."""
        mech_addr = self.config.mech.mech_address

        try:
            contract = self._get_marketplace_contract()
            event_filter = contract.events.MarketplaceRequest.create_filter(
                from_block=from_block,
                to_block=to_block,
            )
            logs = self.bridge.with_retry(lambda: event_filter.get_all_entries())
        except Exception as e:
            logger.error("Failed to fetch events: {}", e)
            return []

        requests = []
        for log in logs:
            try:
                parsed = self._parse_marketplace_event(log, mech_addr)
                requests.extend(parsed)
            except Exception as e:
                logger.warning("Failed to parse event: {}", e)

        return requests

    def _parse_marketplace_event(self, event: Any, mech_addr: Optional[str]) -> list[MechRequest]:
        """Parse a MarketplaceRequest event into MechRequest(s).

        MarketplaceRequest has arrays: requestIds[] and requestDatas[].
        priorityMech is the indexed mech address.
        """
        args = event.get("args", {})
        priority_mech = str(args.get("priorityMech", ""))

        # Filter: only process requests for our mech
        if mech_addr and priority_mech.lower() != mech_addr.lower():
            return []

        request_ids = args.get("requestIds", [])
        request_datas = args.get("requestDatas", [])

        results = []
        for i, rid in enumerate(request_ids):
            if isinstance(rid, bytes):
                rid_hex = rid.hex()
            else:
                rid_hex = str(rid)

            data = request_datas[i] if i < len(request_datas) else b""
            if isinstance(data, str):
                data = data.encode()

            prompt, tool, extra = self._parse_request_data(data)
            results.append(
                MechRequest(
                    request_id=rid_hex,
                    data=data,
                    prompt=prompt,
                    tool=tool,
                    extra_params=extra,
                )
            )

        return results

    @staticmethod
    def _parse_request_data(data: bytes) -> tuple[str, str, dict]:
        """Extract prompt, tool, and params from request data.

        Request data is typically a JSON payload:
        {"prompt": "...", "tool": "...", "nonce": ...}
        """
        prompt = ""
        tool = ""
        extra: dict[str, Any] = {}

        if not data:
            return prompt, tool, extra

        try:
            text = data.decode("utf-8", errors="ignore").strip()
            if text.startswith("{"):
                payload = json.loads(text)
                prompt = payload.get("prompt", "")
                tool = payload.get("tool", "")
                extra = {k: v for k, v in payload.items() if k not in ("prompt", "tool")}
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        return prompt, tool, extra

    async def run(self, callback: Any) -> None:
        """Run the event listener loop.

        Calls callback(request) for each new request.
        Only advances _last_block after ALL callbacks succeed for a batch.
        """
        self._running = True
        interval = self.config.runtime.event_poll_interval
        logger.info("Event listener started (poll every {}s)", interval)

        while self._running:
            requests = await self.poll_once()
            all_ok = True
            for req in requests:
                try:
                    await callback(req)
                except Exception as e:
                    logger.error("Callback failed for {}: {}", req.request_id, e)
                    all_ok = False

            if all_ok:
                self.advance_block()

            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False
