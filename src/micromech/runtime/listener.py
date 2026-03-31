"""On-chain event listener for MechRequest events.

Polls the MechMarketplace contract for Request events and converts
them to MechRequest objects for processing. Resolves IPFS CIDs to
get the actual request payload (prompt, tool).
"""

import asyncio
import json
from typing import Any, Optional

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.models import MechRequest


class EventListener:
    """Polls on-chain MechRequest events from the marketplace contract.

    Requires iwa bridge (or raw web3) for chain access.
    Falls back to no-op if no bridge is available.
    """

    def __init__(
        self,
        config: MicromechConfig,
        chain_config: ChainConfig,
        bridge: Optional[Any] = None,
    ):
        self.config = config
        self.chain_config = chain_config
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
                address=web3.to_checksum_address(self.chain_config.marketplace_address),
                abi=abi,
            )
        return self._marketplace_contract

    async def poll_once(self) -> list[MechRequest]:
        """Poll for new requests since last block.

        Fetches events from chain, resolves IPFS CIDs if needed.
        Does NOT advance _last_block — caller must call advance_block().
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

            # Fetch raw events from chain (sync, in thread)
            raw_requests = await asyncio.to_thread(self._fetch_events, from_block, to_block)

            # Resolve IPFS CIDs for any requests that need it (async)
            resolved = []
            for req in raw_requests:
                resolved_req = await self._resolve_request(req)
                resolved.append(resolved_req)

            self._polled_to_block = to_block

            if resolved:
                logger.info(
                    "Found {} new requests (blocks {}-{})",
                    len(resolved),
                    from_block,
                    to_block,
                )
            return resolved

        except Exception as e:
            logger.error("Event polling failed: {}", e)
            return []

    async def _resolve_request(self, req: MechRequest) -> MechRequest:
        """Resolve IPFS CID in request data if needed."""
        from micromech.ipfs.client import (
            fetch_json_from_ipfs,
            is_ipfs_multihash,
            multihash_to_cid,
        )

        if not req.data or req.prompt:
            return req  # Already has prompt (raw JSON) or no data

        if is_ipfs_multihash(req.data) and self.config.ipfs.enabled:
            try:
                cid = multihash_to_cid(req.data)
                payload = await fetch_json_from_ipfs(cid, gateway=self.config.ipfs.gateway)
                return MechRequest(
                    request_id=req.request_id,
                    chain=req.chain,
                    data=req.data,
                    prompt=payload.get("prompt", ""),
                    tool=payload.get("tool", ""),
                    extra_params={k: v for k, v in payload.items() if k not in ("prompt", "tool")},
                )
            except Exception as e:
                logger.warning("Failed to resolve IPFS for {}: {}", req.request_id, e)

        return req

    def advance_block(self) -> None:
        """Advance _last_block to the last polled block."""
        if self._polled_to_block is not None:
            self._last_block = self._polled_to_block
            self._polled_to_block = None

    def _fetch_events(self, from_block: int, to_block: int) -> list[MechRequest]:
        """Fetch Request events from marketplace contract (sync, runs in thread)."""
        mech_addr = self.chain_config.mech_address

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

        At this stage, request data may be raw JSON or IPFS multihash bytes.
        IPFS resolution happens later in _resolve_request().
        """
        args = event.get("args", {})
        priority_mech = str(args.get("priorityMech", ""))

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

            # Try raw JSON first (off-chain/test path)
            prompt, tool, extra = self._parse_request_data(data)

            results.append(
                MechRequest(
                    request_id=rid_hex,
                    chain=self.chain_config.chain,
                    data=data,
                    prompt=prompt,
                    tool=tool,
                    extra_params=extra,
                )
            )

        return results

    @staticmethod
    def _parse_request_data(data: bytes) -> tuple[str, str, dict]:
        """Try to parse raw bytes as JSON. Returns empty strings if not JSON."""
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
        """Run the event listener loop."""
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
