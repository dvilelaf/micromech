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

# MechMarketplace Request event signature
_REQUEST_EVENT_SIG = "Request(address,bytes32,bytes)"


class EventListener:
    """Polls on-chain MechRequest events from the marketplace contract.

    Requires iwa for chain access. Falls back to no-op if iwa is unavailable.
    """

    def __init__(self, config: MicromechConfig, bridge: Optional[Any] = None):
        self.config = config
        self.bridge = bridge
        self._last_block: Optional[int] = None
        self._polled_to_block: Optional[int] = None
        self._running = False
        self._request_topic: Optional[bytes] = None

    def _get_request_topic(self) -> bytes:
        """Cache keccak hash of event signature."""
        if self._request_topic is None:
            web3 = self.bridge.web3
            self._request_topic = web3.keccak(text=_REQUEST_EVENT_SIG)
        return self._request_topic

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

            # Store what we polled up to — caller advances after processing
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
        web3 = self.bridge.web3
        marketplace_addr = self.config.mech.marketplace_address
        mech_addr = self.config.mech.mech_address
        request_topic = self._get_request_topic()

        try:
            logs = self.bridge.with_retry(
                lambda: web3.eth.get_logs(
                    {
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "address": marketplace_addr,
                        "topics": ["0x" + request_topic.hex()],
                    }
                )
            )
        except Exception as e:
            logger.error("Failed to fetch logs: {}", e)
            return []

        requests = []
        for log in logs:
            try:
                req = self._parse_log(log, mech_addr)
                if req:
                    requests.append(req)
            except Exception as e:
                logger.warning("Failed to parse log: {}", e)

        return requests

    def _parse_log(self, log: Any, mech_addr: Optional[str]) -> Optional[MechRequest]:
        """Parse a raw log into a MechRequest."""
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None

        # topics[1] = indexed mech address (padded to 32 bytes)
        log_mech = "0x" + topics[1].hex()[-40:]

        # Filter: only process requests for our mech
        if mech_addr and log_mech.lower() != mech_addr.lower():
            return None

        # topics[2] = requestId (bytes32)
        request_id = topics[2].hex() if len(topics) > 2 else log["transactionHash"].hex()

        data = bytes(log.get("data", b""))
        prompt, tool, extra = self._parse_request_data(data)

        # Note: topics[1] is the mech, not the requester.
        # Requester address would need to be extracted from the transaction.
        return MechRequest(
            request_id=request_id,
            data=data,
            prompt=prompt,
            tool=tool,
            extra_params=extra,
        )

    @staticmethod
    def _parse_request_data(data: bytes) -> tuple[str, str, dict]:
        """Extract prompt, tool, and params from request data.

        Request data is typically an IPFS CID pointing to a JSON payload:
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

            # Only advance block pointer if all callbacks succeeded
            if all_ok:
                self.advance_block()

            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False
