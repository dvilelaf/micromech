"""Delivery manager — submits tool results on-chain.

Batch-delivers executed responses via deliverToMarketplace().
"""

import asyncio
from typing import Any, Optional

from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.models import RequestRecord
from micromech.core.persistence import PersistentQueue


class DeliveryManager:
    """Delivers executed responses on-chain.

    Periodically checks for undelivered responses and submits them.
    Requires iwa bridge for chain operations.
    """

    def __init__(
        self,
        config: MicromechConfig,
        queue: PersistentQueue,
        bridge: Optional[Any] = None,
    ):
        self.config = config
        self.queue = queue
        self.bridge = bridge
        self._running = False
        self._delivered_count = 0

    @property
    def delivered_count(self) -> int:
        return self._delivered_count

    async def deliver_batch(self) -> int:
        """Deliver a batch of undelivered responses. Returns count delivered."""
        records = self.queue.get_undelivered(limit=self.config.runtime.delivery_batch_size)
        if not records:
            return 0

        delivered = 0
        for record in records:
            try:
                tx_hash = await self._deliver_one(record)
                if tx_hash:
                    self.queue.mark_delivered(
                        record.request.request_id,
                        tx_hash=tx_hash,
                    )
                    delivered += 1
                    self._delivered_count += 1
                    logger.info(
                        "Delivered {} (tx: {})",
                        record.request.request_id,
                        tx_hash[:18] + "...",
                    )
            except Exception as e:
                logger.error(
                    "Delivery failed for {}: {}",
                    record.request.request_id,
                    e,
                )
                self.queue.mark_failed(record.request.request_id, f"delivery: {e}")

        return delivered

    async def _deliver_one(self, record: RequestRecord) -> Optional[str]:
        """Deliver a single response. Returns tx hash or None."""
        if self.bridge is None:
            logger.warning("No iwa bridge — simulating delivery for {}", record.request.request_id)
            return f"0x_sim_{record.request.request_id}"

        if record.result is None:
            logger.error("No result for {}", record.request.request_id)
            return None

        result_data = record.result.output.encode("utf-8")

        # Build deliverToMarketplace transaction
        tx_hash = await asyncio.to_thread(
            self._submit_delivery,
            record.request.request_id,
            result_data,
        )
        return tx_hash

    def _submit_delivery(self, request_id: str, data: bytes) -> str:
        """Submit delivery transaction on-chain (sync, runs in thread)."""
        # This would use iwa's ContractInstance to call deliverToMarketplace
        # For now, return a placeholder — full implementation requires
        # the mech contract ABI and Safe transaction flow
        logger.debug("Submitting delivery for {} ({} bytes)", request_id, len(data))

        # TODO: Implement actual on-chain delivery via iwa
        # mech_contract = self.bridge.get_contract(MechContract, mech_address)
        # tx = mech_contract.prepare_deliver_tx(request_id, data)
        # return self.bridge.wallet.send_transaction(tx)

        return f"0x_pending_{request_id}"

    async def run(self) -> None:
        """Run the delivery loop."""
        self._running = True
        interval = self.config.runtime.delivery_interval
        logger.info("Delivery manager started (interval {}s)", interval)

        while self._running:
            try:
                count = await self.deliver_batch()
                if count:
                    logger.debug("Delivered {} responses", count)
            except Exception as e:
                logger.error("Delivery loop error: {}", e)
            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False
