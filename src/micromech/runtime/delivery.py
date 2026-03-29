"""Delivery manager — submits tool results on-chain.

Calls deliverToMarketplace(requestIds[], datas[]) on the mech contract
via the mech service's Safe multisig.
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
    Requires iwa bridge for chain operations. Without a bridge,
    delivery is skipped (requests stay in EXECUTED state).
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
        self._mech_contract: Optional[Any] = None

    @property
    def delivered_count(self) -> int:
        return self._delivered_count

    def _get_mech_contract(self) -> Any:
        """Lazy-load the mech contract instance."""
        if self._mech_contract is None:
            mech_addr = self.config.mech.mech_address
            if not mech_addr:
                msg = "mech_address not configured"
                raise ValueError(msg)

            web3 = self.bridge.web3
            from micromech.runtime.contracts import load_mech_abi

            abi = load_mech_abi()
            self._mech_contract = web3.eth.contract(
                address=web3.to_checksum_address(mech_addr),
                abi=abi,
            )
        return self._mech_contract

    async def deliver_batch(self) -> int:
        """Deliver a batch of undelivered responses. Returns count delivered."""
        if self.bridge is None:
            return 0

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
        if record.result is None:
            logger.error("No result for {}", record.request.request_id)
            return None

        result_data = record.result.output.encode("utf-8")
        request_id = record.request.request_id

        tx_hash = await asyncio.to_thread(self._submit_delivery, request_id, result_data)
        return tx_hash

    def _submit_delivery(self, request_id: str, data: bytes) -> str:
        """Submit deliverToMarketplace transaction on-chain (sync, runs in thread).

        Calls mech.deliverToMarketplace([requestId], [data]) via direct transaction.
        In production with Safe, this would go through iwa's Safe service.
        """
        mech_contract = self._get_mech_contract()

        # Convert request_id to bytes32
        if request_id.startswith("0x"):
            req_id_bytes = bytes.fromhex(request_id[2:])
        else:
            req_id_bytes = bytes.fromhex(request_id)
        req_id_bytes = req_id_bytes.ljust(32, b"\x00")[:32]

        # Build transaction
        tx = mech_contract.functions.deliverToMarketplace(
            [req_id_bytes],
            [data],
        ).build_transaction(
            {
                "from": self.bridge.web3.to_checksum_address(self.config.mech.mech_address),
                "gas": 500_000,
                "gasPrice": self.bridge.web3.eth.gas_price,
                "nonce": self.bridge.web3.eth.get_transaction_count(
                    self.bridge.web3.to_checksum_address(self.config.mech.mech_address)
                ),
            }
        )

        # Send via iwa wallet or directly
        signed = self.bridge.web3.eth.account.sign_transaction(
            tx, private_key=self._get_signer_key()
        )
        tx_hash = self.bridge.web3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.bridge.web3.eth.wait_for_transaction_receipt(tx_hash)

        if receipt["status"] != 1:
            msg = f"Delivery transaction reverted: {tx_hash.hex()}"
            raise RuntimeError(msg)

        return tx_hash.hex()

    def _get_signer_key(self) -> str:
        """Get the private key for signing delivery transactions."""
        # Use iwa wallet to get the key for the mech account
        account_tag = self.config.mech.account_tag
        try:
            account = self.bridge.wallet.account_service.resolve_account(account_tag)
            return account.key.hex()
        except Exception:
            msg = f"Cannot resolve signer key for account tag '{account_tag}'"
            raise ValueError(msg)

    async def run(self) -> None:
        """Run the delivery loop."""
        self._running = True
        interval = self.config.runtime.delivery_interval

        if self.bridge is None:
            logger.info("Delivery manager: no bridge — delivery disabled")
        else:
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
