"""Delivery manager — submits tool results on-chain.

Calls deliverToMarketplace(requestIds[], datas[]) on the mech contract
via the mech service's Safe multisig.
"""

import asyncio
import json
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
        """Deliver a single response. Returns tx hash or None.

        Pushes the result to IPFS first (if enabled), then delivers the
        IPFS multihash bytes on-chain via deliverToMarketplace.
        """
        if record.result is None:
            logger.error("No result for {}", record.request.request_id)
            return None

        request_id = record.request.request_id

        # Build response payload
        response_payload = json.dumps(
            {
                "requestId": request_id,
                "result": record.result.output,
                "prompt": record.request.prompt,
                "tool": record.request.tool,
            },
            separators=(",", ":"),
        ).encode("utf-8")

        # Push to IPFS and get multihash bytes for on-chain delivery
        if self.config.ipfs.enabled:
            try:
                from micromech.ipfs.client import cid_hex_to_multihash_bytes, push_to_ipfs

                _, cid_hex = await push_to_ipfs(response_payload, api_url=self.config.ipfs.api_url)
                delivery_data = cid_hex_to_multihash_bytes(cid_hex)
            except Exception as e:
                logger.warning("IPFS push failed, delivering raw data: {}", e)
                delivery_data = response_payload
        else:
            delivery_data = response_payload

        tx_hash = await asyncio.to_thread(self._submit_delivery, request_id, delivery_data)
        return tx_hash

    def _submit_delivery(self, request_id: str, data: bytes) -> str:
        """Submit deliverToMarketplace transaction on-chain (sync, runs in thread).

        Calls mech.deliverToMarketplace([requestId], [data]).
        Tries impersonation first (Anvil auto-impersonate mode), then falls
        back to signed transaction via iwa wallet for real deployments.
        """
        mech_contract = self._get_mech_contract()
        from_addr = self.bridge.web3.to_checksum_address(self.config.mech.mech_address)

        # Convert request_id to bytes32
        if request_id.startswith("0x"):
            req_id_bytes = bytes.fromhex(request_id[2:])
        else:
            req_id_bytes = bytes.fromhex(request_id)
        req_id_bytes = req_id_bytes.ljust(32, b"\x00")[:32]

        # Try impersonation first (works on Anvil with --auto-impersonate)
        try:
            tx_hash = self._submit_impersonated(mech_contract, from_addr, req_id_bytes, data)
            return tx_hash
        except Exception as imp_err:
            logger.debug("Impersonation failed ({}), trying signed tx", imp_err)

        # Fall back to signed transaction via iwa wallet
        return self._submit_signed(mech_contract, from_addr, req_id_bytes, data)

    def _submit_impersonated(
        self, contract: Any, from_addr: str, req_id_bytes: bytes, data: bytes
    ) -> str:
        """Submit via impersonation (Anvil). Transact directly from the address."""
        tx_hash = contract.functions.deliverToMarketplace(
            [req_id_bytes],
            [data],
        ).transact(
            {
                "from": from_addr,
                "gas": 500_000,
            }
        )
        receipt = self.bridge.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt["status"] != 1:
            msg = f"Delivery transaction reverted: {tx_hash.hex()}"
            raise RuntimeError(msg)
        return tx_hash.hex()

    def _submit_signed(
        self, contract: Any, from_addr: str, req_id_bytes: bytes, data: bytes
    ) -> str:
        """Submit via signed transaction using iwa wallet key."""
        tx = contract.functions.deliverToMarketplace(
            [req_id_bytes],
            [data],
        ).build_transaction(
            {
                "from": from_addr,
                "gas": 500_000,
                "gasPrice": self.bridge.web3.eth.gas_price,
                "nonce": self.bridge.web3.eth.get_transaction_count(from_addr),
            }
        )

        private_key = self._get_signer_key()
        signed = self.bridge.web3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = self.bridge.web3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.bridge.web3.eth.wait_for_transaction_receipt(tx_hash)

        if receipt["status"] != 1:
            msg = f"Delivery transaction reverted: {tx_hash.hex()}"
            raise RuntimeError(msg)
        return tx_hash.hex()

    def _get_signer_key(self) -> str:
        """Get the private key for signing delivery transactions."""
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
