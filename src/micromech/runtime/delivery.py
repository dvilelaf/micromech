"""Delivery manager — submits tool results on-chain.

Calls deliverToMarketplace(requestIds[], datas[]) on the mech contract
via the mech service's Safe multisig.

Delivery transport strategy:
- Production (bridge has safe_service): _via_safe — Gnosis Safe execTransaction.
- Test/local (Anvil, bridge without safe_service): _via_impersonation — direct
  transact() via Anvil auto-impersonate. This path does NOT work on real nodes.
"""

import asyncio
import json
from typing import TYPE_CHECKING, Any, Optional

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import (
    DEFAULT_DELIVERY_BATCH_SIZE,
    DEFAULT_DELIVERY_INTERVAL,
    IPFS_API_URL,
)
from micromech.core.models import RequestRecord
from micromech.core.persistence import PersistentQueue

if TYPE_CHECKING:
    from micromech.runtime.metrics import MetricsCollector


TX_RECEIPT_TIMEOUT = 120  # seconds


def _wait_and_check_receipt(web3: Any, tx_hash: Any, error_prefix: str) -> str:
    """Wait for transaction receipt and return hex hash. Raises on revert."""
    receipt = web3.eth.wait_for_transaction_receipt(
        tx_hash,
        timeout=TX_RECEIPT_TIMEOUT,
    )
    if receipt["status"] != 1:
        msg = f"{error_prefix} transaction reverted: {tx_hash.hex()}"
        raise RuntimeError(msg)
    return tx_hash.hex()


class DeliveryManager:
    """Delivers executed responses on-chain.

    Periodically checks for undelivered responses and submits them.
    Requires iwa bridge for chain operations. Without a bridge,
    delivery is skipped (requests stay in EXECUTED state).
    """

    def __init__(
        self,
        config: MicromechConfig,
        chain_config: ChainConfig,
        queue: PersistentQueue,
        bridge: Optional[Any] = None,
        metrics: "MetricsCollector | None" = None,
    ):
        self.config = config
        self.chain_config = chain_config
        self.queue = queue
        self.bridge = bridge
        self._running = False
        self._delivered_count = 0
        self._mech_contract: Optional[Any] = None
        self._metrics = metrics
        self._wallet_warning_logged = False
        self._ipfs_warning_logged = False

    @property
    def delivered_count(self) -> int:
        return self._delivered_count

    @property
    def _has_safe(self) -> bool:
        """Check if bridge has Safe service available (production path)."""
        return (
            self.bridge is not None
            and hasattr(self.bridge, "wallet")
            and hasattr(self.bridge.wallet, "safe_service")
        )

    @property
    def _chain_name(self) -> str:
        """Chain name from this delivery manager's config."""
        return self.chain_config.chain

    def _get_mech_contract(self) -> Any:
        """Lazy-load the mech contract instance."""
        if self._mech_contract is None:
            mech_addr = self.chain_config.mech_address
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

        # Check if wallet is available for signing
        try:
            _ = self.bridge.wallet.key_storage
        except Exception:
            if not self._wallet_warning_logged:
                logger.warning(
                    "Delivery skipped: no wallet available. "
                    "Set wallet_password in secrets.env or use the web wizard."
                )
                self._wallet_warning_logged = True
            return 0

        records = self.queue.get_undelivered(
            limit=DEFAULT_DELIVERY_BATCH_SIZE, chain=self._chain_name
        )
        if not records:
            return 0

        delivered = 0
        for record in records:
            try:
                tx_hash, ipfs_hash = await self._deliver_one(record)
                if tx_hash:
                    self.queue.mark_delivered(
                        record.request.request_id,
                        tx_hash=tx_hash,
                        ipfs_hash=ipfs_hash,
                    )
                    delivered += 1
                    self._delivered_count += 1
                    if self._metrics:
                        self._metrics.record_delivery(
                            record.request.request_id, chain=self._chain_name
                        )
                    prompt_short = record.request.prompt[:60] if record.request.prompt else ""
                    logger.info(
                        "Delivered {} tool={} tx={} prompt={}",
                        record.request.request_id[:16] + "...",
                        record.request.tool,
                        tx_hash[:18] + "...",
                        prompt_short,
                    )
            except Exception as e:
                logger.error(
                    "Delivery failed for {}: {}",
                    record.request.request_id,
                    e,
                )
                if self._metrics:
                    self._metrics.record_delivery_failed(
                        record.request.request_id, str(e), chain=self._chain_name
                    )
                self.queue.mark_failed(record.request.request_id, f"delivery: {e}")

        return delivered

    async def _deliver_one(
        self,
        record: RequestRecord,
    ) -> tuple[Optional[str], Optional[str]]:
        """Deliver a single response.

        Returns (tx_hash, ipfs_cid_hex) or (None, None).

        Pushes the result to IPFS first (if enabled), then
        delivers on-chain:
        - On-chain: deliverToMarketplace(requestIds[], datas[])
        - Off-chain: deliverMarketplaceWithSignatures(...)
        """
        if record.result is None:
            logger.error(
                "No result for {}",
                record.request.request_id,
            )
            return None, None

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

        # Push to IPFS and get multihash bytes for delivery
        ipfs_cid_hex: Optional[str] = None
        try:
            from micromech.ipfs.client import (
                cid_hex_to_multihash_bytes,
                push_to_ipfs,
            )

            _, cid_hex = await push_to_ipfs(
                response_payload,
                api_url=IPFS_API_URL,
            )
            delivery_data = cid_hex_to_multihash_bytes(
                cid_hex,
            )
            ipfs_cid_hex = cid_hex
        except Exception as e:
            if not self._ipfs_warning_logged:
                logger.warning(
                    "IPFS unavailable, delivering raw: {}",
                    e,
                )
                self._ipfs_warning_logged = True
            delivery_data = response_payload

        if record.request.is_offchain:
            tx_hash = await asyncio.to_thread(
                self._submit_offchain_delivery,
                record,
                delivery_data,
            )
        else:
            tx_hash = await asyncio.to_thread(
                self._submit_delivery,
                request_id,
                delivery_data,
            )
        return tx_hash, ipfs_cid_hex

    def _submit_delivery(self, request_id: str, data: bytes) -> str:
        """Submit deliverToMarketplace transaction on-chain (sync, runs in thread).

        Calls mech.deliverToMarketplace([requestId], [data]).
        Tries Safe TX first (production), then impersonation (Anvil),
        then signed transaction as last resort.
        """
        mech_contract = self._get_mech_contract()
        # deliverToMarketplace must be called from the service multisig
        from micromech.core.bridge import get_service_info

        svc_info = get_service_info(self.chain_config.chain)
        multisig = svc_info.get("multisig_address")
        if not multisig:
            raise ValueError("multisig_address not configured — cannot deliver")
        from_addr = self.bridge.web3.to_checksum_address(multisig)

        # Convert request_id to bytes32
        try:
            hex_str = request_id[2:] if request_id.startswith("0x") else request_id
            req_id_bytes = bytes.fromhex(hex_str)
        except ValueError:
            # Non-hex IDs (e.g. "http-abc123") — hash to get deterministic bytes32
            import hashlib

            req_id_bytes = hashlib.sha256(request_id.encode()).digest()
        req_id_bytes = req_id_bytes.ljust(32, b"\x00")[:32]

        fn_call = mech_contract.functions.deliverToMarketplace(
            [req_id_bytes],
            [data],
        )
        return self._submit_tx(fn_call, from_addr, "Delivery")

    def _submit_offchain_delivery(self, record: RequestRecord, delivery_data: bytes) -> str:
        """Submit deliverMarketplaceWithSignatures for off-chain (HTTP) requests.

        Calls mech.deliverMarketplaceWithSignatures(requester, deliverWithSignatures[],
        deliveryRates[], paymentData).
        """
        mech_contract = self._get_mech_contract()
        from micromech.core.bridge import get_service_info

        svc_info = get_service_info(self.chain_config.chain)
        multisig = svc_info.get("multisig_address")
        if not multisig:
            raise ValueError("multisig_address not configured — cannot deliver")
        from_addr = self.bridge.web3.to_checksum_address(multisig)

        # requester: the sender from the HTTP request, or our own address
        requester_addr = record.request.sender or from_addr
        requester = self.bridge.web3.to_checksum_address(requester_addr)

        # Build the original request data
        request_data = record.request.data or json.dumps(
            {"prompt": record.request.prompt, "tool": record.request.tool},
            separators=(",", ":"),
        ).encode("utf-8")

        # Signature: from the HTTP request, or empty bytes
        sig_hex = record.request.signature or ""
        signature = bytes.fromhex(sig_hex.removeprefix("0x")) if sig_hex else b""

        # deliverWithSignatures is an array of tuples
        deliver_with_sigs = [(request_data, signature, delivery_data)]

        delivery_rates = [self.chain_config.delivery_rate]
        payment_data = b""

        fn_call = mech_contract.functions.deliverMarketplaceWithSignatures(
            requester,
            deliver_with_sigs,
            delivery_rates,
            payment_data,
        )
        return self._submit_tx(fn_call, from_addr, "Offchain delivery")

    def _submit_tx(self, fn_call: Any, from_addr: str, label: str = "TX") -> str:
        """Submit a contract call via Safe (prod) or impersonation (local/test).

        Production: bridge has safe_service → _via_safe (Gnosis Safe execTransaction).
        Local/test: bridge without safe_service → _via_impersonation (Anvil only).
        Any failure propagates as an exception — no silent fallbacks.
        """
        if self._has_safe:
            return self._via_safe(fn_call, from_addr, label)
        return self._via_impersonation(fn_call, from_addr, label)

    def _via_safe(self, fn_call: Any, from_addr: str, label: str = "TX") -> str:
        """Submit via Gnosis Safe execTransaction (production path)."""
        mech_address = self.bridge.web3.to_checksum_address(
            self.chain_config.mech_address,
        )
        call_data = fn_call.build_transaction({"from": from_addr})["data"]
        tx_hash = self.bridge.wallet.safe_service.execute_safe_transaction(
            safe_address_or_tag=from_addr,
            to=mech_address,
            value=0,
            chain_name=self._chain_name,
            data=call_data,
        )
        logger.info("Safe {} submitted: {}", label, tx_hash)
        return tx_hash if isinstance(tx_hash, str) else tx_hash.hex()

    def _via_impersonation(self, fn_call: Any, from_addr: str, label: str = "TX") -> str:
        """Submit via direct transact() — only works on Anvil (auto-impersonate).

        Used in tests where the bridge has no safe_service. Not valid on real nodes.
        """
        tx_hash = fn_call.transact({"from": from_addr, "gas": 500_000})
        return _wait_and_check_receipt(self.bridge.web3, tx_hash, label)

    async def run(self) -> None:
        """Run the delivery loop."""
        self._running = True
        interval = DEFAULT_DELIVERY_INTERVAL

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
