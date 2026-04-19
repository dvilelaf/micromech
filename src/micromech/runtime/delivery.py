"""Delivery manager — submits tool results on-chain.

Calls deliverToMarketplace(requestIds[], datas[]) on the mech contract
via the mech service's Safe multisig.

Delivery transport strategy:
- Production (bridge has safe_service): _via_safe — Gnosis Safe execTransaction.
- Test/local (Anvil, bridge without safe_service): _via_impersonation — direct
  transact() via Anvil auto-impersonate. This path does NOT work on real nodes.

Batching strategy:
- On-chain requests are accumulated and flushed together in a single Safe TX
  when either DEFAULT_DELIVERY_BATCH_SIZE requests are ready OR the oldest
  request has been waiting DEFAULT_DELIVERY_FLUSH_TIMEOUT seconds.
- IPFS uploads for a batch are parallelized with asyncio.gather.
- Off-chain (HTTP) requests are delivered 1:1 as before.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from loguru import logger

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import (
    DEFAULT_DELIVERY_BATCH_SIZE,
    DEFAULT_DELIVERY_FLUSH_TIMEOUT,
    DEFAULT_DELIVERY_INTERVAL,
    DEFAULT_DELIVERY_MAX_RETRIES,
    DEFAULT_DELIVERY_WORKERS,
    GAS_FALLBACK,
    GAS_FLOOR_DELIVERY,
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


def _decode_delivery_flags(
    web3: Any,
    receipt: Any,
    marketplace_address: str,
    num_requests: int,
) -> list[bool]:
    """Decode MarketplaceDelivery event from a mined TX receipt.

    Returns list[bool] of length num_requests. True = the marketplace accepted
    the delivery (within responseTimeout); False = timeout (contract rejected
    it as late but did not revert the TX).

    Falls back to [True] * num_requests on any parsing error so that a missing
    or undecodable event never incorrectly marks requests as failed.
    """
    from micromech.runtime.contracts import load_marketplace_abi

    try:
        marketplace = web3.eth.contract(
            address=web3.to_checksum_address(marketplace_address),
            abi=load_marketplace_abi(),
        )
        for log in receipt.get("logs", []):
            try:
                event = marketplace.events.MarketplaceDelivery()
                decoded = event.process_log(log)
                flags = list(decoded["args"]["deliveredRequests"])
                if len(flags) == num_requests:
                    return flags
                # Length mismatch — shouldn't happen, but don't corrupt the batch
                from loguru import logger as _logger
                _logger.warning(
                    "MarketplaceDelivery flags mismatch: got {} expected {}",
                    len(flags),
                    num_requests,
                )
                return [True] * num_requests
            except Exception:
                continue  # not our event, try next log
    except Exception:
        pass

    return [True] * num_requests


def _batch_age_seconds(records: list[RequestRecord]) -> float:
    """Return age in seconds of the oldest record's request creation time."""
    now = datetime.now(timezone.utc)
    oldest = min(r.request.created_at for r in records)
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    return (now - oldest).total_seconds()


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
        self._delivery_failures: dict[str, int] = {}
        self._in_flight: set[str] = set()

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

    def _request_id_to_bytes(self, request_id: str) -> bytes:
        """Convert a request ID string to bytes32.

        On-chain IDs are stored as exactly 64 hex chars (bytes32), with or without
        "0x" prefix — converted directly.  IDs with "0x" prefix but wrong length
        raise ValueError (delivery mismatch risk).  Off-chain / non-hex IDs are
        sha256-hashed to get a deterministic bytes32.
        """
        has_prefix = request_id.startswith("0x")
        hex_candidate = request_id[2:] if has_prefix else request_id

        if has_prefix or len(hex_candidate) == 64:
            try:
                req_id_bytes = bytes.fromhex(hex_candidate)
                if len(req_id_bytes) != 32:
                    msg = (
                        f"request_id {request_id[:20]}... is {len(req_id_bytes)} bytes "
                        f"(expected 32); refusing to pad/truncate to avoid delivery mismatch"
                    )
                    raise ValueError(msg)
                return req_id_bytes
            except ValueError:
                raise
        # Non-hex IDs (e.g. "http-abc123") — sha256 for deterministic bytes32
        import hashlib

        return hashlib.sha256(request_id.encode()).digest()

    def _increment_failure(self, request_id: str, error: str) -> None:
        """Increment per-request failure counter; mark_failed after MAX_RETRIES.

        Prevents poison-pill batches from stalling indefinitely: a single broken
        record (bad IPFS payload, malformed ID, etc.) will permanently fail after
        DEFAULT_DELIVERY_MAX_RETRIES consecutive attempts and stop blocking the
        rest of the batch.  Transient errors (RPC blip, TX revert) need several
        failures before they reach the terminal state.
        """
        count = self._delivery_failures.get(request_id, 0) + 1
        self._delivery_failures[request_id] = count
        if count >= DEFAULT_DELIVERY_MAX_RETRIES:
            logger.error(
                "Request {} failed {} consecutive times ({}), marking as failed",
                request_id[:20] + "...",
                count,
                error,
            )
            self.queue.mark_failed(request_id, f"max_retries ({count}x): {error}")
            self._delivery_failures.pop(request_id, None)
        else:
            logger.warning(
                "Request {} failure {}/{}: {}",
                request_id[:20] + "...",
                count,
                DEFAULT_DELIVERY_MAX_RETRIES,
                error,
            )

    async def deliver_batch(self) -> int:
        """Deliver undelivered responses. On-chain: batched; off-chain: 1:1.

        On-chain records are accumulated and flushed together in a single Safe TX
        when DEFAULT_DELIVERY_BATCH_SIZE records are ready or the oldest record
        has been waiting DEFAULT_DELIVERY_FLUSH_TIMEOUT seconds.
        """
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

        # Pull 2× batch size to ensure we get a full on-chain batch even when
        # off-chain records share the limit.  On-chain batch is then capped at
        # DEFAULT_DELIVERY_BATCH_SIZE; off-chain processes all extras 1:1.
        records = self.queue.get_undelivered(
            limit=DEFAULT_DELIVERY_BATCH_SIZE * 2, chain=self._chain_name
        )
        if not records:
            return 0

        onchain = [r for r in records if not r.request.is_offchain][:DEFAULT_DELIVERY_BATCH_SIZE]
        offchain = [r for r in records if r.request.is_offchain]

        delivered = 0

        # --- On-chain: batch by size or time ---
        if onchain:
            full_batch = len(onchain) >= DEFAULT_DELIVERY_BATCH_SIZE
            old_enough = _batch_age_seconds(onchain) >= DEFAULT_DELIVERY_FLUSH_TIMEOUT
            should_flush = full_batch or old_enough
            if should_flush:
                flush_reason = "size" if full_batch else "timeout"
                logger.debug(
                    "Flushing on-chain batch chain={} size={} reason={} oldest_age={:.1f}s",
                    self._chain_name,
                    len(onchain),
                    flush_reason,
                    _batch_age_seconds(onchain),
                )
                delivered += await self._deliver_onchain_batch(onchain)

        # --- Off-chain: 1:1 (unchanged) ---
        for record in offchain:
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
                        "Delivered (offchain) {} tool={} tx={} prompt={}",
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

    async def _deliver_onchain_batch(self, records: list[RequestRecord]) -> int:
        """Prepare (IPFS in parallel) and submit all on-chain records in one Safe TX.

        IPFS uploads run concurrently. Records that fail IPFS prep are marked
        failed individually; the rest are batched into a single deliverToMarketplace call.
        """
        # Parallel IPFS uploads
        prepare_results = await asyncio.gather(
            *[self._prepare_onchain(r) for r in records],
            return_exceptions=True,
        )

        good: list[tuple[RequestRecord, bytes, bytes, Optional[str]]] = []
        for record, result in zip(records, prepare_results):
            if isinstance(result, Exception):
                logger.exception(
                    "IPFS prep failed for {}: {}",
                    record.request.request_id,
                    result,
                )
                if self._metrics:
                    self._metrics.record_delivery_failed(
                        record.request.request_id, str(result), chain=self._chain_name
                    )
                # Use retry counter — IPFS failures may be transient; only
                # mark_failed permanently after MAX_RETRIES consecutive failures.
                self._increment_failure(record.request.request_id, f"ipfs_prep: {result}")
            else:
                req_id_bytes, delivery_data, ipfs_cid_hex = result  # type: ignore[misc]
                good.append((record, req_id_bytes, delivery_data, ipfs_cid_hex))

        if not good:
            return 0

        req_id_bytes_list = [item[1] for item in good]
        datas = [item[2] for item in good]

        try:
            tx_hash, delivered_flags = await asyncio.to_thread(
                self._submit_batch_delivery, req_id_bytes_list, datas
            )
            n_delivered = 0
            n_timed_out = 0
            for (record, _, _, ipfs_cid_hex), accepted in zip(good, delivered_flags):
                req_id = record.request.request_id
                self._delivery_failures.pop(req_id, None)
                if accepted:
                    self.queue.mark_delivered(
                        req_id,
                        tx_hash=tx_hash,
                        ipfs_hash=ipfs_cid_hex,
                    )
                    self._delivered_count += 1
                    n_delivered += 1
                    if self._metrics:
                        self._metrics.record_delivery(
                            req_id, chain=self._chain_name
                        )
                else:
                    # The TX was mined but the contract rejected this request as
                    # a late delivery (arrived after responseTimeout). Mark as
                    # failed so Overview stats reflect the real on-chain outcome.
                    logger.warning(
                        "Request {} timed out on-chain (tx={})",
                        req_id[:16] + "...",
                        tx_hash[:18] + "...",
                    )
                    self.queue.mark_timed_out(
                        req_id,
                        tx_hash=tx_hash,
                        ipfs_hash=ipfs_cid_hex,
                    )
                    n_timed_out += 1
                    if self._metrics:
                        self._metrics.record_delivery_failed(
                            req_id, "on_chain_timeout", chain=self._chain_name
                        )
            ids_short = ", ".join(r.request.request_id[:10] for r, *_ in good)
            logger.info(
                "Batch TX mined: {} delivered, {} timed out (tx={} ids=[{}])",
                n_delivered,
                n_timed_out,
                tx_hash[:18] + "...",
                ids_short,
            )
            return n_delivered
        except Exception as e:
            ids = [r.request.request_id[:16] for r, *_ in good]
            logger.exception(
                "Batch delivery failed chain={} size={} ids={} — will retry (failure {}/{})",
                self._chain_name,
                len(good),
                ids,
                max(
                    (self._delivery_failures.get(r.request.request_id, 0) + 1 for r, *_ in good),
                    default=1,
                ),
                DEFAULT_DELIVERY_MAX_RETRIES,
            )
            for record, *_ in good:
                if self._metrics:
                    self._metrics.record_delivery_failed(
                        record.request.request_id, str(e), chain=self._chain_name
                    )
                # Increment per-record counter; mark_failed only after MAX_RETRIES.
                # TX reverts may be transient (gas, nonce, RPC) — leave in EXECUTED
                # until the retry budget is exhausted.
                self._increment_failure(record.request.request_id, f"tx: {e}")
            return 0

    async def _prepare_onchain(self, record: RequestRecord) -> tuple[bytes, bytes, Optional[str]]:
        """Build payload, upload to IPFS, return (req_id_bytes, delivery_data, ipfs_cid_hex).

        Raises if record has no result. Falls back to raw JSON if IPFS is unavailable.
        """
        if record.result is None:
            raise ValueError(f"No result for {record.request.request_id}")

        request_id = record.request.request_id

        response_payload = json.dumps(
            {
                "requestId": request_id,
                "result": record.result.output,
                "prompt": record.request.prompt,
                "tool": record.request.tool,
            },
            separators=(",", ":"),
        ).encode("utf-8")

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
            delivery_data = cid_hex_to_multihash_bytes(cid_hex)
            ipfs_cid_hex = cid_hex
            if self._ipfs_warning_logged:
                logger.info("IPFS recovered — resuming CID-based delivery")
                self._ipfs_warning_logged = False
        except Exception as e:
            if not self._ipfs_warning_logged:
                logger.warning(
                    "IPFS unavailable, delivering raw: {}",
                    e,
                )
                self._ipfs_warning_logged = True
            delivery_data = response_payload

        req_id_bytes = self._request_id_to_bytes(request_id)
        return req_id_bytes, delivery_data, ipfs_cid_hex

    async def _deliver_one(
        self,
        record: RequestRecord,
    ) -> tuple[Optional[str], Optional[str]]:
        """Deliver a single response (used for off-chain / HTTP requests).

        Returns (tx_hash, ipfs_cid_hex) or (None, None).

        Pushes the result to IPFS first (if enabled), then delivers on-chain:
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
            tx_hash, _ = await asyncio.to_thread(
                self._submit_delivery,
                request_id,
                delivery_data,
            )
        return tx_hash, ipfs_cid_hex

    def _submit_batch_delivery(
        self, req_id_bytes_list: list[bytes], datas: list[bytes]
    ) -> tuple[str, list[bool]]:
        """Submit deliverToMarketplace([ids...], [datas...]) in a single Safe TX.

        Returns (tx_hash, delivered_flags) where delivered_flags[i] is True if
        the marketplace accepted request i (within responseTimeout) or False if
        it was rejected as a late delivery (on-chain timeout).
        """
        mech_contract = self._get_mech_contract()
        from micromech.core.bridge import get_service_info

        svc_info = get_service_info(self.chain_config.chain)
        multisig = svc_info.get("multisig_address")
        if not multisig:
            raise ValueError("multisig_address not configured — cannot deliver")
        from_addr = self.bridge.web3.to_checksum_address(multisig)

        fn_call = mech_contract.functions.deliverToMarketplace(
            req_id_bytes_list,
            datas,
        )
        label = f"BatchDelivery[{len(req_id_bytes_list)}]"
        tx_hash = self._submit_tx(fn_call, from_addr, label)

        # Parse per-request delivery outcome from the MarketplaceDelivery event.
        # The contract accepts late deliveries without reverting, but records them
        # as timeouts (deliveredRequests[i] = False). We must read this to avoid
        # falsely counting timed-out requests as successful deliveries.
        web3 = self.bridge.web3
        try:
            tx_hash_bytes = bytes.fromhex(tx_hash.removeprefix("0x"))
            receipt = web3.eth.get_transaction_receipt(tx_hash_bytes)
            if receipt is None:
                receipt = web3.eth.wait_for_transaction_receipt(
                    tx_hash_bytes, timeout=TX_RECEIPT_TIMEOUT
                )
        except Exception as e:
            logger.warning(
                "Could not fetch receipt for {} to check delivery flags: {}",
                tx_hash[:18],
                e,
            )
            return tx_hash, [True] * len(req_id_bytes_list)

        flags = _decode_delivery_flags(
            web3,
            receipt,
            self.chain_config.marketplace_address,
            len(req_id_bytes_list),
        )
        return tx_hash, flags

    def _submit_delivery(
        self, request_id: str, data: bytes
    ) -> tuple[str, list[bool]]:
        """Submit deliverToMarketplace for a single request.

        Converts request_id to bytes32 and delegates to _submit_batch_delivery.
        Kept for backward compatibility (used by _deliver_one for on-chain path).
        """
        req_id_bytes = self._request_id_to_bytes(request_id)
        return self._submit_batch_delivery([req_id_bytes], [data])

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
        logger.info("Safe {} submitted chain={}: {}", label, self._chain_name, tx_hash)
        return tx_hash if isinstance(tx_hash, str) else tx_hash.hex()

    def _via_impersonation(self, fn_call: Any, from_addr: str, label: str = "TX") -> str:
        """Submit via direct transact() — only works on Anvil (auto-impersonate).

        Used in tests where the bridge has no safe_service. Not valid on real nodes.
        Gas is estimated via iwa's chain_interface (10% buffer, 500_000 on failure);
        floors at GAS_FLOOR_DELIVERY.
        """
        try:
            ci = self.bridge.wallet.chain_interfaces.get(self._chain_name)
            gas = max(ci.estimate_gas(fn_call, {"from": from_addr}), GAS_FLOOR_DELIVERY)
        except Exception as e:
            logger.warning(
                "Gas estimation failed ({}), using fallback {}", type(e).__name__, GAS_FALLBACK
            )
            gas = GAS_FALLBACK
        tx_hash = fn_call.transact({"from": from_addr, "gas": gas})
        return _wait_and_check_receipt(self.bridge.web3, tx_hash, label)

    async def _deliver_concurrent(self) -> int:
        """Deliver up to DEFAULT_DELIVERY_WORKERS requests concurrently.

        Each on-chain request becomes one Safe TX (batch_size=1), preserving
        delivery_delta == nonce_delta for staking liveness.  An in-flight set
        prevents the same request being picked up by two concurrent workers.
        Safe nonce conflicts between workers are handled by the existing
        _refresh_nonce retry path in SafeTransactionExecutor.
        """
        if self.bridge is None:
            return 0

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
            limit=DEFAULT_DELIVERY_WORKERS * 4, chain=self._chain_name
        )
        if not records:
            return 0

        onchain = [
            r for r in records
            if not r.request.is_offchain
            and r.request.request_id not in self._in_flight
        ][:DEFAULT_DELIVERY_WORKERS]

        offchain = [
            r for r in records
            if r.request.is_offchain
            and r.request.request_id not in self._in_flight
        ]

        selected = onchain + offchain
        if not selected:
            return 0

        for r in selected:
            self._in_flight.add(r.request.request_id)

        tasks = (
            [self._deliver_single_onchain(r) for r in onchain]
            + [self._deliver_single_offchain_concurrent(r) for r in offchain]
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _deliver_single_onchain(self, record: RequestRecord) -> bool:
        """Prepare and submit one on-chain request as a single Safe TX.

        One delivery = one Safe TX nonce, preserving staking liveness.
        Always removes the record from in-flight in the finally block.
        """
        try:
            req_id_bytes, delivery_data, ipfs_cid_hex = (
                await self._prepare_onchain(record)
            )
            tx_hash, delivered_flags = await asyncio.to_thread(
                self._submit_batch_delivery,
                [req_id_bytes],
                [delivery_data],
            )
            req_id = record.request.request_id
            self._delivery_failures.pop(req_id, None)
            accepted = delivered_flags[0] if delivered_flags else True
            if accepted:
                self.queue.mark_delivered(
                    req_id,
                    tx_hash=tx_hash,
                    ipfs_hash=ipfs_cid_hex,
                )
                self._delivered_count += 1
                if self._metrics:
                    self._metrics.record_delivery(
                        req_id, chain=self._chain_name
                    )
                logger.info(
                    "Delivered {} tool={} tx={}",
                    req_id[:16] + "...",
                    record.request.tool,
                    tx_hash[:18] + "...",
                )
            else:
                # TX mined but rejected as late by the marketplace contract
                logger.warning(
                    "Request {} timed out on-chain (tx={})",
                    req_id[:16] + "...",
                    tx_hash[:18] + "...",
                )
                self.queue.mark_timed_out(
                    req_id,
                    tx_hash=tx_hash,
                    ipfs_hash=ipfs_cid_hex,
                )
                if self._metrics:
                    self._metrics.record_delivery_failed(
                        req_id, "on_chain_timeout", chain=self._chain_name
                    )
            return accepted
        except Exception as e:
            logger.exception(
                "Delivery failed for {}: {}",
                record.request.request_id[:20] + "...",
                e,
            )
            self._increment_failure(
                record.request.request_id, f"tx: {e}"
            )
            if self._metrics:
                self._metrics.record_delivery_failed(
                    record.request.request_id,
                    str(e),
                    chain=self._chain_name,
                )
            return False
        finally:
            self._in_flight.discard(record.request.request_id)

    async def _deliver_single_offchain_concurrent(
        self, record: RequestRecord
    ) -> bool:
        """Deliver one off-chain request, removing from in-flight when done."""
        try:
            tx_hash, ipfs_hash = await self._deliver_one(record)
            if tx_hash:
                self.queue.mark_delivered(
                    record.request.request_id,
                    tx_hash=tx_hash,
                    ipfs_hash=ipfs_hash,
                )
                self._delivered_count += 1
                if self._metrics:
                    self._metrics.record_delivery(
                        record.request.request_id, chain=self._chain_name
                    )
                prompt_short = (
                    record.request.prompt[:60]
                    if record.request.prompt
                    else ""
                )
                logger.info(
                    "Delivered (offchain) {} tool={} tx={} prompt={}",
                    record.request.request_id[:16] + "...",
                    record.request.tool,
                    tx_hash[:18] + "...",
                    prompt_short,
                )
                return True
            return False
        except Exception as e:
            logger.error(
                "Delivery failed for {}: {}",
                record.request.request_id,
                e,
            )
            self._increment_failure(
                record.request.request_id, f"delivery: {e}"
            )
            if self._metrics:
                self._metrics.record_delivery_failed(
                    record.request.request_id,
                    str(e),
                    chain=self._chain_name,
                )
            return False
        finally:
            self._in_flight.discard(record.request.request_id)

    # TODO(H1): crash recovery for STATUS_DELIVERING
    # If the process dies between _submit_batch_delivery and mark_delivered, the
    # records remain in STATUS_EXECUTED (or a hypothetical STATUS_DELIVERING) and
    # will be re-submitted on next startup, potentially double-delivering.
    # Fix requires a STATUS_DELIVERING state + schema migration to detect in-flight
    # batches on startup and check the chain for the TX hash before re-submitting.

    async def run(self) -> None:
        """Run the delivery loop with concurrent workers.

        Fires DEFAULT_DELIVERY_WORKERS concurrent Safe TXs each tick
        (interval DEFAULT_DELIVERY_INTERVAL seconds).  An in-flight set
        prevents double-delivery across concurrent workers.
        """
        self._running = True
        interval = DEFAULT_DELIVERY_INTERVAL

        if self.bridge is None:
            logger.info("Delivery manager: no bridge — delivery disabled")
        else:
            logger.info(
                "Delivery manager started (interval {}s, workers {})",
                interval,
                DEFAULT_DELIVERY_WORKERS,
            )

        while self._running:
            try:
                count = await self._deliver_concurrent()
                if count:
                    logger.debug("Delivered {} responses", count)
            except Exception as e:
                logger.exception("Delivery loop error: {}", e)
            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False
