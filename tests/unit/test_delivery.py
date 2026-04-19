"""Tests for the delivery manager."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# lxml (from ddgs) and llama_cpp C extensions segfault in the same process
pytestmark = pytest.mark.forked

MULTISIG_ADDR = "0xccA28b516a8c596742Bf23D06324c638230705aE"


@pytest.fixture(autouse=True)
def _mock_service_info():
    """Provide service info (multisig etc.) from iwa mock."""
    info = {
        "service_id": 1,
        "service_key": "gnosis:1",
        "multisig_address": MULTISIG_ADDR,
    }
    with patch(
        "micromech.core.bridge.get_service_info",
        return_value=info,
    ):
        yield


from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import STATUS_DELIVERED, STATUS_EXECUTED, STATUS_FAILED
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager

CHAIN_CFG = ChainConfig(
    chain="gnosis",
    mech_address="0x77af31De935740567Cf4fF1986D04B2c964A786a",
    marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
    factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
    staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
)

CHAIN_CFG_NO_MECH = ChainConfig(
    chain="gnosis",
    marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
    factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
    staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
)


@pytest.fixture
def delivery_no_bridge(queue: PersistentQueue) -> DeliveryManager:
    config = MicromechConfig()
    return DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=None)


@pytest.fixture
def delivery_with_bridge(queue: PersistentQueue) -> DeliveryManager:
    config = MicromechConfig()
    bridge = MagicMock()
    return DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)


class TestDeliveryNoBridge:
    @pytest.mark.asyncio
    async def test_deliver_batch_skips_without_bridge(
        self, delivery_no_bridge: DeliveryManager, queue: PersistentQueue
    ):
        """Without bridge, delivery is skipped entirely."""
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))

        count = await delivery_no_bridge.deliver_batch()
        assert count == 0

        # Request stays in EXECUTED state, not falsely marked as delivered
        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTED

    @pytest.mark.asyncio
    async def test_deliver_batch_empty(self, delivery_no_bridge: DeliveryManager):
        count = await delivery_no_bridge.deliver_batch()
        assert count == 0


class TestDeliveryWithBridge:
    @pytest.mark.asyncio
    async def test_deliver_succeeds_via_safe(
        self,
        delivery_with_bridge: DeliveryManager,
        queue: PersistentQueue,
        monkeypatch,
    ):
        """With bridge and Safe mock, delivery succeeds."""
        # Force flush regardless of batch size or age
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 0)
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))

        count = await delivery_with_bridge.deliver_batch()
        assert count == 1

        record = queue.get_by_id("r1")
        assert record.request.status == "delivered"


class TestDeliveryWithBridgeMultiple:
    @pytest.mark.asyncio
    async def test_deliver_batch_empty_with_bridge(self, delivery_with_bridge: DeliveryManager):
        count = await delivery_with_bridge.deliver_batch()
        assert count == 0

    @pytest.mark.asyncio
    async def test_deliver_no_result_returns_none(
        self, delivery_with_bridge: DeliveryManager, queue: PersistentQueue
    ):
        """Request with no result should not crash."""
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))

        # Artificially remove result from the record
        from micromech.core.persistence import RequestRow

        RequestRow.update(result_output=None, result_error=None).where(
            RequestRow.request_id == "r1"
        ).execute()

        count = await delivery_with_bridge.deliver_batch()
        assert count == 0


class TestViaImpersonation:
    def test_impersonated_success(self, queue: PersistentQueue):
        """_via_impersonation transacts and returns tx hash."""
        config = MicromechConfig()
        bridge = MagicMock()
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}
        bridge.wallet.chain_interfaces.get.return_value.estimate_gas.return_value = 200_000
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        fn_call = MagicMock()
        tx_hash_bytes = b"\xde\xad" + b"\x00" * 30
        fn_call.transact.return_value = tx_hash_bytes

        result = dm._via_impersonation(fn_call, "0x" + "ab" * 20)
        assert result == tx_hash_bytes.hex()

    def test_impersonated_reverted(self, queue: PersistentQueue):
        """Reverted transaction raises RuntimeError."""
        config = MagicMock()
        bridge = MagicMock()
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
        bridge.wallet.chain_interfaces.get.return_value.estimate_gas.return_value = 200_000
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        fn_call = MagicMock()
        tx_hash_bytes = b"\xde\xad" + b"\x00" * 30
        fn_call.transact.return_value = tx_hash_bytes

        with pytest.raises(RuntimeError, match="reverted"):
            dm._via_impersonation(fn_call, "0x" + "ab" * 20)


class TestViaImpersonationDirect:
    def test_impersonation_success(self, queue: PersistentQueue):
        """_via_impersonation transacts and returns hex hash."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        tx_hash_bytes = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        fn_call = MagicMock()
        fn_call.transact.return_value = tx_hash_bytes

        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)
        result = dm._via_impersonation(fn_call, "0x" + "ab" * 20)
        assert result == tx_hash_bytes.hex()

    def test_impersonation_reverted(self, queue: PersistentQueue):
        """Reverted impersonation transaction raises RuntimeError."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        tx_hash_bytes = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}

        fn_call = MagicMock()
        fn_call.transact.return_value = tx_hash_bytes

        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)
        with pytest.raises(RuntimeError, match="reverted"):
            dm._via_impersonation(fn_call, "0x" + "ab" * 20)


class TestGetMechContract:
    def test_lazy_loads_contract(self, queue: PersistentQueue):
        config = MicromechConfig()
        bridge = MagicMock()
        mock_contract = MagicMock()
        bridge.web3.eth.contract.return_value = mock_contract

        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)
        contract = dm._get_mech_contract()
        assert contract is mock_contract
        # Second call returns cached instance
        contract2 = dm._get_mech_contract()
        assert contract2 is mock_contract
        bridge.web3.eth.contract.assert_called_once()

    def test_raises_if_no_mech_address(self, queue: PersistentQueue):
        config = MicromechConfig()  # mech_address is None
        bridge = MagicMock()
        dm = DeliveryManager(
            config=config, chain_config=CHAIN_CFG_NO_MECH, queue=queue, bridge=bridge
        )
        with pytest.raises(ValueError, match="mech_address not configured"):
            dm._get_mech_contract()


class TestSubmitDelivery:
    def test_impersonation_path(self, queue: PersistentQueue):
        """When impersonation succeeds (no Safe), returns (tx_hash, flags)."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # No wallet → _has_safe=False
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_impersonation = MagicMock(return_value="0xdeadbeef")
        dm._via_signed = MagicMock()

        tx_hash, flags = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert tx_hash == "0xdeadbeef"
        # Fallback flags when receipt parsing fails (invalid hex "deadbeef" length)
        assert flags == [True]
        dm._via_signed.assert_not_called()

    def test_impersonation_failure_propagates(self, queue: PersistentQueue):
        """When impersonation fails (no Safe), the error propagates — no silent fallback."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # No wallet → _has_safe=False
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_impersonation = MagicMock(side_effect=RuntimeError("not Anvil"))

        with pytest.raises(RuntimeError, match="not Anvil"):
            dm._submit_delivery("0x" + "aa" * 32, b"data")

    def test_request_id_hex_prefix(self, queue: PersistentQueue):
        """Request IDs with 0x prefix are handled correctly."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # No wallet → _has_safe=False
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_impersonation = MagicMock(return_value="0xtx")

        tx_hash, flags = dm._submit_delivery("0x" + "bb" * 32, b"data")
        assert tx_hash == "0xtx"
        assert flags == [True]


class TestSubmitViaSafe:
    def test_safe_path_success(self, queue: PersistentQueue):
        """When Safe is available and succeeds, returns (tx_hash, flags)."""
        config = MicromechConfig()
        bridge = MagicMock()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_safe = MagicMock(return_value="0xsafe_hash")
        dm._via_impersonation = MagicMock()
        dm._via_signed = MagicMock()

        tx_hash, flags = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert tx_hash == "0xsafe_hash"
        assert flags == [True]  # fallback when receipt parsing fails
        dm._via_impersonation.assert_not_called()
        dm._via_signed.assert_not_called()

    def test_safe_failure_propagates(self, queue: PersistentQueue):
        """When Safe fails, the error propagates — no silent fallback to impersonation."""
        config = MicromechConfig()
        bridge = MagicMock()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_safe = MagicMock(side_effect=RuntimeError("safe tx failed"))
        dm._via_impersonation = MagicMock()

        with pytest.raises(RuntimeError, match="safe tx failed"):
            dm._submit_delivery("0x" + "aa" * 32, b"data")
        dm._via_impersonation.assert_not_called()

    def test_no_safe_skips_to_impersonation(self, queue: PersistentQueue):
        """When bridge has no Safe service, skips directly to impersonation."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # No wallet attr
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        assert not dm._has_safe

        dm._get_mech_contract = MagicMock()
        dm._via_impersonation = MagicMock(return_value="0ximp")
        dm._via_signed = MagicMock()

        tx_hash, flags = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert tx_hash == "0ximp"
        assert flags == [True]

    def test_has_safe_property(self, queue: PersistentQueue):
        """_has_safe returns True only when bridge.wallet.safe_service exists."""
        config = MicromechConfig()

        # No bridge
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=None)
        assert not dm._has_safe

        # Bridge without wallet
        bridge = MagicMock(spec=["web3"])
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)
        assert not dm._has_safe

        # Bridge with wallet and safe_service
        bridge = MagicMock()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)
        assert dm._has_safe

    def test_chain_name_property(self, queue: PersistentQueue):
        config = MicromechConfig()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=None)
        assert dm._chain_name == "gnosis"


class TestDeliveryLifecycle:
    def test_stop(self, delivery_no_bridge: DeliveryManager):
        delivery_no_bridge._running = True
        delivery_no_bridge.stop()
        assert delivery_no_bridge._running is False

    def test_delivered_count(self, delivery_no_bridge: DeliveryManager):
        assert delivery_no_bridge.delivered_count == 0

    @pytest.mark.asyncio
    async def test_run_loop_exits_on_stop(self, queue: PersistentQueue, monkeypatch):
        """Run loop should exit when stop() is called."""
        import asyncio

        monkeypatch.setattr(
            "micromech.runtime.delivery.DEFAULT_DELIVERY_INTERVAL",
            1,
        )
        config = MicromechConfig()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=None)

        async def stop_soon():
            await asyncio.sleep(0.2)
            dm.stop()

        asyncio.create_task(stop_soon())
        await asyncio.wait_for(dm.run(), timeout=3.0)


class TestSubmitTxRouting:
    """Tests for _submit_tx routing: Safe when available, impersonation otherwise."""

    def test_has_safe_uses_safe(self, queue: PersistentQueue):
        """When _has_safe is True, _submit_tx calls _via_safe."""
        config = MicromechConfig()
        bridge = MagicMock()  # has wallet → _has_safe=True
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._via_safe = MagicMock(return_value="0xsafe")
        dm._via_impersonation = MagicMock()

        fn_call = MagicMock()
        result = dm._submit_tx(fn_call, "0x" + "ab" * 20)
        assert result == "0xsafe"
        dm._via_impersonation.assert_not_called()

    def test_no_safe_uses_impersonation(self, queue: PersistentQueue):
        """When _has_safe is False, _submit_tx calls _via_impersonation."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # no wallet → _has_safe=False
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._via_safe = MagicMock()
        dm._via_impersonation = MagicMock(return_value="0ximp")

        fn_call = MagicMock()
        result = dm._submit_tx(fn_call, "0x" + "ab" * 20)
        assert result == "0ximp"
        dm._via_safe.assert_not_called()

    def test_safe_failure_propagates(self, queue: PersistentQueue):
        """Safe failure raises — no silent fallback."""
        config = MicromechConfig()
        bridge = MagicMock()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._via_safe = MagicMock(side_effect=RuntimeError("safe failed"))

        with pytest.raises(RuntimeError, match="safe failed"):
            dm._submit_tx(MagicMock(), "0x" + "ab" * 20)


class TestNonHexRequestId:
    """Test that non-hex request IDs (e.g. http-abc123) are hashed."""

    def test_non_hex_request_id_gets_hashed(self, queue: PersistentQueue):
        import hashlib

        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        dm = DeliveryManager(
            config=config,
            chain_config=CHAIN_CFG,
            queue=queue,
            bridge=bridge,
        )

        mock_contract = MagicMock()
        dm._mech_contract = mock_contract
        dm._via_impersonation = MagicMock(return_value="0xtx")

        dm._submit_delivery("http-abc123", b"data")

        call_args = mock_contract.functions.deliverToMarketplace.call_args[0]
        req_id_bytes = call_args[0][0]
        expected = hashlib.sha256(b"http-abc123").digest()
        assert req_id_bytes == expected


class TestDeliveryBatchFailure:
    """Test that delivery failures mark request as failed."""

    @pytest.mark.asyncio
    async def test_delivery_tx_failure_leaves_in_executed(
        self, queue: PersistentQueue, monkeypatch
    ):
        """TX revert does NOT permanently mark records failed — they stay EXECUTED for retry."""
        monkeypatch.setattr("micromech.runtime.delivery.DEFAULT_DELIVERY_FLUSH_TIMEOUT", 0)
        config = MicromechConfig()
        bridge = MagicMock()
        dm = DeliveryManager(
            config=config,
            chain_config=CHAIN_CFG,
            queue=queue,
            bridge=bridge,
        )

        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))

        # On-chain records go through _prepare_onchain → _submit_batch_delivery.
        # Simulate a TX failure by raising inside _submit_batch_delivery.
        with patch.object(dm, "_submit_batch_delivery", side_effect=RuntimeError("tx reverted")):
            count = await dm.deliver_batch()
        assert count == 0

        # Record must stay in EXECUTED so the next deliver_batch() retries it.
        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTED


class TestDeliverOneIpfs:
    """Tests for _deliver_one IPFS integration."""

    @pytest.mark.asyncio
    async def test_deliver_one_returns_ipfs_cid_hex(self, queue: PersistentQueue):
        """_deliver_one returns (tx_hash, ipfs_cid_hex) when IPFS succeeds."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        dm = DeliveryManager(
            config=config,
            chain_config=CHAIN_CFG,
            queue=queue,
            bridge=bridge,
        )

        req = MechRequest(request_id="r1", prompt="test prompt", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))
        record = queue.get_by_id("r1")

        mock_push = AsyncMock(return_value=("bafkrei_test", "f01551220aabb"))
        dm._submit_delivery = MagicMock(return_value=("0xdeadbeef", [True]))

        with patch("micromech.ipfs.client.push_to_ipfs", mock_push):
            tx_hash, ipfs_cid_hex = await dm._deliver_one(record)

        assert tx_hash == "0xdeadbeef"
        assert ipfs_cid_hex == "f01551220aabb"

    @pytest.mark.asyncio
    async def test_deliver_one_ipfs_failure_fallback_raw(self, queue: PersistentQueue):
        """_deliver_one returns (tx_hash, None) when IPFS fails."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        dm = DeliveryManager(
            config=config,
            chain_config=CHAIN_CFG,
            queue=queue,
            bridge=bridge,
        )

        req = MechRequest(request_id="r2", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r2")
        queue.mark_executed("r2", ToolResult(output="ok"))
        record = queue.get_by_id("r2")

        mock_push = AsyncMock(side_effect=Exception("IPFS down"))
        dm._submit_delivery = MagicMock(return_value=("0xcafe", [True]))

        with patch("micromech.ipfs.client.push_to_ipfs", mock_push):
            tx_hash, ipfs_cid_hex = await dm._deliver_one(record)

        assert tx_hash == "0xcafe"
        assert ipfs_cid_hex is None
        # Verify raw JSON was passed to _submit_delivery
        call_data = dm._submit_delivery.call_args[0][1]
        parsed = json.loads(call_data)
        assert "requestId" in parsed
        assert "result" in parsed

    @pytest.mark.asyncio
    async def test_response_payload_format_valory(self, queue: PersistentQueue):
        """Response payload matches Valory format: requestId, result, prompt, tool."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])
        dm = DeliveryManager(
            config=config,
            chain_config=CHAIN_CFG,
            queue=queue,
            bridge=bridge,
        )

        req = MechRequest(request_id="r4", prompt="my prompt", tool="llm")
        queue.add_request(req)
        queue.mark_executing("r4")
        queue.mark_executed("r4", ToolResult(output="my result"))
        record = queue.get_by_id("r4")

        dm._submit_delivery = MagicMock(return_value=("0xaa", [True]))

        # Mock IPFS push to fail so raw JSON is delivered
        mock_push = AsyncMock(side_effect=Exception("IPFS down"))
        with patch("micromech.ipfs.client.push_to_ipfs", mock_push):
            await dm._deliver_one(record)

        call_data = dm._submit_delivery.call_args[0][1]
        payload = json.loads(call_data)
        assert payload == {
            "requestId": "r4",
            "result": "my result",
            "prompt": "my prompt",
            "tool": "llm",
        }


# ---------------------------------------------------------------------------
# _decode_delivery_flags — unit tests
# ---------------------------------------------------------------------------


from micromech.runtime.delivery import _decode_delivery_flags  # noqa: E402

MARKETPLACE_ADDR = CHAIN_CFG.marketplace_address
TX_HASH_HEX = "0x" + "ab" * 32


def _make_mock_web3_with_flags(flags: list[bool]) -> MagicMock:
    """Build a mock web3 whose MarketplaceDelivery event decodes to `flags`."""
    web3 = MagicMock()
    web3.to_checksum_address.side_effect = lambda x: x

    event_instance = MagicMock()

    def _process_log(log):
        if log.get("_our_event"):
            return {"args": {"deliveredRequests": flags}}
        raise Exception("not our event")

    event_instance.process_log.side_effect = _process_log
    contract_mock = MagicMock()
    contract_mock.events.MarketplaceDelivery.return_value = event_instance
    web3.eth.contract.return_value = contract_mock
    return web3


class TestDecodeDeliveryFlags:
    def test_all_accepted(self):
        flags = [True, True, True]
        web3 = _make_mock_web3_with_flags(flags)
        receipt = {"logs": [{"_our_event": True}]}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 3)
        assert result == [True, True, True]

    def test_partial_timeout(self):
        flags = [True, False, True]
        web3 = _make_mock_web3_with_flags(flags)
        receipt = {"logs": [{"_our_event": True}]}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 3)
        assert result == [True, False, True]

    def test_all_timed_out(self):
        flags = [False, False]
        web3 = _make_mock_web3_with_flags(flags)
        receipt = {"logs": [{"_our_event": True}]}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 2)
        assert result == [False, False]

    def test_no_matching_log_returns_all_true(self):
        """Falls back to all-True when no MarketplaceDelivery log is found."""
        web3 = MagicMock()
        web3.to_checksum_address.side_effect = lambda x: x
        event_instance = MagicMock()
        event_instance.process_log.side_effect = Exception("wrong topic")
        web3.eth.contract.return_value.events.MarketplaceDelivery.return_value = event_instance
        receipt = {"logs": [{"_not_our_event": True}]}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 2)
        assert result == [True, True]

    def test_empty_logs_returns_all_true(self):
        web3 = MagicMock()
        web3.to_checksum_address.side_effect = lambda x: x
        receipt = {"logs": []}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 3)
        assert result == [True, True, True]

    def test_contract_build_fails_returns_all_true(self):
        """If web3.eth.contract() raises, falls back to all-True."""
        web3 = MagicMock()
        web3.eth.contract.side_effect = Exception("RPC error")
        web3.to_checksum_address.side_effect = lambda x: x
        receipt = {"logs": []}
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 2)
        assert result == [True, True]

    def test_length_mismatch_returns_all_true(self):
        """If event flags length != num_requests, falls back to all-True."""
        flags = [True, False]  # 2 flags
        web3 = _make_mock_web3_with_flags(flags)
        receipt = {"logs": [{"_our_event": True}]}
        # Ask for 3 but event only has 2
        result = _decode_delivery_flags(web3, receipt, MARKETPLACE_ADDR, 3)
        assert result == [True, True, True]


# ---------------------------------------------------------------------------
# DeliveryManager: onchain batch timeout handling
# ---------------------------------------------------------------------------


def _add_executed(queue: PersistentQueue, request_id: str) -> None:
    req = MechRequest(request_id=request_id, prompt="p", tool="echo")
    queue.add_request(req)
    queue.mark_executing(request_id)
    queue.mark_executed(request_id, ToolResult(output="ok"))


class TestOnchainBatchTimeout:
    """_deliver_onchain_batch marks timed-out requests as failed."""

    @pytest.fixture
    def dm(self, queue):
        config = MicromechConfig()
        bridge = MagicMock()
        bridge.wallet.key_storage = MagicMock()
        return DeliveryManager(
            config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge
        )

    def _patch_prepare(self, dm):
        async def _fake(record):
            return b"\x00" * 32, b"data", "QmTest"
        return patch.object(dm, "_prepare_onchain", side_effect=_fake)

    @pytest.mark.asyncio
    async def test_all_accepted(self, dm, queue):
        _add_executed(queue, "a")
        _add_executed(queue, "b")
        records = queue.get_undelivered(limit=10)
        with (
            self._patch_prepare(dm),
            patch.object(
                dm, "_submit_batch_delivery", return_value=(TX_HASH_HEX, [True, True])
            ),
        ):
            count = await dm._deliver_onchain_batch(records)
        assert count == 2
        assert queue.get_by_id("a").request.status == STATUS_DELIVERED
        assert queue.get_by_id("b").request.status == STATUS_DELIVERED

    @pytest.mark.asyncio
    async def test_all_timed_out(self, dm, queue):
        _add_executed(queue, "a")
        _add_executed(queue, "b")
        records = queue.get_undelivered(limit=10)
        with (
            self._patch_prepare(dm),
            patch.object(
                dm, "_submit_batch_delivery", return_value=(TX_HASH_HEX, [False, False])
            ),
        ):
            count = await dm._deliver_onchain_batch(records)
        assert count == 0
        for rid in ("a", "b"):
            r = queue.get_by_id(rid)
            assert r.request.status == STATUS_FAILED
            assert r.request.error == "on_chain_timeout"
            assert r.response.delivery_tx_hash == TX_HASH_HEX

    @pytest.mark.asyncio
    async def test_partial_timeout(self, dm, queue):
        """First accepted, second timed out."""
        _add_executed(queue, "a")
        _add_executed(queue, "b")
        records = queue.get_undelivered(limit=10)
        with (
            self._patch_prepare(dm),
            patch.object(
                dm, "_submit_batch_delivery", return_value=(TX_HASH_HEX, [True, False])
            ),
        ):
            count = await dm._deliver_onchain_batch(records)
        assert count == 1
        assert queue.get_by_id("a").request.status == STATUS_DELIVERED
        r_b = queue.get_by_id("b")
        assert r_b.request.status == STATUS_FAILED
        assert r_b.request.error == "on_chain_timeout"


# ---------------------------------------------------------------------------
# DeliveryManager: single-onchain timeout handling
# ---------------------------------------------------------------------------


class TestSingleOnchainTimeout:
    """_deliver_single_onchain marks timed-out requests as failed."""

    @pytest.fixture
    def dm(self, queue):
        config = MicromechConfig()
        bridge = MagicMock()
        bridge.wallet.key_storage = MagicMock()
        return DeliveryManager(
            config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge
        )

    def _patch_prepare(self, dm):
        async def _fake(record):
            return b"\x00" * 32, b"data", "QmTest"
        return patch.object(dm, "_prepare_onchain", side_effect=_fake)

    @pytest.mark.asyncio
    async def test_accepted(self, dm, queue):
        _add_executed(queue, "r1")
        record = queue.get_undelivered(limit=1)[0]
        with (
            self._patch_prepare(dm),
            patch.object(
                dm, "_submit_batch_delivery", return_value=(TX_HASH_HEX, [True])
            ),
        ):
            result = await dm._deliver_single_onchain(record)
        assert result is True
        assert queue.get_by_id("r1").request.status == STATUS_DELIVERED

    @pytest.mark.asyncio
    async def test_timed_out(self, dm, queue):
        _add_executed(queue, "r1")
        record = queue.get_undelivered(limit=1)[0]
        with (
            self._patch_prepare(dm),
            patch.object(
                dm, "_submit_batch_delivery", return_value=(TX_HASH_HEX, [False])
            ),
        ):
            result = await dm._deliver_single_onchain(record)
        assert result is False
        r = queue.get_by_id("r1")
        assert r.request.status == STATUS_FAILED
        assert r.request.error == "on_chain_timeout"
        assert r.response.delivery_tx_hash == TX_HASH_HEX
