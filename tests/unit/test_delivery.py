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
from micromech.core.constants import STATUS_EXECUTED
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
        self, delivery_with_bridge: DeliveryManager, queue: PersistentQueue
    ):
        """With bridge and Safe mock, delivery succeeds."""
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
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        fn_call = MagicMock()
        tx_hash_bytes = b"\xde\xad" + b"\x00" * 30
        fn_call.transact.return_value = tx_hash_bytes

        result = dm._via_impersonation(fn_call, "0x" + "ab" * 20)
        assert result == tx_hash_bytes.hex()

    def test_impersonated_reverted(self, queue: PersistentQueue):
        """Reverted transaction raises RuntimeError."""
        config = MicromechConfig()
        bridge = MagicMock()
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
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
        """When impersonation succeeds (no Safe), returns tx hash without trying signed."""
        config = MicromechConfig()
        bridge = MagicMock(spec=["web3"])  # No wallet → _has_safe=False
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_impersonation = MagicMock(return_value="0xdeadbeef")
        dm._via_signed = MagicMock()

        result = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert result == "0xdeadbeef"
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

        result = dm._submit_delivery("0x" + "bb" * 32, b"data")
        assert result == "0xtx"


class TestSubmitViaSafe:
    def test_safe_path_success(self, queue: PersistentQueue):
        """When Safe is available and succeeds, uses Safe TX."""
        config = MicromechConfig()
        bridge = MagicMock()
        dm = DeliveryManager(config=config, chain_config=CHAIN_CFG, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._via_safe = MagicMock(return_value="0xsafe_hash")
        dm._via_impersonation = MagicMock()
        dm._via_signed = MagicMock()

        result = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert result == "0xsafe_hash"
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

        result = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert result == "0ximp"

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
    async def test_delivery_failure_marks_failed(self, queue: PersistentQueue):
        from micromech.core.constants import STATUS_FAILED

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

        async def exploding_deliver(record):
            raise RuntimeError("tx reverted")

        dm._deliver_one = exploding_deliver

        count = await dm.deliver_batch()
        assert count == 0

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED
        assert "delivery" in record.request.error


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
        dm._submit_delivery = MagicMock(return_value="0xdeadbeef")

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
        dm._submit_delivery = MagicMock(return_value="0xcafe")

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

        dm._submit_delivery = MagicMock(return_value="0xaa")

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
