"""Tests for the delivery manager."""

from unittest.mock import MagicMock

import pytest

from micromech.core.config import MicromechConfig
from micromech.core.constants import STATUS_EXECUTED
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager


@pytest.fixture
def delivery_no_bridge(queue: PersistentQueue) -> DeliveryManager:
    config = MicromechConfig()
    return DeliveryManager(config=config, queue=queue, bridge=None)


@pytest.fixture
def delivery_with_bridge(queue: PersistentQueue) -> DeliveryManager:
    config = MicromechConfig()
    bridge = MagicMock()
    return DeliveryManager(config=config, queue=queue, bridge=bridge)


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
    async def test_deliver_raises_not_implemented(
        self, delivery_with_bridge: DeliveryManager, queue: PersistentQueue
    ):
        """With bridge but no real implementation, delivery fails gracefully."""
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))

        # Should fail (NotImplementedError) and mark as failed
        count = await delivery_with_bridge.deliver_batch()
        assert count == 0

        record = queue.get_by_id("r1")
        assert record.request.status == "failed"
        assert "failed" == record.request.status
        assert record.request.error is not None


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


class TestSubmitImpersonated:
    def test_impersonated_success(self, queue: PersistentQueue):
        """_submit_impersonated transacts and returns tx hash."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)

        mock_contract = MagicMock()
        tx_hash_bytes = b"\xde\xad" + b"\x00" * 30
        mock_contract.functions.deliverToMarketplace.return_value.transact.return_value = (
            tx_hash_bytes
        )

        result = dm._submit_impersonated(mock_contract, "0x" + "ab" * 20, b"\x01" * 32, b"data")
        assert result == tx_hash_bytes.hex()

    def test_impersonated_reverted(self, queue: PersistentQueue):
        """Reverted transaction raises RuntimeError."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)

        mock_contract = MagicMock()
        tx_hash_bytes = b"\xde\xad" + b"\x00" * 30
        mock_contract.functions.deliverToMarketplace.return_value.transact.return_value = (
            tx_hash_bytes
        )

        with pytest.raises(RuntimeError, match="reverted"):
            dm._submit_impersonated(mock_contract, "0x" + "ab" * 20, b"\x01" * 32, b"data")


class TestSubmitSigned:
    def test_signed_success(self, queue: PersistentQueue):
        """_submit_signed builds, signs, and sends transaction."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        bridge.web3.eth.gas_price = 1000
        bridge.web3.eth.get_transaction_count.return_value = 5
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}
        tx_hash_bytes = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.send_raw_transaction.return_value = tx_hash_bytes
        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"signed_tx"
        bridge.web3.eth.account.sign_transaction.return_value = mock_signed

        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        # Mock _get_signer_key
        dm._get_signer_key = MagicMock(return_value="0x" + "ff" * 32)

        mock_contract = MagicMock()
        result = dm._submit_signed(mock_contract, "0x" + "ab" * 20, b"\x01" * 32, b"data")
        assert result == tx_hash_bytes.hex()

    def test_signed_reverted(self, queue: PersistentQueue):
        """Reverted signed transaction raises RuntimeError."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        bridge.web3.eth.gas_price = 1000
        bridge.web3.eth.get_transaction_count.return_value = 0
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 0}
        tx_hash_bytes = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.send_raw_transaction.return_value = tx_hash_bytes
        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"signed_tx"
        bridge.web3.eth.account.sign_transaction.return_value = mock_signed

        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        dm._get_signer_key = MagicMock(return_value="0x" + "ff" * 32)

        mock_contract = MagicMock()
        with pytest.raises(RuntimeError, match="reverted"):
            dm._submit_signed(mock_contract, "0x" + "ab" * 20, b"\x01" * 32, b"data")


class TestGetSignerKey:
    def test_get_signer_key_success(self, queue: PersistentQueue):
        config = MicromechConfig()
        bridge = MagicMock()
        mock_account = MagicMock()
        mock_account.key.hex.return_value = "deadbeef" * 8
        bridge.wallet.account_service.resolve_account.return_value = mock_account

        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        key = dm._get_signer_key()
        assert key == "deadbeef" * 8

    def test_get_signer_key_failure(self, queue: PersistentQueue):
        config = MicromechConfig()
        bridge = MagicMock()
        bridge.wallet.account_service.resolve_account.side_effect = RuntimeError("not found")

        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        with pytest.raises(ValueError, match="Cannot resolve signer key"):
            dm._get_signer_key()


class TestGetMechContract:
    def test_lazy_loads_contract(self, queue: PersistentQueue):
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        mock_contract = MagicMock()
        bridge.web3.eth.contract.return_value = mock_contract

        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        contract = dm._get_mech_contract()
        assert contract is mock_contract
        # Second call returns cached instance
        contract2 = dm._get_mech_contract()
        assert contract2 is mock_contract
        bridge.web3.eth.contract.assert_called_once()

    def test_raises_if_no_mech_address(self, queue: PersistentQueue):
        config = MicromechConfig()  # mech_address is None
        bridge = MagicMock()
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)
        with pytest.raises(ValueError, match="mech_address not configured"):
            dm._get_mech_contract()


class TestSubmitDelivery:
    def test_impersonation_path(self, queue: PersistentQueue):
        """When impersonation succeeds, returns tx hash without trying signed."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._submit_impersonated = MagicMock(return_value="0xdeadbeef")
        dm._submit_signed = MagicMock()

        result = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert result == "0xdeadbeef"
        dm._submit_signed.assert_not_called()

    def test_fallback_to_signed(self, queue: PersistentQueue):
        """When impersonation fails, falls back to signed."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._submit_impersonated = MagicMock(side_effect=RuntimeError("not Anvil"))
        dm._submit_signed = MagicMock(return_value="0xcafebabe")

        result = dm._submit_delivery("0x" + "aa" * 32, b"data")
        assert result == "0xcafebabe"

    def test_request_id_hex_prefix(self, queue: PersistentQueue):
        """Request IDs with 0x prefix are handled correctly."""
        config = MicromechConfig(mech={"mech_address": "0x" + "ab" * 20})
        bridge = MagicMock()
        dm = DeliveryManager(config=config, queue=queue, bridge=bridge)

        dm._get_mech_contract = MagicMock()
        dm._submit_impersonated = MagicMock(return_value="0xtx")

        result = dm._submit_delivery("0x" + "bb" * 32, b"data")
        assert result == "0xtx"


class TestDeliveryLifecycle:
    def test_stop(self, delivery_no_bridge: DeliveryManager):
        delivery_no_bridge._running = True
        delivery_no_bridge.stop()
        assert delivery_no_bridge._running is False

    def test_delivered_count(self, delivery_no_bridge: DeliveryManager):
        assert delivery_no_bridge.delivered_count == 0

    @pytest.mark.asyncio
    async def test_run_loop_exits_on_stop(self, queue: PersistentQueue):
        """Run loop should exit when stop() is called."""
        import asyncio

        from micromech.core.config import RuntimeConfig

        config = MicromechConfig(runtime=RuntimeConfig(delivery_interval=1))
        dm = DeliveryManager(config=config, queue=queue, bridge=None)

        async def stop_soon():
            await asyncio.sleep(0.2)
            dm.stop()

        asyncio.create_task(stop_soon())
        await asyncio.wait_for(dm.run(), timeout=3.0)
