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
        assert "not yet implemented" in record.request.error.lower()


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
