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


class TestDeliveryLifecycle:
    def test_stop(self, delivery_no_bridge: DeliveryManager):
        delivery_no_bridge._running = True
        delivery_no_bridge.stop()
        assert delivery_no_bridge._running is False
