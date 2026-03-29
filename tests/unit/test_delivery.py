"""Tests for the delivery manager."""

import pytest

from micromech.core.config import MicromechConfig
from micromech.core.constants import STATUS_DELIVERED
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager


@pytest.fixture
def delivery(queue: PersistentQueue) -> DeliveryManager:
    config = MicromechConfig()
    return DeliveryManager(config=config, queue=queue, bridge=None)


class TestDeliveryManager:
    @pytest.mark.asyncio
    async def test_deliver_batch_empty(self, delivery: DeliveryManager):
        count = await delivery.deliver_batch()
        assert count == 0

    @pytest.mark.asyncio
    async def test_deliver_batch_with_results(
        self, delivery: DeliveryManager, queue: PersistentQueue
    ):
        # Add an executed request
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))

        count = await delivery.deliver_batch()
        assert count == 1
        assert delivery.delivered_count == 1

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_DELIVERED

    @pytest.mark.asyncio
    async def test_deliver_multiple(self, delivery: DeliveryManager, queue: PersistentQueue):
        for i in range(3):
            req = MechRequest(request_id=f"r{i}", prompt="test", tool="echo")
            queue.add_request(req)
            queue.mark_executing(f"r{i}")
            queue.mark_executed(f"r{i}", ToolResult(output=f"result{i}"))

        count = await delivery.deliver_batch()
        assert count == 3
        assert delivery.delivered_count == 3

    @pytest.mark.asyncio
    async def test_does_not_redeliver(self, delivery: DeliveryManager, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="test", tool="echo")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="result"))

        count1 = await delivery.deliver_batch()
        count2 = await delivery.deliver_batch()
        assert count1 == 1
        assert count2 == 0

    def test_stop(self, delivery: DeliveryManager):
        delivery._running = True
        delivery.stop()
        assert delivery._running is False
