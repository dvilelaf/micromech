"""Tests for the persistent request queue."""

from datetime import datetime, timedelta, timezone

import pytest

from micromech.core.constants import (
    STATUS_DELIVERED,
    STATUS_EXECUTED,
    STATUS_EXECUTING,
    STATUS_FAILED,
    STATUS_PENDING,
)
from micromech.core.errors import PersistenceError
from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue


class TestPersistentQueueBasics:
    def test_create_db(self, queue: PersistentQueue):
        assert queue.db_path.exists()

    def test_add_and_get(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="hello", tool="echo")
        queue.add_request(req)
        record = queue.get_by_id("r1")
        assert record is not None
        assert record.request.request_id == "r1"
        assert record.request.prompt == "hello"
        assert record.request.tool == "echo"
        assert record.request.status == STATUS_PENDING

    def test_add_idempotent(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1", prompt="hello")
        queue.add_request(req)
        queue.add_request(req)  # should not raise
        pending = queue.get_pending()
        assert len(pending) == 1

    def test_get_nonexistent(self, queue: PersistentQueue):
        assert queue.get_by_id("nonexistent") is None


class TestStatusTransitions:
    def test_pending_to_executing(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")
        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTING

    def test_executing_to_executed(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")

        result = ToolResult(output="answer", execution_time=1.5)
        queue.mark_executed("r1", result)

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTED
        assert record.result is not None
        assert record.result.output == "answer"
        assert record.result.execution_time == 1.5

    def test_executed_with_error_is_failed(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")

        result = ToolResult(error="tool crashed", execution_time=0.1)
        queue.mark_executed("r1", result)

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED
        assert record.result.error == "tool crashed"

    def test_executed_to_delivered(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))
        queue.mark_delivered("r1", tx_hash="0x" + "f" * 64, ipfs_hash="QmTest")

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_DELIVERED
        assert record.response is not None
        assert record.response.delivery_tx_hash == "0x" + "f" * 64
        assert record.response.ipfs_hash == "QmTest"
        assert record.response.delivered_at is not None

    def test_mark_failed(self, queue: PersistentQueue):
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_failed("r1", "delivery timeout")

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED
        assert record.request.error == "delivery timeout"

    def test_mark_timed_out(self, queue: PersistentQueue):
        """mark_timed_out stores FAILED status with on_chain_timeout error and tx_hash."""
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))

        tx = "0x" + "ab" * 32
        queue.mark_timed_out("r1", tx_hash=tx, ipfs_hash="QmTimeout")

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED
        assert record.request.error == "on_chain_timeout"
        # tx_hash and ipfs_hash are stored for traceability
        assert record.response is not None
        assert record.response.delivery_tx_hash == tx
        assert record.response.ipfs_hash == "QmTimeout"

    def test_mark_timed_out_without_ipfs(self, queue: PersistentQueue):
        """mark_timed_out works when ipfs_hash is None."""
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))

        queue.mark_timed_out("r1", tx_hash="0x" + "cc" * 32)

        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_FAILED
        assert record.request.error == "on_chain_timeout"


class TestStateTransitionGuards:
    """Test that invalid state transitions are rejected."""

    def test_executing_nonexistent_raises(self, queue: PersistentQueue):
        with pytest.raises(PersistenceError, match="not found"):
            queue.mark_executing("nonexistent")

    def test_executed_without_executing_raises(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1"))
        with pytest.raises(PersistenceError, match="not in executing"):
            queue.mark_executed("r1", ToolResult(output="ok"))

    def test_delivered_without_executed_raises(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1"))
        queue.mark_executing("r1")
        with pytest.raises(PersistenceError, match="not in executed"):
            queue.mark_delivered("r1", tx_hash="0x" + "0" * 64)

    def test_timed_out_on_pending_raises(self, queue: PersistentQueue):
        """mark_timed_out requires EXECUTED state, not PENDING."""
        queue.add_request(MechRequest(request_id="r1"))
        with pytest.raises(PersistenceError, match="not in executed"):
            queue.mark_timed_out("r1", tx_hash="0x" + "0" * 64)

    def test_timed_out_on_nonexistent_raises(self, queue: PersistentQueue):
        with pytest.raises(PersistenceError, match="not found"):
            queue.mark_timed_out("nonexistent", tx_hash="0x" + "0" * 64)

    def test_delivered_on_pending_raises(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1"))
        with pytest.raises(PersistenceError, match="not in executed"):
            queue.mark_delivered("r1", tx_hash="0x" + "0" * 64)

    def test_double_executing_is_idempotent(self, queue: PersistentQueue):
        """Calling mark_executing on already-executing should not raise."""
        queue.add_request(MechRequest(request_id="r1"))
        queue.mark_executing("r1")
        queue.mark_executing("r1")  # should not raise
        record = queue.get_by_id("r1")
        assert record.request.status == STATUS_EXECUTING

    def test_failed_nonexistent_raises(self, queue: PersistentQueue):
        with pytest.raises(PersistenceError, match="not found"):
            queue.mark_failed("nonexistent", "error")

    def test_executing_delivered_raises(self, queue: PersistentQueue):
        """Cannot go back from delivered to executing."""
        queue.add_request(MechRequest(request_id="r1"))
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok"))
        queue.mark_delivered("r1", tx_hash="0x" + "0" * 64)
        with pytest.raises(PersistenceError):
            queue.mark_executing("r1")


class TestQueries:
    def _seed(self, queue: PersistentQueue) -> None:
        """Seed queue with requests in various states."""
        for i in range(3):
            queue.add_request(MechRequest(request_id=f"pending-{i}"))

        queue.add_request(MechRequest(request_id="exec-1"))
        queue.mark_executing("exec-1")

        queue.add_request(MechRequest(request_id="done-1"))
        queue.mark_executing("done-1")
        queue.mark_executed("done-1", ToolResult(output="yes"))

        queue.add_request(MechRequest(request_id="done-2"))
        queue.mark_executing("done-2")
        queue.mark_executed("done-2", ToolResult(output="no"))

        queue.add_request(MechRequest(request_id="delivered-1"))
        queue.mark_executing("delivered-1")
        queue.mark_executed("delivered-1", ToolResult(output="ok"))
        queue.mark_delivered("delivered-1", "0x" + "0" * 64)

    def test_get_pending(self, queue: PersistentQueue):
        self._seed(queue)
        pending = queue.get_pending()
        assert len(pending) == 3
        assert all(r.request.status == STATUS_PENDING for r in pending)

    def test_get_executing(self, queue: PersistentQueue):
        self._seed(queue)
        executing = queue.get_executing()
        assert len(executing) == 1
        assert executing[0].request.request_id == "exec-1"

    def test_get_undelivered(self, queue: PersistentQueue):
        self._seed(queue)
        undelivered = queue.get_undelivered()
        assert len(undelivered) == 2
        ids = {r.request.request_id for r in undelivered}
        assert ids == {"done-1", "done-2"}

    def test_get_undelivered_limit(self, queue: PersistentQueue):
        self._seed(queue)
        undelivered = queue.get_undelivered(limit=1)
        assert len(undelivered) == 1

    def test_get_recent(self, queue: PersistentQueue):
        self._seed(queue)
        recent = queue.get_recent(limit=100)
        assert len(recent) == 7

    def test_count_by_status(self, queue: PersistentQueue):
        self._seed(queue)
        counts = queue.count_by_status()
        assert counts[STATUS_PENDING] == 3
        assert counts[STATUS_EXECUTING] == 1
        assert counts[STATUS_EXECUTED] == 2
        assert counts[STATUS_DELIVERED] == 1

    def test_count_by_status_empty_db(self, queue: PersistentQueue):
        counts = queue.count_by_status()
        assert counts[STATUS_PENDING] == 0
        assert counts[STATUS_EXECUTING] == 0
        assert counts[STATUS_EXECUTED] == 0
        assert counts[STATUS_DELIVERED] == 0
        assert counts[STATUS_FAILED] == 0


class TestCleanup:
    def test_cleanup_old_records(self, queue: PersistentQueue):
        req = MechRequest(
            request_id="old-1",
            created_at=datetime.now(timezone.utc) - timedelta(days=60),
        )
        queue.add_request(req)
        queue.mark_executing("old-1")
        queue.mark_executed("old-1", ToolResult(output="ok"))
        queue.mark_delivered("old-1", "0x" + "0" * 64)

        from micromech.core.persistence import RequestRow

        RequestRow.update(updated_at=datetime.now(timezone.utc) - timedelta(days=60)).where(
            RequestRow.request_id == "old-1"
        ).execute()

        req2 = MechRequest(request_id="new-1")
        queue.add_request(req2)
        queue.mark_executing("new-1")
        queue.mark_executed("new-1", ToolResult(output="ok"))
        queue.mark_delivered("new-1", "0x" + "1" * 64)

        deleted = queue.cleanup(days=30)
        assert deleted == 1
        assert queue.get_by_id("old-1") is None
        assert queue.get_by_id("new-1") is not None

    def test_cleanup_does_not_touch_pending(self, queue: PersistentQueue):
        req = MechRequest(request_id="old-pending")
        queue.add_request(req)

        from micromech.core.persistence import RequestRow

        RequestRow.update(updated_at=datetime.now(timezone.utc) - timedelta(days=60)).where(
            RequestRow.request_id == "old-pending"
        ).execute()

        deleted = queue.cleanup(days=30)
        assert deleted == 0
        assert queue.get_by_id("old-pending") is not None


class TestRecovery:
    """Test restart recovery scenarios."""

    def test_recover_pending_on_restart(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1", prompt="q1", tool="echo"))
        queue.add_request(MechRequest(request_id="r2", prompt="q2", tool="llm"))

        queue.close()
        queue2 = PersistentQueue(queue.db_path)
        try:
            pending = queue2.get_pending()
            assert len(pending) == 2
            assert {r.request.request_id for r in pending} == {"r1", "r2"}
        finally:
            queue2.close()

    def test_recover_executing_as_interrupted(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1"))
        queue.mark_executing("r1")

        queue.close()
        queue2 = PersistentQueue(queue.db_path)
        try:
            executing = queue2.get_executing()
            assert len(executing) == 1
            assert executing[0].request.request_id == "r1"
        finally:
            queue2.close()

    def test_recover_undelivered(self, queue: PersistentQueue):
        queue.add_request(MechRequest(request_id="r1"))
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="answer"))

        queue.close()
        queue2 = PersistentQueue(queue.db_path)
        try:
            undelivered = queue2.get_undelivered()
            assert len(undelivered) == 1
            assert undelivered[0].result.output == "answer"
        finally:
            queue2.close()


class TestDataIntegrity:
    """Test that complex data survives persistence roundtrip."""

    def test_extra_params_roundtrip(self, queue: PersistentQueue):
        params = {"model": "qwen", "temperature": 0.7, "nested": {"key": [1, 2, 3]}}
        req = MechRequest(request_id="r1", extra_params=params)
        queue.add_request(req)
        record = queue.get_by_id("r1")
        assert record.request.extra_params == params

    def test_binary_data_roundtrip(self, queue: PersistentQueue):
        data = bytes(range(256))
        req = MechRequest(request_id="r1", data=data)
        queue.add_request(req)
        record = queue.get_by_id("r1")
        assert record.request.data == data

    def test_result_metadata_roundtrip(self, queue: PersistentQueue):
        meta = {"model": "qwen", "tokens_used": 42, "cost": 0.001}
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        queue.mark_executing("r1")
        queue.mark_executed("r1", ToolResult(output="ok", metadata=meta))
        record = queue.get_by_id("r1")
        assert record.result.metadata == meta

    def test_unicode_prompt(self, queue: PersistentQueue):
        prompt = "¿Será ETH > $10k? 日本語テスト"
        req = MechRequest(request_id="r1", prompt=prompt)
        queue.add_request(req)
        record = queue.get_by_id("r1")
        assert record.request.prompt == prompt

    def test_datetime_timezone_roundtrip(self, queue: PersistentQueue):
        """Datetimes should preserve UTC timezone after roundtrip."""
        req = MechRequest(request_id="r1")
        queue.add_request(req)
        record = queue.get_by_id("r1")
        assert record.request.created_at.tzinfo is not None
        assert record.updated_at.tzinfo is not None
