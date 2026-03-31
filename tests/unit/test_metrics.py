"""Tests for MetricsCollector."""

import time

from micromech.runtime.metrics import MetricsCollector


class TestMetricsCollector:
    def test_initial_state(self):
        m = MetricsCollector()
        assert m.requests_received == 0
        assert m.executions_completed == 0
        assert m.deliveries_completed == 0
        assert m.uptime_seconds >= 0
        assert m.avg_execution_time == 0.0
        assert m.p95_execution_time == 0.0
        assert m.success_rate == 100.0

    def test_record_request_received(self):
        m = MetricsCollector()
        m.record_request_received("req-1", "echo", False)
        assert m.requests_received == 1
        events = m.get_recent_events()
        assert len(events) == 1
        assert events[0]["event"] == "request_received"
        assert events[0]["request_id"] == "req-1"
        assert events[0]["tool"] == "echo"

    def test_record_execution_lifecycle(self):
        m = MetricsCollector()
        m.record_execution_started("req-1", "echo")
        assert m.executions_started == 1

        m.record_execution_done("req-1", "echo", 1.5)
        assert m.executions_completed == 1
        assert m.avg_execution_time == 1.5
        assert m.p95_execution_time == 1.5

    def test_record_execution_failed(self):
        m = MetricsCollector()
        m.record_execution_failed("req-1", "echo", "timeout", 2.0)
        assert m.executions_failed == 1
        assert m.success_rate == 0.0

    def test_success_rate_calculation(self):
        m = MetricsCollector()
        m.record_execution_done("r1", "echo", 1.0)
        m.record_execution_done("r2", "echo", 1.0)
        m.record_execution_failed("r3", "echo", "err", 1.0)
        # 2 success, 1 fail = 66.7%
        assert abs(m.success_rate - 66.7) < 0.1

    def test_record_delivery(self):
        m = MetricsCollector()
        m.record_delivery("req-1")
        assert m.deliveries_completed == 1

    def test_record_delivery_failed(self):
        m = MetricsCollector()
        m.record_delivery_failed("req-1", "tx reverted")
        assert m.deliveries_failed == 1

    def test_get_live_snapshot(self):
        m = MetricsCollector()
        m.record_request_received("r1", "echo", False)
        m.record_execution_done("r1", "echo", 2.0)
        m.record_delivery("r1")

        snap = m.get_live_snapshot()
        assert snap["requests_received"] == 1
        assert snap["executions_completed"] == 1
        assert snap["deliveries_completed"] == 1
        assert snap["avg_execution_time"] == 2.0
        assert snap["success_rate"] == 100.0
        assert snap["uptime"] >= 0

    def test_get_events_since(self):
        m = MetricsCollector()
        before = time.time()
        m.record_request_received("r1", "echo", False)
        m.record_request_received("r2", "echo", True)

        events = m.get_events_since(before - 1)
        assert len(events) == 2

        events = m.get_events_since(time.time() + 1)
        assert len(events) == 0

    def test_get_recent_events_limit(self):
        m = MetricsCollector()
        for i in range(10):
            m.record_request_received(f"r{i}", "echo", False)

        events = m.get_recent_events(3)
        assert len(events) == 3
        # Should be the 3 most recent
        assert events[-1]["request_id"] == "r9"

    def test_events_deque_maxlen(self):
        m = MetricsCollector()
        for i in range(250):
            m.record_request_received(f"r{i}", "echo", False)

        events = m.get_recent_events(300)
        assert len(events) == 200  # maxlen=200

    def test_p95_execution_time(self):
        m = MetricsCollector()
        # Add 20 executions: 19 fast + 1 slow
        for _ in range(19):
            m.record_execution_done("r", "echo", 1.0)
        m.record_execution_done("r", "echo", 10.0)

        assert m.p95_execution_time == 10.0
        assert abs(m.avg_execution_time - 1.45) < 0.01

    def test_event_to_dict_format(self):
        m = MetricsCollector()
        m.record_request_received("req-abc", "llm", True)
        ev = m.get_recent_events(1)[0]
        assert "timestamp" in ev
        assert "iso" in ev
        assert ev["event"] == "request_received"
        assert ev["request_id"] == "req-abc"
        assert ev["tool"] == "llm"
        assert ev["is_offchain"] is True

    def test_chain_recorded_in_events(self):
        m = MetricsCollector()
        m.record_request_received("r1", "echo", False, chain="gnosis")
        m.record_execution_started("r2", "llm", chain="base")
        m.record_execution_done("r3", "echo", 1.0, chain="ethereum")
        m.record_execution_failed("r4", "echo", "err", chain="polygon")
        m.record_delivery("r5", chain="optimism")
        m.record_delivery_failed("r6", "err", chain="arbitrum")

        events = m.get_recent_events(10)
        assert events[0]["chain"] == "gnosis"
        assert events[1]["chain"] == "base"
        assert events[2]["chain"] == "ethereum"
        assert events[3]["chain"] == "polygon"
        assert events[4]["chain"] == "optimism"
        assert events[5]["chain"] == "arbitrum"

    def test_chain_default_empty_string(self):
        m = MetricsCollector()
        m.record_request_received("r1", "echo", False)
        ev = m.get_recent_events(1)[0]
        assert ev["chain"] == ""
