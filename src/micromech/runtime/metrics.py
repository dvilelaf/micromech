"""In-memory metrics collector for the dashboard.

Tracks request lifecycle events since server start. Provides counters,
averages, and a rolling event log for real-time streaming.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class MetricsEvent:
    """A single event in the activity feed."""

    timestamp: float
    # request_received, execution_started, execution_done, delivered, failed, etc.
    event_type: str
    request_id: str
    chain: str = ""
    tool: str = ""
    is_offchain: bool = False
    execution_time: float = 0.0
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "iso": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
            "event": self.event_type,
            "request_id": self.request_id,
            "chain": self.chain,
            "tool": self.tool,
            "is_offchain": self.is_offchain,
            "execution_time": round(self.execution_time, 3) if self.execution_time else 0,
            "error": self.error,
        }


@dataclass
class MetricsCollector:
    """In-memory metrics — resets on restart, no persistence needed.

    Thread-safe for asyncio single-threaded event loop (all calls are
    non-yielding dataclass mutations).
    """

    start_time: float = field(default_factory=time.time)

    # Counters
    requests_received: int = 0
    executions_started: int = 0
    executions_completed: int = 0
    executions_failed: int = 0
    deliveries_completed: int = 0
    deliveries_failed: int = 0

    # Rolling execution times (last 1000)
    _exec_times: deque[float] = field(default_factory=lambda: deque(maxlen=1000))

    # Event log for real-time feed (last 200 events)
    _events: deque[MetricsEvent] = field(default_factory=lambda: deque(maxlen=200))

    @property
    def uptime_seconds(self) -> int:
        return int(time.time() - self.start_time)

    @property
    def avg_execution_time(self) -> float:
        if not self._exec_times:
            return 0.0
        return sum(self._exec_times) / len(self._exec_times)

    @property
    def p95_execution_time(self) -> float:
        if not self._exec_times:
            return 0.0
        sorted_times = sorted(self._exec_times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[min(idx, len(sorted_times) - 1)]

    @property
    def success_rate(self) -> float:
        total = self.executions_completed + self.executions_failed
        if total == 0:
            return 100.0
        return (self.executions_completed / total) * 100.0

    def record_request_received(
        self, request_id: str, tool: str, is_offchain: bool, chain: str = ""
    ) -> None:
        self.requests_received += 1
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="request_received",
                request_id=request_id,
                chain=chain,
                tool=tool,
                is_offchain=is_offchain,
            )
        )

    def record_execution_started(self, request_id: str, tool: str, chain: str = "") -> None:
        self.executions_started += 1
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="execution_started",
                request_id=request_id,
                chain=chain,
                tool=tool,
            )
        )

    def record_execution_done(
        self, request_id: str, tool: str, execution_time: float, chain: str = ""
    ) -> None:
        self.executions_completed += 1
        self._exec_times.append(execution_time)
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="execution_done",
                request_id=request_id,
                chain=chain,
                tool=tool,
                execution_time=execution_time,
            )
        )

    def record_execution_failed(
        self, request_id: str, tool: str, error: str,
        execution_time: float = 0.0, chain: str = "",
    ) -> None:
        self.executions_failed += 1
        if execution_time:
            self._exec_times.append(execution_time)
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="failed",
                request_id=request_id,
                chain=chain,
                tool=tool,
                error=error,
                execution_time=execution_time,
            )
        )

    def record_delivery(self, request_id: str, chain: str = "") -> None:
        self.deliveries_completed += 1
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="delivered",
                request_id=request_id,
                chain=chain,
            )
        )

    def record_delivery_failed(self, request_id: str, error: str, chain: str = "") -> None:
        self.deliveries_failed += 1
        self._events.append(
            MetricsEvent(
                timestamp=time.time(),
                event_type="delivery_failed",
                request_id=request_id,
                chain=chain,
                error=error,
            )
        )

    def get_live_snapshot(self) -> dict[str, Any]:
        """Lightweight snapshot for SSE streaming (no DB hit)."""
        return {
            "uptime": self.uptime_seconds,
            "requests_received": self.requests_received,
            "executions_started": self.executions_started,
            "executions_completed": self.executions_completed,
            "executions_failed": self.executions_failed,
            "deliveries_completed": self.deliveries_completed,
            "deliveries_failed": self.deliveries_failed,
            "avg_execution_time": round(self.avg_execution_time, 3),
            "p95_execution_time": round(self.p95_execution_time, 3),
            "success_rate": round(self.success_rate, 1),
        }

    def get_events_since(self, since_ts: float) -> list[dict[str, Any]]:
        """Get events after a timestamp (for incremental SSE updates)."""
        return [e.to_dict() for e in self._events if e.timestamp > since_ts]

    def get_recent_events(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get most recent events."""
        events = list(self._events)[-limit:]
        return [e.to_dict() for e in events]
