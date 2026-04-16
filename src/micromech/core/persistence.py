"""Persistent request queue backed by SQLite via Peewee.

Guarantees: no lost requests across restarts. Uses WAL mode with synchronous=NORMAL
for crash safety (WAL+NORMAL protects against process crashes) and concurrent read access.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import peewee as pw
from loguru import logger

from micromech.core.constants import (
    DEFAULT_CHAIN,
    STATUS_DELIVERED,
    STATUS_EXECUTED,
    STATUS_EXECUTING,
    STATUS_FAILED,
    STATUS_PENDING,
)
from micromech.core.errors import PersistenceError
from micromech.core.models import MechRequest, MechResponse, RequestRecord, ToolResult


class RequestRow(pw.Model):
    """Persistent storage for mech requests and their lifecycle."""

    request_id = pw.CharField(primary_key=True)
    chain = pw.CharField(default=DEFAULT_CHAIN, index=True)
    sender = pw.CharField(default="")
    data = pw.BlobField(default=b"")
    prompt = pw.TextField(default="")
    tool = pw.CharField(default="")
    extra_params = pw.TextField(default="{}")
    created_at = pw.DateTimeField(index=True)
    timeout = pw.IntegerField(default=300)
    delivery_method = pw.CharField(default="marketplace")
    is_offchain = pw.BooleanField(default=False)
    status = pw.CharField(default=STATUS_PENDING, index=True)
    error = pw.TextField(null=True)

    # Tool result (populated after execution)
    result_output = pw.TextField(null=True)
    result_time = pw.FloatField(null=True)
    result_error = pw.TextField(null=True)
    result_metadata = pw.TextField(default="{}")

    # Delivery (populated after delivery)
    ipfs_hash = pw.CharField(null=True)
    delivery_tx_hash = pw.CharField(null=True)
    delivered_at = pw.DateTimeField(null=True)

    updated_at = pw.DateTimeField(index=True)

    class Meta:
        table_name = "requests"
        indexes = (
            (("status", "created_at"), False),
            (("chain", "status", "created_at"), False),
        )


def _chain_filter(query: pw.ModelSelect, chain: Optional[str]) -> pw.ModelSelect:
    """Apply chain filter if specified. None means all chains."""
    if chain is not None:
        return query.where(RequestRow.chain == chain)
    return query


class PersistentQueue:
    """SQLite-backed request queue with no-loss guarantee.

    Each instance creates its own database connection. Only one PersistentQueue
    should be active per database file in a given process.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = pw.SqliteDatabase(
            str(db_path),
            pragmas={
                "journal_mode": "wal",
                "cache_size": -64 * 1024,  # 64MB
                "synchronous": "normal",
            },
        )
        RequestRow.bind(self._db)
        self._db.connect()
        self._db.create_tables([RequestRow])
        self._migrate()
        logger.info("Database initialized at {}", db_path)

    def _migrate(self) -> None:
        """Auto-migrate: add chain column if missing (for existing single-chain DBs)."""
        try:
            self._db.execute_sql("SELECT chain FROM requests LIMIT 1")
        except pw.OperationalError:
            self._db.execute_sql(
                f"ALTER TABLE requests ADD COLUMN chain VARCHAR(32) DEFAULT '{DEFAULT_CHAIN}'"
            )
            self._db.execute_sql("CREATE INDEX IF NOT EXISTS idx_requests_chain ON requests(chain)")
            logger.info("Migrated database: added chain column")

    def close(self) -> None:
        if not self._db.is_closed():
            self._db.close()

    def add_request(self, request: MechRequest) -> None:
        """Insert a new request. Idempotent — skips if request_id exists."""
        now = datetime.now(timezone.utc)
        try:
            RequestRow.create(
                request_id=request.request_id,
                chain=request.chain,
                sender=request.sender,
                data=request.data,
                prompt=request.prompt,
                tool=request.tool,
                extra_params=json.dumps(request.extra_params),
                created_at=request.created_at,
                timeout=request.timeout,
                delivery_method=request.delivery_method,
                is_offchain=request.is_offchain,
                status=request.status,
                error=request.error,
                updated_at=now,
            )
            logger.debug("Persisted request {} (chain={})", request.request_id, request.chain)
        except pw.IntegrityError:
            logger.debug("Request {} already exists, skipping", request.request_id)

    def mark_executing(self, request_id: str) -> None:
        """Mark request as currently executing. Must be in PENDING or EXECUTING state."""
        rows = (
            RequestRow.update(
                status=STATUS_EXECUTING,
                updated_at=datetime.now(timezone.utc),
            )
            .where(
                RequestRow.request_id == request_id,
                RequestRow.status.in_([STATUS_PENDING, STATUS_EXECUTING]),
            )
            .execute()
        )
        if rows == 0:
            raise PersistenceError(
                f"Cannot mark {request_id} as executing: not found or invalid state"
            )

    def mark_executed(self, request_id: str, result: ToolResult) -> None:
        """Store tool result. Must be in EXECUTING state."""
        status = STATUS_EXECUTED if result.success else STATUS_FAILED
        rows = (
            RequestRow.update(
                status=status,
                result_output=result.output,
                result_time=result.execution_time,
                result_error=result.error,
                result_metadata=json.dumps(result.metadata),
                updated_at=datetime.now(timezone.utc),
            )
            .where(
                RequestRow.request_id == request_id,
                RequestRow.status == STATUS_EXECUTING,
            )
            .execute()
        )
        if rows == 0:
            raise PersistenceError(
                f"Cannot mark {request_id} as executed: not found or not in executing state"
            )

    def mark_delivered(
        self, request_id: str, tx_hash: str, ipfs_hash: Optional[str] = None
    ) -> None:
        """Mark request as delivered on-chain. Must be in EXECUTED state."""
        now = datetime.now(timezone.utc)
        rows = (
            RequestRow.update(
                status=STATUS_DELIVERED,
                delivery_tx_hash=tx_hash,
                ipfs_hash=ipfs_hash,
                delivered_at=now,
                updated_at=now,
            )
            .where(
                RequestRow.request_id == request_id,
                RequestRow.status == STATUS_EXECUTED,
            )
            .execute()
        )
        if rows == 0:
            raise PersistenceError(
                f"Cannot mark {request_id} as delivered: not found or not in executed state"
            )

    def mark_failed(self, request_id: str, error: str) -> None:
        """Mark request as permanently failed."""
        rows = (
            RequestRow.update(
                status=STATUS_FAILED,
                error=error,
                updated_at=datetime.now(timezone.utc),
            )
            .where(RequestRow.request_id == request_id)
            .execute()
        )
        if rows == 0:
            raise PersistenceError(f"Cannot mark {request_id} as failed: not found")

    def get_pending(self) -> list[RequestRecord]:
        """Get all pending requests (for recovery on restart)."""
        return self._query_by_status(STATUS_PENDING)

    def get_executing(self) -> list[RequestRecord]:
        """Get requests stuck in executing (interrupted by crash)."""
        return self._query_by_status(STATUS_EXECUTING)

    def get_undelivered(self, limit: int = 10, chain: Optional[str] = None) -> list[RequestRecord]:
        """Get executed but not yet delivered requests, optionally filtered by chain."""
        query = (
            RequestRow.select()
            .where(RequestRow.status == STATUS_EXECUTED)
            .order_by(RequestRow.created_at.asc())
        )
        query = _chain_filter(query, chain)
        return [self._row_to_record(r) for r in query.limit(limit)]

    def get_by_id(self, request_id: str) -> Optional[RequestRecord]:
        """Get a single request by ID."""
        try:
            row = RequestRow.get_by_id(request_id)
            return self._row_to_record(row)
        except RequestRow.DoesNotExist:
            return None

    def get_recent(self, limit: int = 50, chain: Optional[str] = None) -> list[RequestRecord]:
        """Get recent requests across all statuses, optionally filtered by chain."""
        query = RequestRow.select().order_by(RequestRow.created_at.desc())
        query = _chain_filter(query, chain)
        return [self._row_to_record(r) for r in query.limit(limit)]

    def count_by_status(self, chain: Optional[str] = None) -> dict[str, int]:
        """Count requests grouped by status. Returns 0 for all known statuses."""
        all_statuses = {
            STATUS_PENDING: 0,
            STATUS_EXECUTING: 0,
            STATUS_EXECUTED: 0,
            STATUS_DELIVERED: 0,
            STATUS_FAILED: 0,
        }
        query = RequestRow.select(
            RequestRow.status, pw.fn.COUNT(RequestRow.request_id).alias("cnt")
        ).group_by(RequestRow.status)
        query = _chain_filter(query, chain)
        for row in query:
            all_statuses[row.status] = row.cnt
        return all_statuses

    def count_by_chain(self) -> dict[str, int]:
        """Count total requests per chain."""
        result: dict[str, int] = {}
        for row in RequestRow.select(
            RequestRow.chain, pw.fn.COUNT(RequestRow.request_id).alias("cnt")
        ).group_by(RequestRow.chain):
            result[row.chain] = row.cnt
        return result

    def tool_stats(self, chain: Optional[str] = None) -> list[dict]:
        """Per-tool stats: count, success count, avg execution time."""
        query = (
            RequestRow.select(
                RequestRow.tool,
                pw.fn.COUNT(RequestRow.request_id).alias("total"),
                pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_DELIVERED, 1)], 0)).alias(
                    "delivered"
                ),
                pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_FAILED, 1)], 0)).alias(
                    "failed"
                ),
                pw.fn.AVG(RequestRow.result_time).alias("avg_time"),
            )
            .where(RequestRow.tool != "")
            .group_by(RequestRow.tool)
            .order_by(pw.fn.COUNT(RequestRow.request_id).desc())
        )
        query = _chain_filter(query, chain)
        return [
            {
                "tool": r.tool,
                "total": r.total,
                "delivered": int(r.delivered or 0),
                "failed": int(r.failed or 0),
                "avg_time": round(float(r.avg_time or 0), 3),
            }
            for r in query
        ]

    def _time_series_stats(
        self,
        group_expr: Any,
        label: str,
        cutoff: datetime,
        chain: Optional[str] = None,
    ) -> list[dict]:
        """Generic time-series aggregation (shared by daily/monthly)."""
        query = (
            RequestRow.select(
                group_expr.alias(label),
                pw.fn.COUNT(RequestRow.request_id).alias("total"),
                pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_DELIVERED, 1)], 0)).alias(
                    "delivered"
                ),
                pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_FAILED, 1)], 0)).alias(
                    "failed"
                ),
            )
            .where(RequestRow.created_at >= cutoff)
            .group_by(group_expr)
            .order_by(group_expr.asc())
        )
        query = _chain_filter(query, chain)
        return [
            {
                label: str(getattr(r, label)),
                "total": r.total,
                "delivered": int(r.delivered or 0),
                "failed": int(r.failed or 0),
            }
            for r in query
        ]

    def daily_stats(self, days: int = 30, chain: Optional[str] = None) -> list[dict]:
        """Daily request counts for the last N days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return self._time_series_stats(
            pw.fn.DATE(RequestRow.created_at),
            "day",
            cutoff,
            chain,
        )

    def monthly_stats(self, months: int = 12, chain: Optional[str] = None) -> list[dict]:
        """Monthly request counts."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=months * 31)
        return self._time_series_stats(
            pw.fn.strftime("%Y-%m", RequestRow.created_at),
            "month",
            cutoff,
            chain,
        )

    def onchain_offchain_counts(self, chain: Optional[str] = None) -> dict[str, int]:
        """Count on-chain vs off-chain requests."""
        base_q = _chain_filter(RequestRow.select(), chain)
        onchain = base_q.where(RequestRow.is_offchain == False).count()  # noqa: E712
        offchain = base_q.where(RequestRow.is_offchain == True).count()  # noqa: E712
        return {"onchain": onchain, "offchain": offchain}

    def count_delivered_since(
        self,
        hours: int = 24,
        chain: Optional[str] = None,
    ) -> int:
        """Count delivered requests in the last N hours."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = RequestRow.select().where(
            RequestRow.status == STATUS_DELIVERED,
            RequestRow.delivered_at >= cutoff,
        )
        query = _chain_filter(query, chain)
        return query.count()

    def period_stats(
        self,
        hours: int = 24,
        chain: Optional[str] = None,
    ) -> dict:
        """Aggregate stats for the last N hours from DB.

        Returns received, delivered, failed counts plus avg execution time
        and success rate — all sourced from persistent storage so they
        survive container restarts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = RequestRow.select(
            pw.fn.COUNT(RequestRow.request_id).alias("total"),
            pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_DELIVERED, 1)], 0)).alias(
                "delivered"
            ),
            pw.fn.SUM(pw.Case(None, [(RequestRow.status == STATUS_FAILED, 1)], 0)).alias("failed"),
            pw.fn.AVG(RequestRow.result_time).alias("avg_time"),
        ).where(RequestRow.created_at >= cutoff)
        query = _chain_filter(query, chain)
        row = query.tuples().first()
        if not row:
            return {
                "received": 0,
                "delivered": 0,
                "failed": 0,
                "avg_time": 0.0,
                "success_rate": 0.0,
            }
        total, delivered, failed, avg_time = row
        total = int(total or 0)
        delivered = int(delivered or 0)
        failed = int(failed or 0)
        avg_time = round(float(avg_time or 0), 3)
        success_rate = round(delivered / total * 100, 1) if total > 0 else 0.0
        return {
            "received": total,
            "delivered": delivered,
            "failed": failed,
            "avg_time": avg_time,
            "success_rate": success_rate,
        }

    def cleanup(self, days: int = 30) -> int:
        """Remove delivered/failed requests older than N days. Returns count deleted."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        deleted = (
            RequestRow.delete()
            .where(
                RequestRow.status.in_([STATUS_DELIVERED, STATUS_FAILED]),
                RequestRow.updated_at < cutoff,
            )
            .execute()
        )
        if deleted:
            logger.info("Cleaned up {} old records", deleted)
        return deleted

    def _query_by_status(self, status: str) -> list[RequestRecord]:
        rows = (
            RequestRow.select()
            .where(RequestRow.status == status)
            .order_by(RequestRow.created_at.asc())
        )
        return [self._row_to_record(r) for r in rows]

    @staticmethod
    def _row_to_record(row: RequestRow) -> RequestRecord:
        # Ensure timezone-aware datetimes from SQLite
        created_at = row.created_at
        if created_at and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        updated_at = row.updated_at
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        request = MechRequest.model_construct(
            request_id=row.request_id,
            chain=row.chain,
            sender=row.sender,
            data=bytes(row.data) if row.data else b"",
            prompt=row.prompt,
            tool=row.tool,
            extra_params=json.loads(row.extra_params) if row.extra_params else {},
            created_at=created_at,
            timeout=row.timeout,
            delivery_method=row.delivery_method,
            is_offchain=row.is_offchain,
            status=row.status,
            error=row.error,
        )

        result = None
        if row.result_output is not None or row.result_error is not None:
            result = ToolResult.model_construct(
                output=row.result_output or "",
                execution_time=row.result_time or 0.0,
                error=row.result_error,
                metadata=json.loads(row.result_metadata) if row.result_metadata else {},
            )

        response = None
        if row.delivery_tx_hash:
            delivered_at = row.delivered_at
            if delivered_at and delivered_at.tzinfo is None:
                delivered_at = delivered_at.replace(tzinfo=timezone.utc)
            response = MechResponse.model_construct(
                request_id=row.request_id,
                result=row.result_output or "",
                ipfs_hash=row.ipfs_hash,
                delivery_tx_hash=row.delivery_tx_hash,
                delivered_at=delivered_at,
            )

        return RequestRecord.model_construct(
            request=request,
            result=result,
            response=response,
            updated_at=updated_at,
        )
