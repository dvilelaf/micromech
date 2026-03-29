"""Persistent request queue backed by SQLite via Peewee.

Guarantees: no lost requests across restarts. Uses WAL mode with synchronous=FULL
for crash safety and concurrent read access.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import peewee as pw
from loguru import logger

from micromech.core.constants import (
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
                "synchronous": "full",
            },
        )
        RequestRow.bind(self._db)
        self._db.connect()
        self._db.create_tables([RequestRow])
        logger.info("Database initialized at {}", db_path)

    def close(self) -> None:
        if not self._db.is_closed():
            self._db.close()

    def add_request(self, request: MechRequest) -> None:
        """Insert a new request. Idempotent — skips if request_id exists."""
        now = datetime.now(timezone.utc)
        try:
            RequestRow.create(
                request_id=request.request_id,
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
            logger.debug("Persisted request {}", request.request_id)
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

    def get_undelivered(self, limit: int = 10) -> list[RequestRecord]:
        """Get executed but not yet delivered requests."""
        rows = (
            RequestRow.select()
            .where(RequestRow.status == STATUS_EXECUTED)
            .order_by(RequestRow.created_at.asc())
            .limit(limit)
        )
        return [self._row_to_record(r) for r in rows]

    def get_by_id(self, request_id: str) -> Optional[RequestRecord]:
        """Get a single request by ID."""
        try:
            row = RequestRow.get_by_id(request_id)
            return self._row_to_record(row)
        except RequestRow.DoesNotExist:
            return None

    def get_recent(self, limit: int = 50) -> list[RequestRecord]:
        """Get recent requests across all statuses."""
        rows = RequestRow.select().order_by(RequestRow.created_at.desc()).limit(limit)
        return [self._row_to_record(r) for r in rows]

    def count_by_status(self) -> dict[str, int]:
        """Count requests grouped by status. Returns 0 for all known statuses."""
        all_statuses = {
            STATUS_PENDING: 0,
            STATUS_EXECUTING: 0,
            STATUS_EXECUTED: 0,
            STATUS_DELIVERED: 0,
            STATUS_FAILED: 0,
        }
        for row in RequestRow.select(
            RequestRow.status, pw.fn.COUNT(RequestRow.request_id).alias("cnt")
        ).group_by(RequestRow.status):
            all_statuses[row.status] = row.cnt
        return all_statuses

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
