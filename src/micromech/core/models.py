"""Data models for requests, responses, and tool results (all Pydantic-validated)."""

from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from micromech.core.constants import (
    DEFAULT_CHAIN,
    DELIVERY_MARKETPLACE,
    STATUS_PENDING,
    validate_eth_address,
)

StatusType = Literal["pending", "executing", "executed", "delivered", "failed"]
DeliveryMethodType = Literal["marketplace", "legacy"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MechRequest(BaseModel):
    """Incoming mech request (on-chain or off-chain)."""

    request_id: str
    chain: str = DEFAULT_CHAIN
    sender: str = ""
    data: bytes = b""

    # Parsed metadata
    prompt: str = ""
    tool: str = ""
    extra_params: dict[str, Any] = Field(default_factory=dict)

    # Timing
    created_at: datetime = Field(default_factory=_utcnow)
    timeout: int = Field(default=300, ge=1)

    # Delivery
    delivery_method: DeliveryMethodType = DELIVERY_MARKETPLACE
    is_offchain: bool = False
    signature: Optional[str] = None  # hex-encoded signature for off-chain delivery

    # Status
    status: StatusType = STATUS_PENDING
    error: Optional[str] = None

    @field_validator("sender")
    @classmethod
    def validate_sender(cls, v: str) -> str:
        if v:
            validate_eth_address(v)
        return v


class ToolResult(BaseModel):
    """Result of tool execution."""

    output: str = ""
    execution_time: float = 0.0
    error: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.error is None


class MechResponse(BaseModel):
    """Final response ready for on-chain delivery."""

    request_id: str
    result: str = ""
    ipfs_hash: Optional[str] = None
    delivery_tx_hash: Optional[str] = None
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None


class RequestRecord(BaseModel):
    """Full record combining request + result + response for persistence."""

    request: MechRequest
    result: Optional[ToolResult] = None
    response: Optional[MechResponse] = None
    updated_at: datetime = Field(default_factory=_utcnow)
