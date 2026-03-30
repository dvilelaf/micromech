"""HTTP endpoints for off-chain requests and status.

Provides a FastAPI app for:
- POST /request — submit off-chain requests
- GET /status — server health and queue stats
- GET /health — simple health check
"""

import uuid
from typing import Any, Callable, Optional

from loguru import logger
from pydantic import BaseModel, Field

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
except ImportError as e:
    raise ImportError(
        "HTTP server requires fastapi. Install with: pip install micromech[web]"
    ) from e

from micromech.core.constants import validate_eth_address
from micromech.core.models import MechRequest

MAX_PROMPT_LENGTH = 10_000


class RequestPayload(BaseModel):
    """HTTP request payload (Valory-compatible)."""

    prompt: str = Field(max_length=MAX_PROMPT_LENGTH)
    tool: str = "echo"
    request_id: Optional[str] = None
    sender: Optional[str] = None
    signature: Optional[str] = None  # hex-encoded signature for off-chain delivery
    extra_params: dict[str, Any] = Field(default_factory=dict)


class StatusResponse(BaseModel):
    """Server status response."""

    status: str = "running"
    queue: dict[str, int] = Field(default_factory=dict)
    tools: list[str] = Field(default_factory=list)
    delivered_total: int = 0


class ResultResponse(BaseModel):
    """Response for GET /result/{request_id}."""

    request_id: str
    status: str
    tool: str = ""
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None


def create_app(
    on_request: Callable,
    get_status: Callable,
    get_result: Optional[Callable] = None,
) -> FastAPI:
    """Create the FastAPI app with endpoints.

    Args:
        on_request: async callback(MechRequest) for new requests.
        get_status: callable returning StatusResponse dict.
        get_result: optional callable(request_id) returning RequestRecord or None.
    """
    app = FastAPI(title="micromech", version="0.0.1")

    @app.post("/request")
    async def submit_request(payload: RequestPayload) -> JSONResponse:
        """Submit an off-chain request."""
        request_id = payload.request_id or f"http-{uuid.uuid4().hex[:12]}"
        sender = payload.sender or ""

        try:
            if sender:
                validate_eth_address(sender)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid sender address")

        req = MechRequest(
            request_id=request_id,
            sender=sender,
            prompt=payload.prompt,
            tool=payload.tool,
            extra_params=payload.extra_params,
            is_offchain=True,
            signature=payload.signature,
        )

        try:
            await on_request(req)
            logger.info("HTTP request accepted: {} (tool={})", request_id, payload.tool)
            return JSONResponse(
                status_code=202,
                content={"request_id": request_id, "status": "accepted"},
            )
        except Exception as e:
            logger.error("Failed to accept request {}: {}", request_id, e)
            raise HTTPException(status_code=500, detail="Internal server error")

    @app.get("/status")
    async def server_status() -> StatusResponse:
        """Get server status and queue stats."""
        return StatusResponse(**get_status())

    @app.get("/result/{request_id}")
    async def get_request_result(request_id: str) -> ResultResponse:
        """Get the result for a specific request.

        Returns the tool output once executed, or current status if still pending.
        """
        if get_result is None:
            raise HTTPException(status_code=501, detail="Result lookup not configured")

        record = get_result(request_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Request not found")

        import json as _json

        response = ResultResponse(
            request_id=request_id,
            status=record.request.status,
            tool=record.request.tool,
            error=record.request.error,
        )

        if record.result:
            response.execution_time = record.result.execution_time
            if record.result.error:
                response.error = record.result.error
            else:
                try:
                    response.result = _json.loads(record.result.output)
                except (ValueError, TypeError):
                    response.result = {"raw": record.result.output}

        return response

    @app.get("/health")
    async def health_check() -> dict:
        """Simple health check."""
        return {"status": "ok"}

    return app
