"""Web UI application — dashboard, metrics API, and SSE stream."""

import asyncio
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from micromech.core.config import MicromechConfig

if TYPE_CHECKING:
    from micromech.core.persistence import PersistentQueue
    from micromech.runtime.metrics import MetricsCollector

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


def create_web_app(
    get_status: Callable[[], dict],
    get_recent: Callable,
    get_tools: Callable[[], list[dict]],
    on_request: Callable,
    queue: "PersistentQueue | None" = None,
    metrics: "MetricsCollector | None" = None,
) -> FastAPI:
    """Create the web UI FastAPI app.

    Args:
        get_status: Returns server status dict.
        get_recent: Returns recent request records.
        get_tools: Returns list of tool metadata dicts.
        on_request: Async callback for new requests.
        queue: PersistentQueue for aggregate DB queries.
        metrics: MetricsCollector for real-time in-memory metrics.
    """
    app = FastAPI(title="micromech dashboard", docs_url=None, redoc_url=None)

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request=request, name="dashboard.html")

    # --- Status API ---

    @app.get("/api/status")
    async def api_status() -> dict:
        return get_status()

    @app.get("/api/chains")
    async def api_chains() -> list[str]:
        status = get_status()
        return status.get("chains", [])

    @app.get("/api/requests")
    async def api_requests(limit: int = 50, chain: Optional[str] = None) -> list[dict]:
        records = get_recent(min(limit, 200), chain)
        return [_record_to_dict(r) for r in records]

    @app.get("/api/tools")
    async def api_tools() -> list[dict]:
        return get_tools()

    # --- Metrics API ---

    @app.get("/api/metrics/live")
    async def metrics_live() -> dict:
        """In-memory metrics snapshot (no DB hit)."""
        status = get_status()
        result: dict[str, Any] = {
            "queue": status.get("queue", {}),
            "delivered_total": status.get("delivered_total", 0),
        }
        if metrics:
            result["live"] = metrics.get_live_snapshot()
        return result

    @app.get("/api/metrics/events")
    async def metrics_events(since: float = 0, limit: int = 50) -> list[dict]:
        """Recent activity events from in-memory buffer."""
        if not metrics:
            return []
        if since > 0:
            return metrics.get_events_since(since)
        return metrics.get_recent_events(limit)

    @app.get("/api/metrics/tools")
    async def metrics_tools(chain: Optional[str] = None) -> list[dict]:
        """Per-tool aggregate stats from DB."""
        if not queue:
            return []
        return queue.tool_stats(chain=chain)

    @app.get("/api/metrics/daily")
    async def metrics_daily(days: int = 30, chain: Optional[str] = None) -> list[dict]:
        """Daily request counts from DB."""
        if not queue:
            return []
        return queue.daily_stats(min(days, 365), chain=chain)

    @app.get("/api/metrics/monthly")
    async def metrics_monthly(months: int = 12, chain: Optional[str] = None) -> list[dict]:
        """Monthly request counts from DB."""
        if not queue:
            return []
        return queue.monthly_stats(min(months, 36), chain=chain)

    @app.get("/api/metrics/channels")
    async def metrics_channels(chain: Optional[str] = None) -> dict:
        """On-chain vs off-chain request counts."""
        if not queue:
            return {"onchain": 0, "offchain": 0}
        return queue.onchain_offchain_counts(chain=chain)

    # --- SSE Stream ---

    @app.get("/api/metrics/stream")
    async def metrics_stream() -> StreamingResponse:
        """Server-Sent Events stream for real-time dashboard updates.

        Pushes:
        - Every 2s: live metrics snapshot (in-memory, no DB)
        - Every 10s: queue status (light DB query)
        - New events since last push
        """

        async def event_generator():
            last_event_ts = time.time()
            tick = 0
            while True:
                await asyncio.sleep(2)
                tick += 1

                payload: dict[str, Any] = {"type": "tick", "timestamp": time.time()}

                # Always include live metrics
                if metrics:
                    payload["live"] = metrics.get_live_snapshot()
                    # Include new events since last push
                    new_events = metrics.get_events_since(last_event_ts)
                    if new_events:
                        payload["events"] = new_events
                        last_event_ts = time.time()

                # Every 10s: include queue counts from DB
                if tick % 5 == 0:
                    status = get_status()
                    payload["queue"] = status.get("queue", {})
                    payload["delivered_total"] = status.get("delivered_total", 0)

                yield f"data: {json.dumps(payload)}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # --- Management API ---

    @app.post("/api/management/{action}")
    async def management_action(action: str, body: dict = {}) -> dict:
        """Execute a management action (stake, unstake, claim, checkpoint)."""
        try:
            from micromech.management import MechLifecycle

            config = MicromechConfig.load()
            chain = body.get("chain", "gnosis")
            lc = MechLifecycle(config, chain_name=chain)
            service_key = body.get("service_key", "")

            if action == "stake":
                ok = lc.stake(service_key, body.get("contract"))
                return {"success": ok, "action": "stake"}
            elif action == "unstake":
                ok = lc.unstake(service_key, body.get("contract"))
                return {"success": ok, "action": "unstake"}
            elif action == "claim":
                ok = lc.claim_rewards(service_key)
                return {"success": ok, "action": "claim"}
            elif action == "checkpoint":
                ok = lc.checkpoint(service_key)
                return {"success": ok, "action": "checkpoint"}
            elif action == "status":
                status = lc.get_status(service_key)
                return {"success": True, "data": status}
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    return app


def _record_to_dict(record: Any) -> dict:
    """Convert a RequestRecord to a JSON-safe dict."""
    r = record.request
    result = {
        "request_id": r.request_id,
        "chain": r.chain,
        "status": r.status,
        "tool": r.tool,
        "prompt": r.prompt[:100] if r.prompt else "",
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "is_offchain": r.is_offchain,
    }
    if record.result:
        result["execution_time"] = round(record.result.execution_time, 2)
        result["error"] = record.result.error
    if record.response:
        result["tx_hash"] = record.response.delivery_tx_hash
    return result
