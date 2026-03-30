"""Web UI application — dashboard and management console."""

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from micromech.core.config import MicromechConfig

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


def create_web_app(
    get_status: Callable[[], dict],
    get_recent: Callable[[int], list],
    get_tools: Callable[[], list[dict]],
    on_request: Callable,
) -> FastAPI:
    """Create the web UI FastAPI app.

    Args:
        get_status: Returns server status dict.
        get_recent: Returns recent request records.
        get_tools: Returns list of tool metadata dicts.
        on_request: Async callback for new requests.
    """
    app = FastAPI(title="micromech dashboard", docs_url=None, redoc_url=None)

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> HTMLResponse:
        status = get_status()
        recent = get_recent(20)
        tools = get_tools()
        queue = status.get("queue", {})
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "pending": queue.get("pending", 0),
                "executing": queue.get("executing", 0),
                "executed": queue.get("executed", 0),
                "delivered_total": status.get("delivered_total", 0),
                "recent": recent,
                "tools": tools,
            },
        )

    @app.get("/api/status")
    async def api_status() -> dict:
        return get_status()

    @app.get("/api/requests")
    async def api_requests(limit: int = 50) -> list[dict]:
        records = get_recent(min(limit, 200))
        return [_record_to_dict(r) for r in records]

    @app.get("/api/tools")
    async def api_tools() -> list[dict]:
        return get_tools()

    @app.post("/api/management/{action}")
    async def management_action(action: str, body: dict = {}) -> dict:
        """Execute a management action (stake, unstake, claim, checkpoint)."""
        try:
            from micromech.management import MechLifecycle

            config = MicromechConfig.load()
            lc = MechLifecycle(config)
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
        "status": r.status,
        "tool": r.tool,
        "prompt": r.prompt[:100],
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "is_offchain": r.is_offchain,
    }
    if record.result:
        result["execution_time"] = round(record.result.execution_time, 2)
        result["error"] = record.result.error
    if record.response:
        result["tx_hash"] = record.response.delivery_tx_hash
    return result
