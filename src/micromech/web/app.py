"""Web UI application — dashboard, metrics API, and SSE stream."""

import asyncio
import json
import queue as stdlib_queue
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from fastapi import FastAPI, Header, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from micromech.core.config import DEFAULT_CONFIG_PATH, MicromechConfig

if TYPE_CHECKING:
    from micromech.core.persistence import PersistentQueue
    from micromech.runtime.metrics import MetricsCollector

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

# CSRF header required on state-changing endpoints (browsers won't send
# this in simple cross-origin requests)
CSRF_HEADER = "X-Micromech-Action"

_setup_needed: Optional[bool] = None

# Deploy concurrency guard
_deploy_lock = asyncio.Lock()


def _needs_setup() -> bool:
    """Check if micromech needs initial setup (no config or no deployed service).

    Cached after first check — cleared when setup completes via _clear_setup_cache().
    """
    global _setup_needed  # noqa: PLW0603
    if _setup_needed is not None:
        return _setup_needed
    try:
        if not DEFAULT_CONFIG_PATH.exists():
            _setup_needed = True
            return True
        cfg = MicromechConfig.load(DEFAULT_CONFIG_PATH)
        for chain_cfg in cfg.chains.values():
            if chain_cfg.setup_complete:
                _setup_needed = False
                return False
        _setup_needed = True
        return True
    except Exception:
        _setup_needed = True
        return True


def _clear_setup_cache() -> None:
    """Clear the setup-needed cache (call after successful deployment)."""
    global _setup_needed  # noqa: PLW0603
    _setup_needed = None


def _valid_chain(chain_name: str) -> bool:
    """Check if chain_name is a known chain."""
    from micromech.core.constants import CHAIN_DEFAULTS
    return chain_name in CHAIN_DEFAULTS


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
    async def dashboard(request: Request):
        if _needs_setup():
            return RedirectResponse(url="/setup", status_code=302)
        return templates.TemplateResponse(request=request, name="dashboard.html")

    @app.get("/setup", response_class=HTMLResponse)
    async def setup_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request=request, name="setup.html")

    # --- Setup API ---

    @app.get("/api/setup/state")
    async def setup_state() -> dict:
        """Get current setup state."""
        wallet_exists = False
        wallet_address = None
        needs_password = False

        try:
            import micromech.core.bridge as _bridge

            # Try cached key_storage first (set by POST /api/setup/wallet)
            if _bridge._cached_key_storage is not None:
                wallet_exists = True
                wallet_address = str(
                    _bridge._cached_key_storage.get_address_by_tag("master")
                )
            elif _bridge._cached_wallet is not None:
                wallet_exists = True
                wallet_address = _bridge._cached_wallet.master_account.address
            else:
                # Try loading wallet (needs password in env)
                from iwa.core.wallet import Wallet
                _bridge._cached_wallet = Wallet()
                wallet_exists = True
                wallet_address = _bridge._cached_wallet.master_account.address
        except Exception:
            # Wallet() failed — user needs to provide password via web form.
            needs_password = True

        # Check if wallet file exists on disk (to distinguish create vs unlock)
        wallet_file_exists = False
        try:
            from pathlib import Path

            from iwa.core.constants import WALLET_PATH
            wallet_file_exists = Path(WALLET_PATH).exists()
        except Exception:
            pass

        config_exists = DEFAULT_CONFIG_PATH.exists()
        chains_deployed: dict[str, dict] = {}

        if config_exists:
            try:
                cfg = MicromechConfig.load(DEFAULT_CONFIG_PATH)
                for name, chain_cfg in cfg.chains.items():
                    chains_deployed[name] = {
                        "state": chain_cfg.detect_setup_state(),
                        "complete": chain_cfg.setup_complete,
                        "service_id": chain_cfg.service_id,
                        "mech_address": chain_cfg.mech_address,
                    }
            except Exception:
                pass

        any_complete = any(c["complete"] for c in chains_deployed.values())
        if not wallet_exists:
            step = "wallet"
        elif not any_complete and not chains_deployed:
            step = "config"
        elif not any_complete:
            step = "deploy"
        else:
            step = "complete"

        return {
            "wallet_exists": wallet_exists,
            "wallet_address": wallet_address,
            "needs_password": needs_password,
            "wallet_file_exists": wallet_file_exists,
            "config_exists": config_exists,
            "chains": chains_deployed,
            "step": step,
        }

    @app.post("/api/setup/wallet")
    async def setup_wallet(
        request: Request,
        x_micromech_action: Optional[str] = Header(None),
    ) -> dict:
        """Create or unlock wallet. Body: {password: str}.

        If no wallet exists, creates a new one and returns address + mnemonic.
        If wallet exists but locked, unlocks it and returns address.
        """
        if not x_micromech_action:
            return JSONResponse(
                {"error": "Missing X-Micromech-Action header"}, 403
            )

        body = await request.json()
        password = body.get("password", "")
        if not password or len(password) < 4:
            return {"error": "Password too short (min 4 characters)."}

        def _create_or_unlock():
            from pathlib import Path

            from iwa.core.constants import WALLET_PATH
            from iwa.core.keys import KeyStorage

            import micromech.core.bridge as _bridge

            wallet_path = Path(WALLET_PATH)
            wallet_existed = wallet_path.exists()

            # Create or load KeyStorage with the user-provided password
            ks = KeyStorage(path=wallet_path, password=password)
            address = ks.get_address_by_tag("master")
            if not address:
                msg = "Wallet creation failed"
                raise RuntimeError(msg)

            # Verify password is correct by trying to decrypt the private key
            if wallet_existed:
                try:
                    ks._get_private_key(str(address))
                except Exception:
                    return {"error": "Incorrect password."}

            # Store password + key_storage for subsequent operations
            _bridge._wallet_password = password
            _bridge._cached_key_storage = ks

            # If wallet didn't exist before, retrieve mnemonic for backup
            mnemonic = None
            if not wallet_existed:
                try:
                    mnemonic = ks.decrypt_mnemonic()
                except Exception:
                    pass

            return {
                "address": str(address),
                "mnemonic": mnemonic,
                "created": not wallet_existed,
            }

        try:
            result = await asyncio.to_thread(_create_or_unlock)
            return result
        except Exception:
            logger.exception("Wallet creation/unlock failed")
            return {"error": "Failed to create or unlock wallet. Check password."}

    @app.get("/api/setup/balance")
    async def setup_balance(chain: str = "gnosis") -> dict:
        """Check wallet balances for setup funding."""
        if not _valid_chain(chain):
            return {"error": "Unknown chain", "sufficient": False}
        try:
            from micromech.core.bridge import check_balances
            from micromech.core.constants import MIN_NATIVE_WEI, MIN_OLAS_WHOLE

            native, olas = check_balances(chain)
            min_native = MIN_NATIVE_WEI.get(chain, 0.1) / 1e18
            return {
                "native_balance": native,
                "olas_balance": olas,
                "native_required": min_native,
                "olas_required": MIN_OLAS_WHOLE,
                "native_sufficient": native >= min_native,
                "olas_sufficient": olas >= MIN_OLAS_WHOLE,
                "sufficient": native >= min_native and olas >= MIN_OLAS_WHOLE,
            }
        except Exception:
            logger.exception("Balance check failed for {}", chain)
            return {"error": "Balance check failed", "sufficient": False}

    @app.post("/api/setup/deploy")
    async def setup_deploy(
        request: Request,
        x_micromech_action: Optional[str] = Header(None),
    ):
        """Deploy service via real-time SSE stream.

        Requires X-Micromech-Action header (CSRF protection).
        Only one deploy at a time (concurrency guard).
        """
        if not x_micromech_action:
            return JSONResponse(
                {"error": "Missing X-Micromech-Action header"}, 403
            )

        body = await request.json() if request.headers.get("content-type") else {}
        chain_name = body.get("chain", "gnosis")

        if not _valid_chain(chain_name):
            return JSONResponse({"error": f"Unknown chain: {chain_name}"}, 400)

        if _deploy_lock.locked():
            return JSONResponse(
                {"error": "Deploy already in progress"}, 409
            )

        progress_q: stdlib_queue.Queue[dict] = stdlib_queue.Queue()

        def _run_deploy() -> dict:
            """Run full_deploy in a thread, pushing events to queue."""
            from micromech.core.config import ChainConfig
            from micromech.core.constants import CHAIN_DEFAULTS
            from micromech.management import MechLifecycle

            if DEFAULT_CONFIG_PATH.exists():
                cfg = MicromechConfig.load(DEFAULT_CONFIG_PATH)
            else:
                cfg = MicromechConfig()
            defaults = CHAIN_DEFAULTS.get(chain_name, {})

            if chain_name not in cfg.chains:
                cfg.chains[chain_name] = ChainConfig(
                    chain=chain_name,
                    marketplace_address=defaults.get("marketplace", ""),
                    factory_address=defaults.get("factory", ""),
                    staking_address=defaults.get("staking", ""),
                )

            def on_progress(step, total, msg, success=True):
                progress_q.put({
                    "step": step, "total": total,
                    "message": msg, "success": success,
                })

            # Verify bridge state before deploy
            import micromech.core.bridge as _br
            logger.info(
                "Deploy: bridge state: password={}, ks={}, wallet={}",
                bool(_br._wallet_password),
                _br._cached_key_storage is not None,
                _br._cached_wallet is not None,
            )

            lc = MechLifecycle(cfg, chain_name=chain_name)
            result = lc.full_deploy(on_progress=on_progress)

            # Update config with results
            cfg.chains[chain_name].apply_deploy_result(result)
            cfg.save(DEFAULT_CONFIG_PATH)

            return result

        async def deploy_stream():
            async with _deploy_lock:
                loop = asyncio.get_event_loop()
                future = loop.run_in_executor(None, _run_deploy)
                try:
                    while not future.done():
                        await asyncio.sleep(0.5)
                        while not progress_q.empty():
                            evt = progress_q.get_nowait()
                            yield f"data: {json.dumps(evt)}\n\n"

                    result = future.result()
                    # Drain remaining events
                    while not progress_q.empty():
                        evt = progress_q.get_nowait()
                        yield f"data: {json.dumps(evt)}\n\n"

                    _clear_setup_cache()
                    yield f"data: {json.dumps({'step': 'done', 'result': result})}\n\n"
                except Exception:
                    logger.exception("Deploy failed for {}", chain_name)
                    err = {"step": "error", "message": "Deployment failed. Check server logs."}
                    yield f"data: {json.dumps(err)}\n\n"

        return StreamingResponse(
            deploy_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    @app.get("/api/setup/chains")
    async def setup_chains() -> list[dict]:
        """Available chains for setup."""
        from micromech.core.constants import CHAIN_DEFAULTS
        return [
            {"name": name, "contracts": contracts}
            for name, contracts in CHAIN_DEFAULTS.items()
        ]

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

    # --- Staking & Health API (Phase 4) ---

    @app.get("/api/staking/status")
    async def staking_status(chain: Optional[str] = None) -> dict:
        """Get staking status for all configured chains (or one)."""
        def _get_staking() -> dict:
            from micromech.management import MechLifecycle

            config = MicromechConfig.load()
            results = {}
            chains_to_check = (
                {chain: config.chains[chain]}
                if chain and chain in config.chains
                else config.enabled_chains
            )
            for name, cfg in chains_to_check.items():
                if not cfg.service_key:
                    results[name] = {"status": "not_configured"}
                    continue
                try:
                    lc = MechLifecycle(config, chain_name=name)
                    status = lc.get_status(cfg.service_key)
                    results[name] = status or {"status": "unknown"}
                except Exception:
                    results[name] = {"status": "error"}
            return results

        try:
            return await asyncio.to_thread(_get_staking)
        except Exception:
            logger.exception("Staking status check failed")
            return {"error": "Staking status check failed"}

    @app.get("/api/health")
    async def health_check() -> dict:
        """Health check with per-chain RPC status."""
        health: dict[str, Any] = {
            "status": "ok",
            "timestamp": time.time(),
        }

        if metrics:
            health["uptime"] = metrics.uptime_seconds
            health["requests_received"] = metrics.requests_received
            health["deliveries_completed"] = metrics.deliveries_completed

        # Per-chain health
        chain_health: dict[str, Any] = {}
        status_data = get_status()
        for chain_name in status_data.get("chains", []):
            chain_health[chain_name] = {"status": "listening"}
        health["chains"] = chain_health

        return health

    # --- Management API ---

    @app.post("/api/management/{action}")
    async def management_action(
        action: str,
        request: Request,
        x_micromech_action: Optional[str] = Header(None),
    ) -> dict:
        """Execute a management action (stake, unstake, claim, checkpoint).

        Requires X-Micromech-Action header (CSRF protection).
        """
        if not x_micromech_action:
            return JSONResponse(
                {"success": False, "error": "Missing X-Micromech-Action header"},
                403,
            )

        body = await request.json() if request.headers.get("content-type") else {}

        def _run_action() -> dict:
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

        try:
            return await asyncio.to_thread(_run_action)
        except Exception:
            logger.exception("Management action '{}' failed", action)
            return {"success": False, "error": "Action failed. Check server logs."}

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
