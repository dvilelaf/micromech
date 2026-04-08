"""Web UI application — dashboard, metrics API, and SSE stream."""

import asyncio
import json
import logging
import os
import queue as stdlib_queue
import re
import secrets
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from fastapi import FastAPI, Header, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from micromech.core.config import MicromechConfig

if TYPE_CHECKING:
    from micromech.core.persistence import PersistentQueue
    from micromech.runtime.metrics import MetricsCollector

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

# CSRF header required on state-changing endpoints (browsers won't send
# this in simple cross-origin requests)
CSRF_HEADER = "X-Micromech-Action"

# --- Auth token ---
# Generated once at startup, printed to the console. Required on all
# state-changing endpoints via X-Auth-Token header (or ?token= query param
# for SSE streams). Read-only GET endpoints (status, health, metrics) are
# open so monitoring tools work without auth.
_AUTH_TOKEN: str = os.environ.get("MICROMECH_AUTH_TOKEN", "") or secrets.token_urlsafe(32)

# --- Simple rate limiter ---
_RATE_LIMITS: dict[str, tuple[int, int]] = {
    # endpoint_key: (max_requests, window_seconds)
    "/api/setup/wallet": (10, 60),  # 10 attempts per minute (brute-force protection)
    "/request": (60, 60),  # 60 requests per minute
    "/api/metadata/publish": (3, 60),  # 3 per minute (each costs gas)
}
_MAX_TRACKED_IPS = 1000
_rate_counters: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

# SSE connection limit
_MAX_SSE_CONNECTIONS = 10
_sse_semaphore: asyncio.Semaphore | None = None


def _get_sse_semaphore() -> asyncio.Semaphore:
    """Lazy-init SSE semaphore (must be created within an event loop)."""
    global _sse_semaphore  # noqa: PLW0603
    if _sse_semaphore is None:
        _sse_semaphore = asyncio.Semaphore(_MAX_SSE_CONNECTIONS)
    return _sse_semaphore


# --- Log streaming (module-level so dedup works across create_web_app calls) ---
_log_queues: list[stdlib_queue.Queue] = []
_log_sink_registered = False

_SENSITIVE_RE = re.compile(
    r'token=[^ &"\']+|password=[^ &"\']+|api_key=[^ &"\']+|apikey=[^ &"\']+|/v[23]/[a-zA-Z0-9_-]{20,}'
)


def _redact_sensitive(msg: str) -> str:
    """Strip sensitive values from log messages before sending to SSE clients."""
    return _SENSITIVE_RE.sub("***", msg)


def _push_log_line(ts: str, level: str, msg: str) -> None:
    """Push a log line to all connected SSE clients."""
    data = json.dumps({"ts": ts, "level": level, "msg": msg})
    for q in _log_queues[:]:
        try:
            q.put_nowait(data)
        except Exception:
            pass


def _log_sink(message: Any) -> None:
    """Loguru sink that pushes log lines to all connected SSE clients."""
    record = message.record
    # Format like terminal: timestamp | LEVEL | module:func:line - message
    module = record.get("name", "")
    func = record.get("function", "")
    line_no = record.get("line", "")
    location = f"{module}:{func}:{line_no}" if module else ""
    ts = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    msg_text = record["message"]
    full_msg = (
        f"{ts} | {record['level'].name:<8} | {location} - {msg_text}" if location else msg_text
    )
    _push_log_line(
        ts=record["time"].strftime("%H:%M:%S.%f")[:-3],
        level=record["level"].name,
        msg=_redact_sensitive(full_msg),
    )


class _StdlibLogHandler(logging.Handler):
    """Capture stdlib logging (uvicorn, iwa) into the SSE log stream."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            import datetime

            ts = datetime.datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]
            _push_log_line(
                ts=ts, level=record.levelname, msg=_redact_sensitive(self.format(record))
            )
        except Exception:
            pass


def _rate_limited(endpoint: str, client_ip: str) -> bool:
    """Check if a client has exceeded the rate limit for an endpoint."""
    if endpoint not in _RATE_LIMITS:
        return False
    max_req, window = _RATE_LIMITS[endpoint]
    now = time.time()
    bucket = _rate_counters[endpoint]
    # Evict oldest IPs if we exceed the cap (before adding new ones)
    while len(bucket) >= _MAX_TRACKED_IPS and client_ip not in bucket:
        oldest_ip = min(
            bucket,
            key=lambda ip: bucket[ip][-1] if bucket[ip] else 0,
        )
        del bucket[oldest_ip]
    # Prune old entries for this IP
    bucket[client_ip] = [t for t in bucket.get(client_ip, []) if now - t < window]
    if len(bucket[client_ip]) >= max_req:
        return True
    bucket[client_ip].append(now)
    return False


_TRUST_PROXY: bool = os.environ.get("MICROMECH_TRUST_PROXY", "").lower() in ("1", "true", "yes")


def _get_client_ip(request: Request) -> str:
    """Extract client IP. Only trusts X-Forwarded-For when MICROMECH_TRUST_PROXY is set."""
    if _TRUST_PROXY:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _check_auth(request: Request) -> Optional[JSONResponse]:
    """Validate auth token from header or query param. Returns error response or None."""
    token = request.headers.get("X-Auth-Token") or request.query_params.get("token")
    if not token or not secrets.compare_digest(token, _AUTH_TOKEN):
        return JSONResponse({"error": "Invalid or missing auth token"}, 401)
    return None


def get_auth_token() -> str:
    """Return the current auth token (for CLI to display)."""
    return _AUTH_TOKEN


_setup_needed: Optional[bool] = None

# Deploy concurrency guard — per-chain locks allow parallel multi-chain deploy
_deploy_locks: dict[str, asyncio.Lock] = {}

# Config file read/write lock (thread-safe, for parallel deploys)
_config_lock = threading.Lock()


def _get_deploy_lock(chain_name: str) -> asyncio.Lock:
    """Get or create a per-chain deploy lock."""
    if chain_name not in _deploy_locks:
        _deploy_locks[chain_name] = asyncio.Lock()
    return _deploy_locks[chain_name]


def _needs_setup() -> bool:
    """Check if micromech needs initial setup (no config or no deployed service).

    Cached after first check — cleared when setup completes via _clear_setup_cache().
    """
    global _setup_needed  # noqa: PLW0603
    if _setup_needed is not None:
        return _setup_needed
    try:
        cfg = MicromechConfig.load()
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
    runtime_manager: "Any | None" = None,
    metadata_manager: "Any | None" = None,
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

    # Security headers middleware
    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net/npm/chart.js@4/; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src https://fonts.gstatic.com; "
            "connect-src 'self'; "
            "img-src 'self' data:; "
            "frame-ancestors 'none'"
        )
        return response

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        if _needs_setup():
            # Preserve auth token in redirect
            token = request.query_params.get("token", "")
            url = f"/setup?token={token}" if token else "/setup"
            return RedirectResponse(url=url, status_code=302)
        # Auth: token can be passed as ?token= for initial access
        token = request.query_params.get("token", "")
        if not (token and secrets.compare_digest(token, _AUTH_TOKEN)):
            if _check_auth(request) is not None:
                return HTMLResponse(
                    "<html><body style='font-family:sans-serif;text-align:center;padding:4em'>"
                    "<h2>micromech</h2>"
                    "<p>Access this page with <code>?token=YOUR_AUTH_TOKEN</code></p>"
                    "<p style='color:#666'>The auth token is printed in the server console at startup.</p>"
                    "</body></html>",
                    status_code=401,
                )
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={"auth_token": _AUTH_TOKEN},
        )

    @app.get("/setup", response_class=HTMLResponse)
    async def setup_page(request: Request) -> HTMLResponse:
        # Auth check: token can be passed as ?token= query param for initial access
        token = request.query_params.get("token")
        if token and secrets.compare_digest(token, _AUTH_TOKEN):
            # Valid token in URL — serve the page
            pass
        elif _check_auth(request) is not None:
            # No valid auth — show a minimal login prompt instead of leaking the token
            return HTMLResponse(
                "<html><body style='font-family:sans-serif;text-align:center;padding:4em'>"
                "<h2>micromech setup</h2>"
                "<p>Access this page with <code>?token=YOUR_AUTH_TOKEN</code></p>"
                "<p style='color:#666'>The auth token is printed in the server console at startup.</p>"
                "</body></html>",
                status_code=401,
            )
        return templates.TemplateResponse(
            request=request,
            name="setup.html",
            context={"auth_token": _AUTH_TOKEN},
        )

    # --- Setup API ---

    @app.get("/api/setup/state")
    async def setup_state(request: Request) -> dict:
        """Get current setup state."""
        authenticated = _check_auth(request) is None
        wallet_exists = False
        wallet_address = None
        needs_password = False

        try:
            import micromech.core.bridge as _bridge

            # Only report cached wallet (already unlocked in this session).
            # Never auto-unlock via Wallet() — user must confirm password
            # through POST /api/setup/wallet first.
            if _bridge._cached_key_storage is not None:
                wallet_exists = True
                wallet_address = str(_bridge._cached_key_storage.get_address_by_tag("master"))
            elif _bridge._cached_wallet is not None:
                wallet_exists = True
                wallet_address = _bridge._cached_wallet.master_account.address
            else:
                needs_password = True
        except Exception:
            needs_password = True

        # Check if wallet file exists on disk (to distinguish create vs unlock)
        wallet_file_exists = False
        try:
            from pathlib import Path

            from iwa.core.constants import WALLET_PATH

            wallet_file_exists = Path(WALLET_PATH).exists()
        except Exception:
            pass

        chains_deployed: dict[str, dict] = {}

        try:
            cfg = MicromechConfig.load()
            for name, chain_cfg in cfg.chains.items():
                from micromech.core.bridge import get_service_info

                svc = get_service_info(name)
                chains_deployed[name] = {
                    "state": chain_cfg.detect_setup_state(),
                    "complete": chain_cfg.setup_complete,
                    "service_id": svc.get("service_id"),
                    "service_key": svc.get("service_key"),
                    "mech_address": chain_cfg.mech_address,
                    "multisig_address": svc.get("multisig_address"),
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

        result = {
            "wallet_exists": wallet_exists,
            "wallet_address": wallet_address,
            "needs_password": needs_password,
            "wallet_file_exists": wallet_file_exists,
            "config_exists": bool(chains_deployed),
            "chains": chains_deployed,
            "step": step,
        }

        # Strip sensitive addresses when not authenticated
        if not authenticated:
            result["wallet_address"] = None
            for chain_data in result["chains"].values():
                chain_data.pop("mech_address", None)
                chain_data.pop("multisig_address", None)

        return result

    @app.post("/api/setup/wallet")
    async def setup_wallet(
        request: Request,
        x_micromech_action: Optional[str] = Header(None),
    ) -> dict:
        """Create or unlock wallet. Body: {password: str}.

        If no wallet exists, creates a new one and returns address + mnemonic.
        If wallet exists but locked, unlocks it and returns address.
        """
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err
        if not x_micromech_action:
            return JSONResponse({"error": "Missing X-Micromech-Action header"}, 403)
        if _rate_limited("/api/setup/wallet", _get_client_ip(request)):
            return JSONResponse({"error": "Too many attempts. Try again later."}, 429)

        body = await request.json()
        password = body.get("password", "")
        if not password or len(password) < 8:
            return JSONResponse({"error": "Password too short (min 8 characters)."}, 400)

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
                    msg = "Incorrect password."
                    raise PermissionError(msg)

            # Store key_storage for get_wallet() fallback.
            # KeyStorage already holds the decrypted key material,
            # so the plaintext password is not stored in memory.
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
            return JSONResponse(
                result,
                headers={"Cache-Control": "no-store"},
            )
        except PermissionError:
            return JSONResponse({"error": "Incorrect password."}, 403)
        except Exception:
            logger.exception("Wallet creation/unlock failed")
            return JSONResponse(
                {"error": "Failed to create or unlock wallet."},
                status_code=500,
            )

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
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err
        if not x_micromech_action:
            return JSONResponse({"error": "Missing X-Micromech-Action header"}, 403)

        body = await request.json() if request.headers.get("content-type") else {}
        chain_name = body.get("chain", "gnosis")

        if not _valid_chain(chain_name):
            return JSONResponse({"error": f"Unknown chain: {chain_name}"}, 400)

        chain_lock = _get_deploy_lock(chain_name)
        if chain_lock.locked():
            return JSONResponse({"error": f"Deploy already in progress for {chain_name}"}, 409)

        progress_q: stdlib_queue.Queue[dict] = stdlib_queue.Queue()

        def _run_deploy() -> dict:
            """Run full_deploy in a thread, pushing events to queue."""
            from micromech.core.config import ChainConfig
            from micromech.core.constants import CHAIN_DEFAULTS
            from micromech.management import MechLifecycle

            # Lock for config read/write to prevent parallel deploys
            # from overwriting each other's results
            with _config_lock:
                cfg = MicromechConfig.load()
                defaults = CHAIN_DEFAULTS.get(chain_name, {})
                if chain_name not in cfg.chains:
                    cfg.chains[chain_name] = ChainConfig(
                        chain=chain_name,
                        marketplace_address=defaults.get("marketplace", ""),
                        factory_address=defaults.get("factory", ""),
                        staking_address=defaults.get("staking", ""),
                    )
                cfg.save()

            def on_progress(step, total, msg, success=True):
                progress_q.put(
                    {
                        "step": step,
                        "total": total,
                        "message": msg,
                        "success": success,
                    }
                )

            lc = MechLifecycle(cfg, chain_name=chain_name)
            result = lc.full_deploy(on_progress=on_progress)

            # Save results with lock (re-read to merge with other deploys)
            with _config_lock:
                fresh_cfg = MicromechConfig.load()
                fresh_cfg.chains[chain_name].apply_deploy_result(result)
                fresh_cfg.save()

            return result

        async def deploy_stream():
            async with chain_lock:
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

                    # Auto-start runtime after successful deploy
                    runtime_started = False
                    if runtime_manager:
                        runtime_started = await runtime_manager.start()

                    done_evt = {
                        "step": "done",
                        "result": result,
                        "runtime_started": runtime_started,
                    }
                    yield f"data: {json.dumps(done_evt)}\n\n"
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
            {"name": name, "contracts": contracts} for name, contracts in CHAIN_DEFAULTS.items()
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

    @app.get("/api/setup/tools")
    async def api_setup_tools() -> list[dict]:
        """All available tool packages (builtin + custom) with enabled/disabled status."""
        from micromech.core.constants import CUSTOM_TOOLS_DIR
        from micromech.ipfs.metadata import scan_tool_packages

        builtin_dir = Path(__file__).parent.parent / "tools"
        all_tools = scan_tool_packages(builtin_dir, source="builtin")
        all_tools.extend(scan_tool_packages(CUSTOM_TOOLS_DIR, source="custom"))
        cfg = MicromechConfig.load()
        disabled = set(cfg.disabled_tools)
        return [
            {
                "name": t["name"],
                "description": t["description"],
                "version": t["version"],
                "tools": t["allowed_tools"],
                "enabled": t["name"] not in disabled,
                "source": t.get("source", "builtin"),
            }
            for t in all_tools
        ]

    @app.post("/api/setup/tools")
    async def api_setup_tools_save(request: Request):
        """Save which tools are enabled/disabled."""
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err
        csrf = request.headers.get(CSRF_HEADER)
        if not csrf:
            return JSONResponse({"error": f"Missing {CSRF_HEADER} header"}, 403)

        body = await request.json()
        disabled = body.get("disabled_tools", [])
        if not isinstance(disabled, list):
            return JSONResponse({"error": "disabled_tools must be a list"}, 400)
        # Validate: only strings allowed
        disabled = [str(d) for d in disabled if isinstance(d, str)]

        cfg = MicromechConfig.load()
        cfg.disabled_tools = disabled
        cfg.save()
        return {
            "status": "saved",
            "disabled_tools": disabled,
            "restart_required": True,
        }

    # --- Metadata API ---

    @app.get("/api/metadata")
    async def api_metadata_status() -> dict:
        """Current tool metadata state: staleness, hashes, tools list."""
        if not metadata_manager:
            return {"error": "Metadata manager not configured"}
        try:
            status = metadata_manager.get_status()
            state = (
                "not_registered"
                if status.ipfs_cid is None
                else ("stale" if status.needs_update else "up_to_date")
            )
            return {
                "status": state,
                "computed_hash": status.computed_hash,
                "stored_hash": status.stored_hash,
                "ipfs_cid": status.ipfs_cid,
                "needs_update": status.needs_update,
                "changed_packages": status.changed_packages,
                "tools": status.tools,
            }
        except Exception as e:
            return {"error": str(e)}

    _metadata_publish_lock = asyncio.Lock()

    @app.post("/api/metadata/publish")
    async def api_metadata_publish(request: Request):
        """Publish tool metadata to IPFS + update on-chain hash (SSE stream)."""
        if not metadata_manager:
            return JSONResponse({"error": "Metadata manager not configured"}, 501)

        auth_err = _check_auth(request)
        if auth_err:
            return auth_err

        csrf = request.headers.get(CSRF_HEADER)
        if not csrf:
            return JSONResponse(
                {"error": f"Missing {CSRF_HEADER} header"},
                403,
            )

        if _rate_limited("/api/metadata/publish", _get_client_ip(request)):
            return JSONResponse({"error": "Rate limit exceeded"}, 429)

        if _metadata_publish_lock.locked():
            return JSONResponse({"error": "Publish already in progress"}, 409)

        import asyncio
        import queue as stdlib_queue

        progress_q: stdlib_queue.Queue[dict] = stdlib_queue.Queue()

        async def publish_stream():
            async with _metadata_publish_lock:

                def on_progress(step: str, msg: str) -> None:
                    progress_q.put({"step": step, "message": msg})

                task = asyncio.create_task(
                    metadata_manager.publish(on_progress=on_progress),
                )

                try:
                    while not task.done():
                        await asyncio.sleep(0.3)
                        while not progress_q.empty():
                            evt = progress_q.get_nowait()
                            yield f"data: {json.dumps(evt)}\n\n"

                    # Drain remaining events
                    while not progress_q.empty():
                        evt = progress_q.get_nowait()
                        yield f"data: {json.dumps(evt)}\n\n"

                    result = task.result()
                    done_evt = {
                        "step": "done",
                        "success": result.success,
                        "ipfs_cid": result.ipfs_cid,
                        "onchain_hash": result.onchain_hash,
                        "error": result.error,
                    }
                    yield f"data: {json.dumps(done_evt)}\n\n"

                except (Exception, asyncio.CancelledError) as e:
                    yield f"data: {json.dumps({'step': 'error', 'message': str(e)})}\n\n"

        from starlette.responses import StreamingResponse

        return StreamingResponse(
            publish_stream(),
            media_type="text/event-stream",
        )

    _REQUEST_ID_RE = re.compile(r"^(http-|0x)?[a-f0-9-]{1,66}$", re.IGNORECASE)

    @app.get("/result/{request_id}")
    async def get_result_by_id(request_id: str) -> dict:
        """Get result for a specific request (used by demo poller)."""
        if not _REQUEST_ID_RE.match(request_id):
            return JSONResponse({"error": "Invalid request ID format"}, 400)
        if not queue:
            return JSONResponse({"error": "Not configured"}, 501)
        record = queue.get_by_id(request_id)
        if record is None:
            return JSONResponse({"error": "Not found"}, 404)
        result = _record_to_dict(record)
        if record.result:
            try:
                result["result"] = json.loads(record.result.output)
            except (ValueError, TypeError):
                result["result"] = {"raw": record.result.output}
        return result

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
    async def metrics_stream(request: Request) -> StreamingResponse:
        """Server-Sent Events stream for real-time dashboard updates.

        Requires auth via ?token= query param (SSE can't set headers).
        Limited to _MAX_SSE_CONNECTIONS concurrent streams.
        """
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err

        sem = _get_sse_semaphore()
        # Acquire eagerly (before returning StreamingResponse) to avoid TOCTOU
        try:
            await asyncio.wait_for(sem.acquire(), timeout=1)
        except asyncio.TimeoutError:
            return JSONResponse(
                {"error": "Too many SSE connections"},
                status_code=429,
            )

        async def event_generator():
            try:
                last_event_ts = time.time()
                tick = 0
                while True:
                    await asyncio.sleep(2)
                    tick += 1

                    payload: dict[str, Any] = {
                        "type": "tick",
                        "timestamp": time.time(),
                    }

                    if runtime_manager:
                        payload["runtime_state"] = runtime_manager.state

                    # Use passed metrics, or get from runtime manager
                    _mc = metrics or (runtime_manager.metrics if runtime_manager else None)
                    if _mc:
                        payload["live"] = _mc.get_live_snapshot()
                        new_events = _mc.get_events_since(last_event_ts)
                        if new_events:
                            payload["events"] = new_events
                            last_event_ts = time.time()

                    # Include queue counts on first tick and every 5 ticks
                    if tick <= 1 or tick % 5 == 0:
                        status = get_status()
                        payload["queue"] = status.get("queue", {})
                        payload["delivered_total"] = status.get(
                            "delivered_total",
                            0,
                        )
                        # Populate live from status if no metrics collector
                        if not metrics and "metrics" in status:
                            payload["live"] = status["metrics"]

                    yield f"data: {json.dumps(payload)}\n\n"
            finally:
                sem.release()

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
            from micromech.core.bridge import get_service_info

            for name, cfg in chains_to_check.items():
                svc = get_service_info(name)
                svc_key = svc.get("service_key")
                if not svc_key:
                    results[name] = {"status": "not_configured"}
                    continue
                try:
                    lc = MechLifecycle(config, chain_name=name)
                    status = lc.get_status(svc_key)
                    results[name] = status or {"status": "unknown"}
                except Exception:
                    results[name] = {"status": "error"}
            return results

        try:
            return await asyncio.to_thread(_get_staking)
        except Exception:
            logger.exception("Staking status check failed")
            return {"error": "Staking status check failed"}

    @app.get("/api/karma")
    async def karma_status(chain: Optional[str] = None) -> dict:
        """Get mech karma and delivery counts for configured chains."""

        def _get_karma() -> dict:
            from micromech.core.bridge import IwaBridge
            from micromech.runtime.contracts import (
                KARMA_ABI,
                load_marketplace_abi,
            )

            config = MicromechConfig.load()
            results = {}
            chains_to_check = (
                {chain: config.chains[chain]}
                if chain and chain in config.chains
                else config.enabled_chains
            )
            for name, cfg in chains_to_check.items():
                if not cfg.mech_address:
                    results[name] = {"karma": None, "error": "no mech address"}
                    continue
                try:
                    bridge = IwaBridge(chain_name=name)
                    w3 = bridge.web3

                    cs = w3.to_checksum_address
                    marketplace = w3.eth.contract(
                        address=cs(cfg.marketplace_address),
                        abi=load_marketplace_abi(),
                    )

                    # Get karma contract address and query mech karma
                    karma_addr = bridge.with_retry(lambda: marketplace.functions.karma().call())
                    karma_contract = w3.eth.contract(
                        address=karma_addr,
                        abi=KARMA_ABI,
                    )
                    mech_addr = cs(cfg.mech_address)
                    mech_karma = bridge.with_retry(
                        lambda _ma=mech_addr: karma_contract.functions.mapMechKarma(_ma).call()
                    )

                    # Get delivery count (uses multisig address)
                    deliveries = 0
                    from micromech.core.bridge import get_service_info

                    _svc = get_service_info(name)
                    _multisig = _svc.get("multisig_address")
                    if _multisig:
                        ms_addr = cs(_multisig)
                        deliveries = bridge.with_retry(
                            lambda _ms=ms_addr: marketplace.functions.mapMechServiceDeliveryCounts(
                                _ms
                            ).call()
                        )

                    # Timeouts = deliveries - karma (karma = deliveries - penalties)
                    timeouts = max(0, deliveries - mech_karma)

                    results[name] = {
                        "karma": mech_karma,
                        "deliveries": deliveries,  # type: ignore[dict-item]
                        "timeouts": timeouts,
                    }
                except Exception as e:
                    logger.warning("Karma check failed for {}: {}", name, e)
                    results[name] = {"karma": None, "error": "Karma check failed"}
            return results

        try:
            return await asyncio.to_thread(_get_karma)
        except Exception:
            logger.exception("Karma check failed")
            return {"error": "Karma check failed"}

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

    # --- Runtime Control API ---

    @app.get("/api/runtime/status")
    async def runtime_status() -> dict:
        """Get runtime state (stopped, starting, running, error)."""
        if runtime_manager:
            return runtime_manager.get_status()
        return {"state": "unavailable"}

    _RUNTIME_ACTIONS = {"start", "stop", "restart"}

    @app.post("/api/runtime/{action}")
    async def runtime_control(
        action: str,
        request: Request,
        x_micromech_action: Optional[str] = Header(None),
    ) -> dict:
        """Start, stop, or restart the mech runtime."""
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err
        if not x_micromech_action:
            return JSONResponse({"error": "Missing X-Micromech-Action header"}, 403)
        if action not in _RUNTIME_ACTIONS:
            return JSONResponse({"error": "Unknown action"}, 404)
        if not runtime_manager:
            return JSONResponse({"error": "Runtime manager not available"}, 503)

        handler = {
            "start": runtime_manager.start,
            "stop": runtime_manager.stop,
            "restart": runtime_manager.restart,
        }
        ok = await handler[action]()
        return {"success": ok, "state": runtime_manager.state}

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
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err
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
            # Use service_key from request body, falling back to iwa
            service_key = body.get("service_key", "") or ""
            if not service_key:
                from micromech.core.bridge import get_service_info

                svc = get_service_info(chain)
                service_key = svc.get("service_key", "")

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

    # --- Log streaming ---

    # Register loguru sink (module-level, only once)
    global _log_sink_registered  # noqa: PLW0603
    if not _log_sink_registered:
        logger.add(_log_sink, level="DEBUG", format="{message}")
        # Also capture stdlib logging (uvicorn, iwa, etc.)
        stdlib_handler = _StdlibLogHandler()
        stdlib_handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(stdlib_handler)
        # Uvicorn loggers don't propagate — attach handler directly
        for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
            uv_logger = logging.getLogger(name)
            uv_logger.addHandler(stdlib_handler)
        _log_sink_registered = True

    @app.get("/api/logs/stream")
    async def logs_stream(request: Request) -> StreamingResponse:
        """SSE stream of real-time loguru output."""
        auth_err = _check_auth(request)
        if auth_err:
            return auth_err

        if len(_log_queues) >= _MAX_SSE_CONNECTIONS:
            return JSONResponse(
                {"error": "Too many log connections"},
                status_code=429,
            )

        log_q: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=500)
        _log_queues.append(log_q)

        async def generate():
            try:
                while True:
                    await asyncio.sleep(0.3)
                    while not log_q.empty():
                        try:
                            data = log_q.get_nowait()
                            yield f"data: {data}\n\n"
                        except Exception:
                            break
            finally:
                _log_queues.remove(log_q)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    return app


def _record_to_dict(record: Any) -> dict:
    """Convert a RequestRecord to a JSON-safe dict."""
    r = record.request
    # Convert requestData multihash to IPFS CID for linking
    request_ipfs_cid = None
    if r.data and len(r.data) == 34 and r.data[:2] == b"\x12\x20":
        try:
            from micromech.ipfs.client import multihash_to_cid

            request_ipfs_cid = multihash_to_cid(r.data)
        except Exception:
            pass
    result = {
        "request_id": r.request_id,
        "request_ipfs_cid": request_ipfs_cid,
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
        result["ipfs_hash"] = record.response.ipfs_hash
    return result
