"""Web UI application — dashboard, metrics API, and SSE stream."""

import asyncio
import json
import logging
import os
import queue as stdlib_queue
import re
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.web.dependencies import (
    rate_limit,
    require_csrf_header,
    verify_auth,
    verify_auth_or_setup_mode,
)

if TYPE_CHECKING:
    from micromech.core.persistence import PersistentQueue
    from micromech.runtime.metrics import MetricsCollector

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

# --- Simple rate limiter ---
_RATE_LIMITS: dict[str, tuple[int, int]] = {
    # endpoint_key: (max_requests, window_seconds)
    "/api/setup/wallet": (10, 60),  # 10 attempts per minute (brute-force protection)
    "/api/setup/state": (30, 60),  # 30 per minute — does config + chain I/O per call
    "/request": (60, 60),  # 60 requests per minute
    "/api/metadata/publish": (3, 60),  # 3 per minute (each costs gas)
    # Hot-reload does filesystem I/O and importlib — cap to avoid DoS via
    # an authenticated attacker spamming reloads.
    "/api/tools/reload": (6, 60),
    # Secrets file I/O — cap reads and writes to avoid hammering the filesystem.
    "/api/setup/secrets/get": (30, 60),
    "/api/setup/secrets/post": (10, 60),
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
    """Extract client IP, preferring the LAST hop in X-Forwarded-For.

    When MICROMECH_TRUST_PROXY=1 we read X-Forwarded-For. The right-most
    entry is the hop closest to our server (the last trusted proxy); the
    left-most entry is client-controlled and spoofable. Using the right-
    most entry prevents rate-limit evasion via forged headers.
    """
    if _TRUST_PROXY:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            parts = [p.strip() for p in forwarded.split(",") if p.strip()]
            if parts:
                return parts[-1]
    if request.client:
        return request.client.host
    return "unknown"


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
    reload_tools: Optional[Callable[[], list[str] | Awaitable[list[str]]]] = None,
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

    # Explicit CORS: deny all cross-origin requests.
    # The CSRF header (X-Micromech-Action) relies on browsers sending a preflight
    # for custom headers — but only if CORS is not permissively configured.
    # This makes the protection intentional rather than accidental.
    from fastapi.middleware.cors import CORSMiddleware

    app.add_middleware(CORSMiddleware, allow_origins=[])

    # NOTE: Authentication is enforced via FastAPI dependencies on the
    # routers defined below (verify_auth / verify_auth_or_setup_mode).
    # The previous bearer_auth middleware was removed because it parsed
    # scope["path"] manually and silently failed when the app was mounted
    # as a sub-app (e.g. app.mount("/dashboard", web_app)) — the prefixed
    # path bypassed every ``startswith("/api/")`` check. See
    # ``test_auth_enforced_when_mounted_as_sub_app`` for the regression guard.

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
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net/npm/chart.js@4/; "
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

    # ------------------------------------------------------------------
    # Routers — auth is attached at the ROUTER level (fail-closed).
    #
    #   public_router   : no auth (HTML shell, health, /result/{id})
    #   setup_router    : auth bypassed only while _needs_setup() == True
    #   protected_router: verify_auth required on every request
    #
    # Adding a new /api/* endpoint to ``protected_router`` automatically
    # inherits auth; forgetting to register it on any router surfaces as
    # a failure in ``test_all_api_endpoints_covered_by_router`` (CI gate).
    # ------------------------------------------------------------------
    public_router = APIRouter()
    setup_router = APIRouter(
        prefix="/api/setup",
        dependencies=[Depends(verify_auth_or_setup_mode)],
    )
    protected_router = APIRouter(
        prefix="/api",
        dependencies=[Depends(verify_auth)],
    )

    @public_router.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        if _needs_setup():
            root = request.scope.get("root_path", "")
            return RedirectResponse(url=f"{root}/setup", status_code=302)
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={"api_base": request.scope.get("root_path", "")},
        )

    @public_router.get("/setup", response_class=HTMLResponse)
    async def setup_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="setup.html",
            context={"api_base": request.scope.get("root_path", "")},
        )

    # --- Setup API ---

    @setup_router.get("/state", dependencies=[Depends(rate_limit("/api/setup/state"))])
    async def setup_state(request: Request) -> dict:
        """Get current setup state."""
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

        return {
            "wallet_exists": wallet_exists,
            "wallet_address": wallet_address,
            "needs_password": needs_password,
            "wallet_file_exists": wallet_file_exists,
            "config_exists": bool(chains_deployed),
            "chains": chains_deployed,
            "step": step,
        }

    @setup_router.post(
        "/wallet",
        dependencies=[
            Depends(require_csrf_header),
            Depends(rate_limit("/api/setup/wallet")),
        ],
    )
    async def setup_wallet(request: Request) -> dict:
        """Create or unlock wallet. Body: {password: str}.

        If no wallet exists, creates a new one and returns address + mnemonic.
        If wallet exists but locked, unlocks it and returns address.
        """
        body = await request.json()
        password = body.get("password", "")
        if not password or len(password) < 8:
            return JSONResponse({"error": "Password too short (min 8 characters)."}, 400)
        if len(password) > 128:
            return JSONResponse({"error": "Password too long (max 128 characters)."}, 400)

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
            # Persist password ONLY when a new wallet is created so the container
            # can auto-unlock on restart.  Re-unlocks of an existing wallet do NOT
            # overwrite the stored value — the user may have intentionally cleared
            # it from secrets.env for security reasons.
            if result.get("created"):
                try:
                    from pydantic import SecretStr

                    from micromech.core.secrets_file import write_secret
                    from micromech.secrets import secrets as _secrets

                    write_secret("wallet_password", password)
                    # Same password protects the web UI from this point on.
                    # User may change WEBUI_PASSWORD in secrets.env at any time.
                    write_secret("webui_password", password)
                    # Update in-memory singleton so auth kicks in immediately
                    # without requiring a restart.
                    _secrets.webui_password = SecretStr(password)
                except Exception:
                    logger.warning(
                        "Could not write wallet_password/webui_password to secrets.env (non-fatal)"
                    )
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

    @setup_router.get(
        "/secrets",
        dependencies=[Depends(rate_limit("/api/setup/secrets/get"))],
    )
    async def get_secrets(request: Request) -> dict:
        """Return editable secrets (sensitive values masked)."""
        try:
            from micromech.core.secrets_file import EDITABLE_KEYS, SENSITIVE_KEYS, read_secrets_file

            all_secrets = read_secrets_file()
            result = {}
            for key in EDITABLE_KEYS:
                value = all_secrets.get(key, "")
                result[key] = "***" if (key in SENSITIVE_KEYS and value) else value
            return JSONResponse(result)
        except Exception:
            logger.exception("Failed to read secrets file")
            return JSONResponse({"error": "Could not read secrets file"}, 500)

    @setup_router.post(
        "/secrets",
        dependencies=[
            Depends(require_csrf_header),
            Depends(rate_limit("/api/setup/secrets/post")),
        ],
    )
    async def save_secrets(request: Request) -> dict:
        """Write editable secrets to secrets.env."""
        try:
            from micromech.core.secrets_file import EDITABLE_KEYS, write_secrets

            body = await request.json()
            updates: dict[str, str] = {}
            for k, v in body.items():
                if k not in EDITABLE_KEYS:
                    continue
                if not isinstance(v, str):
                    return JSONResponse({"error": f"Value for '{k}' must be a string"}, 400)
                if v == "***":
                    continue
                updates[k] = v
            write_secrets(updates)
            # Hot-reload webui_password into the in-memory singleton so auth
            # takes effect immediately without a restart.
            if "webui_password" in updates:
                from pydantic import SecretStr as _SecretStr

                from micromech.secrets import secrets as _live_secrets

                new_pw = updates["webui_password"]
                _live_secrets.webui_password = _SecretStr(new_pw) if new_pw else None
            return JSONResponse({"status": "ok", "saved": list(updates.keys())})
        except ValueError as e:
            return JSONResponse({"error": str(e)}, 400)
        except Exception:
            logger.exception("Failed to write secrets file")
            return JSONResponse({"error": "Could not write secrets file"}, 500)

    @setup_router.get("/balance")
    async def setup_balance(chain: str = "gnosis") -> dict:
        """Check wallet balances for setup funding."""
        if not _valid_chain(chain):
            return {"error": "Unknown chain", "sufficient": False}
        try:
            from micromech.core.bridge import check_balances
            from micromech.core.constants import MIN_NATIVE_WEI, MIN_OLAS_WHOLE

            result = check_balances(chain)
            if result is None:
                return {
                    "error": "RPC unavailable — balance unknown",
                    "sufficient": False,
                }
            native, olas = result
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

    @setup_router.post(
        "/deploy",
        dependencies=[Depends(require_csrf_header)],
    )
    async def setup_deploy(request: Request):
        """Deploy service via real-time SSE stream.

        CSRF header enforced by dependency. Only one deploy at a time
        (concurrency guard).
        """
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
                # Track rollback events across ALL yield points (poll loop + exception handler)
                # so we never emit a redundant generic error after rollback already reported its
                # own outcome (done ✓ or failed ⚠ with actionable recovery command).
                rollback_concluded = False

                def _yield_progress(evt: dict) -> str:
                    nonlocal rollback_concluded
                    if evt.get("step") in ("rollback_done", "rollback_failed"):
                        rollback_concluded = True
                    return f"data: {json.dumps(evt)}\n\n"

                try:
                    while not future.done():
                        await asyncio.sleep(0.5)
                        while not progress_q.empty():
                            yield _yield_progress(progress_q.get_nowait())

                    result = future.result()
                    # Drain remaining events
                    while not progress_q.empty():
                        yield _yield_progress(progress_q.get_nowait())

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
                    # Drain remaining events (rollback progress from management.py)
                    while not progress_q.empty():
                        yield _yield_progress(progress_q.get_nowait())
                    # Only emit generic error if rollback never concluded —
                    # rollback_done already shows ✓, rollback_failed already shows ⚠
                    # with the actionable recovery command; the generic error would overwrite both.
                    if not rollback_concluded:
                        err = {
                            "step": "error",
                            "message": "Deployment failed — check logs for details.",
                        }
                        yield f"data: {json.dumps(err)}\n\n"

        return StreamingResponse(
            deploy_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    @setup_router.get("/chains")
    async def setup_chains() -> list[dict]:
        """Available chains for setup."""
        from micromech.core.constants import CHAIN_DEFAULTS

        return [
            {"name": name, "contracts": contracts} for name, contracts in CHAIN_DEFAULTS.items()
        ]

    # --- Status API ---

    @protected_router.get("/status")
    async def api_status() -> dict:
        return get_status()

    @protected_router.get("/chains")
    async def api_chains() -> list[str]:
        status = get_status()
        return status.get("chains", [])

    @protected_router.get("/requests")
    async def api_requests(limit: int = 50, chain: Optional[str] = None) -> list[dict]:
        records = get_recent(min(limit, 200), chain)
        return [_record_to_dict(r) for r in records]

    @protected_router.get("/tools")
    async def api_tools() -> list[dict]:
        return get_tools()

    @setup_router.get("/tools")
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

    @setup_router.post(
        "/tools",
        dependencies=[Depends(require_csrf_header)],
    )
    async def api_setup_tools_save(request: Request):
        """Save which tools are enabled/disabled."""
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
            "hot_reloadable": reload_tools is not None,
        }

    @protected_router.post(
        "/tools/reload",
        dependencies=[
            Depends(require_csrf_header),
            Depends(rate_limit("/api/tools/reload")),
        ],
    )
    async def api_tools_reload(request: Request):
        """Hot-reload the tool registry (builtins + custom), honoring disabled_tools.

        Must be called AFTER saving disabled_tools via POST /api/setup/tools.
        Returns the new list of active tool IDs.
        """
        if reload_tools is None:
            return JSONResponse(
                {"error": "Hot reload not available (server not wired)"},
                501,
            )
        try:
            result = reload_tools()
            # Support both sync and async reload_tools implementations so
            # tests (which pass simple lambdas) and the real async server
            # method can share this endpoint.
            if asyncio.iscoroutine(result):
                tool_ids = await result
            else:
                tool_ids = result
        except Exception as e:
            # Log full error internally, return a generic message to avoid
            # leaking filesystem paths or stack traces to the client.
            logger.error("Tool hot-reload failed: {}", e)
            return JSONResponse(
                {"error": "Tool reload failed — see server logs"},
                500,
            )
        return {"status": "reloaded", "tools": tool_ids}

    # --- Metadata API ---

    @protected_router.get("/metadata")
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

    @protected_router.post(
        "/metadata/publish",
        dependencies=[
            Depends(require_csrf_header),
            Depends(rate_limit("/api/metadata/publish")),
        ],
    )
    async def api_metadata_publish(request: Request):
        """Publish tool metadata to IPFS + update on-chain hash (SSE stream)."""
        if not metadata_manager:
            return JSONResponse({"error": "Metadata manager not configured"}, 501)

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

    @public_router.get("/result/{request_id}")
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

    @protected_router.get("/metrics/live")
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

    @protected_router.get("/metrics/events")
    async def metrics_events(since: float = 0, limit: int = 50) -> list[dict]:
        """Recent activity events from in-memory buffer."""
        if not metrics:
            return []
        if since > 0:
            return metrics.get_events_since(since)
        return metrics.get_recent_events(limit)

    @protected_router.get("/metrics/tools")
    async def metrics_tools(chain: Optional[str] = None) -> list[dict]:
        """Per-tool aggregate stats from DB."""
        if not queue:
            return []
        return queue.tool_stats(chain=chain)

    @protected_router.get("/metrics/daily")
    async def metrics_daily(days: int = 30, chain: Optional[str] = None) -> list[dict]:
        """Daily request counts from DB."""
        if not queue:
            return []
        return queue.daily_stats(min(days, 365), chain=chain)

    @protected_router.get("/metrics/monthly")
    async def metrics_monthly(months: int = 12, chain: Optional[str] = None) -> list[dict]:
        """Monthly request counts from DB."""
        if not queue:
            return []
        return queue.monthly_stats(min(months, 36), chain=chain)

    @protected_router.get("/metrics/channels")
    async def metrics_channels(chain: Optional[str] = None) -> dict:
        """On-chain vs off-chain request counts."""
        if not queue:
            return {"onchain": 0, "offchain": 0}
        return queue.onchain_offchain_counts(chain=chain)

    # --- SSE Stream ---

    @protected_router.get("/metrics/stream")
    async def metrics_stream(request: Request) -> StreamingResponse:
        """Server-Sent Events stream for real-time dashboard updates.

        Limited to _MAX_SSE_CONNECTIONS concurrent streams.
        """
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
                    if await request.is_disconnected():
                        break

                    payload: dict[str, Any] = {
                        "type": "tick",
                        "timestamp": time.time(),
                    }

                    if runtime_manager:
                        payload["runtime_state"] = runtime_manager.state
                    else:
                        # Standalone mode (micromech run): derive state from get_status
                        payload["runtime_state"] = get_status().get("status", "running")

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
                        # DB-backed period stats for overview cards
                        if queue:
                            payload["stats_24h"] = queue.period_stats(hours=24)
                            payload["stats_7d"] = queue.period_stats(hours=168)

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

    @protected_router.get("/staking/status")
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

    @protected_router.get("/karma")
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

    @protected_router.get("/marketplace/pending-payments")
    async def marketplace_pending_payments(chain: Optional[str] = None) -> dict:
        """Get pending xDAI claimable from the marketplace balance tracker for each chain."""

        def _get_payments() -> dict:
            from micromech.core.bridge import IwaBridge
            from micromech.core.marketplace import get_balance_tracker_address, get_pending_balance

            config = MicromechConfig.load()
            chains_to_check = (
                {chain: config.chains[chain]}
                if chain and chain in config.chains
                else config.enabled_chains
            )
            results = {}
            for name, cfg in chains_to_check.items():
                if not cfg.mech_address or not cfg.marketplace_address:
                    results[name] = {"pending": 0.0, "error": "not configured"}
                    continue
                try:
                    bridge = IwaBridge(chain_name=name)
                    bt_addr = get_balance_tracker_address(
                        bridge, name, cfg.mech_address, cfg.marketplace_address
                    )
                    if not bt_addr:
                        results[name] = {"pending": 0.0}
                        continue
                    pending = get_pending_balance(bridge, bt_addr, cfg.mech_address)
                    results[name] = {"pending": round(pending, 6)}
                except Exception as e:
                    logger.warning("Pending payments check failed for {}: {}", name, e)
                    results[name] = {"pending": 0.0, "error": "check failed"}
            return results

        try:
            return await asyncio.to_thread(_get_payments)
        except Exception:
            logger.exception("Pending payments check failed")
            return {"error": "Pending payments check failed"}

    @public_router.get("/api/health")
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

    @protected_router.get("/runtime/status")
    async def runtime_status() -> dict:
        """Get runtime state (stopped, starting, running, error)."""
        if runtime_manager:
            return runtime_manager.get_status()
        return {"state": "unavailable"}

    _RUNTIME_ACTIONS = {"start", "stop", "restart"}

    @protected_router.post(
        "/runtime/{action}",
        dependencies=[Depends(require_csrf_header)],
    )
    async def runtime_control(action: str, request: Request) -> dict:
        """Start, stop, or restart the mech runtime."""
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

    @protected_router.post(
        "/management/{action}",
        dependencies=[Depends(require_csrf_header)],
    )
    async def management_action(action: str, request: Request) -> dict:
        """Execute a management action (stake, unstake, claim, checkpoint).

        CSRF header enforced by dependency.
        """
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
                amount = lc.claim_rewards(service_key)
                return {"success": amount > 0, "action": "claim", "amount_olas": amount}
            elif action == "checkpoint":
                ok = lc.checkpoint(service_key)
                return {"success": ok, "action": "checkpoint"}
            elif action == "status":
                status = lc.get_status(service_key)
                return {"success": True, "data": status}
            elif action == "withdraw":
                from micromech.core.bridge import IwaBridge, get_service_info
                from micromech.core.marketplace import (
                    get_balance_tracker_address,
                    get_pending_balance,
                )
                from micromech.tasks.payment_withdraw import (
                    _drain_mech_to_safe,
                    _transfer_to_master,
                    _withdraw,
                )

                chain_cfg = config.enabled_chains.get(chain)
                if not chain_cfg or not chain_cfg.mech_address:
                    return {"success": False, "error": "No mech_address configured for this chain"}

                bridge = IwaBridge(chain_name=chain)
                bt_address = get_balance_tracker_address(
                    bridge, chain, chain_cfg.mech_address, chain_cfg.marketplace_address
                )
                if not bt_address:
                    return {"success": False, "error": "Could not find balance tracker address"}

                balance = get_pending_balance(bridge, bt_address, chain_cfg.mech_address)
                if balance <= 0:
                    return {"success": True, "action": "withdraw", "data": {"amount_xdai": 0.0, "message": "No pending payments"}}

                svc_info = get_service_info(chain)
                multisig = svc_info.get("multisig_address")
                if not multisig:
                    return {"success": False, "error": "No multisig_address found"}

                _withdraw(bridge, chain, bt_address, chain_cfg.mech_address, multisig)

                web3 = bridge.web3
                mech_wei = bridge.with_retry(
                    lambda: web3.eth.get_balance(web3.to_checksum_address(chain_cfg.mech_address))
                )
                _drain_mech_to_safe(bridge, chain, chain_cfg.mech_address, multisig, mech_wei)
                _transfer_to_master(bridge, chain, multisig, mech_wei)

                amount_xdai = round(mech_wei / 1e18, 6)
                return {"success": True, "action": "withdraw", "data": {"amount_xdai": amount_xdai}}
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

    @protected_router.get("/logs/stream")
    async def logs_stream(request: Request) -> StreamingResponse:
        """SSE stream of real-time loguru output."""
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
                    if await request.is_disconnected():
                        break
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

    # Register routers on the app. Order does not matter for routing
    # (paths are distinct), but public_router goes first by convention.
    app.include_router(public_router)
    app.include_router(setup_router)
    app.include_router(protected_router)

    return app


def _record_to_dict(record: Any) -> dict:
    """Convert a RequestRecord to a JSON-safe dict."""
    r = record.request
    # Convert requestData multihash to IPFS CID for linking
    request_ipfs_cid = None
    if r.data:
        try:
            from micromech.ipfs.client import multihash_to_cid, normalize_to_multihash

            mh = normalize_to_multihash(r.data)
            if mh is not None:
                request_ipfs_cid = multihash_to_cid(mh)
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
