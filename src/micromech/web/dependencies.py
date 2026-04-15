"""FastAPI dependencies for the micromech web UI.

Auth is enforced at the router level (see ``app.py``) rather than in an
HTTP middleware so the checks are insensitive to sub-app mount prefixes
(e.g. ``app.mount("/dashboard", web_app)``), which previously caused
``bearer_auth`` to silently fail when Starlette populated ``scope["path"]``
with the full prefixed path.

The pattern mirrors ``iwa.web.dependencies.verify_auth`` — returns ``None``
on success and raises ``HTTPException`` on failure so FastAPI's normal
error flow applies.

Rate-limiter state lives in ``app.py`` (``_rate_counters``) and is reached
via lazy import here to avoid a circular import at module load time.
"""

import secrets as _secrets
from typing import Callable, Optional

from fastapi import Header, HTTPException, Request

CSRF_HEADER = "X-Micromech-Action"


def rate_limit(endpoint_key: str) -> Callable:
    """Dependency factory enforcing the rate limit registered in app.py."""

    def _dep(request: Request) -> None:
        # Lazy import: app.py imports this module at top level.
        from micromech.web.app import _get_client_ip, _rate_limited

        if _rate_limited(endpoint_key, _get_client_ip(request)):
            raise HTTPException(status_code=429, detail="Too many requests. Try again later.")

    return _dep


def _get_webui_password() -> Optional[str]:
    """Read WEBUI_PASSWORD from the live secrets singleton (lazy, for tests)."""
    from micromech.secrets import secrets

    if secrets.webui_password:
        return secrets.webui_password.get_secret_value()
    return None


def _check_token(
    authorization: Optional[str], query_token: Optional[str], password: str
) -> bool:
    """Timing-safe check of Bearer header or ?token= query param."""
    if authorization:
        scheme, _, param = authorization.partition(" ")
        if scheme.lower() == "bearer" and param and _secrets.compare_digest(param, password):
            return True
    if query_token and _secrets.compare_digest(query_token, password):
        return True
    return False


def verify_auth(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> None:
    """Enforce Bearer (or ``?token=`` for SSE) authentication.

    - If WEBUI_PASSWORD is unset, allow all requests (fresh install).
      This preserves the existing fresh-install semantics on
      ``protected_router`` so the read-only dashboard endpoints can
      bootstrap before the wizard is run.
    - Accepts ``Authorization: Bearer <password>`` OR ``?token=<password>``.
    - Raises 401 with ``WWW-Authenticate: Bearer`` on failure.

    **Security note** — The "password is None ⇒ allow" branch is
    intentionally NOT applied to ``verify_auth_or_setup_mode``, which is
    what guards the mutating ``/api/setup/*`` endpoints. That dependency
    fails closed when no password is set and ``_needs_setup()`` is False,
    so clearing ``webui_password`` from ``secrets.env`` post-deploy
    cannot expose wallet re-creation / secrets rewrite. The CI gate
    ``test_every_api_route_has_auth_dependency`` enforces that every
    ``/api/*`` route lists one of the two dependencies, so the risk of
    reusing plain ``verify_auth`` on a new mutating endpoint is bounded
    by the allow-list review in that test.
    """
    password = _get_webui_password()
    if not password:
        return  # fresh install, no password configured yet

    query_token = request.query_params.get("token")
    if _check_token(authorization, query_token, password):
        return

    raise HTTPException(
        status_code=401,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )


def verify_auth_or_setup_mode(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> None:
    """Bypass auth while the wizard is still running; otherwise require a token.

    Two explicit branches, fail-closed on anything in between:

    1. ``_needs_setup() == True``: the initial wizard is open — allow the
       request through so ``POST /api/setup/wallet`` is reachable without
       credentials (no password exists yet).
    2. ``_needs_setup() == False``: the system is set up — demand a valid
       Bearer token or ``?token=`` parameter. **Unlike** plain
       ``verify_auth``, we do NOT honour the "password is None ⇒ allow"
       fresh-install exception here: once setup is complete, a missing
       ``webui_password`` must be treated as a misconfiguration, not a
       free pass. Otherwise an operator clearing ``webui_password`` from
       ``secrets.env`` post-deploy would silently expose every setup
       endpoint (wallet re-creation, secrets rewrite) to unauthenticated
       clients.
    """
    # Lazy import to avoid circular: dependencies -> app -> dependencies.
    from micromech.web.app import _needs_setup

    if _needs_setup():
        return

    password = _get_webui_password()
    unauthorized = HTTPException(
        status_code=401,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not password:
        raise unauthorized

    query_token = request.query_params.get("token")
    if _check_token(authorization, query_token, password):
        return
    raise unauthorized


def require_csrf_header(
    x_micromech_action: Optional[str] = Header(None, alias=CSRF_HEADER),
) -> None:
    """Reject requests missing the custom CSRF header.

    Browsers will not send custom headers on simple cross-origin requests
    without triggering a preflight; combined with the deny-all CORS policy
    this blocks cross-site form submissions targeting mutating endpoints.
    """
    if not x_micromech_action:
        raise HTTPException(
            status_code=403,
            detail=f"Missing {CSRF_HEADER} header",
        )
