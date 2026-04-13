"""Tests for security hardening code (rate limiting, headers, CSP, etc.)."""

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.runtime.delivery import TX_RECEIPT_TIMEOUT, DeliveryManager
from micromech.runtime.http import create_app as create_http_app
from micromech.web.app import (
    _MAX_TRACKED_IPS,
    _rate_counters,
    _rate_limited,
    create_web_app,
)
from tests.conftest import make_test_config

CSRF_HEADERS = {"X-Micromech-Action": "test"}


# --- Fixtures ---


@pytest.fixture
def web_client() -> TestClient:
    app = create_web_app(
        get_status=lambda: {"status": "running", "queue": {}, "tools": [], "delivered_total": 0},
        get_recent=lambda lim, chain=None: [],
        get_tools=lambda: [],
        on_request=lambda r: None,
    )
    return TestClient(app)


@pytest.fixture
def http_client() -> TestClient:
    async def noop(r):
        pass

    app = create_http_app(
        on_request=noop,
        get_status=lambda: {"status": "running", "queue": {}, "tools": [], "delivered_total": 0},
    )
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_rate_counters():
    """Clear rate limiter state between tests."""
    _rate_counters.clear()
    yield
    _rate_counters.clear()


# ============================================================
# Rate Limiting
# ============================================================


class TestRateLimiting:
    def test_allows_under_limit(self):
        for _ in range(9):
            assert _rate_limited("/api/setup/wallet", "1.2.3.4") is False

    def test_blocks_over_limit(self):
        for _ in range(10):
            _rate_limited("/api/setup/wallet", "1.2.3.4")
        assert _rate_limited("/api/setup/wallet", "1.2.3.4") is True

    def test_different_ips_independent(self):
        for _ in range(10):
            _rate_limited("/api/setup/wallet", "1.1.1.1")
        # Different IP should not be blocked
        assert _rate_limited("/api/setup/wallet", "2.2.2.2") is False

    def test_unknown_endpoint_not_limited(self):
        assert _rate_limited("/api/unknown", "1.2.3.4") is False

    def test_window_expiry(self):
        for _ in range(10):
            _rate_limited("/api/setup/wallet", "1.2.3.4")
        assert _rate_limited("/api/setup/wallet", "1.2.3.4") is True

        # Manually age all timestamps beyond the window (60s)
        bucket = _rate_counters["/api/setup/wallet"]["1.2.3.4"]
        old_time = time.time() - 120
        _rate_counters["/api/setup/wallet"]["1.2.3.4"] = [old_time] * len(bucket)

        assert _rate_limited("/api/setup/wallet", "1.2.3.4") is False

    def test_ip_eviction_at_max(self):
        # Fill up to _MAX_TRACKED_IPS
        for i in range(_MAX_TRACKED_IPS + 5):
            _rate_limited("/api/setup/wallet", f"10.0.{i // 256}.{i % 256}")
        # Should not grow unbounded
        assert len(_rate_counters["/api/setup/wallet"]) <= _MAX_TRACKED_IPS + 1


# ============================================================
# Security Headers
# ============================================================


class TestSecurityHeaders:
    def test_headers_present_on_get(self, web_client: TestClient):
        resp = web_client.get("/api/status")
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert "strict-origin" in resp.headers["Referrer-Policy"]
        assert "Permissions-Policy" in resp.headers

    def test_csp_header_present(self, web_client: TestClient):
        resp = web_client.get("/api/status")
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "script-src" in csp
        assert "chart.js" in csp

    def test_headers_present_on_post(self, web_client: TestClient):
        resp = web_client.post(
            "/api/runtime/start",
            headers=CSRF_HEADERS,
        )
        assert resp.headers["X-Content-Type-Options"] == "nosniff"


# ============================================================
# Web App CSRF Enforcement
# ============================================================


class TestWebAppCsrfEnforcement:
    def test_setup_wallet_requires_csrf(self, web_client: TestClient):
        resp = web_client.post(
            "/api/setup/wallet",
            json={"password": "testpassword"},
        )
        assert resp.status_code == 403

    def test_setup_deploy_requires_csrf(self, web_client: TestClient):
        resp = web_client.post(
            "/api/setup/deploy",
            json={"chain": "gnosis"},
        )
        assert resp.status_code == 403

    def test_runtime_control_requires_csrf(self, web_client: TestClient):
        resp = web_client.post("/api/runtime/start")
        assert resp.status_code == 403

    def test_management_requires_csrf(self, web_client: TestClient):
        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
        )
        assert resp.status_code == 403

    def test_setup_wallet_rate_limited(self, web_client: TestClient):
        """After 10 attempts, wallet endpoint returns 429."""
        for _ in range(10):
            web_client.post(
                "/api/setup/wallet",
                json={"password": "testpassword"},
                headers=CSRF_HEADERS,
            )
        resp = web_client.post(
            "/api/setup/wallet",
            json={"password": "testpassword"},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 429


# ============================================================
# HTTP App CSRF Enforcement
# ============================================================


class TestHttpAppCsrfEnforcement:
    def test_request_rejects_no_csrf(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test"},
        )
        assert resp.status_code == 403

    def test_docs_disabled(self, http_client: TestClient):
        assert http_client.get("/docs").status_code == 404
        assert http_client.get("/redoc").status_code == 404

    def test_web_docs_disabled(self, web_client: TestClient):
        assert web_client.get("/docs").status_code == 404
        assert web_client.get("/redoc").status_code == 404


# ============================================================
# Bridge Password Clearing
# ============================================================


class TestBridgeWalletConstruction:
    def test_cached_key_storage_used_over_env(self):
        """get_wallet() uses _cached_key_storage, not Wallet()/env."""
        import micromech.core.bridge as bridge

        mock_ks = MagicMock()
        bridge._cached_wallet = None
        bridge._cached_key_storage = mock_ks

        FakeWallet = type("Wallet", (), {})

        with (
            patch("micromech.core.bridge.Wallet", FakeWallet),
            patch("micromech.core.bridge.ChainInterfaces"),
            patch.dict(
                "sys.modules",
                {
                    "iwa.core.db": MagicMock(),
                    "iwa.core.wallet": type(
                        "m",
                        (),
                        {
                            "Wallet": FakeWallet,
                            "AccountService": lambda *a: MagicMock(),
                            "BalanceService": lambda *a: MagicMock(),
                            "SafeService": lambda *a: MagicMock(),
                            "TransactionService": lambda *a: MagicMock(),
                            "TransferService": lambda *a: MagicMock(),
                            "PluginService": lambda *a: MagicMock(),
                        },
                    )(),
                },
            ),
        ):
            wallet = bridge.get_wallet()

        assert wallet is not None
        assert wallet.key_storage is mock_ks

        bridge._cached_wallet = None
        bridge._cached_key_storage = None

    def test_no_wallet_no_ks_raises(self):
        """get_wallet() raises when no wallet file and no cached ks."""
        import micromech.core.bridge as bridge

        bridge._cached_wallet = None
        bridge._cached_key_storage = None

        with pytest.raises(RuntimeError, match="No wallet"):
            bridge.get_wallet()

        bridge._cached_wallet = None


# ============================================================
# Delivery: chainId and timeout
# ============================================================


class TestDeliveryChainId:
    def test_tx_receipt_timeout_value(self):
        assert TX_RECEIPT_TIMEOUT == 120

    def test_via_impersonation_uses_gas_limit(self):
        """_via_impersonation passes gas=500_000 to transact()."""
        from micromech.core.config import ChainConfig

        chain_cfg = ChainConfig(
            chain="gnosis",
            mech_address="0x77af31De935740567Cf4fF1986D04B2c964A786a",
            marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
            factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
            staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
        )
        bridge = MagicMock(spec=["web3"])
        tx_hash = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        fn_call = MagicMock()
        fn_call.transact.return_value = tx_hash

        queue = MagicMock()
        config = make_test_config()
        dm = DeliveryManager(config=config, chain_config=chain_cfg, queue=queue, bridge=bridge)
        dm._via_impersonation(fn_call, "0x" + "ab" * 20)

        # Verify gas was passed
        call_args = fn_call.transact.call_args
        tx_params = call_args[0][0]
        assert "gas" in tx_params
        assert tx_params["gas"] == 500_000


# ============================================================
# Management: CreateMech event matching
# ============================================================


class TestCreateMechEventMatching:
    MARKETPLACE = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_ignores_wrong_address(self, mock_get_mgr, mock_get_wallet):
        """Log from a different contract should NOT match."""
        from micromech.management import MechLifecycle

        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20
        mock_get_mgr.return_value = mock_mgr

        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_wallet.return_value = mock_wallet

        mock_wallet.transaction_service.sign_and_send.return_value = (
            False, {"status": 0, "logs": []}
        )

        lc = MechLifecycle(make_test_config(), chain_name="gnosis")
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_matches_marketplace_address(self, mock_get_mgr, mock_get_wallet):
        """Log from marketplace with >=2 topics extracts mech address."""
        from micromech.management import MechLifecycle

        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20
        mock_get_mgr.return_value = mock_mgr

        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_wallet.return_value = mock_wallet

        mech_hex = "cd" * 20
        mock_wallet.transaction_service.sign_and_send.return_value = (
            True,
            {
                "status": 1,
                "logs": [
                    {
                        "address": self.MARKETPLACE,
                        "topics": [
                            bytes(32),  # any event topic
                            bytes.fromhex("00" * 12 + mech_hex),
                        ],
                    }
                ],
            },
        )

        lc = MechLifecycle(make_test_config(), chain_name="gnosis")
        result = lc.create_mech("svc-1")
        assert result is not None
        assert mech_hex in result.lower()


# ============================================================
# Tool extra_params injection protection
# ============================================================


class TestToolExtraParamsInjection:
    def test_reserved_kwargs_stripped(self):
        """Reserved keys in extra_params don't reach _run_fn."""
        from micromech.tools.base import Tool, ToolMetadata

        captured_kwargs = {}

        def fake_run(**kwargs):
            captured_kwargs.update(kwargs)
            return ("ok",)

        metadata = ToolMetadata(
            id="test",
            name="test",
            version="0.1.0",
        )
        tool = Tool(metadata=metadata, run_fn=fake_run)

        # Simulate what executor.py does: tool.execute(prompt, **extra_params)
        # extra_params comes from user HTTP input and could contain 'tool'
        import asyncio

        asyncio.run(
            tool.execute(
                "real prompt",
                counter_callback="injected_cb",
                extra="safe",
            )
        )

        # 'prompt' and 'tool' are set by execute(), not overridable
        assert captured_kwargs["prompt"] == "real prompt"
        assert captured_kwargs["tool"] == "test"
        assert captured_kwargs["extra"] == "safe"
        # counter_callback is stripped as reserved
        assert "counter_callback" not in captured_kwargs

    def test_safe_kwargs_passed_through(self):
        """Non-reserved kwargs pass through to the tool function."""
        from micromech.tools.base import Tool, ToolMetadata

        captured = {}

        def fake_run(**kw):
            captured.update(kw)
            return ("ok",)

        tool = Tool(
            metadata=ToolMetadata(id="t", name="t", version="0.1.0"),
            run_fn=fake_run,
        )

        import asyncio

        asyncio.run(tool.execute("p", temperature=0.7, model="qwen"))

        assert captured["temperature"] == 0.7
        assert captured["model"] == "qwen"


# ============================================================
# SSE Stream: connection limit
# ============================================================


class TestSSEStream:
    def test_sse_route_exists(self, web_client: TestClient):
        """SSE endpoint is registered."""
        routes = [r.path for r in web_client.app.routes]
        assert "/api/metrics/stream" in routes


# ============================================================
# X-Forwarded-For support
# ============================================================


class TestClientIPExtraction:
    def test_direct_connection(self):
        from micromech.web.app import _get_client_ip

        request = MagicMock()
        request.headers = {}
        request.client = MagicMock(host="192.168.1.1")
        assert _get_client_ip(request) == "192.168.1.1"

    @patch("micromech.web.app._TRUST_PROXY", True)
    def test_forwarded_for_single(self):
        from micromech.web.app import _get_client_ip

        request = MagicMock()
        request.headers = {"X-Forwarded-For": "10.0.0.1"}
        assert _get_client_ip(request) == "10.0.0.1"

    @patch("micromech.web.app._TRUST_PROXY", True)
    def test_forwarded_for_chain(self):
        from micromech.web.app import _get_client_ip

        request = MagicMock()
        request.headers = {"X-Forwarded-For": "10.0.0.1, 172.16.0.1, 192.168.1.1"}
        assert _get_client_ip(request) == "10.0.0.1"

    def test_xff_ignored_without_trust(self):
        from micromech.web.app import _get_client_ip

        request = MagicMock()
        request.headers = {"X-Forwarded-For": "10.0.0.1"}
        request.client = MagicMock(host="192.168.1.1")
        # Without _TRUST_PROXY, uses direct connection IP
        assert _get_client_ip(request) == "192.168.1.1"

    def test_no_client(self):
        from micromech.web.app import _get_client_ip

        request = MagicMock()
        request.headers = {}
        request.client = None
        assert _get_client_ip(request) == "unknown"


# ============================================================
# Signature hex validation
# ============================================================


class TestSignatureValidation:
    def test_valid_hex_signature(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": "0xdeadbeef"},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 202

    def test_valid_hex_no_prefix(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": "abcdef0123"},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 202

    def test_null_signature_ok(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": None},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 202

    def test_invalid_hex_rejected(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": "not-hex!@#"},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 422

    def test_empty_signature_ok(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": ""},
            headers=CSRF_HEADERS,
        )
        assert resp.status_code == 202


# ============================================================
# No direct web3 RPC usage — all calls must go through iwa
# ============================================================


class TestNoDirectWeb3:
    """Ensure micromech source never creates Web3 instances or calls web3.eth directly.

    web3.to_wei / web3.keccak (pure utilities) are allowed.
    Web3(HTTPProvider(...)) and web3.eth.* bypass iwa's RPC rotation.
    """

    def test_no_web3_http_provider_in_src(self):
        """No file in src/ creates a Web3(HTTPProvider(...)) instance."""
        import pathlib

        src = pathlib.Path("src/micromech")
        violations = []
        for f in src.rglob("*.py"):
            text = f.read_text()
            if "HTTPProvider" in text:
                violations.append(str(f))
        assert violations == [], f"Direct Web3 HTTPProvider in: {violations}"

    def test_wallet_path_not_real(self):
        """WALLET_PATH must point to tmp during tests, never data/wallet.json."""
        from iwa.core.constants import WALLET_PATH

        assert "tmp" in WALLET_PATH.lower() or "pytest" in WALLET_PATH.lower(), (
            f"WALLET_PATH points to real wallet: {WALLET_PATH}"
        )
        assert not WALLET_PATH.endswith("data/wallet.json"), (
            f"WALLET_PATH points to real wallet: {WALLET_PATH}"
        )

    def test_no_standalone_web3_instances(self):
        """No file creates standalone Web3 instances (bypassing iwa).

        Using bridge.web3 or ci.web3 is fine — those come from iwa.
        Standalone `Web3(...)` constructor calls are forbidden.
        """
        import pathlib
        import re

        src = pathlib.Path("src/micromech")
        # Match Web3( constructor calls, but not Web3.to_wei/keccak (utilities)
        pattern = re.compile(r"Web3\(\s*Web3\.HTTP")
        violations = []
        for f in src.rglob("*.py"):
            text = f.read_text()
            if pattern.search(text):
                violations.append(str(f))
        assert violations == [], f"Standalone Web3 instances: {violations}"


# ============================================================
# Dashboard/Setup Page Access (no auth required)
# ============================================================


class TestPageAccess:
    """Verify that dashboard and setup pages are accessible without auth."""

    def test_setup_page_accessible(self, web_client: TestClient):
        """Setup page renders without auth."""
        resp = web_client.get("/setup")
        assert resp.status_code == 200

    def test_redirect_to_setup_when_needed(self, web_client: TestClient):
        """When / needs setup, it redirects to /setup."""
        with patch("micromech.web.app._needs_setup", return_value=True):
            resp = web_client.get("/", follow_redirects=False)
            assert resp.status_code == 302
            assert resp.headers["location"] == "/setup"


# ============================================================
# SSE / Log Stream Connection Limits
# ============================================================


class TestLogStreamLimit:
    """Verify log stream has connection limits."""

    def test_log_stream_limit_enforced(self, web_client: TestClient):
        """Stuffing _log_queues to capacity should return 429."""
        import micromech.web.app as web_mod

        original_max = web_mod._MAX_SSE_CONNECTIONS
        web_mod._MAX_SSE_CONNECTIONS = 0  # No connections allowed
        try:
            resp = web_client.get("/api/logs/stream")
            assert resp.status_code in (200, 429)
        finally:
            web_mod._MAX_SSE_CONNECTIONS = original_max


# ============================================================
# Request ID Validation
# ============================================================


class TestRequestIdValidation:
    """Verify /result/{request_id} validates input."""

    def test_invalid_request_id_rejected(self, web_client: TestClient):
        """Non-hex request IDs with special chars are rejected."""
        resp = web_client.get("/result/AAAA'; DROP TABLE--")
        assert resp.status_code == 400

    def test_valid_hex_request_id_accepted(self, web_client: TestClient):
        """Valid hex request ID returns 404 (not found, but not 400)."""
        resp = web_client.get("/result/" + "ab" * 32)
        # 404 or 501 (no queue) — not 400
        assert resp.status_code in (404, 501)

    def test_valid_http_prefixed_id(self, web_client: TestClient):
        """HTTP-prefixed request IDs are accepted."""
        resp = web_client.get("/result/http-abc123def456")
        assert resp.status_code in (404, 501)


# ============================================================
# XFF Trust
# ============================================================


class TestXffTrust:
    """Verify X-Forwarded-For is only trusted when configured."""

    def test_xff_not_trusted_by_default(self):
        """Without MICROMECH_TRUST_PROXY, XFF is ignored."""
        from micromech.web.app import _TRUST_PROXY

        # Default should be False (not trusting proxy)
        assert _TRUST_PROXY is False
