"""Tests for security hardening code (auth, rate limiting, headers, CSP, etc.)."""

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from micromech.core.config import IpfsConfig, MicromechConfig
from micromech.runtime.delivery import TX_RECEIPT_TIMEOUT, DeliveryManager
from micromech.runtime.http import create_app as create_http_app
from micromech.web.app import (
    _MAX_TRACKED_IPS,
    _check_auth,
    _rate_counters,
    _rate_limited,
    create_web_app,
    get_auth_token,
)

AUTH_TOKEN = get_auth_token()
AUTH_HEADERS = {"X-Auth-Token": AUTH_TOKEN, "X-Micromech-Action": "test"}


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
# Auth Token
# ============================================================


class TestAuthToken:
    def test_get_auth_token_returns_nonempty_string(self):
        token = get_auth_token()
        assert isinstance(token, str)
        assert len(token) > 16

    def test_get_auth_token_stable_within_process(self):
        assert get_auth_token() == get_auth_token()

    def test_check_auth_valid_header(self):
        request = MagicMock()
        request.headers = {"X-Auth-Token": AUTH_TOKEN}
        request.query_params = {}
        result = _check_auth(request)
        assert result is None  # no error

    def test_check_auth_valid_query_param(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {"token": AUTH_TOKEN}
        result = _check_auth(request)
        assert result is None

    def test_check_auth_missing_token(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        result = _check_auth(request)
        assert result is not None
        assert result.status_code == 401

    def test_check_auth_wrong_token(self):
        request = MagicMock()
        request.headers = {"X-Auth-Token": "wrong-token-value"}
        request.query_params = {}
        result = _check_auth(request)
        assert result is not None
        assert result.status_code == 401


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
            headers=AUTH_HEADERS,
        )
        assert resp.headers["X-Content-Type-Options"] == "nosniff"


# ============================================================
# Web App Auth Enforcement
# ============================================================


class TestWebAppAuthEnforcement:
    def test_setup_wallet_requires_auth(self, web_client: TestClient):
        resp = web_client.post(
            "/api/setup/wallet",
            json={"password": "testpassword"},
            headers={"X-Micromech-Action": "wallet"},
        )
        assert resp.status_code == 401

    def test_setup_wallet_requires_csrf(self, web_client: TestClient):
        resp = web_client.post(
            "/api/setup/wallet",
            json={"password": "testpassword"},
            headers={"X-Auth-Token": AUTH_TOKEN},
        )
        assert resp.status_code == 403

    def test_setup_deploy_requires_auth(self, web_client: TestClient):
        resp = web_client.post(
            "/api/setup/deploy",
            json={"chain": "gnosis"},
            headers={"X-Micromech-Action": "deploy"},
        )
        assert resp.status_code == 401

    def test_runtime_control_requires_auth(self, web_client: TestClient):
        resp = web_client.post(
            "/api/runtime/start",
            headers={"X-Micromech-Action": "start"},
        )
        assert resp.status_code == 401

    def test_runtime_control_requires_csrf(self, web_client: TestClient):
        resp = web_client.post(
            "/api/runtime/start",
            headers={"X-Auth-Token": AUTH_TOKEN},
        )
        assert resp.status_code == 403

    def test_management_requires_auth(self, web_client: TestClient):
        resp = web_client.post(
            "/api/management/stake",
            json={"service_key": "svc-1"},
            headers={"X-Micromech-Action": "test"},
        )
        assert resp.status_code == 401

    def test_setup_wallet_rate_limited(self, web_client: TestClient):
        """After 10 attempts, wallet endpoint returns 429."""
        for _ in range(10):
            web_client.post(
                "/api/setup/wallet",
                json={"password": "testpassword"},
                headers=AUTH_HEADERS,
            )
        resp = web_client.post(
            "/api/setup/wallet",
            json={"password": "testpassword"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 429


# ============================================================
# HTTP App Auth Enforcement
# ============================================================


class TestHttpAppAuthEnforcement:
    def test_request_rejects_no_auth(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test"},
            headers={"X-Micromech-Action": "test"},
        )
        assert resp.status_code == 401

    def test_request_rejects_wrong_auth(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test"},
            headers={"X-Auth-Token": "bad", "X-Micromech-Action": "test"},
        )
        assert resp.status_code == 401

    def test_request_rejects_no_csrf(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test"},
            headers={"X-Auth-Token": AUTH_TOKEN},
        )
        assert resp.status_code == 403

    def test_docs_disabled(self, http_client: TestClient):
        assert http_client.get("/docs").status_code == 404
        assert http_client.get("/redoc").status_code == 404

    def test_web_docs_disabled(self, web_client: TestClient):
        assert web_client.get("/docs").status_code == 404
        assert web_client.get("/redoc").status_code == 404


# ============================================================
# IpfsConfig URL Validation
# ============================================================


class TestIpfsConfigValidation:
    def test_valid_http_url(self):
        cfg = IpfsConfig(gateway="http://localhost:8080/ipfs/")
        assert cfg.gateway.startswith("http://")

    def test_valid_https_url(self):
        cfg = IpfsConfig(gateway="https://gateway.example.com/ipfs/")
        assert cfg.gateway.startswith("https://")

    def test_invalid_url_no_scheme(self):
        with pytest.raises(ValidationError):
            IpfsConfig(gateway="just-a-string")

    def test_invalid_url_ftp(self):
        with pytest.raises(ValidationError):
            IpfsConfig(gateway="ftp://files.example.com/")

    def test_api_url_validated(self):
        with pytest.raises(ValidationError):
            IpfsConfig(api_url="not-a-url")

    def test_api_url_valid(self):
        cfg = IpfsConfig(api_url="http://localhost:5001")
        assert cfg.api_url == "http://localhost:5001"


# ============================================================
# Bridge Password Clearing
# ============================================================


class TestBridgePasswordClearing:
    def test_password_cleared_after_get_wallet(self):
        """After get_wallet() via fallback, _wallet_password is None."""
        import micromech.core.bridge as bridge

        # Use a real class that supports object.__new__
        class FakeWallet:
            pass

        # Make FakeWallet() raise to trigger fallback path,
        # but keep the class usable for object.__new__
        def failing_init(self, *a, **kw):
            raise AttributeError("no password")

        FakeWallet.__init__ = failing_init

        bridge._cached_wallet = None
        bridge._wallet_password = "test_password"
        bridge._cached_key_storage = MagicMock()

        fake_mod = type("m", (), {
            "Wallet": FakeWallet,
            "AccountService": lambda *a: MagicMock(),
            "BalanceService": lambda *a: MagicMock(),
            "SafeService": lambda *a: MagicMock(),
            "TransactionService": lambda *a: MagicMock(),
            "TransferService": lambda *a: MagicMock(),
            "PluginService": lambda *a: MagicMock(),
        })()

        with (
            patch("micromech.core.bridge.Wallet", FakeWallet),
            patch("micromech.core.bridge.ChainInterfaces"),
            patch.dict("sys.modules", {
                "iwa.core.constants": MagicMock(WALLET_PATH="/tmp/t"),
                "iwa.core.db": MagicMock(),
                "iwa.core.keys": MagicMock(
                    KeyStorage=MagicMock(return_value=MagicMock()),
                ),
                "iwa.core.wallet": fake_mod,
            }),
        ):
            wallet = bridge.get_wallet()

        assert wallet is not None
        assert bridge._wallet_password is None

        # cleanup
        bridge._cached_wallet = None
        bridge._cached_key_storage = None


# ============================================================
# Delivery: chainId and timeout
# ============================================================


class TestDeliveryChainId:
    def test_tx_receipt_timeout_value(self):
        assert TX_RECEIPT_TIMEOUT == 120

    def test_via_signed_includes_chain_id(self):
        from micromech.core.config import ChainConfig

        chain_cfg = ChainConfig(
            chain="gnosis",
            mech_address="0x77af31De935740567Cf4fF1986D04B2c964A786a",
            multisig_address="0xccA28b516a8c596742Bf23D06324c638230705aE",
            marketplace_address="0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
            factory_address="0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
            staking_address="0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
        )
        bridge = MagicMock()
        bridge.web3.eth.chain_id = 100
        bridge.web3.eth.gas_price = 1000
        bridge.web3.eth.get_transaction_count.return_value = 0
        bridge.web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}
        tx_hash = b"\xca\xfe" + b"\x00" * 30
        bridge.web3.eth.send_raw_transaction.return_value = tx_hash
        bridge.web3.eth.account.sign_transaction.return_value = MagicMock(raw_transaction=b"tx")

        queue = MagicMock()
        config = MicromechConfig()
        dm = DeliveryManager(config=config, chain_config=chain_cfg, queue=queue, bridge=bridge)
        dm._get_signer_key = MagicMock(return_value="0x" + "ff" * 32)

        fn_call = MagicMock()
        dm._via_signed(fn_call, "0x" + "ab" * 20)

        # Verify chainId was passed to build_transaction
        call_args = fn_call.build_transaction.call_args
        tx_params = call_args[0][0]
        assert "chainId" in tx_params
        assert tx_params["chainId"] == 100


# ============================================================
# Management: CreateMech event matching
# ============================================================


class TestCreateMechEventMatching:
    MARKETPLACE = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
    CORRECT_TOPIC = bytes.fromhex(
        "67a2e45041c70013518c5b9b849a6944"
        "a6c17ff44d66be1c707020460ecbd1db"
    )

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_ignores_wrong_topic(self, mock_get_mgr, mock_get_wallet):
        """Log with correct address but wrong topic should NOT match."""
        from micromech.management import MechLifecycle

        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20
        mock_get_mgr.return_value = mock_mgr

        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_wallet.return_value = mock_wallet

        tx_hash = b"\xde\xad" + b"\x00" * 30
        mock_web3.eth.contract.return_value.functions.create.return_value.transact.return_value = tx_hash
        mock_web3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "logs": [
                {
                    "address": self.MARKETPLACE,
                    "topics": [
                        bytes(32),  # wrong topic
                        bytes.fromhex("00" * 12 + "cd" * 20),
                    ],
                }
            ],
        }

        lc = MechLifecycle(MicromechConfig(), chain_name="gnosis")
        result = lc.create_mech("svc-1")
        assert result is None

    @patch("micromech.core.bridge.get_wallet")
    @patch("micromech.management._get_service_manager")
    def test_ignores_wrong_address(self, mock_get_mgr, mock_get_wallet):
        """Log with correct topic but wrong emitting address should NOT match."""
        from micromech.management import MechLifecycle

        mock_mgr = MagicMock()
        mock_mgr.service.service_id = 42
        mock_mgr.service.service_owner_eoa_address = "0x" + "aa" * 20
        mock_get_mgr.return_value = mock_mgr

        mock_web3 = MagicMock()
        mock_wallet = MagicMock()
        mock_wallet.chain_interfaces.get.return_value.web3 = mock_web3
        mock_get_wallet.return_value = mock_wallet

        tx_hash = b"\xde\xad" + b"\x00" * 30
        mock_web3.eth.contract.return_value.functions.create.return_value.transact.return_value = tx_hash
        mock_web3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "logs": [
                {
                    "address": "0x" + "ff" * 20,  # wrong address
                    "topics": [
                        self.CORRECT_TOPIC,
                        bytes.fromhex("00" * 12 + "cd" * 20),
                    ],
                }
            ],
        }

        lc = MechLifecycle(MicromechConfig(), chain_name="gnosis")
        result = lc.create_mech("svc-1")
        assert result is None


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
            id="test", name="test", version="0.1.0",
        )
        tool = Tool(metadata=metadata, run_fn=fake_run)

        # Simulate what executor.py does: tool.execute(prompt, **extra_params)
        # extra_params comes from user HTTP input and could contain 'tool'
        import asyncio
        asyncio.run(tool.execute(
            "real prompt",
            counter_callback="injected_cb",
            extra="safe",
        ))

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
# SSE Stream: auth + connection limit
# ============================================================


class TestSSEStream:
    def test_sse_requires_auth(self, web_client: TestClient):
        """SSE stream without auth token returns 401."""
        resp = web_client.get("/api/metrics/stream")
        assert resp.status_code == 401

    def test_sse_route_exists_with_auth(self, web_client: TestClient):
        """SSE endpoint is registered and requires auth."""
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

    def test_forwarded_for_single(self):
        from micromech.web.app import _get_client_ip
        request = MagicMock()
        request.headers = {"X-Forwarded-For": "10.0.0.1"}
        assert _get_client_ip(request) == "10.0.0.1"

    def test_forwarded_for_chain(self):
        from micromech.web.app import _get_client_ip
        request = MagicMock()
        request.headers = {"X-Forwarded-For": "10.0.0.1, 172.16.0.1, 192.168.1.1"}
        assert _get_client_ip(request) == "10.0.0.1"

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
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202

    def test_valid_hex_no_prefix(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": "abcdef0123"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202

    def test_null_signature_ok(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": None},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202

    def test_invalid_hex_rejected(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": "not-hex!@#"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422

    def test_empty_signature_ok(self, http_client: TestClient):
        resp = http_client.post(
            "/request",
            json={"prompt": "test", "signature": ""},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202
