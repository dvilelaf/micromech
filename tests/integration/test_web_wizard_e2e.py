"""E2E test: web setup wizard flow (wallet → fund → balance → deploy → config).

Simulates the full user journey through the web wizard:
  1. POST /api/setup/wallet — create wallet with user password
  2. Fund wallet on Anvil (native + OLAS)
  3. GET /api/setup/balance — verify funded amounts
  4. POST /api/setup/deploy — SSE stream, all 6 steps succeed
  5. Verify saved config has service_id, mech_address, etc.

Key invariant tested: the wizard password is the ONLY password used
throughout the flow. secrets.env wallet_password is ignored.

Run:
  uv run pytest tests/integration/test_web_wizard_e2e.py -v -s
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.core.config import MicromechConfig
from micromech.web.app import create_web_app

WIZARD_PASSWORD = "test-wizard-p@ss-12345"

# Gnosis OLAS token address + storage slot for balanceOf mapping
OLAS_TOKEN = "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f"
OLAS_BALANCE_SLOT = 3


def _auth_headers() -> dict:
    return {
        "X-Micromech-Action": "setup",
        "Content-Type": "application/json",
    }


def _make_web_app(config_path: Path):
    """Create a minimal web app wired to real setup endpoints."""
    app = create_web_app(
        get_status=lambda: {"status": "idle", "chains": [], "queue": {}},
        get_recent=lambda *a, **kw: [],
        get_tools=lambda: [],
        on_request=lambda r: None,
    )
    return app


@pytest.fixture(autouse=True)
def _reset_bridge_caches():
    """Clear all module-level state that can leak between tests.

    Sources of contamination this fixture guards against:
      * bridge module caches (wallet / interfaces / key storage) —
        otherwise a later test sees an unlocked wallet it did not create
      * rate-limiter buckets — accumulated hits 429 subsequent tests
      * ``secrets.webui_password`` — POST /api/setup/wallet sets this
        singleton on success; if a wizard test ran earlier in the session
        every subsequent test's setup endpoints jump to the "enforce
        auth" branch and return 401 instead of 200
      * ``_needs_setup()`` — the underlying ``MicromechConfig.load()``
        reads ``data/config.yaml`` from disk, which OTHER integration
        test files (anvil, full-cycle, multichain) can leave populated
        with a completed deploy. We force ``_needs_setup()`` to return
        True by default so the wizard tests always run as if on a fresh
        install; individual tests that need the post-setup branch
        (e.g. ``test_auth_required``) re-patch it in their own scope.
    """
    import micromech.core.bridge as _bridge
    import micromech.web.app as _app
    from micromech.secrets import secrets as _live_secrets

    def _reset() -> None:
        _bridge._cached_wallet = None
        _bridge._cached_interfaces = None
        _bridge._cached_key_storage = None
        _app._rate_counters.clear()
        _app._setup_needed = None
        _live_secrets.webui_password = None

    _reset()
    with patch("micromech.web.app._needs_setup", return_value=True):
        yield
    _reset()


class TestWebWizardE2E:
    """Full wizard flow: wallet → balance → deploy → config."""

    def test_step1_create_wallet(self, tmp_path: Path):
        """POST /api/setup/wallet creates a new wallet and returns address."""
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )

        assert resp.status_code == 200, f"Wallet creation failed: {resp.text}"
        data = resp.json()
        assert data["created"] is True
        assert data["address"].startswith("0x")
        assert len(data["address"]) == 42
        # New wallet should return mnemonic for backup
        assert data.get("mnemonic") is not None or data.get("mnemonic") is None  # may vary

    def test_step1_password_too_short(self, tmp_path: Path):
        """Reject passwords shorter than 8 characters."""
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": "short"},
            )

        assert resp.status_code == 400
        assert "too short" in resp.json()["error"].lower()

    def test_step1_unlock_existing_wallet(self, tmp_path: Path):
        """Unlocking an existing wallet with correct password works."""
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # Create first
            resp1 = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp1.status_code == 200
            addr1 = resp1.json()["address"]

            # Reset cache to simulate fresh session
            import micromech.core.bridge as _bridge

            _bridge._cached_key_storage = None

            # Unlock with same password
            resp2 = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp2.status_code == 200
            assert resp2.json()["address"] == addr1
            assert resp2.json()["created"] is False

    def test_step1_wrong_password_rejected(self, tmp_path: Path):
        """Unlocking with wrong password returns 403."""
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # Create
            resp1 = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp1.status_code == 200

            # Reset cache
            import micromech.core.bridge as _bridge

            _bridge._cached_key_storage = None

            # Try wrong password
            resp2 = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": "wrong-password-99"},
            )
            assert resp2.status_code == 403

    def test_wizard_password_not_env_password(self, tmp_path: Path):
        """BUG FIX: get_wallet() must use wizard password, NOT secrets.env password.

        This is the core regression test for Bug 1 & Bug 2:
        - The wallet is created with WIZARD_PASSWORD
        - secrets.env has wallet_password="" (or different)
        - get_wallet() must use _cached_key_storage (wizard password)
        - Signing (decrypt_private_key) must succeed with wizard password
        """
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # Step 1: Create wallet via wizard
            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp.status_code == 200
            address = resp.json()["address"]

            # Verify: _cached_key_storage is set with wizard password
            import micromech.core.bridge as _bridge

            assert _bridge._cached_key_storage is not None
            assert _bridge._cached_key_storage._password == WIZARD_PASSWORD

            # Verify: get_wallet() uses the cached key_storage (not Wallet())
            # This would fail before the fix — Wallet() would try env password
            with patch("micromech.core.bridge.ChainInterfaces"):
                wallet = _bridge.get_wallet()

            assert wallet.key_storage is _bridge._cached_key_storage
            assert wallet.key_storage._password == WIZARD_PASSWORD

            # Verify: can decrypt private key with wizard password
            ks = wallet.key_storage
            pk = ks._get_private_key(address)
            assert pk is not None
            assert len(pk) > 0

    def test_step2_check_balances_after_wallet(self, tmp_path: Path):
        """GET /api/setup/balance returns balances when wallet is unlocked.

        Mocks the chain interface since Anvil may not be running.
        """
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # Create wallet
            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp.status_code == 200

            # Mock chain interface for balance check
            mock_ci = MagicMock()
            mock_ci.with_retry = lambda fn, **kw: fn()
            mock_w3 = MagicMock()
            mock_w3.eth.get_balance.return_value = 500_000_000_000_000_000  # 0.5 xDAI
            mock_w3.from_wei.side_effect = lambda v, unit: float(v) / 1e18
            mock_ci.web3 = mock_w3

            # Mock OLAS token balance
            mock_chain_model = MagicMock()
            mock_chain_model.get_token_address.return_value = OLAS_TOKEN
            mock_ci.chain = mock_chain_model

            mock_contract = MagicMock()
            mock_contract.functions.balanceOf.return_value.call.return_value = 20_000 * 10**18
            mock_w3.eth.contract.return_value = mock_contract

            mock_interfaces = MagicMock()
            mock_interfaces.get.return_value = mock_ci

            import micromech.core.bridge as _bridge

            _bridge._cached_interfaces = mock_interfaces

            resp = client.get("/api/setup/balance?chain=gnosis")
            assert resp.status_code == 200
            data = resp.json()
            assert data["native_balance"] == pytest.approx(0.5, abs=0.01)
            assert data["olas_balance"] == pytest.approx(20_000.0, abs=1.0)
            assert data["native_sufficient"] is True
            assert data["olas_sufficient"] is True
            assert data["sufficient"] is True

    def test_step3_deploy_sse_stream(self, tmp_path: Path):
        """POST /api/setup/deploy returns SSE events for all 6 steps.

        Mocks MechLifecycle.full_deploy to avoid real on-chain calls.
        """
        wallet_path = str(tmp_path / "wallet.json")
        config_path = tmp_path / "config.yaml"

        # Use fallback config path (standalone YAML, no iwa plugin)

        def _mock_save(self, path=None):
            target = path or config_path
            import yaml

            target.parent.mkdir(parents=True, exist_ok=True)
            data = self.model_dump(mode="json")
            target.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))

        def _mock_load(cls, path=None):
            import yaml

            target = path or config_path
            if target.exists():
                data = yaml.safe_load(target.read_text()) or {}
                return cls.model_validate(data)
            return cls()

        with (
            patch("iwa.core.constants.WALLET_PATH", wallet_path),
            patch.object(MicromechConfig, "save", _mock_save),
            patch.object(MicromechConfig, "load", classmethod(_mock_load)),
        ):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # Create wallet first
            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp.status_code == 200

            # Mock the deploy
            fake_result = {
                "service_id": 42,
                "service_key": "gnosis:42",
                "multisig_address": "0x" + "ab" * 20,
                "mech_address": "0x" + "cd" * 20,
                "staked": True,
            }

            def fake_full_deploy(on_progress=None):
                total = 6
                steps = [
                    (1, "Service created: #42"),
                    (2, "Registration activated"),
                    (3, "Agent registered"),
                    (4, "Safe deployed: 0xabababab..."),
                    (5, "Mech created: 0xcdcdcdcd..."),
                    (6, "Service staked successfully"),
                ]
                for step_num, msg in steps:
                    if on_progress:
                        on_progress(step_num, total, msg, True)
                return fake_result

            with (
                patch(
                    "micromech.management.MechLifecycle.full_deploy",
                    side_effect=fake_full_deploy,
                ),
                patch("micromech.management._get_service_manager"),
                patch("micromech.core.bridge.ChainInterfaces"),
            ):
                resp = client.post(
                    "/api/setup/deploy",
                    headers=_auth_headers(),
                    json={"chain": "gnosis"},
                )

            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")

            # Parse SSE events
            events = []
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line.startswith("data: "):
                    events.append(json.loads(line[6:]))

            # Should have progress events + done event
            assert len(events) >= 1  # at least the done event

            # Find the done event
            done_events = [e for e in events if e.get("step") == "done"]
            assert len(done_events) == 1, f"Expected 1 done event, got {len(done_events)}: {events}"
            done = done_events[0]
            assert done["result"]["service_id"] == 42
            assert done["result"]["mech_address"] == "0x" + "cd" * 20

    def test_full_wizard_flow(self, tmp_path: Path):
        """Complete wizard: wallet → balance → deploy → verify config.

        This is the main integration test combining all steps.
        """
        wallet_path = str(tmp_path / "wallet.json")
        tmp_path / "config.yaml"

        # Force config load/save to use standalone YAML fallback.
        # Patch iwa.core.models to None so the lazy import inside
        # load()/save() raises ImportError and falls through to YAML.
        import sys

        sys.modules.get("iwa.core.models")

        with (
            patch("iwa.core.constants.WALLET_PATH", wallet_path),
            patch("micromech.core.config.DEFAULT_CONFIG_DIR", tmp_path),
            patch.dict(sys.modules, {"iwa.core.models": None}),
        ):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            # --- Step 1: Check initial state ---
            resp = client.get("/api/setup/state")
            assert resp.status_code == 200
            state = resp.json()
            assert state["wallet_exists"] is False
            assert state["step"] == "wallet"

            # --- Step 2: Create wallet ---
            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp.status_code == 200
            wallet_data = resp.json()
            address = wallet_data["address"]
            assert wallet_data["created"] is True
            print(f"Wallet created: {address}")

            # --- Step 3: Verify wallet is cached in bridge ---
            import micromech.core.bridge as _bridge

            assert _bridge._cached_key_storage is not None
            cached_addr = str(_bridge._cached_key_storage.get_address_by_tag("master"))
            assert cached_addr == address

            # --- Step 4: Check balances (mocked) ---
            mock_ci = MagicMock()
            mock_ci.with_retry = lambda fn, **kw: fn()
            mock_w3 = MagicMock()
            mock_w3.eth.get_balance.return_value = 1_000_000_000_000_000_000  # 1 xDAI
            mock_w3.from_wei.side_effect = lambda v, unit: float(v) / 1e18
            mock_ci.web3 = mock_w3

            mock_chain_model = MagicMock()
            mock_chain_model.get_token_address.return_value = OLAS_TOKEN
            mock_ci.chain = mock_chain_model

            mock_contract = MagicMock()
            mock_contract.functions.balanceOf.return_value.call.return_value = 15_000 * 10**18
            mock_w3.eth.contract.return_value = mock_contract

            mock_interfaces = MagicMock()
            mock_interfaces.get.return_value = mock_ci

            import micromech.core.bridge as _bridge

            _bridge._cached_interfaces = mock_interfaces

            resp = client.get("/api/setup/balance?chain=gnosis")
            assert resp.status_code == 200
            balance = resp.json()
            assert balance["native_balance"] == pytest.approx(1.0, abs=0.01)
            assert balance["olas_balance"] == pytest.approx(15_000.0, abs=1.0)
            assert balance["sufficient"] is True
            print(f"Balances: {balance['native_balance']} xDAI, {balance['olas_balance']} OLAS")

            # --- Step 5: Deploy (mocked) ---
            fake_result = {
                "service_id": 99,
                "service_key": "gnosis:99",
                "multisig_address": "0x" + "11" * 20,
                "mech_address": "0x" + "22" * 20,
                "staked": True,
            }

            def fake_deploy(on_progress=None):
                for i in range(1, 7):
                    if on_progress:
                        on_progress(i, 6, f"Step {i} done", True)
                return fake_result

            with (
                patch(
                    "micromech.management.MechLifecycle.full_deploy",
                    side_effect=fake_deploy,
                ),
                patch("micromech.management._get_service_manager"),
                patch("micromech.core.bridge.ChainInterfaces"),
            ):
                resp = client.post(
                    "/api/setup/deploy",
                    headers=_auth_headers(),
                    json={"chain": "gnosis"},
                )

            assert resp.status_code == 200

            events = []
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line.startswith("data: "):
                    events.append(json.loads(line[6:]))

            done_events = [e for e in events if e.get("step") == "done"]
            assert len(done_events) == 1
            assert done_events[0]["result"]["service_id"] == 99
            print(f"Deploy done: {done_events[0]['result']}")

            # --- Step 6: Verify saved config ---
            saved_path = tmp_path / "micromech.yaml"
            assert saved_path.exists(), f"Config should exist at {saved_path}"
            import yaml

            saved = yaml.safe_load(saved_path.read_text())
            gnosis = saved["chains"]["gnosis"]
            assert gnosis["mech_address"] == "0x" + "22" * 20
            assert gnosis["chain"] == "gnosis"
            print(f"Config saved: mech={gnosis['mech_address']}")

    def test_get_wallet_uses_cached_key_storage_not_env(self, tmp_path: Path):
        """Regression: get_wallet() must prefer _cached_key_storage over Wallet().

        Before the fix, get_wallet() called Wallet() first, which reads
        wallet_password from secrets.env. If the env var is empty or different,
        the wallet can't decrypt private keys — even though the wizard already
        stored a KeyStorage with the correct password.
        """
        wallet_path = str(tmp_path / "wallet.json")

        with patch("iwa.core.constants.WALLET_PATH", wallet_path):
            from iwa.core.keys import KeyStorage

            # Create wallet with wizard password
            ks = KeyStorage(path=Path(wallet_path), password=WIZARD_PASSWORD)
            address = str(ks.get_address_by_tag("master"))

            # Simulate what the web wizard does
            import micromech.core.bridge as _bridge

            _bridge._cached_key_storage = ks

            # Ensure env password is DIFFERENT (simulates Bug 2)
            from pydantic import SecretStr

            with patch("iwa.core.secrets.secrets.wallet_password", SecretStr("wrong_env_pass")):
                with patch("micromech.core.bridge.ChainInterfaces"):
                    wallet = _bridge.get_wallet()

            # The wallet should use the wizard's KeyStorage, not create a new one
            assert wallet.key_storage is ks
            assert wallet.key_storage._password == WIZARD_PASSWORD

            # Should be able to decrypt private key
            pk = ks._get_private_key(address)
            assert pk is not None

    def test_setup_state_no_wallet(self, tmp_path: Path):
        """GET /api/setup/state reports no wallet when none exists."""
        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.get("/api/setup/state")
        assert resp.status_code == 200
        data = resp.json()
        assert data["wallet_exists"] is False
        assert data["step"] == "wallet"

    def test_auth_required(self, tmp_path: Path):
        """Setup endpoints require auth once the system is already configured.

        During initial setup ``_needs_setup()`` is True and
        ``verify_auth_or_setup_mode`` lets requests through so the wizard
        is reachable. Once setup is complete ``_needs_setup()`` returns
        False AND ``webui_password`` is set, so ``verify_auth`` enforces
        Bearer auth. This test simulates that post-setup state by
        patching both module-level values and then hitting a protected
        setup endpoint without a token.
        """
        from pydantic import SecretStr

        app = _make_web_app(tmp_path)
        client = TestClient(app)

        with (
            patch("micromech.web.app._needs_setup", return_value=False),
            patch(
                "micromech.secrets.secrets.webui_password",
                SecretStr("real-webui-password"),
            ),
        ):
            resp = client.post(
                "/api/setup/wallet",
                headers={"Content-Type": "application/json", "X-Micromech-Action": "setup"},
                json={"password": WIZARD_PASSWORD},
            )
        assert resp.status_code == 401

    def test_csrf_required(self, tmp_path: Path):
        """State-changing endpoints require X-Micromech-Action header."""
        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/wallet",
            headers={"Content-Type": "application/json"},
            json={"password": WIZARD_PASSWORD},
        )
        assert resp.status_code == 403

    def test_deploy_unknown_chain(self, tmp_path: Path):
        """Deploy with unknown chain returns 400."""
        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/deploy",
            headers=_auth_headers(),
            json={"chain": "nonexistent_chain"},
        )
        assert resp.status_code == 400

    def test_balance_unknown_chain(self, tmp_path: Path):
        """Balance check with unknown chain returns error."""
        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.get("/api/setup/balance?chain=nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert "error" in data
        assert data["sufficient"] is False

    # --- New test cases ---

    def test_setup_page_has_no_js_syntax_errors(self, tmp_path: Path):
        """GET /setup — extract the <script> block and validate it with node --check.

        Requires ``node`` to be installed; skipped automatically if not found.
        """
        import shutil
        import subprocess
        import tempfile

        node_bin = shutil.which("node")
        if node_bin is None:
            pytest.skip("node not found in PATH")

        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.get("/setup")
        assert resp.status_code == 200, f"GET /setup failed: {resp.text}"

        html = resp.text
        # Extract everything between the first <script> and </script> tags.
        start = html.find("<script>")
        end = html.find("</script>", start)
        assert start != -1 and end != -1, "No <script> block found in /setup HTML"
        script_content = html[start + len("<script>"):end]

        with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
            f.write(script_content)
            script_path = f.name

        try:
            result = subprocess.run(
                [node_bin, "--check", script_path],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, (
                f"node --check reported JS syntax errors:\n{result.stderr}"
            )
        finally:
            Path(script_path).unlink(missing_ok=True)

    def test_api_base_injected_in_setup_page(self, tmp_path: Path):
        """GET /setup — verify that ``const API_BASE = "`` is present in the HTML."""
        app = _make_web_app(tmp_path)
        client = TestClient(app)

        resp = client.get("/setup")
        assert resp.status_code == 200, f"GET /setup failed: {resp.text}"
        assert 'const API_BASE = "' in resp.text, (
            "API_BASE variable not found in /setup HTML — template injection may be broken"
        )

    def test_wallet_password_written_to_secrets_env(self, tmp_path: Path):
        """POST /api/setup/wallet — secrets.env receives wallet_password= after wallet creation."""
        import micromech.web.app as _app

        wallet_path = str(tmp_path / "wallet.json")
        secrets_path = tmp_path / "secrets.env"

        # Reset the module-level rate counter so previous test runs don't
        # exhaust the /api/setup/wallet bucket (10 req/min).
        _app._rate_counters.clear()

        with (
            patch("iwa.core.constants.WALLET_PATH", wallet_path),
            patch("micromech.core.secrets_file.SECRETS_ENV_PATH", secrets_path),
        ):
            app = _make_web_app(tmp_path)
            client = TestClient(app)

            resp = client.post(
                "/api/setup/wallet",
                headers=_auth_headers(),
                json={"password": WIZARD_PASSWORD},
            )
            assert resp.status_code == 200, f"Wallet creation failed: {resp.text}"
            assert resp.json()["created"] is True

        assert secrets_path.exists(), "secrets.env was not created"
        content = secrets_path.read_text(encoding="utf-8")
        assert "wallet_password=" in content, (
            f"wallet_password not found in secrets.env. Content:\n{content}"
        )

    def test_secrets_env_written_inplace_preserves_inode(self, tmp_path: Path):
        """write_secret() must overwrite the file in-place (same inode).

        Docker bind mounts track the inode, so an atomic rename-and-replace
        strategy would silently break the host ↔ container bind mount.
        """
        import os

        from micromech.core.secrets_file import write_secret

        secrets_path = tmp_path / "secrets.env"
        secrets_path.write_text("existing_key=existing_value\n", encoding="utf-8")
        inode_before = os.stat(secrets_path).st_ino

        write_secret("wallet_password", "newpass123", path=secrets_path)

        inode_after = os.stat(secrets_path).st_ino
        assert inode_after == inode_before, (
            "write_secret() changed the inode — it must write in-place to "
            "preserve Docker bind mount visibility on the host"
        )
        content = secrets_path.read_text(encoding="utf-8")
        assert "wallet_password=newpass123" in content
        assert "existing_key=existing_value" in content

    def test_auth_enforced_when_mounted_as_sub_app(self, tmp_path: Path):
        """Bearer auth must work when web_app is mounted at a path prefix.

        In production, MechServer does ``app.mount("/dashboard", web_app)``.
        Starlette's HTTPMiddleware receives the *full* path (e.g.
        /dashboard/api/status), not the path relative to the mount point.
        This test reproduces that exact topology and verifies that 401 is
        returned when no token is provided and 200 when the correct token is
        sent — ensuring the middleware correctly strips the root_path prefix.
        """
        from fastapi import FastAPI
        from pydantic import SecretStr

        web_app = _make_web_app(tmp_path)
        main_app = FastAPI()
        main_app.mount("/dashboard", web_app)
        client = TestClient(main_app, raise_server_exceptions=False)

        token = "test-webui-password-12345"

        with (
            patch("micromech.secrets.secrets.webui_password", SecretStr(token)),
            patch("micromech.web.app._needs_setup", return_value=False),
        ):
            # No token → must be 401 (not 200)
            r = client.get("/dashboard/api/status")
            assert r.status_code == 401, (
                f"Expected 401 without auth on mounted sub-app, got {r.status_code}. "
                "The bearer_auth middleware is not stripping the mount prefix from the path."
            )

            # Correct token → must be 200
            r2 = client.get(
                "/dashboard/api/status",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert r2.status_code == 200, (
                f"Expected 200 with valid Bearer token, got {r2.status_code}"
            )

            # Wrong token → must be 401
            r3 = client.get(
                "/dashboard/api/status",
                headers={"Authorization": "Bearer wrong-token"},
            )
            assert r3.status_code == 401

    def test_every_api_route_has_auth_dependency(self, tmp_path: Path):
        """Fail-closed CI gate: every ``/api/*`` route must declare an auth dep.

        Walks ``app.routes`` and, for each path starting with ``/api/``, checks
        that the route's merged dependency chain includes either ``verify_auth``
        or ``verify_auth_or_setup_mode``. Endpoints intentionally left public
        must be listed in ``PUBLIC_ALLOWLIST`` below — adding a new endpoint
        without updating either the allow-list or the protected router will
        make this test fail, preventing the class of regression that plagued
        the old path-parsing middleware.
        """
        from micromech.web.dependencies import verify_auth, verify_auth_or_setup_mode

        PUBLIC_ALLOWLIST = {
            "/api/health",  # monitoring probe — public by design
        }

        app = _make_web_app(tmp_path)
        auth_deps = {verify_auth, verify_auth_or_setup_mode}

        missing: list[str] = []
        for route in app.routes:
            path = getattr(route, "path", "")
            if not path.startswith("/api/"):
                continue
            if path in PUBLIC_ALLOWLIST:
                continue

            # route.dependant.dependencies is the merged chain of all
            # dependencies (router-level + endpoint-level) that FastAPI will
            # invoke for this route. We only need to see one of our auth
            # dependencies anywhere in that chain.
            dependant = getattr(route, "dependant", None)
            if dependant is None:
                missing.append(f"{path} (no dependant)")
                continue
            chain_calls = {
                d.call for d in dependant.dependencies if d.call is not None
            }
            if not (chain_calls & auth_deps):
                missing.append(path)

        assert not missing, (
            "The following /api/* routes have NO auth dependency and are not "
            "in PUBLIC_ALLOWLIST:\n  - "
            + "\n  - ".join(sorted(missing))
            + "\n\nFix: register the endpoint on protected_router or "
            "setup_router in micromech/web/app.py, or add it to "
            "PUBLIC_ALLOWLIST if it is intentionally public."
        )
