"""Tests for /api/marketplace/pending-payments endpoint."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.web.app import create_web_app

_MECH = "0x1111111111111111111111111111111111111111"
_MARKET = "0x2222222222222222222222222222222222222222"
_BT = "0x3333333333333333333333333333333333333333"
_MECH_B = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_MARKET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _app(**kw):
    defaults = dict(
        get_status=lambda: {
            "status": "running",
            "queue": {"pending": 0},
            "tools": [],
            "delivered_total": 0,
        },
        get_recent=lambda limit=20, chain=None: [],
        get_tools=lambda: [],
        on_request=AsyncMock(),
    )
    defaults.update(kw)
    return create_web_app(**defaults)


def _client(**kw):
    return TestClient(_app(**kw), raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _reset_rate_counters():
    import micromech.secrets as secrets_mod
    from micromech.web import app as app_mod

    # Clear rate limiter state to avoid 429 cross-test contamination
    app_mod._rate_counters.clear()

    # Ensure webui_password is unset so auth middleware lets tests through
    orig = secrets_mod.secrets
    mock_s = MagicMock()
    mock_s.webui_password = None
    secrets_mod.secrets = mock_s

    yield

    secrets_mod.secrets = orig
    app_mod._rate_counters.clear()


def _chain_cfg(mech=_MECH, market=_MARKET):
    cfg = MagicMock()
    cfg.mech_address = mech
    cfg.marketplace_address = market
    return cfg


def _mock_config(chains):
    cfg = MagicMock()
    cfg.chains = chains
    cfg.enabled_chains = chains
    return cfg


# ---------------------------------------------------------------------------
# Successful pending payment retrieval
# ---------------------------------------------------------------------------


class TestPendingPaymentsSuccess:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_returns_pending_balance(self, _mock):
        """Happy path: returns pending xDAI for a configured chain."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=1.5,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        assert resp.json()["gnosis"]["pending"] == 1.5

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_zero_pending_balance(self, _mock):
        """Zero balance is returned correctly."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=0.0,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        assert resp.json()["gnosis"]["pending"] == 0.0

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_balance_rounded_to_6_decimals(self, _mock):
        """Balance is rounded to 6 decimal places."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()
        raw = 1.123456789

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=raw,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        assert resp.json()["gnosis"]["pending"] == round(raw, 6)


# ---------------------------------------------------------------------------
# Not configured / missing addresses
# ---------------------------------------------------------------------------


class TestPendingPaymentsNotConfigured:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_no_mech_address_returns_not_configured(self, _mock):
        """Chain without mech_address gets error=not configured."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg(mech=None)})

        with patch(
            "micromech.web.app.MicromechConfig.load",
            return_value=mock_cfg,
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()["gnosis"]
        assert data["pending"] == 0.0
        assert data["error"] == "not configured"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_no_marketplace_address_returns_not_configured(self, _mock):
        """Chain without marketplace_address gets error=not configured."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg(market=None)})

        with patch(
            "micromech.web.app.MicromechConfig.load",
            return_value=mock_cfg,
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()["gnosis"]
        assert data["pending"] == 0.0
        assert data["error"] == "not configured"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_zero_bt_address_returns_zero_pending(self, _mock):
        """When balance tracker resolves to None, returns pending=0."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=None,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()["gnosis"]
        assert data["pending"] == 0.0
        assert "error" not in data

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_empty_chains(self, _mock):
        """No enabled chains → empty dict."""
        c = _client()
        with patch(
            "micromech.web.app.MicromechConfig.load",
            return_value=_mock_config({}),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        assert resp.json() == {}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestPendingPaymentsErrors:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_bridge_exception_returns_error(self, _mock):
        """IwaBridge raising an exception is caught per-chain."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                side_effect=RuntimeError("rpc down"),
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()["gnosis"]
        assert data["pending"] == 0.0
        assert data["error"] == "check failed"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_get_pending_balance_exception_returns_error(self, _mock):
        """_get_pending_balance raising is caught per-chain."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                side_effect=Exception("contract error"),
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()["gnosis"]
        assert data["pending"] == 0.0
        assert data["error"] == "check failed"

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_config_load_exception_returns_error(self, _mock):
        """MicromechConfig.load() raising propagates as top-level error."""
        c = _client()

        with patch(
            "micromech.web.app.MicromechConfig.load",
            side_effect=Exception("config corrupt"),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Chain filter parameter
# ---------------------------------------------------------------------------


class TestPendingPaymentsChainFilter:
    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_chain_filter_queries_only_that_chain(self, _mock):
        """?chain=gnosis returns only gnosis, ignoring base."""
        c = _client()
        chains = {
            "gnosis": _chain_cfg(),
            "base": _chain_cfg(mech=_MECH_B, market=_MARKET_B),
        }
        mock_cfg = MagicMock()
        mock_cfg.chains = chains
        mock_cfg.enabled_chains = chains
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=2.5,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments?chain=gnosis")

        assert resp.status_code == 200
        data = resp.json()
        assert "gnosis" in data
        assert "base" not in data

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_unknown_chain_falls_back_to_all(self, _mock):
        """?chain=unknown (not in config) falls back to enabled_chains."""
        c = _client()
        mock_cfg = _mock_config({"gnosis": _chain_cfg()})
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=0.0,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments?chain=unknown")

        assert resp.status_code == 200
        # Falls back to all enabled_chains when chain not found
        assert "gnosis" in resp.json()

    @patch("micromech.web.app._needs_setup", return_value=False)
    def test_multiple_chains_all_returned(self, _mock):
        """Without ?chain= filter, all enabled chains are queried."""
        c = _client()
        mock_cfg = _mock_config(
            {
                "gnosis": _chain_cfg(),
                "base": _chain_cfg(mech=_MECH_B, market=_MARKET_B),
            }
        )
        mock_bridge = MagicMock()

        with (
            patch(
                "micromech.web.app.MicromechConfig.load",
                return_value=mock_cfg,
            ),
            patch(
                "micromech.core.bridge.IwaBridge",
                return_value=mock_bridge,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_balance_tracker_address",
                return_value=_BT,
            ),
            patch(
                "micromech.tasks.payment_withdraw._get_pending_balance",
                return_value=0.5,
            ),
        ):
            resp = c.get("/api/marketplace/pending-payments")

        assert resp.status_code == 200
        data = resp.json()
        assert "gnosis" in data
        assert "base" in data
