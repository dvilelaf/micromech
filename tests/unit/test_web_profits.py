"""Tests for the WebUI profits endpoints."""

from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from micromech.web.app import create_web_app

MULTISIG = "0x0EE0CA8A2fc8a5d9aa92a80Ae4e6A86DcAc81953"


@dataclass
class _Tx:
    tx_hash: str
    token: str
    amount_wei: str
    value_eur: float
    gas_value_eur: float
    timestamp: dt.datetime
    from_address: str = MULTISIG
    from_tag: str = "service_3098_multisig"
    to_address: str = "0xmaster"
    to_tag: str = "master"
    chain: str = "gnosis"


class _Query:
    def __init__(self, rows: list[_Tx]):
        self.rows = rows

    def where(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self

    def order_by(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self

    def __iter__(self):
        return iter(self.rows)


@pytest.fixture(autouse=True)
def _reset_rate_counters():
    from micromech.web import app as app_mod

    app_mod._rate_counters.clear()
    yield
    app_mod._rate_counters.clear()


def _client() -> TestClient:
    return TestClient(
        create_web_app(
            get_status=lambda: {"status": "running", "queue": {}},
            get_recent=lambda limit=20, chain=None: [],
            get_tools=lambda: [],
            on_request=AsyncMock(),
        ),
        raise_server_exceptions=False,
    )


@contextmanager
def _patch_profit_sources(rows: list[_Tx]):
    mock_cfg = MagicMock()
    mock_cfg.enabled_chains = {"gnosis": MagicMock()}
    with (
        patch("micromech.web.app.MicromechConfig.load", return_value=mock_cfg),
        patch(
            "micromech.core.bridge.get_service_info",
            return_value={"multisig_address": MULTISIG},
        ),
        patch("iwa.core.db.SentTransaction.select", return_value=_Query(rows)),
    ):
        yield


def test_profits_summary_counts_only_xdai_withdrawals():
    rows = [
        _Tx(
            tx_hash="0xxdai",
            token="xDAI",
            amount_wei=str(278_570_000_000_000_000_000),
            value_eur=237.371284,
            gas_value_eur=0.12,
            timestamp=dt.datetime(2026, 4, 28, 12, 0, 0),
        ),
        _Tx(
            tx_hash="0xolas",
            token="OLAS",
            amount_wei=str(339_253_356_482_000_000_000),
            value_eur=11.961117,
            gas_value_eur=0.03,
            timestamp=dt.datetime(2026, 4, 28, 13, 0, 0),
        ),
    ]

    c = _client()
    with _patch_profit_sources(rows):
        resp = c.get("/api/profits/summary?year=2026")

    assert resp.status_code == 200
    data = resp.json()
    assert data["total_xdai"] == 278.57
    assert data["total_eur"] == 237.37
    assert data["total_gas_eur"] == 0.12
    assert data["total_withdrawals"] == 1
    assert data["months"][3]["xdai"] == 278.57
    assert data["months"][3]["count"] == 1


def test_profits_withdrawals_table_counts_only_xdai_withdrawals():
    rows = [
        _Tx(
            tx_hash="0xxdai",
            token="xDAI",
            amount_wei=str(1_000_000_000_000_000_000),
            value_eur=0.85,
            gas_value_eur=0.01,
            timestamp=dt.datetime(2026, 4, 28, 12, 0, 0),
        ),
        _Tx(
            tx_hash="0xolas",
            token="OLAS",
            amount_wei=str(100_000_000_000_000_000_000),
            value_eur=3.60,
            gas_value_eur=0.01,
            timestamp=dt.datetime(2026, 4, 28, 13, 0, 0),
        ),
    ]

    c = _client()
    with _patch_profit_sources(rows):
        resp = c.get("/api/profits/withdrawals?year=2026")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["tx_hash"] == "0xxdai"
    assert data[0]["xdai"] == 1.0
    assert data[0]["eur"] == 0.85
