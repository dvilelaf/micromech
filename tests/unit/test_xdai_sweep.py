"""Tests for tasks/xdai_sweep.py."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.tasks.notifications import NotificationService
from micromech.tasks.xdai_sweep import xdai_sweep_task
from tests.conftest import make_test_config

MASTER_ADDR = "0x" + "f" * 40
DEST_ADDR = "0x" + "a" * 40


def _make_wallet(balance=35.0, dest_addr=DEST_ADDR):
    wallet = MagicMock()
    wallet.master_account.address = MASTER_ADDR
    wallet.get_native_balance_eth.return_value = balance
    wallet.account_service.get_address_by_tag.return_value = dest_addr
    wallet.send.return_value = "0xtxhash"
    return wallet


def _make_bridge(wallet):
    bridge = MagicMock()
    bridge.wallet = wallet
    return bridge


def _make_config(**kw):
    cfg = make_test_config(**kw)
    cfg.xdai_sweep_enabled = True
    cfg.xdai_sweep_tag = "sweep_dest"
    cfg.xdai_sweep_threshold_xdai = 30.0
    cfg.xdai_sweep_amount_xdai = 20.0
    cfg.xdai_sweep_interval_hours = 6
    return cfg


class TestXdaiSweepTask:
    @pytest.mark.asyncio
    async def test_skips_when_tag_empty(self):
        cfg = _make_config()
        cfg.xdai_sweep_tag = ""
        wallet = _make_wallet()
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_wallet", return_value=wallet):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_balance_below_threshold(self):
        cfg = _make_config()
        wallet = _make_wallet(balance=10.0)
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_wallet", return_value=wallet):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_tag_not_found(self):
        cfg = _make_config()
        wallet = _make_wallet(balance=50.0)
        wallet.account_service.get_address_by_tag.side_effect = Exception("not found")
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        mock_iwa_config = MagicMock()
        mock_iwa_config.core.whitelist.get.return_value = None

        with (
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
            patch("iwa.core.models.Config", return_value=mock_iwa_config),
        ):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolves_tag_from_whitelist(self):
        """Falls back to iwa whitelist when wallet tag lookup raises."""
        cfg = _make_config()
        wallet = _make_wallet(balance=50.0)
        wallet.account_service.get_address_by_tag.side_effect = Exception("not found")
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        mock_iwa_config = MagicMock()
        mock_iwa_config.core.whitelist.get.return_value = DEST_ADDR

        with (
            patch("micromech.core.bridge.get_wallet", return_value=wallet),
            patch("iwa.core.models.Config", return_value=mock_iwa_config),
        ):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_called_once()
        notification.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sweeps_when_balance_above_threshold(self):
        cfg = _make_config()
        wallet = _make_wallet(balance=35.0)
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_wallet", return_value=wallet):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_called_once()
        call_kw = wallet.send.call_args.kwargs
        assert call_kw["from_address_or_tag"] == "master"
        assert call_kw["to_address_or_tag"] == DEST_ADDR
        assert call_kw["amount_wei"] == int(20.0 * 1e18)
        assert call_kw["chain_name"] == "gnosis"
        notification.send.assert_awaited_once()
        msg = notification.send.call_args[0][1]
        assert "20.0000" in msg

    @pytest.mark.asyncio
    async def test_skips_when_no_gnosis_bridge(self):
        cfg = _make_config()
        wallet = _make_wallet(balance=50.0)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_wallet", return_value=wallet):
            await xdai_sweep_task({}, notification, cfg)

        wallet.send.assert_not_called()
        notification.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        """Exceptions are caught and do not propagate."""
        cfg = _make_config()
        wallet = _make_wallet(balance=50.0)
        wallet.get_native_balance_eth.side_effect = RuntimeError("rpc error")
        bridge = _make_bridge(wallet)
        notification = NotificationService()
        notification.send = AsyncMock()

        with patch("micromech.core.bridge.get_wallet", return_value=wallet):
            await xdai_sweep_task({"gnosis": bridge}, notification, cfg)

        wallet.send.assert_not_called()
        notification.send.assert_not_called()
