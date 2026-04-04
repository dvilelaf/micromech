"""Tests for core/bridge.py — get_wallet() and check_balances()."""

from unittest.mock import MagicMock, patch

import pytest

import micromech.core.bridge as bridge


@pytest.fixture(autouse=True)
def reset_bridge_cache():
    """Reset module-level cache before each test."""
    bridge._cached_wallet = None
    bridge._cached_interfaces = None
    bridge._cached_key_storage = None
    yield
    bridge._cached_wallet = None
    bridge._cached_interfaces = None
    bridge._cached_key_storage = None


class TestGetWallet:
    def test_returns_cached_wallet(self):
        mock_wallet = MagicMock()
        bridge._cached_wallet = mock_wallet
        assert bridge.get_wallet() is mock_wallet

    @patch("micromech.core.bridge.Wallet")
    @patch("micromech.core.bridge.ChainInterfaces")
    def test_normal_wallet_creation(self, mock_ci, mock_wallet_cls, tmp_path):
        # Create wallet file so get_wallet() tries Wallet()
        (tmp_path / "wallet.json").touch()
        with patch("iwa.core.constants.WALLET_PATH", str(tmp_path / "wallet.json")):
            mock_wallet = MagicMock()
            mock_wallet_cls.return_value = mock_wallet
            result = bridge.get_wallet()
            assert result is mock_wallet
            assert bridge._cached_wallet is mock_wallet

    @patch("micromech.core.bridge.Wallet")
    @patch("micromech.core.bridge.ChainInterfaces")
    def test_injects_chain_interfaces(self, mock_ci, mock_wallet_cls, tmp_path):
        (tmp_path / "wallet.json").touch()
        with patch("iwa.core.constants.WALLET_PATH", str(tmp_path / "wallet.json")):
            mock_wallet = MagicMock(spec=[])  # no chain_interfaces attr
            mock_wallet_cls.return_value = mock_wallet
            result = bridge.get_wallet()
        assert hasattr(result, "chain_interfaces")

    def test_no_wallet_no_ks_raises(self):
        """When no wallet file and no cached ks, get_wallet raises."""
        bridge._cached_key_storage = None
        with pytest.raises(RuntimeError, match="No wallet"):
            bridge.get_wallet()

    def test_fallback_caches_result(self, tmp_path):
        """When get_wallet succeeds, result is cached for next call."""
        (tmp_path / "wallet.json").touch()
        mock_wallet = MagicMock()
        with patch("iwa.core.constants.WALLET_PATH", str(tmp_path / "wallet.json")):
            with patch("micromech.core.bridge.Wallet", return_value=mock_wallet):
                result1 = bridge.get_wallet()
        # Second call returns cached
        result2 = bridge.get_wallet()
        assert result1 is result2 is mock_wallet


class TestCheckBalances:
    @patch("micromech.core.bridge._IWA_AVAILABLE", False)
    def test_returns_zero_without_iwa(self):
        assert bridge.check_balances("gnosis") == (0.0, 0.0)

    def test_returns_zero_on_exception(self):
        bridge._cached_key_storage = MagicMock()
        bridge._cached_key_storage.get_address_by_tag.side_effect = Exception("boom")
        assert bridge.check_balances("gnosis") == (0.0, 0.0)

    def test_uses_cached_key_storage_address(self):
        mock_ks = MagicMock()
        mock_ks.get_address_by_tag.return_value = "0x" + "11" * 20
        bridge._cached_key_storage = mock_ks

        mock_ci = MagicMock()
        mock_ci.get.return_value = None  # chain not found → returns early
        bridge._cached_interfaces = mock_ci

        assert bridge.check_balances("gnosis") == (0.0, 0.0)
        mock_ks.get_address_by_tag.assert_called_with("master")


class TestIwaBridge:
    def test_require_iwa_raises_without_iwa(self):
        with patch("micromech.core.bridge._IWA_AVAILABLE", False):
            with pytest.raises(ImportError, match="iwa is required"):
                bridge.require_iwa()

    def test_iwa_bridge_stores_chain_name(self):
        with patch("micromech.core.bridge._IWA_AVAILABLE", True):
            with patch("micromech.core.bridge.require_iwa"):
                b = bridge.IwaBridge(chain_name="base")
                assert b.chain_name == "base"
