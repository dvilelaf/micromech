"""Tests for iwa bridge module."""

from unittest.mock import MagicMock, patch

import pytest

from micromech.integration.iwa_bridge import IwaBridge, require_iwa


class TestRequireIwa:
    def test_raises_when_not_available(self):
        with patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", False):
            with pytest.raises(ImportError, match="iwa is required"):
                require_iwa()

    def test_no_raise_when_available(self):
        with patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", True):
            require_iwa()  # should not raise


class TestIwaBridge:
    @patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", False)
    def test_init_without_iwa_raises(self):
        with pytest.raises(ImportError):
            IwaBridge()

    @patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", True)
    @patch("micromech.integration.iwa_bridge.Wallet", create=True)
    def test_wallet_lazy_load(self, mock_wallet_cls):
        mock_wallet_cls.return_value = MagicMock()
        bridge = IwaBridge()
        assert bridge._wallet is None
        _ = bridge.wallet
        mock_wallet_cls.assert_called_once()
        assert bridge._wallet is not None

    @patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", True)
    @patch("micromech.integration.iwa_bridge.ChainInterfaces", create=True)
    def test_chain_interface_lazy_load(self, mock_ci_cls):
        mock_ci = MagicMock()
        mock_ci_cls.return_value = mock_ci
        bridge = IwaBridge(chain_name="gnosis")
        assert bridge._chain_interface is None
        _ = bridge.chain_interface
        mock_ci.get.assert_called_once_with("gnosis")

    @patch("micromech.integration.iwa_bridge._IWA_AVAILABLE", True)
    @patch("micromech.integration.iwa_bridge.ChainInterfaces", create=True)
    def test_with_retry_delegates(self, mock_ci_cls):
        mock_ci = MagicMock()
        mock_ci_cls.return_value = mock_ci
        mock_interface = MagicMock()
        mock_ci.get.return_value = mock_interface

        bridge = IwaBridge()
        fn = MagicMock(return_value=42)
        bridge.with_retry(fn, timeout=10)
        mock_interface.with_retry.assert_called_once_with(fn, timeout=10)
