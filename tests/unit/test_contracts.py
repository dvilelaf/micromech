"""Tests for runtime/contracts.py — ABI constants and load functions."""

from unittest.mock import patch

from micromech.runtime.contracts import (
    COMPLEMENTARY_SERVICE_METADATA_ABI,
    COMPLEMENTARY_SERVICE_METADATA_ADDRESS,
    MARKETPLACE_REQUEST_ABI,
    MECH_DELIVER_ABI,
    load_marketplace_abi,
    load_mech_abi,
)


class TestABIConstants:
    def test_mech_deliver_abi_is_list(self):
        assert isinstance(MECH_DELIVER_ABI, list)
        assert len(MECH_DELIVER_ABI) == 2
        assert MECH_DELIVER_ABI[0]["name"] == "deliverToMarketplace"
        assert MECH_DELIVER_ABI[1]["name"] == "deliverMarketplaceWithSignatures"

    def test_marketplace_request_abi_is_list(self):
        assert isinstance(MARKETPLACE_REQUEST_ABI, list)
        assert len(MARKETPLACE_REQUEST_ABI) == 3
        names = {entry.get("name") for entry in MARKETPLACE_REQUEST_ABI}
        assert "request" in names
        assert "MarketplaceRequest" in names
        assert "Deliver" in names

    def test_metadata_abi_has_change_hash(self):
        assert isinstance(COMPLEMENTARY_SERVICE_METADATA_ABI, list)
        names = {entry["name"] for entry in COMPLEMENTARY_SERVICE_METADATA_ABI}
        assert "changeHash" in names
        assert "tokenURI" in names

    def test_metadata_address_contains_gnosis(self):
        assert "gnosis" in COMPLEMENTARY_SERVICE_METADATA_ADDRESS
        assert COMPLEMENTARY_SERVICE_METADATA_ADDRESS["gnosis"].startswith("0x")


class TestLoadMechAbi:
    def test_returns_list_without_iwa(self):
        """When iwa ABIs are not available, returns the bundled minimal ABI."""
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_mech_abi()
        assert isinstance(abi, list)
        assert abi == MECH_DELIVER_ABI

    def test_loads_from_iwa_path_if_exists(self, tmp_path):
        """When iwa ABI path exists and file is present, loads from it."""
        import json

        abi_file = tmp_path / "mech_new.json"
        fake_abi = [{"name": "fromIwa", "type": "function"}]
        abi_file.write_text(json.dumps(fake_abi))

        with patch("micromech.runtime.contracts._IWA_ABI_PATH", tmp_path):
            abi = load_mech_abi()
        assert abi == fake_abi

    def test_falls_back_if_iwa_file_missing(self, tmp_path):
        """When iwa path exists but file is missing, returns bundled ABI."""
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", tmp_path):
            abi = load_mech_abi()
        assert abi == MECH_DELIVER_ABI


class TestLoadMarketplaceAbi:
    def test_returns_list_without_iwa(self):
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_marketplace_abi()
        assert isinstance(abi, list)
        assert abi == MARKETPLACE_REQUEST_ABI

    def test_loads_from_iwa_path_if_exists(self, tmp_path):
        import json

        abi_file = tmp_path / "mech_marketplace.json"
        fake_abi = [{"name": "fromIwa", "type": "function"}]
        abi_file.write_text(json.dumps(fake_abi))

        with patch("micromech.runtime.contracts._IWA_ABI_PATH", tmp_path):
            abi = load_marketplace_abi()
        assert abi == fake_abi

    def test_falls_back_if_iwa_file_missing(self, tmp_path):
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", tmp_path):
            abi = load_marketplace_abi()
        assert abi == MARKETPLACE_REQUEST_ABI
