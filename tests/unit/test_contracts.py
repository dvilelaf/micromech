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
        assert len(MARKETPLACE_REQUEST_ABI) == 4
        names = {entry.get("name") for entry in MARKETPLACE_REQUEST_ABI}
        assert "request" in names
        assert "MarketplaceRequest" in names
        assert "Deliver" in names
        assert "MarketplaceDelivery" in names

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


class TestMechAbiHasDeliverEvent:
    """Verify the mech ABI contains a Deliver event with 'data' field.

    The demo poller decodes Deliver events from the mech contract (not the
    marketplace). The marketplace emits MarketplaceDelivery which has no
    delivery data. The mech emits Deliver with args.data containing the
    IPFS multihash.
    """

    def test_bundled_mech_abi_has_no_deliver_event(self):
        """The bundled minimal mech ABI has only functions, no events.

        This is expected — the full ABI (from iwa) has the Deliver event.
        The poller uses load_mech_abi() which prefers the iwa ABI.
        """
        events = [e for e in MECH_DELIVER_ABI if e.get("type") == "event"]
        # Bundled ABI is intentionally minimal (functions only)
        assert len(events) == 0

    def test_marketplace_abi_has_marketplace_delivery_event(self):
        """The marketplace ABI contains MarketplaceDelivery event."""
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_marketplace_abi()
        events = [e for e in abi if e.get("type") == "event"]
        event_names = {e["name"] for e in events}
        assert "MarketplaceDelivery" in event_names

    def test_marketplace_delivery_has_request_ids(self):
        """MarketplaceDelivery event has requestIds field (bytes32[])."""
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_marketplace_abi()
        md_events = [e for e in abi if e.get("name") == "MarketplaceDelivery"]
        assert len(md_events) == 1
        input_names = {inp["name"] for inp in md_events[0]["inputs"]}
        assert "requestIds" in input_names

    def test_marketplace_delivery_has_no_delivery_data_field(self):
        """MarketplaceDelivery does NOT have a 'data' or 'deliveryData' field.

        This is why the poller must decode Deliver from the mech contract
        instead — only the mech Deliver event carries the actual response data.
        """
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_marketplace_abi()
        md_events = [e for e in abi if e.get("name") == "MarketplaceDelivery"]
        assert len(md_events) == 1
        input_names = {inp["name"] for inp in md_events[0]["inputs"]}
        assert "data" not in input_names
        assert "deliveryData" not in input_names

    def test_marketplace_abi_has_deliver_event_with_data(self):
        """The marketplace ABI includes a Deliver event that has 'deliveryData'.

        This Deliver event in the marketplace ABI is for decoding logs
        emitted by the MECH contract in the same transaction.
        """
        with patch("micromech.runtime.contracts._IWA_ABI_PATH", None):
            abi = load_marketplace_abi()
        deliver_events = [e for e in abi if e.get("name") == "Deliver" and e.get("type") == "event"]
        assert len(deliver_events) == 1
        input_names = {inp["name"] for inp in deliver_events[0]["inputs"]}
        assert "deliveryData" in input_names
