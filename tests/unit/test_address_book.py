"""Tests for core/address_book.py."""

from unittest.mock import MagicMock

import pytest

import micromech.core.address_book as ab


@pytest.fixture(autouse=True)
def _clean_dynamic():
    """Reset dynamic entries between tests."""
    ab._DYNAMIC.clear()
    yield
    ab._DYNAMIC.clear()


class TestFmtAddr:
    def test_static_mech_marketplace_checksummed(self):
        assert ab.fmt_addr("0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB") == "MECH_MARKETPLACE"

    def test_static_mech_marketplace_lowercase(self):
        assert ab.fmt_addr("0x735faab1c4ec41128c367afb5c3bac73509f70bb") == "MECH_MARKETPLACE"

    def test_static_valory_mech(self):
        assert ab.fmt_addr("0xC05e7412439bD7e91730a6880E18d5D5873F632C") == "VALORY_MECH"

    def test_static_proteus_mech(self):
        assert ab.fmt_addr("0x33Ca1E117c4254b2eE8CD7Ef1621739431a37396") == "PROTEUS_MECH"

    def test_unknown_address_returned_as_is(self):
        addr = "0x" + "ab" * 20
        assert ab.fmt_addr(addr) == addr

    def test_dynamic_overrides_static(self):
        addr = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
        ab.register_address(addr, "MY_CUSTOM_NAME")
        assert ab.fmt_addr(addr) == "MY_CUSTOM_NAME"

    def test_dynamic_registration(self):
        addr = "0x" + "cc" * 20
        ab.register_address(addr, "MY_WALLET")
        assert ab.fmt_addr(addr) == "MY_WALLET"

    def test_dynamic_case_insensitive(self):
        ab.register_address("0x" + "DD" * 20, "SOME_ADDR")
        assert ab.fmt_addr("0x" + "dd" * 20) == "SOME_ADDR"


class TestPatcher:
    def test_replaces_address_in_message(self):
        record: dict = {"message": "Sending to 0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB now"}
        ab.address_book_patcher(record)
        assert "MECH_MARKETPLACE" in record["message"]
        assert "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB" not in record["message"]

    def test_replaces_multiple_addresses(self):
        record: dict = {
            "message": (
                "marketplace=0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB "
                "mech=0xC05e7412439bD7e91730a6880E18d5D5873F632C"
            )
        }
        ab.address_book_patcher(record)
        assert "MECH_MARKETPLACE" in record["message"]
        assert "VALORY_MECH" in record["message"]

    def test_unknown_address_unchanged(self):
        addr = "0x" + "ee" * 20
        record: dict = {"message": f"sent to {addr}"}
        ab.address_book_patcher(record)
        assert addr in record["message"]

    def test_replaces_dynamic_address(self):
        addr = "0x" + "ff" * 20
        ab.register_address(addr, "MASTER")
        record: dict = {"message": f"balance of {addr}"}
        ab.address_book_patcher(record)
        assert "MASTER" in record["message"]
        assert addr not in record["message"]


class TestLoadWalletTags:
    def test_loads_tags_from_wallet(self):
        addr = "0x" + "11" * 20
        account = MagicMock()
        account.tag = "my_safe"
        wallet = MagicMock()
        wallet.account_service.get_account_data.return_value = {addr: account}

        ab.load_wallet_tags(wallet)
        assert ab.fmt_addr(addr) == "my_safe"

    def test_none_wallet_is_safe(self):
        ab.load_wallet_tags(None)

    def test_exception_is_swallowed(self):
        wallet = MagicMock()
        wallet.account_service.get_account_data.side_effect = RuntimeError("fail")
        ab.load_wallet_tags(wallet)
