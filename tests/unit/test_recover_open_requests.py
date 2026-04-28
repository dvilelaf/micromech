"""Unit tests for scripts/recover_open_requests.py.

Tests cover: helpers, checkpoint I/O, discover_open_requests, deliver_all,
_revalidate_open, and validation utilities.
All web3/contract calls are mocked so no network is required.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# scripts/ is not in the package, so inject it into sys.path for import
sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
import recover_open_requests as rec  # noqa: E402

# ── Helpers ────────────────────────────────────────────────────────────────────


def _rid(n: int) -> bytes:
    """Create a deterministic 32-byte requestId from an integer."""
    return n.to_bytes(32, "big")


def _rid_hex(n: int) -> str:
    return rec.b32_to_hex(_rid(n))


TEST_SAFE = "0x" + "0" * 40
TEST_MECH = "0x" + "1" * 40
TEST_MARKETPLACE = "0x" + "2" * 40


def _runtime(tmp_path: Path | None = None) -> rec.RuntimeConfig:
    base = tmp_path or Path("/tmp")
    return rec.RuntimeConfig(
        chain="gnosis",
        mech_addr=TEST_MECH,
        safe_addr=TEST_SAFE,
        marketplace_addr=TEST_MARKETPLACE,
        config_path=base / "config.yaml",
        wallet_path=base / "wallet.json",
        delivery_rate=10,
    )


# ── b32_to_hex / hex_to_b32 ────────────────────────────────────────────────────


class TestHexConversions:
    def test_round_trip(self):
        for n in (0, 1, 255, 2**128, 2**256 - 1):
            b = n.to_bytes(32, "big")
            assert rec.hex_to_b32(rec.b32_to_hex(b)) == b

    def test_prefix(self):
        assert rec.b32_to_hex(b"\x00" * 32).startswith("0x")

    def test_hex_to_b32_with_prefix(self):
        h = "0x" + "ab" * 32
        assert rec.hex_to_b32(h) == bytes.fromhex("ab" * 32)

    def test_hex_to_b32_without_prefix(self):
        h = "ab" * 32
        assert rec.hex_to_b32(h) == bytes.fromhex("ab" * 32)


# ── _valid_hex32 ───────────────────────────────────────────────────────────────


class TestValidHex32:
    def test_valid(self):
        assert rec._valid_hex32("0x" + "a" * 64)

    def test_too_short(self):
        assert not rec._valid_hex32("0x" + "a" * 63)

    def test_too_long(self):
        assert not rec._valid_hex32("0x" + "a" * 65)

    def test_no_prefix(self):
        assert not rec._valid_hex32("a" * 64)

    def test_invalid_chars(self):
        assert not rec._valid_hex32("0x" + "g" * 64)


# ── _validate_private_key ──────────────────────────────────────────────────────


class TestValidatePrivateKey:
    def test_valid(self):
        rec._validate_private_key("0x" + "a" * 64)  # should not raise

    def test_missing_prefix(self):
        with pytest.raises(ValueError):
            rec._validate_private_key("a" * 64)

    def test_wrong_length(self):
        with pytest.raises(ValueError):
            rec._validate_private_key("0x" + "a" * 63)

    def test_empty(self):
        with pytest.raises(ValueError):
            rec._validate_private_key("")


# ── load_checkpoint / save_checkpoint ─────────────────────────────────────────


class TestCheckpoint:
    def test_default_checkpoint_lives_next_to_script(self):
        assert rec.DEFAULT_CHECKPOINT == Path(rec.__file__).resolve().with_name("recover.json")

    def test_default_queue_lives_next_to_script(self):
        assert rec.DEFAULT_QUEUE == Path(rec.__file__).resolve().with_name(
            "recover_queue.sqlite"
        )

    def test_load_missing_returns_defaults(self, tmp_path):
        cp = rec.load_checkpoint(tmp_path / "missing.json")
        assert cp["open_requests"] == []
        assert cp["delivered"] == []
        assert cp["last_scanned_block"] is None

    def test_round_trip(self, tmp_path):
        path = tmp_path / "cp.json"
        data = {
            "open_requests": [_rid_hex(1), _rid_hex(2)],
            "delivered": [_rid_hex(0)],
            "last_scanned_block": 999,
            "scan_from_block": 100,
        }
        rec.save_checkpoint(path, data)
        loaded = rec.load_checkpoint(path)
        assert loaded["open_requests"] == data["open_requests"]
        assert loaded["last_scanned_block"] == 999

    def test_save_sets_permissions(self, tmp_path):
        path = tmp_path / "cp.json"
        rec.save_checkpoint(path, {"open_requests": [], "delivered": []})
        assert oct(path.stat().st_mode)[-3:] == "600"

    def test_corrupted_checkpoint_returns_defaults(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json}")
        cp = rec.load_checkpoint(path)
        assert cp["open_requests"] == []

    def test_invalid_entries_filtered(self, tmp_path):
        path = tmp_path / "cp.json"
        data = {
            "open_requests": [_rid_hex(1), "notahex", "0x" + "z" * 64],
            "delivered": [],
            "last_scanned_block": None,
            "scan_from_block": None,
        }
        path.write_text(json.dumps(data))
        cp = rec.load_checkpoint(path)
        assert cp["open_requests"] == [_rid_hex(1)]


# ── RequestQueue ──────────────────────────────────────────────────────────────


class TestRequestQueue:
    def test_enqueue_and_get_open(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        q.enqueue_open(_rid_hex(2), 20)
        q.enqueue_open(_rid_hex(1), 10)

        assert q.get_open(10) == [_rid(1), _rid(2)]
        assert q.counts() == {"open": 2}

    def test_mark_delivered_and_skipped(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        q.enqueue_open(_rid_hex(1), 1)
        q.enqueue_open(_rid_hex(2), 2)

        q.mark_delivering([_rid(1), _rid(2)])
        q.mark_delivered([_rid(1)])
        q.mark_skipped([_rid(2)], "no_longer_status_2")

        assert q.get_open(10) == []
        assert q.counts() == {"delivered": 1, "skipped": 1}

    def test_delivered_is_not_reopened_by_duplicate_discovery(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        q.enqueue_open(_rid_hex(1), 1)
        q.mark_delivered([_rid(1)])
        q.enqueue_open(_rid_hex(1), 2)

        assert q.get_open(10) == []
        assert q.counts() == {"delivered": 1}

    def test_remember_skipped_prevents_requeue(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        q.remember_skipped(_rid_hex(1), 1, "payment_type_mismatch")
        q.enqueue_open(_rid_hex(1), 1)

        assert q.get_open(10) == []
        assert q.counts() == {"skipped": 1}


# ── _revalidate_open ──────────────────────────────────────────────────────────


class TestRevalidateOpen:
    def _marketplace_mock(self, statuses: dict[int, int]):
        """Return a mock w3 whose marketplace.getRequestStatus returns statuses[n]."""
        marketplace = MagicMock()

        def status_for(rid):
            n = int.from_bytes(rid, "big")
            fn = MagicMock()
            fn.call.return_value = statuses.get(n, 0)
            return fn

        marketplace.functions.getRequestStatus.side_effect = status_for
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.return_value = marketplace
        return w3

    def test_keeps_only_status2(self):
        w3 = self._marketplace_mock({1: 2, 2: 3, 3: 2, 4: 0})
        rids = [_rid(1), _rid(2), _rid(3), _rid(4)]
        with patch("time.sleep"):
            result = rec._revalidate_open(w3, rids, TEST_MARKETPLACE, delay=0)
        assert result == [_rid(1), _rid(3)]

    def test_empty_input(self):
        w3 = self._marketplace_mock({})
        assert rec._revalidate_open(w3, [], TEST_MARKETPLACE, delay=0) == []

    def test_rpc_failure_skips_entry(self):
        marketplace = MagicMock()
        marketplace.functions.getRequestStatus.side_effect = Exception("RPC down")
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.return_value = marketplace
        with patch("time.sleep"):
            result = rec._revalidate_open(w3, [_rid(1)], TEST_MARKETPLACE, delay=0)
        assert result == []


# ── deliver_batch_real ────────────────────────────────────────────────────────


class TestDeliverBatchReal:
    """Covers receipt handling, simulation filtering, and abort-on-sim-failure."""

    _SAFE = "0x" + "0" * 40
    _MECH = "0x" + "1" * 40
    _MARKETPLACE = "0x" + "2" * 40
    _RPC = "https://rpc.example.com"
    _PK = "0x" + "a" * 64

    def _setup_w3(self, sim_flags=None, sim_exc=None):
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        mech = MagicMock()
        fn = MagicMock()
        if sim_exc:
            fn.call.side_effect = sim_exc
        else:
            fn.call.return_value = sim_flags or []
        fn.build_transaction.return_value = {"data": "0xdeadbeef"}
        mech.functions.deliverToMarketplace.return_value = fn
        w3.eth.contract.return_value = mech
        return w3

    def _mock_safe_eth(self, tx_hash=b"\x01" * 32, receipt_status=1, receipt_exc=None):
        mock_ec = MagicMock()
        mock_safe = MagicMock()
        mock_safe_tx = MagicMock()
        mock_safe_tx.execute.return_value = (tx_hash, MagicMock())
        if receipt_exc:
            mock_ec.w3.eth.wait_for_transaction_receipt.side_effect = receipt_exc
        else:
            mock_ec.w3.eth.wait_for_transaction_receipt.return_value = {"status": receipt_status}
        mock_safe.build_multisig_tx.return_value = mock_safe_tx
        return mock_ec, mock_safe, mock_safe_tx

    def test_simulation_filters_non_deliverable(self):
        """Only requests with flag=True from simulation are sent."""
        batch = [_rid(1), _rid(2), _rid(3)]
        w3 = self._setup_w3(sim_flags=[True, False, True])
        mock_ec, mock_safe, _ = self._mock_safe_eth(receipt_status=1)

        with (
            patch("safe_eth.eth.EthereumClient", return_value=mock_ec),
            patch("safe_eth.safe.Safe", return_value=mock_safe),
            patch("recover_open_requests._delivered_ids_from_receipt_or_status", return_value=[_rid(1), _rid(3)]),
        ):
            ok, delivered = rec.deliver_batch_real(
                batch, self._SAFE, self._MECH, self._MARKETPLACE, self._RPC, self._PK, w3
            )

        assert ok is True
        assert _rid(1) in delivered
        assert _rid(2) not in delivered
        assert _rid(3) in delivered

    def test_tx_revert_returns_false_empty(self):
        """receipt.status != 1 → (False, []) — nothing marked delivered."""
        batch = [_rid(1)]
        w3 = self._setup_w3(sim_flags=[True])
        mock_ec, mock_safe, _ = self._mock_safe_eth(receipt_status=0)

        with (
            patch("safe_eth.eth.EthereumClient", return_value=mock_ec),
            patch("safe_eth.safe.Safe", return_value=mock_safe),
        ):
            ok, delivered = rec.deliver_batch_real(
                batch, self._SAFE, self._MECH, self._MARKETPLACE, self._RPC, self._PK, w3
            )

        assert ok is False
        assert delivered == []

    def test_receipt_timeout_returns_false_empty(self):
        """wait_for_transaction_receipt raises → (False, []) for safety."""
        batch = [_rid(1)]
        w3 = self._setup_w3(sim_flags=[True])
        mock_ec, mock_safe, _ = self._mock_safe_eth(receipt_exc=TimeoutError("timeout"))

        with (
            patch("safe_eth.eth.EthereumClient", return_value=mock_ec),
            patch("safe_eth.safe.Safe", return_value=mock_safe),
        ):
            ok, delivered = rec.deliver_batch_real(
                batch, self._SAFE, self._MECH, self._MARKETPLACE, self._RPC, self._PK, w3
            )

        assert ok is False
        assert delivered == []

    def test_simulation_failure_aborts_batch(self):
        """NEW-1: simulation exception → abort batch, Safe TX never built."""
        batch = [_rid(1), _rid(2)]
        w3 = self._setup_w3(sim_exc=Exception("RPC error"))
        mock_ec, mock_safe, _ = self._mock_safe_eth(receipt_status=1)

        with (
            patch("safe_eth.eth.EthereumClient", return_value=mock_ec),
            patch("safe_eth.safe.Safe", return_value=mock_safe),
        ):
            ok, delivered = rec.deliver_batch_real(
                batch, self._SAFE, self._MECH, self._MARKETPLACE, self._RPC, self._PK, w3
            )

        mock_safe.build_multisig_tx.assert_not_called()
        assert ok is False
        assert delivered == []

    def test_all_filtered_by_simulation_skips_tx(self):
        """All flags=False → (True, []) with no Safe TX submitted."""
        batch = [_rid(1), _rid(2)]
        w3 = self._setup_w3(sim_flags=[False, False])
        mock_ec, mock_safe, _ = self._mock_safe_eth(receipt_status=1)

        with (
            patch("safe_eth.eth.EthereumClient", return_value=mock_ec),
            patch("safe_eth.safe.Safe", return_value=mock_safe),
        ):
            ok, delivered = rec.deliver_batch_real(
                batch, self._SAFE, self._MECH, self._MARKETPLACE, self._RPC, self._PK, w3
            )

        mock_safe.build_multisig_tx.assert_not_called()
        assert ok is True
        assert delivered == []


# ── discover_open_requests ────────────────────────────────────────────────────


class TestDiscoverOpenRequests:
    def _setup_mocks(self, event_rids_by_window: dict, statuses: dict[int, int]):
        """
        event_rids_by_window: {(from_b, to_b): [rid_int, ...]}
        statuses: {rid_int: status}
        """
        marketplace = MagicMock()

        def get_logs(from_block, to_block):
            key = (from_block, to_block)
            rids = event_rids_by_window.get(key, [])
            logs = []
            for n in rids:
                log = MagicMock()
                log.__getitem__ = lambda self, k, _n=n: (
                    {"requestIds": [_rid(_n)], "requestDatas": [b""]}
                    if k == "args"
                    else MagicMock()
                )
                logs.append(log)
            return logs

        marketplace.events.MarketplaceRequest.get_logs.side_effect = get_logs

        def status_for(rid):
            n = int.from_bytes(rid, "big")
            fn = MagicMock()
            fn.call.return_value = statuses.get(n, 0)
            return fn

        marketplace.functions.getRequestStatus.side_effect = status_for
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.return_value = marketplace
        return w3

    def test_empty_range_returns_empty(self):
        with patch("time.sleep"):
            result = rec.discover_open_requests(
                ["http://fake"], TEST_MARKETPLACE, scan_from=100, scan_to=50, delay_logs=0, delay_status=0
            )
        assert result == []

    def test_finds_open_requests(self, tmp_path):
        # scan_to=200, LOG_WINDOW=1000 → single window (100, 200)
        w3 = self._setup_mocks(
            {(100, 200): [1, 2]},
            {1: 2, 2: 3},
        )
        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests(
                ["http://fake"],
                TEST_MARKETPLACE,
                scan_from=100,
                scan_to=200,
                delay_logs=0,
                delay_status=0,
            )
        assert _rid(1) in result
        assert _rid(2) not in result

    def test_finds_open_requests_enqueues(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        w3 = self._setup_mocks({(100, 200): [1, 2]}, {1: 2, 2: 3})
        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            rec.discover_open_requests(
                ["http://fake"],
                TEST_MARKETPLACE,
                scan_from=100,
                scan_to=200,
                delay_logs=0,
                delay_status=0,
                queue=q,
            )
        assert q.get_open(10) == [_rid(1)]

    def test_mech_queue_discovery_filters_expired(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        marketplace = MagicMock()
        mech = MagicMock()

        def status_for(rid):
            n = int.from_bytes(rid, "big")
            fn = MagicMock()
            fn.call.return_value = {1: 2, 2: 1, 3: 3}.get(n, 0)
            return fn

        marketplace.functions.getRequestStatus.side_effect = status_for
        mech.functions.numUndeliveredRequests.return_value.call.return_value = 3

        def get_ids(size, offset):
            fn = MagicMock()
            ids = [_rid(1), _rid(2), _rid(3)]
            fn.call.return_value = ids[offset : offset + size]
            return fn

        mech.functions.getUndeliveredRequestIds.side_effect = get_ids
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.side_effect = lambda address, abi: (
            marketplace if address == TEST_MARKETPLACE else mech
        )

        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests_from_mech_queues(
                ["http://fake"],
                TEST_MARKETPLACE,
                [TEST_MECH],
                page_size=2,
                delay_status=0,
                checkpoint=tmp_path / "recover.json",
                queue=q,
            )

        assert result == [_rid(1)]
        assert q.get_open(10) == [_rid(1)]

    def test_mech_queue_discovery_filters_payment_type(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        marketplace = MagicMock()
        mech = MagicMock()
        good_payment = b"\x11" * 32
        bad_payment = b"\x22" * 32

        marketplace.functions.getRequestStatus.return_value.call.return_value = 2

        def request_info(rid):
            fn = MagicMock()
            fn.call.return_value = (
                TEST_SAFE,
                TEST_MECH,
                TEST_MECH,
                1,
                1,
                good_payment if rid == _rid(1) else bad_payment,
            )
            return fn

        marketplace.functions.mapRequestIdInfos.side_effect = request_info
        mech.functions.numUndeliveredRequests.return_value.call.return_value = 2
        mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_rid(1), _rid(2)]
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.side_effect = lambda address, abi: (
            marketplace if address == TEST_MARKETPLACE else mech
        )

        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests_from_mech_queues(
                ["http://fake"],
                TEST_MARKETPLACE,
                [TEST_MECH],
                delay_status=0,
                queue=q,
                payment_type=good_payment,
            )

        assert result == [_rid(1)]
        assert q.get_open(10) == [_rid(1)]

    def test_mech_queue_discovery_skips_payment_mismatch_before_status(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        marketplace = MagicMock()
        mech = MagicMock()
        good_payment = b"\x11" * 32
        bad_payment = b"\x22" * 32

        def request_info(rid):
            fn = MagicMock()
            fn.call.return_value = (TEST_SAFE, TEST_MECH, TEST_MECH, 1, 20, bad_payment)
            return fn

        marketplace.functions.mapRequestIdInfos.side_effect = request_info
        mech.functions.numUndeliveredRequests.return_value.call.return_value = 1
        mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_rid(1)]
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.side_effect = lambda address, abi: (
            marketplace if address == TEST_MARKETPLACE else mech
        )

        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests_from_mech_queues(
                ["http://fake"],
                TEST_MARKETPLACE,
                [TEST_MECH],
                delay_status=0,
                queue=q,
                payment_type=good_payment,
            )

        assert result == []
        marketplace.functions.getRequestStatus.assert_not_called()
        assert q.counts() == {"skipped": 1}

    def test_mech_queue_discovery_does_not_persist_young_request(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        marketplace = MagicMock()
        mech = MagicMock()
        payment = b"\x11" * 32

        marketplace.functions.mapRequestIdInfos.return_value.call.return_value = (
            TEST_SAFE,
            TEST_MECH,
            TEST_MECH,
            200,
            20,
            payment,
        )
        mech.functions.numUndeliveredRequests.return_value.call.return_value = 1
        mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_rid(1)]
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.side_effect = lambda address, abi: (
            marketplace if address == TEST_MARKETPLACE else mech
        )

        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests_from_mech_queues(
                ["http://fake"],
                TEST_MARKETPLACE,
                [TEST_MECH],
                delay_status=0,
                queue=q,
                payment_type=payment,
                cutoff_block=100,
            )

        assert result == []
        marketplace.functions.getRequestStatus.assert_not_called()
        assert q.counts() == {}

    def test_mech_queue_discovery_filters_delivery_rate(self, tmp_path):
        marketplace = MagicMock()
        mech = MagicMock()
        payment = b"\x11" * 32
        marketplace.functions.getRequestStatus.return_value.call.return_value = 2

        def request_info(rid):
            fn = MagicMock()
            fn.call.return_value = (
                TEST_SAFE,
                TEST_MECH,
                TEST_MECH,
                1,
                20 if rid == _rid(1) else 5,
                payment,
            )
            return fn

        marketplace.functions.mapRequestIdInfos.side_effect = request_info
        mech.functions.numUndeliveredRequests.return_value.call.return_value = 2
        mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_rid(1), _rid(2)]
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.side_effect = lambda address, abi: (
            marketplace if address == TEST_MARKETPLACE else mech
        )

        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests_from_mech_queues(
                ["http://fake"],
                TEST_MARKETPLACE,
                [TEST_MECH],
                delay_status=0,
                payment_type=payment,
                delivery_rate=10,
            )

        assert result == [_rid(1)]

    def test_discovers_priority_mechs_from_create_events(self, tmp_path):
        mech_addr = "0x" + "a" * 40
        marketplace = MagicMock()
        event_log = MagicMock()
        event_log.__getitem__ = lambda self, k: (
            {"mech": mech_addr} if k == "args" else MagicMock()
        )
        marketplace.events.CreateMech.get_logs.return_value = [event_log]
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.contract.return_value = marketplace
        cache = tmp_path / "recover_mechs.json"

        with (
            patch("recover_open_requests._connect", return_value=(w3, "http://fake")),
            patch("recover_open_requests._make_marketplace", return_value=marketplace),
            patch("time.sleep"),
        ):
            result = rec.discover_priority_mechs(
                ["http://fake"],
                TEST_MARKETPLACE,
                cache_path=cache,
                from_block=100,
                to_block=100,
                window=1,
            )

        assert result == [rec.Web3.to_checksum_address(mech_addr)]
        cached = json.loads(cache.read_text())
        assert cached["last_scanned_block"] == 100
        assert cached["mechs"] == result

    def test_discovers_priority_mechs_from_blockscout(self, tmp_path):
        mech_addr = "0x" + "b" * 40
        topic0 = "0x" + rec.Web3.keccak(text="MarketplaceRequest(address,address,uint256,bytes32[],bytes[])").hex()
        topic1 = "0x" + "0" * 24 + mech_addr[2:]
        cache = tmp_path / "recover_mechs.json"

        with patch(
            "recover_open_requests._blockscout_get_json",
            return_value={
                "items": [
                    {
                        "block_number": 123,
                        "topics": [topic0, topic1],
                        "decoded": {
                            "parameters": [
                                {"name": "priorityMech", "value": mech_addr},
                            ]
                        },
                    }
                ],
                "next_page_params": None,
            },
        ):
            result = rec.discover_priority_mechs_blockscout(
                TEST_MARKETPLACE,
                cache_path=cache,
                from_block=1,
                to_block=200,
            )

        assert result == [rec.Web3.to_checksum_address(mech_addr)]
        assert json.loads(cache.read_text())["mechs"] == result

    def test_max_open_stops_early(self):
        # Two windows each with 3 open requests
        events = {(0, 999): [1, 2, 3], (1000, 1999): [4, 5, 6]}
        statuses = {n: 2 for n in range(1, 7)}
        w3 = self._setup_mocks(events, statuses)
        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            result = rec.discover_open_requests(
                ["http://fake"],
                TEST_MARKETPLACE,
                scan_from=0,
                scan_to=2000,
                max_open=3,
                delay_logs=0,
                delay_status=0,
            )
        assert len(result) == 3

    def test_checkpoint_saves_scan_from_block(self, tmp_path):
        cp_path = tmp_path / "cp.json"
        w3 = self._setup_mocks({}, {})
        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            rec.discover_open_requests(
                ["http://fake"],
                TEST_MARKETPLACE,
                scan_from=500,
                scan_to=600,
                delay_logs=0,
                delay_status=0,
                checkpoint=cp_path,
            )
        cp = rec.load_checkpoint(cp_path)
        assert cp["scan_from_block"] == 500
        assert cp["last_scanned_block"] is not None

    def test_scan_from_block_sticky_on_resume(self, tmp_path):
        """BLK-3: scan_from_block in checkpoint must not be overwritten on resume."""
        cp_path = tmp_path / "cp.json"
        # Pre-populate checkpoint with scan_from_block=500, last_scanned_block=599
        rec.save_checkpoint(
            cp_path,
            {
                "open_requests": [],
                "delivered": [],
                "scan_from_block": 500,
                "last_scanned_block": 599,
            },
        )
        w3 = self._setup_mocks({(600, 1000): []}, {})
        with (
            patch("recover_open_requests.Web3.HTTPProvider"),
            patch("recover_open_requests.Web3", return_value=w3),
            patch("time.sleep"),
        ):
            rec.discover_open_requests(
                ["http://fake"],
                TEST_MARKETPLACE,
                scan_from=700,  # different from stored 500 — must be ignored
                scan_to=1000,
                delay_logs=0,
                delay_status=0,
                checkpoint=cp_path,
                resume_from=600,
            )
        cp = rec.load_checkpoint(cp_path)
        assert cp["scan_from_block"] == 500  # sticky, not overwritten


# ── deliver_all ───────────────────────────────────────────────────────────────


class TestDeliverAll:
    def _mock_w3(self):
        w3 = MagicMock()
        w3.to_checksum_address.side_effect = lambda x: x
        return w3

    def test_anvil_mode_marks_delivered(self, tmp_path):
        cp_path = tmp_path / "cp.json"
        w3 = self._mock_w3()
        rids = [_rid(1), _rid(2)]

        with patch(
            "recover_open_requests.deliver_batch_anvil",
            return_value=(True, rids),
        ) as mock_deliver:
            total = rec.deliver_all(
                rids,
                runtime=_runtime(tmp_path),
                mode="anvil",
                w3=w3,
                delay_tx=0,
                batch_size=20,
                checkpoint=cp_path,
            )

        assert total == 2
        mock_deliver.assert_called_once()
        cp = rec.load_checkpoint(cp_path)
        assert _rid_hex(1) in cp["delivered"]
        assert _rid_hex(2) in cp["delivered"]

    def test_skips_already_delivered(self, tmp_path):
        cp_path = tmp_path / "cp.json"
        rec.save_checkpoint(
            cp_path,
            {"open_requests": [], "delivered": [_rid_hex(1)], "last_scanned_block": None},
        )
        w3 = self._mock_w3()

        with patch(
            "recover_open_requests.deliver_batch_anvil",
            return_value=(True, [_rid(2)]),
        ) as mock_deliver:
            total = rec.deliver_all(
                [_rid(1), _rid(2)],
                runtime=_runtime(tmp_path),
                mode="anvil",
                w3=w3,
                delay_tx=0,
                checkpoint=cp_path,
            )

        # Only rid 2 should be submitted
        args = mock_deliver.call_args[0]
        assert _rid(1) not in args[1]
        assert _rid(2) in args[1]
        assert total == 1

    def test_real_mode_relies_on_batch_simulation(self, tmp_path):
        w3 = self._mock_w3()
        rids = [_rid(1), _rid(2)]

        with (
            patch("recover_open_requests._revalidate_open") as mock_rev,
            patch(
                "recover_open_requests.deliver_batch_real",
                return_value=(True, rids),
            ),
        ):
            rec.deliver_all(
                rids,
                runtime=_runtime(tmp_path),
                mode="real",
                w3=w3,
                private_key="0x" + "a" * 64,
                rpc_url="http://fake",
                delay_tx=0,
            )

        mock_rev.assert_not_called()

    def test_real_mode_reopens_queue_on_simulation_failure(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        q.enqueue_open(_rid_hex(1), 1)
        w3 = self._mock_w3()

        with (
            patch(
                "recover_open_requests.deliver_batch_real",
                return_value=(False, []),
            ),
        ):
            total = rec.deliver_all(
                [_rid(1)],
                runtime=_runtime(tmp_path),
                mode="real",
                w3=w3,
                private_key="0x" + "a" * 64,
                rpc_url="http://fake",
                delay_tx=0,
                queue=q,
            )

        assert total == 0
        assert q.get_open(10) == [_rid(1)]
        assert q.counts() == {"open": 1}

    def test_batch_error_continues_to_next(self, tmp_path):
        """Exception in one batch should not abort subsequent batches."""
        cp_path = tmp_path / "cp.json"
        w3 = self._mock_w3()
        rids = [_rid(i) for i in range(3)]

        call_count = 0

        def side_effect(w3_, batch, safe, mech, marketplace):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated failure")
            return True, batch

        with patch("recover_open_requests.deliver_batch_anvil", side_effect=side_effect):
            total = rec.deliver_all(
                rids,
                runtime=_runtime(tmp_path),
                mode="anvil",
                w3=w3,
                delay_tx=0,
                batch_size=1,
                checkpoint=cp_path,
            )

        # batch 1 failed, batches 2 and 3 succeeded
        assert total == 2

    def test_partial_delivery_respected(self, tmp_path):
        """Only IDs returned by deliver_batch should be marked delivered."""
        cp_path = tmp_path / "cp.json"
        w3 = self._mock_w3()
        rids = [_rid(1), _rid(2), _rid(3)]

        with patch(
            "recover_open_requests.deliver_batch_anvil",
            return_value=(True, [_rid(1), _rid(3)]),  # rid 2 not delivered
        ):
            total = rec.deliver_all(
                rids,
                runtime=_runtime(tmp_path),
                mode="anvil",
                w3=w3,
                delay_tx=0,
                checkpoint=cp_path,
            )

        assert total == 2
        cp = rec.load_checkpoint(cp_path)
        assert _rid_hex(1) in cp["delivered"]
        assert _rid_hex(2) not in cp["delivered"]
        assert _rid_hex(3) in cp["delivered"]

    def test_queue_marks_delivered_and_skipped_after_batch(self, tmp_path):
        q = rec.RequestQueue(tmp_path / "recover_queue.sqlite")
        for rid in [_rid(1), _rid(2), _rid(3)]:
            q.enqueue_open(rec.b32_to_hex(rid), 1)
        w3 = self._mock_w3()

        with patch(
            "recover_open_requests.deliver_batch_anvil",
            return_value=(True, [_rid(1), _rid(3)]),
        ):
            total = rec.deliver_all(
                [_rid(1), _rid(2), _rid(3)],
                runtime=_runtime(tmp_path),
                mode="anvil",
                w3=w3,
                delay_tx=0,
                queue=q,
            )

        assert total == 2
        assert q.counts() == {"delivered": 2, "skipped": 1}
