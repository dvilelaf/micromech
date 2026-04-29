"""Tests for the low-RPC mech queue scanner."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import ChainConfig, MicromechConfig
from micromech.core.constants import CHAIN_DEFAULTS
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.queue_scanner import MechQueueScanner, QueueCandidate
from micromech.tools.registry import ToolRegistry

MECH_ADDR = "0x" + "ab" * 20
OTHER_MECH = "0x" + "cd" * 20


def _request_id(n: int) -> bytes:
    return n.to_bytes(32, "big")


def _make_config(**kwargs) -> MicromechConfig:
    gnosis = CHAIN_DEFAULTS["gnosis"]
    return MicromechConfig(
        chains={
            "gnosis": ChainConfig(
                chain="gnosis",
                marketplace_address=gnosis["marketplace"],
                factory_address=gnosis["factory"],
                staking_address=gnosis["staking"],
                mech_address=MECH_ADDR,
            )
        },
        queue_scanner_page_size=2,
        **kwargs,
    )


def _scanner(tmp_path, cfg: MicromechConfig, bridge=None):
    queue = PersistentQueue(tmp_path / "test.db")
    registry = ToolRegistry()
    registry._tools = {"echo": object()}
    queued: set[str] = set()
    enqueue = AsyncMock(side_effect=lambda req: queued.add(req.request_id))
    bridge = bridge or MagicMock()
    bridge.with_retry.side_effect = lambda fn, **_kw: fn()
    bridge.web3.to_checksum_address.side_effect = lambda addr: addr
    scanner = MechQueueScanner(
        config=cfg,
        chain_config=cfg.enabled_chains["gnosis"],
        bridge=bridge,
        queue=queue,
        registry=registry,
        queued_ids=queued,
        enqueue=enqueue,
    )
    return scanner, enqueue, queued


def _mock_mech(count: int, pages: list[list[bytes]]) -> MagicMock:
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = count
    calls = []
    for page in pages:
        call = MagicMock()
        call.call.return_value = page
        calls.append(call)
    mech.functions.getUndeliveredRequestIds.side_effect = calls
    return mech


@pytest.mark.asyncio
async def test_own_scanner_paginates_and_enqueues(tmp_path):
    """Own reconciliation reads queue pages and enqueues missing own IDs."""
    cfg = _make_config()
    scanner, enqueue, _queued = _scanner(tmp_path, cfg)
    mech = _mock_mech(3, [[_request_id(1), _request_id(2)], [_request_id(3)]])
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.return_value = 1
    scanner._mech_contracts[MECH_ADDR.lower()] = mech
    scanner._marketplace = marketplace

    async def resolve(candidate):
        return MechRequest(
            request_id=candidate.request_id_hex,
            chain="gnosis",
            tool="echo",
            priority_mech=MECH_ADDR,
        )

    with patch.object(scanner, "_resolve_request_payload", side_effect=resolve):
        await scanner.scan_once()

    assert enqueue.await_count == 3
    mech.functions.numUndeliveredRequests.assert_called_once()
    mech.functions.getUndeliveredRequestIds.assert_any_call(2, 0)
    mech.functions.getUndeliveredRequestIds.assert_any_call(1, 2)


@pytest.mark.asyncio
async def test_own_scanner_dedups_queued_ids(tmp_path):
    """A second scan does not enqueue an ID that is already queued."""
    cfg = _make_config()
    scanner, enqueue, queued = _scanner(tmp_path, cfg)
    mech = _mock_mech(1, [[_request_id(1)]])
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.return_value = 1
    scanner._mech_contracts[MECH_ADDR.lower()] = mech
    scanner._marketplace = marketplace

    async def resolve(candidate):
        return MechRequest(
            request_id=candidate.request_id_hex,
            chain="gnosis",
            tool="echo",
            priority_mech=MECH_ADDR,
        )

    with patch.object(scanner, "_resolve_request_payload", side_effect=resolve):
        await scanner.scan_once()
        call = MagicMock()
        call.call.return_value = [_request_id(1)]
        mech.functions.getUndeliveredRequestIds.side_effect = [call]
        await scanner.scan_once()

    assert len(queued) == 1
    assert enqueue.await_count == 1


@pytest.mark.asyncio
async def test_fallback_scanner_validates_status_payment_and_tool(tmp_path):
    """Fallback enqueues only expired, compatible candidates with known tools."""
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
    )
    scanner, enqueue, _queued = _scanner(tmp_path, cfg)
    own_mech = _mock_mech(0, [])
    other_mech = _mock_mech(
        3,
        [[_request_id(1), _request_id(2)], [_request_id(3)]],
    )
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.side_effect = [1, 2, 2]
    scanner._mech_contracts[MECH_ADDR.lower()] = own_mech
    scanner._mech_contracts[OTHER_MECH.lower()] = other_mech
    scanner._marketplace = marketplace

    async def resolve(candidate):
        tool = "missing" if candidate.request_id == _request_id(2) else "echo"
        return MechRequest(
            request_id=candidate.request_id_hex,
            chain="gnosis",
            tool=tool,
            priority_mech=OTHER_MECH,
        )

    with (
        patch.object(scanner, "_payment_is_compatible", return_value=True),
        patch.object(scanner, "_resolve_request_payload", side_effect=resolve),
    ):
        await scanner.scan_once()

    assert enqueue.await_count == 1
    enqueued = enqueue.await_args.args[0]
    assert enqueued.request_id == _request_id(3).hex()
    other_mech.functions.numUndeliveredRequests.assert_called_once()
    other_mech.functions.getUndeliveredRequestIds.assert_any_call(2, 0)
    other_mech.functions.getUndeliveredRequestIds.assert_any_call(1, 2)


@pytest.mark.asyncio
async def test_fallback_scanner_short_circuits_before_payload_resolution(tmp_path):
    """Rejected fallback candidates do not trigger event/IPFS payload work."""
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
    )
    scanner, enqueue, _queued = _scanner(tmp_path, cfg)
    scanner._mech_contracts[MECH_ADDR.lower()] = _mock_mech(0, [])
    scanner._mech_contracts[OTHER_MECH.lower()] = _mock_mech(
        2,
        [[_request_id(1), _request_id(2)]],
    )
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.side_effect = [1, 2]
    scanner._marketplace = marketplace
    resolve = AsyncMock()

    with (
        patch.object(scanner, "_payment_is_compatible", return_value=False),
        patch.object(scanner, "_resolve_request_payload", resolve),
    ):
        await scanner.scan_once()

    resolve.assert_not_awaited()
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_fallback_scanner_uses_persistent_cursor(tmp_path):
    """Fallback scans a bounded number of pages and resumes next cycle."""
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_fallback_pages_per_cycle=1,
    )
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    page_calls: list[tuple[int, int]] = []

    def get_ids(size, offset):
        page_calls.append((size, offset))
        call = MagicMock()
        call.call.return_value = [_request_id(offset + 1), _request_id(offset + 2)]
        return call

    mech.functions.getUndeliveredRequestIds.side_effect = get_ids
    scanner._mech_contracts[OTHER_MECH.lower()] = mech

    with patch.object(scanner, "_handle_candidate", new=AsyncMock()):
        await scanner._scan_mech(OTHER_MECH, mode="fallback")
        await scanner._scan_mech(OTHER_MECH, mode="fallback")

    assert page_calls == [(2, 0), (2, 2)]
    assert scanner.queue.get_queue_scanner_cursor(
        chain="gnosis",
        mech_address=OTHER_MECH,
        mode="fallback",
    ) == 4


@pytest.mark.asyncio
async def test_fallback_scanner_cursor_wraps_at_end(tmp_path):
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_fallback_pages_per_cycle=1,
    )
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    scanner.queue.set_queue_scanner_cursor(
        chain="gnosis",
        mech_address=OTHER_MECH,
        mode="fallback",
        next_offset=4,
        last_count=5,
    )
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_request_id(5)]
    scanner._mech_contracts[OTHER_MECH.lower()] = mech

    with patch.object(scanner, "_handle_candidate", new=AsyncMock()):
        await scanner._scan_mech(OTHER_MECH, mode="fallback")

    mech.functions.getUndeliveredRequestIds.assert_called_once_with(1, 4)
    assert scanner.queue.get_queue_scanner_cursor(
        chain="gnosis",
        mech_address=OTHER_MECH,
        mode="fallback",
    ) == 0


@pytest.mark.asyncio
async def test_fallback_scanner_cursor_advances_on_empty_page(tmp_path):
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_fallback_pages_per_cycle=1,
    )
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    mech.functions.getUndeliveredRequestIds.return_value.call.return_value = []
    scanner._mech_contracts[OTHER_MECH.lower()] = mech

    with patch.object(scanner, "_handle_candidate", new=AsyncMock()):
        await scanner._scan_mech(OTHER_MECH, mode="fallback")

    mech.functions.getUndeliveredRequestIds.assert_called_once_with(2, 0)
    assert scanner.queue.get_queue_scanner_cursor(
        chain="gnosis",
        mech_address=OTHER_MECH,
        mode="fallback",
    ) == 2


@pytest.mark.asyncio
async def test_fallback_scanner_reuses_cursor_after_restart(tmp_path):
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_fallback_pages_per_cycle=1,
    )
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [
        _request_id(1),
        _request_id(2),
    ]
    scanner._mech_contracts[OTHER_MECH.lower()] = mech

    with patch.object(scanner, "_handle_candidate", new=AsyncMock()):
        await scanner._scan_mech(OTHER_MECH, mode="fallback")
    scanner.queue.close()

    restarted, _enqueue2, _queued2 = _scanner(tmp_path, cfg)
    restarted_mech = MagicMock()
    restarted_mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    restarted_mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [
        _request_id(3),
        _request_id(4),
    ]
    restarted._mech_contracts[OTHER_MECH.lower()] = restarted_mech

    with patch.object(restarted, "_handle_candidate", new=AsyncMock()):
        await restarted._scan_mech(OTHER_MECH, mode="fallback")

    restarted_mech.functions.getUndeliveredRequestIds.assert_called_once_with(2, 2)


@pytest.mark.asyncio
async def test_fallback_scanner_persists_cursor_before_candidate_handling(tmp_path):
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_fallback_pages_per_cycle=1,
    )
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    mech = MagicMock()
    mech.functions.numUndeliveredRequests.return_value.call.return_value = 5
    mech.functions.getUndeliveredRequestIds.return_value.call.return_value = [_request_id(1)]
    scanner._mech_contracts[OTHER_MECH.lower()] = mech

    with (
        patch.object(scanner, "_handle_candidate", new=AsyncMock(side_effect=RuntimeError("boom"))),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await scanner._scan_mech(OTHER_MECH, mode="fallback")

    assert scanner.queue.get_queue_scanner_cursor(
        chain="gnosis",
        mech_address=OTHER_MECH,
        mode="fallback",
    ) == 2


@pytest.mark.asyncio
async def test_own_scanner_ignores_cursor_budget(tmp_path):
    cfg = _make_config(queue_scanner_fallback_pages_per_cycle=1)
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    mech = _mock_mech(5, [[_request_id(1), _request_id(2)], [_request_id(3), _request_id(4)], [_request_id(5)]])
    scanner._mech_contracts[MECH_ADDR.lower()] = mech

    with patch.object(scanner, "_handle_candidate", new=AsyncMock()):
        await scanner._scan_mech(MECH_ADDR, mode="own")

    mech.functions.getUndeliveredRequestIds.assert_any_call(2, 0)
    mech.functions.getUndeliveredRequestIds.assert_any_call(2, 2)
    mech.functions.getUndeliveredRequestIds.assert_any_call(1, 4)


@pytest.mark.asyncio
async def test_scanner_reuses_event_lookup_for_candidates_in_same_scan(tmp_path):
    """Two IDs from the same mech share one bounded event lookup."""
    cfg = _make_config(
        fallback_mode_enabled=True,
        fallback_mech_addresses=[OTHER_MECH],
        queue_scanner_event_lookback_blocks=100,
    )
    scanner, enqueue, _queued = _scanner(tmp_path, cfg)
    scanner.bridge.web3.eth.block_number = 1000
    own_mech = _mock_mech(0, [])
    own_mech.functions.paymentType.return_value.call.return_value = b"\xba" * 32
    scanner._mech_contracts[MECH_ADDR.lower()] = own_mech
    scanner._mech_contracts[OTHER_MECH.lower()] = _mock_mech(
        2,
        [[_request_id(1), _request_id(2)]],
    )
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.return_value = 2
    marketplace.functions.mapRequestIdInfos.return_value.call.return_value = (
        OTHER_MECH,
        "0x" + "00" * 20,
        "0x" + "11" * 20,
        1,
        10**18,
        b"\xba" * 32,
    )
    event = {
        "args": {
            "priorityMech": OTHER_MECH,
            "requester": "0x" + "11" * 20,
            "requestIds": [_request_id(1), _request_id(2)],
            "requestDatas": [
                b'{"prompt":"p1","tool":"echo"}',
                b'{"prompt":"p2","tool":"echo"}',
            ],
        }
    }
    marketplace.events.MarketplaceRequest.get_logs.return_value = [event]
    scanner._marketplace = marketplace

    await scanner.scan_once()

    assert enqueue.await_count == 2
    marketplace.events.MarketplaceRequest.get_logs.assert_called_once()


@pytest.mark.asyncio
async def test_scanner_skips_resolved_requests_without_known_tool(tmp_path):
    """Queue reconciliation fails closed instead of falling back to echo."""
    cfg = _make_config()
    scanner, enqueue, _queued = _scanner(tmp_path, cfg)
    scanner._mech_contracts[MECH_ADDR.lower()] = _mock_mech(1, [[_request_id(1)]])
    marketplace = MagicMock()
    marketplace.functions.getRequestStatus.return_value.call.return_value = 1
    scanner._marketplace = marketplace

    async def resolve(candidate):
        return MechRequest(
            request_id=candidate.request_id_hex,
            chain="gnosis",
            tool="",
            priority_mech=MECH_ADDR,
        )

    with patch.object(scanner, "_resolve_request_payload", side_effect=resolve):
        await scanner.scan_once()

    enqueue.assert_not_awaited()


def test_find_request_event_uses_priority_mech_filter(tmp_path):
    """Payload lookup is targeted by priorityMech, never an unfiltered log scan."""
    cfg = _make_config(queue_scanner_event_lookback_blocks=100)
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    scanner.bridge.web3.eth.block_number = 1000
    marketplace = MagicMock()
    event = {
        "args": {
            "priorityMech": OTHER_MECH,
            "requester": "0x" + "11" * 20,
            "requestIds": [_request_id(1)],
            "requestDatas": [b'{"prompt":"p","tool":"echo"}'],
        }
    }
    marketplace.events.MarketplaceRequest.get_logs.return_value = [event]
    scanner._marketplace = marketplace

    found = scanner._find_request_event(
        QueueCandidate(_request_id(1), OTHER_MECH, "fallback")
    )

    assert found is event
    kwargs = marketplace.events.MarketplaceRequest.get_logs.call_args.kwargs
    assert kwargs["from_block"] == 900
    assert kwargs["to_block"] == 1000
    assert kwargs["argument_filters"] == {"priorityMech": OTHER_MECH}
    assert kwargs["argument_filters"] != {}
    marketplace.functions.mapRequestIdInfos.assert_not_called()


def test_payment_compatibility_uses_onchain_info_order(tmp_path):
    """mapRequestIdInfos()[3] is responseTimeout; rate/payment are [4]/[5]."""
    cfg = _make_config()
    cfg.enabled_chains["gnosis"].delivery_rate = 10
    scanner, _enqueue, _queued = _scanner(tmp_path, cfg)
    marketplace = MagicMock()
    marketplace.functions.mapRequestIdInfos.return_value.call.return_value = (
        OTHER_MECH,
        "0x" + "00" * 20,
        "0x" + "11" * 20,
        1,
        10,
        b"\xba" * 32,
    )
    scanner._marketplace = marketplace

    with patch.object(scanner, "_our_mech_payment_type", return_value=b"\xba" * 32):
        assert scanner._payment_is_compatible(_request_id(1)) is True
