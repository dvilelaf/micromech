"""Unit tests for checkpoint task eviction detection."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.tasks.checkpoint import _check_eviction_events, checkpoint_task


@pytest.fixture(autouse=True)
def reset_last_alerted_epoch():
    """Reset the module-level epoch tracker between tests."""
    import micromech.tasks.checkpoint as cp_mod

    cp_mod._last_alerted_epoch.clear()
    yield
    cp_mod._last_alerted_epoch.clear()


def _make_contract(events: dict) -> MagicMock:
    contract = MagicMock()
    contract.chain_interface.web3.eth.block_number = 100_000
    contract.get_checkpoint_events.return_value = events
    return contract


def _make_notif() -> AsyncMock:
    return AsyncMock()


# ---------------------------------------------------------------------------
# _check_eviction_events
# ---------------------------------------------------------------------------


async def test_no_alert_when_rewarded():
    """Service got reward → no alert sent."""
    contract = _make_contract(
        {"epoch": 5, "rewarded_services": {3098: 1000}, "inactivity_warnings": [], "evicted_services": []}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_not_awaited()


async def test_alert_on_inactivity_warning():
    """Service appears in inactivity_warnings → alert sent."""
    contract = _make_contract(
        {"epoch": 5, "rewarded_services": {}, "inactivity_warnings": [3098], "evicted_services": []}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_awaited_once()
    title, body = notif.send.call_args[0]
    assert "Inactivity warning" in body or "inactivity" in body.lower()


async def test_alert_on_eviction():
    """Service appears in evicted_services → alert sent with EVICTED message."""
    contract = _make_contract(
        {"epoch": 5, "rewarded_services": {}, "inactivity_warnings": [], "evicted_services": [3098]}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_awaited_once()
    title, body = notif.send.call_args[0]
    assert "EVICTED" in body


async def test_no_alert_for_other_service():
    """Another service is evicted → no alert for our service."""
    contract = _make_contract(
        {"epoch": 5, "rewarded_services": {3098: 500}, "inactivity_warnings": [9999], "evicted_services": [8888]}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_not_awaited()


async def test_deduplication_same_epoch():
    """Same epoch → alert sent only once."""
    contract = _make_contract(
        {"epoch": 7, "rewarded_services": {}, "inactivity_warnings": [3098], "evicted_services": []}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    assert notif.send.await_count == 1


async def test_deduplication_different_epochs():
    """New epoch → second alert is sent."""
    notif = _make_notif()

    contract_ep7 = _make_contract(
        {"epoch": 7, "rewarded_services": {}, "inactivity_warnings": [3098], "evicted_services": []}
    )
    await _check_eviction_events(contract_ep7, service_id=3098, chain_name="gnosis", notification_service=notif)

    contract_ep8 = _make_contract(
        {"epoch": 8, "rewarded_services": {}, "inactivity_warnings": [3098], "evicted_services": []}
    )
    await _check_eviction_events(contract_ep8, service_id=3098, chain_name="gnosis", notification_service=notif)

    assert notif.send.await_count == 2


async def test_no_alert_when_no_checkpoint_events():
    """No checkpoint events found (epoch=None) → no alert."""
    contract = _make_contract(
        {"epoch": None, "rewarded_services": {}, "inactivity_warnings": [], "evicted_services": []}
    )
    notif = _make_notif()
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_not_awaited()


async def test_exception_in_get_checkpoint_events_does_not_raise():
    """Error in get_checkpoint_events → logged, no exception propagated."""
    contract = MagicMock()
    contract.chain_interface.web3.eth.block_number = 100_000
    contract.get_checkpoint_events.side_effect = RuntimeError("RPC error")
    notif = _make_notif()
    # Should not raise
    await _check_eviction_events(contract, service_id=3098, chain_name="gnosis", notification_service=notif)
    notif.send.assert_not_awaited()


# ---------------------------------------------------------------------------
# checkpoint_task integration
# ---------------------------------------------------------------------------


async def test_checkpoint_task_calls_eviction_check_after_checkpoint():
    """After a successful checkpoint, eviction check is triggered."""
    from datetime import datetime, timedelta, timezone

    notif = _make_notif()
    config = MagicMock()
    config.checkpoint_alert_enabled = False

    lifecycle = MagicMock()
    lifecycle.chain_config.staking_address = "0xABC"
    lifecycle.chain_config.chain = "gnosis"
    lifecycle.get_status.return_value = {"is_staked": True}
    lifecycle.checkpoint.return_value = True

    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=2)

    with (
        patch("micromech.core.bridge.get_service_info", return_value={"service_key": "gnosis:3098", "service_id": 3098}),
        patch("iwa.plugins.olas.contracts.staking.StakingContract") as MockContract,
        patch("micromech.tasks.checkpoint._check_eviction_events", new_callable=AsyncMock) as mock_check,
    ):
        mock_contract_instance = MagicMock()
        mock_contract_instance.get_next_epoch_start.return_value = past
        MockContract.return_value = mock_contract_instance

        await checkpoint_task({"gnosis": lifecycle}, notif, config)

    mock_check.assert_awaited_once()
    call_kwargs = mock_check.call_args[1]
    assert call_kwargs["service_id"] == 3098
    assert call_kwargs["chain_name"] == "gnosis"


async def test_checkpoint_task_no_eviction_check_when_epoch_active():
    """When epoch is still active, eviction check is NOT triggered."""
    from datetime import datetime, timedelta, timezone

    notif = _make_notif()
    config = MagicMock()

    lifecycle = MagicMock()
    lifecycle.chain_config.staking_address = "0xABC"
    lifecycle.chain_config.chain = "gnosis"
    lifecycle.get_status.return_value = {"is_staked": True}

    future_epoch_end = datetime.now(timezone.utc) + timedelta(hours=12)

    with (
        patch("micromech.core.bridge.get_service_info", return_value={"service_key": "gnosis:3098", "service_id": 3098}),
        patch("iwa.plugins.olas.contracts.staking.StakingContract") as MockContract,
        patch("micromech.tasks.checkpoint._check_eviction_events", new_callable=AsyncMock) as mock_check,
    ):
        mock_contract_instance = MagicMock()
        mock_contract_instance.get_next_epoch_start.return_value = future_epoch_end
        MockContract.return_value = mock_contract_instance

        await checkpoint_task({"gnosis": lifecycle}, notif, config)

    mock_check.assert_not_awaited()
