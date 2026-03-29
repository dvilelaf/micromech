"""Tests for the event listener."""

import asyncio
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from micromech.core.config import MicromechConfig, RuntimeConfig
from micromech.core.models import MechRequest
from micromech.runtime.listener import EventListener

MECH_ADDR = "0x" + "ab" * 20
OTHER_MECH = "0x" + "cd" * 20


def _make_event(
    mech_addr: str,
    request_ids: list[bytes] | None = None,
    request_datas: list[bytes] | None = None,
) -> dict:
    """Create a mock decoded MarketplaceRequest event."""
    if request_ids is None:
        request_ids = [b"\xaa" * 32]
    if request_datas is None:
        request_datas = [b""]
    return {
        "args": {
            "priorityMech": mech_addr,
            "requester": "0x" + "00" * 20,
            "numRequests": len(request_ids),
            "requestIds": request_ids,
            "requestDatas": request_datas,
        },
        "event": "MarketplaceRequest",
        "blockNumber": 1000,
    }


class TestParseRequestData:
    def test_parse_json_payload(self):
        listener = EventListener(MicromechConfig())
        data = json.dumps({"prompt": "Will ETH hit 10k?", "tool": "llm"}).encode()
        prompt, tool, extra = listener._parse_request_data(data)
        assert prompt == "Will ETH hit 10k?"
        assert tool == "llm"
        assert extra == {}

    def test_parse_json_with_extra_params(self):
        listener = EventListener(MicromechConfig())
        data = json.dumps(
            {
                "prompt": "test",
                "tool": "llm",
                "model": "qwen",
                "nonce": 42,
            }
        ).encode()
        prompt, tool, extra = listener._parse_request_data(data)
        assert prompt == "test"
        assert tool == "llm"
        assert extra == {"model": "qwen", "nonce": 42}

    def test_parse_empty_data(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(b"")
        assert prompt == ""
        assert tool == ""

    def test_parse_invalid_json(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(b"not json")
        assert prompt == ""
        assert tool == ""

    def test_parse_binary_data(self):
        listener = EventListener(MicromechConfig())
        prompt, tool, extra = listener._parse_request_data(bytes(range(256)))
        assert prompt == ""
        assert tool == ""


class TestParseMarketplaceEvent:
    def test_parse_valid_event(self):
        config = MicromechConfig(mech={"mech_address": MECH_ADDR})
        listener = EventListener(config)
        data = json.dumps({"prompt": "hello", "tool": "echo"}).encode()
        event = _make_event(MECH_ADDR, request_datas=[data])

        reqs = listener._parse_marketplace_event(event, MECH_ADDR)
        assert len(reqs) == 1
        assert reqs[0].prompt == "hello"
        assert reqs[0].tool == "echo"

    def test_filters_other_mech(self):
        listener = EventListener(MicromechConfig())
        event = _make_event(OTHER_MECH)
        reqs = listener._parse_marketplace_event(event, MECH_ADDR)
        assert reqs == []

    def test_accepts_when_no_mech_filter(self):
        listener = EventListener(MicromechConfig())
        event = _make_event(MECH_ADDR)
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 1

    def test_request_id_from_bytes(self):
        listener = EventListener(MicromechConfig())
        rid = b"\xee" * 32
        event = _make_event(MECH_ADDR, request_ids=[rid])
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 1
        assert reqs[0].request_id == "ee" * 32

    def test_multiple_requests_in_event(self):
        listener = EventListener(MicromechConfig())
        data1 = json.dumps({"prompt": "q1", "tool": "echo"}).encode()
        data2 = json.dumps({"prompt": "q2", "tool": "llm"}).encode()
        event = _make_event(
            MECH_ADDR,
            request_ids=[b"\x01" * 32, b"\x02" * 32],
            request_datas=[data1, data2],
        )
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 2
        assert reqs[0].prompt == "q1"
        assert reqs[1].prompt == "q2"


class TestFetchEvents:
    def test_fetch_with_mock_contract(self):
        config = MicromechConfig(mech={"mech_address": MECH_ADDR})
        bridge = MagicMock()
        data = json.dumps({"prompt": "q1", "tool": "echo"}).encode()

        mock_filter = MagicMock()
        mock_filter.get_all_entries.return_value = [_make_event(MECH_ADDR, request_datas=[data])]
        bridge.with_retry.side_effect = lambda fn, **kw: fn()

        listener = EventListener(config, bridge=bridge)
        mock_contract = MagicMock()
        mock_contract.events.MarketplaceRequest.create_filter.return_value = mock_filter
        listener._marketplace_contract = mock_contract

        requests = listener._fetch_events(100, 200)
        assert len(requests) == 1
        assert requests[0].prompt == "q1"

    def test_fetch_handles_exception(self):
        config = MicromechConfig(mech={"mech_address": MECH_ADDR})
        bridge = MagicMock()

        listener = EventListener(config, bridge=bridge)
        mock_contract = MagicMock()
        mock_contract.events.MarketplaceRequest.create_filter.side_effect = Exception("rpc")
        listener._marketplace_contract = mock_contract

        requests = listener._fetch_events(100, 200)
        assert requests == []


class TestPollOnce:
    @pytest.mark.asyncio
    async def test_poll_without_bridge_returns_empty(self):
        listener = EventListener(MicromechConfig(), bridge=None)
        result = await listener.poll_once()
        assert result == []

    @pytest.mark.asyncio
    async def test_poll_no_new_blocks(self):
        bridge = MagicMock()
        bridge.with_retry.side_effect = lambda fn, **kw: fn()
        bridge.web3.eth.block_number = 100

        listener = EventListener(MicromechConfig(), bridge=bridge)
        listener._last_block = 100
        result = await listener.poll_once()
        assert result == []

    @pytest.mark.asyncio
    async def test_poll_handles_exception(self):
        bridge = MagicMock()
        bridge.with_retry.side_effect = Exception("timeout")

        listener = EventListener(MicromechConfig(), bridge=bridge)
        result = await listener.poll_once()
        assert result == []


class TestAdvanceBlock:
    def test_advance_block(self):
        listener = EventListener(MicromechConfig())
        listener._polled_to_block = 500
        listener.advance_block()
        assert listener._last_block == 500
        assert listener._polled_to_block is None

    def test_advance_block_noop_when_none(self):
        listener = EventListener(MicromechConfig())
        listener._last_block = 100
        listener.advance_block()
        assert listener._last_block == 100


class TestRunLoop:
    @pytest.mark.asyncio
    async def test_run_stops_on_stop(self):
        config = MicromechConfig(runtime=RuntimeConfig(event_poll_interval=1))
        listener = EventListener(config, bridge=None)

        async def callback(req):
            pass

        async def stop_soon():
            await asyncio.sleep(0.2)
            listener.stop()

        asyncio.create_task(stop_soon())
        await asyncio.wait_for(listener.run(callback), timeout=3.0)
        assert listener._running is False

    @pytest.mark.asyncio
    async def test_run_with_mock_events(self):
        config = MicromechConfig(runtime=RuntimeConfig(event_poll_interval=1))
        listener = EventListener(config, bridge=None)
        received = []

        async def callback(req):
            received.append(req)

        call_count = 0

        async def mock_poll():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                req = MechRequest(request_id="r1", prompt="test", tool="echo")
                listener._polled_to_block = 100
                return [req]
            listener.stop()
            return []

        listener.poll_once = mock_poll
        await asyncio.wait_for(listener.run(callback), timeout=5.0)
        assert len(received) == 1
        assert listener._last_block == 100

    def test_stop(self):
        listener = EventListener(MicromechConfig())
        listener._running = True
        listener.stop()
        assert listener._running is False

    @pytest.mark.asyncio
    async def test_run_handles_callback_error(self):
        """Callback errors don't crash the loop but prevent block advance."""
        config = MicromechConfig(runtime=RuntimeConfig(event_poll_interval=1))
        listener = EventListener(config, bridge=None)
        advanced_after_error = None

        async def failing_callback(req):
            raise RuntimeError("callback boom")

        call_count = 0

        async def mock_poll():
            nonlocal call_count, advanced_after_error
            call_count += 1
            if call_count == 1:
                req = MechRequest(request_id="r1", prompt="test", tool="echo")
                listener._polled_to_block = 200
                return [req]
            # Capture whether block was advanced after the first (failed) iteration
            advanced_after_error = listener._last_block
            listener.stop()
            return []

        listener.poll_once = mock_poll
        await asyncio.wait_for(listener.run(failing_callback), timeout=5.0)
        # Block should NOT have advanced after the failed callback iteration
        assert advanced_after_error is None


class TestResolveRequest:
    """Test _resolve_request with IPFS multihash data."""

    @pytest.mark.asyncio
    async def test_resolve_ipfs_multihash(self):
        """Requests with IPFS multihash data get resolved via IPFS."""
        config = MicromechConfig(ipfs={"enabled": True})
        listener = EventListener(config)

        # Build a valid 34-byte multihash (0x12 0x20 + 32-byte sha256)
        digest = hashlib.sha256(b"test payload").digest()
        multihash_data = bytes([0x12, 0x20]) + digest

        req = MechRequest(
            request_id="r1",
            data=multihash_data,
            prompt="",  # Empty — needs IPFS resolution
            tool="",
        )

        ipfs_payload = {"prompt": "Will ETH hit 10k?", "tool": "llm", "nonce": 42}
        with patch(
            "micromech.ipfs.client.fetch_json_from_ipfs",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = ipfs_payload
            resolved = await listener._resolve_request(req)

        assert resolved.prompt == "Will ETH hit 10k?"
        assert resolved.tool == "llm"
        assert resolved.extra_params == {"nonce": 42}
        assert resolved.request_id == "r1"

    @pytest.mark.asyncio
    async def test_resolve_skips_when_prompt_present(self):
        """If prompt is already set, IPFS resolution is skipped."""
        config = MicromechConfig(ipfs={"enabled": True})
        listener = EventListener(config)

        req = MechRequest(
            request_id="r1",
            data=b"\x12\x20" + b"\x00" * 32,
            prompt="already set",
            tool="echo",
        )

        resolved = await listener._resolve_request(req)
        assert resolved.prompt == "already set"

    @pytest.mark.asyncio
    async def test_resolve_skips_when_ipfs_disabled(self):
        """With IPFS disabled, multihash data is not resolved."""
        config = MicromechConfig(ipfs={"enabled": False})
        listener = EventListener(config)

        digest = hashlib.sha256(b"test").digest()
        req = MechRequest(
            request_id="r1",
            data=bytes([0x12, 0x20]) + digest,
            prompt="",
            tool="",
        )

        resolved = await listener._resolve_request(req)
        assert resolved.prompt == ""  # Not resolved

    @pytest.mark.asyncio
    async def test_resolve_handles_ipfs_error(self):
        """IPFS fetch failure returns original request."""
        config = MicromechConfig(ipfs={"enabled": True})
        listener = EventListener(config)

        digest = hashlib.sha256(b"test").digest()
        req = MechRequest(
            request_id="r1",
            data=bytes([0x12, 0x20]) + digest,
            prompt="",
            tool="",
        )

        with patch(
            "micromech.ipfs.client.fetch_json_from_ipfs",
            new_callable=AsyncMock,
            side_effect=RuntimeError("gateway timeout"),
        ):
            resolved = await listener._resolve_request(req)

        assert resolved is req  # Returns original

    @pytest.mark.asyncio
    async def test_resolve_non_multihash_data(self):
        """Non-multihash binary data is returned as-is."""
        config = MicromechConfig(ipfs={"enabled": True})
        listener = EventListener(config)

        req = MechRequest(
            request_id="r1",
            data=b"not a multihash",
            prompt="",
            tool="",
        )

        resolved = await listener._resolve_request(req)
        assert resolved is req


class TestGetMarketplaceContract:
    def test_lazy_loads_contract(self):
        config = MicromechConfig(
            mech={"mech_address": MECH_ADDR},
        )
        bridge = MagicMock()
        mock_contract = MagicMock()
        bridge.web3.eth.contract.return_value = mock_contract

        listener = EventListener(config, bridge=bridge)
        contract = listener._get_marketplace_contract()
        assert contract is mock_contract

        # Second call returns cached
        contract2 = listener._get_marketplace_contract()
        assert contract2 is mock_contract
        bridge.web3.eth.contract.assert_called_once()


class TestParseMarketplaceEventEdgeCases:
    def test_string_request_data(self):
        """String request data is converted to bytes."""
        listener = EventListener(MicromechConfig())
        data_str = json.dumps({"prompt": "hello", "tool": "echo"})
        event = _make_event(
            MECH_ADDR,
            request_datas=[data_str],
        )
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 1
        assert reqs[0].prompt == "hello"

    def test_string_request_id(self):
        """String request IDs (not bytes) are handled."""
        listener = EventListener(MicromechConfig())
        event = {
            "args": {
                "priorityMech": MECH_ADDR,
                "requestIds": ["abc123"],
                "requestDatas": [b""],
            },
        }
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 1
        assert reqs[0].request_id == "abc123"

    def test_missing_request_data(self):
        """When requestDatas is shorter than requestIds, empty data is used."""
        listener = EventListener(MicromechConfig())
        event = _make_event(
            MECH_ADDR,
            request_ids=[b"\x01" * 32, b"\x02" * 32],
            request_datas=[json.dumps({"prompt": "q1"}).encode()],
        )
        reqs = listener._parse_marketplace_event(event, None)
        assert len(reqs) == 2
        assert reqs[0].prompt == "q1"
        assert reqs[1].prompt == ""  # Missing data -> empty
