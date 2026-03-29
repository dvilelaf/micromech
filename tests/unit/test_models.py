"""Tests for data models."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from micromech.core.constants import STATUS_PENDING
from micromech.core.models import (
    MechRequest,
    MechResponse,
    RequestRecord,
    ToolResult,
)

VALID_ADDR = "0x" + "a" * 40


class TestMechRequest:
    def test_minimal(self):
        req = MechRequest(request_id="req-1")
        assert req.request_id == "req-1"
        assert req.status == STATUS_PENDING
        assert req.prompt == ""
        assert req.tool == ""
        assert req.is_offchain is False
        assert isinstance(req.created_at, datetime)

    def test_full(self):
        req = MechRequest(
            request_id="req-2",
            sender=VALID_ADDR,
            prompt="Will ETH hit 10k?",
            tool="llm",
            extra_params={"model": "qwen"},
            timeout=60,
            is_offchain=True,
        )
        assert req.sender == VALID_ADDR
        assert req.prompt == "Will ETH hit 10k?"
        assert req.tool == "llm"
        assert req.timeout == 60
        assert req.is_offchain is True

    def test_invalid_sender(self):
        with pytest.raises(ValidationError):
            MechRequest(request_id="r", sender="bad")

    def test_invalid_sender_non_hex(self):
        with pytest.raises(ValidationError):
            MechRequest(request_id="r", sender="0x" + "Z" * 40)

    def test_empty_sender_is_valid(self):
        req = MechRequest(request_id="r", sender="")
        assert req.sender == ""

    def test_invalid_status(self):
        with pytest.raises(ValidationError):
            MechRequest(request_id="r", status="banana")

    def test_invalid_delivery_method(self):
        with pytest.raises(ValidationError):
            MechRequest(request_id="r", delivery_method="pigeon")

    def test_valid_delivery_methods(self):
        req_m = MechRequest(request_id="r1", delivery_method="marketplace")
        req_l = MechRequest(request_id="r2", delivery_method="legacy")
        assert req_m.delivery_method == "marketplace"
        assert req_l.delivery_method == "legacy"

    def test_timeout_must_be_positive(self):
        with pytest.raises(ValidationError):
            MechRequest(request_id="r", timeout=0)

    def test_data_bytes(self):
        req = MechRequest(request_id="r", data=b"\x00\x01\x02")
        assert req.data == b"\x00\x01\x02"

    def test_extra_params_default_empty(self):
        req = MechRequest(request_id="r")
        assert req.extra_params == {}

    def test_created_at_is_utc(self):
        req = MechRequest(request_id="r")
        assert req.created_at.tzinfo is not None


class TestToolResult:
    def test_success(self):
        r = ToolResult(output="yes", execution_time=1.5)
        assert r.success is True
        assert r.output == "yes"
        assert r.execution_time == 1.5

    def test_failure(self):
        r = ToolResult(error="timeout", execution_time=300.0)
        assert r.success is False
        assert r.error == "timeout"

    def test_metadata(self):
        r = ToolResult(output="ok", metadata={"model": "qwen", "tokens": 42})
        assert r.metadata["tokens"] == 42


class TestMechResponse:
    def test_minimal(self):
        resp = MechResponse(request_id="req-1", result="answer")
        assert resp.request_id == "req-1"
        assert resp.delivery_tx_hash is None
        assert resp.delivered_at is None

    def test_delivered(self):
        now = datetime.now(timezone.utc)
        resp = MechResponse(
            request_id="req-1",
            result="answer",
            ipfs_hash="QmTest",
            delivery_tx_hash="0x" + "f" * 64,
            delivered_at=now,
        )
        assert resp.delivery_tx_hash.startswith("0x")
        assert resp.delivered_at == now


class TestRequestRecord:
    def test_minimal(self):
        rec = RequestRecord(request=MechRequest(request_id="r1"))
        assert rec.request.request_id == "r1"
        assert rec.result is None
        assert rec.response is None

    def test_full(self):
        rec = RequestRecord(
            request=MechRequest(request_id="r1", prompt="test"),
            result=ToolResult(output="yes", execution_time=1.0),
            response=MechResponse(request_id="r1", result="yes"),
        )
        assert rec.result.success is True
        assert rec.response.result == "yes"
