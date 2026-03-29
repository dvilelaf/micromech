"""Tests for custom exceptions."""

from micromech.core.errors import (
    ConfigError,
    DeliveryError,
    MechError,
    PersistenceError,
    RequestError,
    ToolExecutionError,
)


class TestExceptions:
    def test_mech_error(self):
        e = MechError("base error")
        assert str(e) == "base error"

    def test_config_error_is_mech_error(self):
        e = ConfigError("bad config")
        assert isinstance(e, MechError)

    def test_tool_execution_error(self):
        e = ToolExecutionError("llm", "timeout")
        assert e.tool_id == "llm"
        assert "llm" in str(e)
        assert "timeout" in str(e)

    def test_delivery_error(self):
        e = DeliveryError("req-1", "rpc fail")
        assert e.request_id == "req-1"
        assert "req-1" in str(e)
        assert "rpc fail" in str(e)

    def test_request_error_is_mech_error(self):
        assert isinstance(RequestError("bad"), MechError)

    def test_persistence_error_is_mech_error(self):
        assert isinstance(PersistenceError("db"), MechError)
