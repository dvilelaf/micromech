"""Tests for custom exceptions."""

from micromech.core.errors import (
    MechError,
    PersistenceError,
    ToolExecutionError,
)


class TestExceptions:
    def test_mech_error(self):
        e = MechError("base error")
        assert str(e) == "base error"

    def test_tool_execution_error(self):
        e = ToolExecutionError("llm", "timeout")
        assert e.tool_id == "llm"
        assert "llm" in str(e)
        assert "timeout" in str(e)

    def test_persistence_error_is_mech_error(self):
        assert isinstance(PersistenceError("db"), MechError)
