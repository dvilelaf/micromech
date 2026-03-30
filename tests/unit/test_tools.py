"""Tests for the tool system."""

import json

import pytest
from pydantic import ValidationError

from micromech.core.errors import ToolExecutionError
from micromech.tools.base import Tool, ToolMetadata
from micromech.tools.registry import ToolNotFoundError, ToolRegistry


def _make_tool(tool_id: str = "dummy", timeout: int = 10) -> Tool:
    """Create a simple test tool."""

    def run_fn(**kwargs):
        prompt = kwargs.get("prompt", "")
        return f"dummy:{prompt}", prompt, None, None

    metadata = ToolMetadata(id=tool_id, name="Dummy", version="1.0.0", timeout=timeout)
    return Tool(metadata=metadata, run_fn=run_fn)


def _make_slow_tool() -> Tool:
    """Create a tool that sleeps too long for timeout testing."""

    def run_fn(**kwargs):
        import time

        time.sleep(10)
        return "never", None, None, None

    metadata = ToolMetadata(id="slow", name="Slow", timeout=1)
    return Tool(metadata=metadata, run_fn=run_fn)


class TestToolMetadata:
    def test_basic(self):
        meta = ToolMetadata(id="test")
        assert meta.id == "test"
        assert meta.name == "test"
        assert meta.version == "0.1.0"
        assert meta.timeout == 60

    def test_custom_name(self):
        meta = ToolMetadata(id="test", name="Custom Name")
        assert meta.name == "Custom Name"

    def test_timeout_bounds(self):
        with pytest.raises(ValidationError):
            ToolMetadata(id="test", timeout=0)
        with pytest.raises(ValidationError):
            ToolMetadata(id="test", timeout=3601)

    def test_id_validation_lowercase(self):
        ToolMetadata(id="my-tool")
        ToolMetadata(id="tool123")

    def test_id_validation_rejects_invalid(self):
        with pytest.raises(ValidationError):
            ToolMetadata(id="MyTool")
        with pytest.raises(ValidationError):
            ToolMetadata(id="my tool")
        with pytest.raises(ValidationError):
            ToolMetadata(id="../etc")


class TestEchoTool:
    def test_registry_loads_echo(self):
        reg = ToolRegistry()
        reg.load_builtins()
        assert reg.has("echo")

    @pytest.mark.asyncio
    async def test_execute(self):
        reg = ToolRegistry()
        reg.load_builtins()
        tool = reg.get("echo")
        result = await tool.execute("hello")
        data = json.loads(result)
        assert "p_yes" in data
        assert "p_no" in data
        assert data["p_yes"] + data["p_no"] == 1.0

    @pytest.mark.asyncio
    async def test_execute_with_timeout(self):
        reg = ToolRegistry()
        reg.load_builtins()
        tool = reg.get("echo")
        result = await tool.execute_with_timeout("hello")
        data = json.loads(result)
        assert "p_yes" in data


class TestTimeoutEnforcement:
    @pytest.mark.asyncio
    async def test_slow_tool_times_out(self):
        tool = _make_slow_tool()
        with pytest.raises(ToolExecutionError, match="Timed out"):
            await tool.execute_with_timeout("test")


class TestToolRegistry:
    def test_register_and_get(self):
        reg = ToolRegistry()
        tool = _make_tool()
        reg.register(tool)
        assert reg.get("dummy") is tool

    def test_get_nonexistent_raises(self):
        reg = ToolRegistry()
        with pytest.raises(ToolNotFoundError, match="nonexistent"):
            reg.get("nonexistent")

    def test_has(self):
        reg = ToolRegistry()
        assert reg.has("dummy") is False
        reg.register(_make_tool())
        assert reg.has("dummy") is True

    def test_list_tools(self):
        reg = ToolRegistry()
        reg.register(_make_tool("tool-a"))
        reg.register(_make_tool("tool-b"))
        assert len(reg.list_tools()) == 2

    def test_tool_ids(self):
        reg = ToolRegistry()
        reg.register(_make_tool("tool-a"))
        reg.register(_make_tool("tool-b"))
        assert set(reg.tool_ids) == {"tool-a", "tool-b"}

    def test_load_builtins(self):
        reg = ToolRegistry()
        reg.load_builtins()
        assert reg.has("echo")
