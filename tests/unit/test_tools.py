"""Tests for the tool system."""

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from micromech.core.errors import ToolExecutionError
from micromech.tools.base import Tool, ToolMetadata
from micromech.tools.builtin.echo import EchoTool
from micromech.tools.registry import ToolNotFoundError, ToolRegistry


class DummyTool(Tool):
    """Minimal tool for testing."""

    metadata = ToolMetadata(id="dummy", name="Dummy", version="1.0.0", timeout=10)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        return f"dummy:{prompt}"


class SlowTool(Tool):
    """Tool that takes too long — for timeout testing."""

    metadata = ToolMetadata(id="slow", name="Slow", timeout=1)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        await asyncio.sleep(10)
        return "never reached"


class FailingTool(Tool):
    """Tool that always raises."""

    metadata = ToolMetadata(id="failing", name="Failing", timeout=5)

    async def execute(self, prompt: str, **kwargs: Any) -> str:
        msg = "intentional failure"
        raise RuntimeError(msg)


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
        ToolMetadata(id="my-tool-v2")

    def test_id_validation_rejects_invalid(self):
        with pytest.raises(ValidationError):
            ToolMetadata(id="MyTool")  # uppercase
        with pytest.raises(ValidationError):
            ToolMetadata(id="my tool")  # space
        with pytest.raises(ValidationError):
            ToolMetadata(id="123tool")  # starts with number
        with pytest.raises(ValidationError):
            ToolMetadata(id="../etc")  # path traversal


class TestEchoTool:
    def test_metadata(self):
        tool = EchoTool()
        assert tool.metadata.id == "echo"
        assert tool.metadata.timeout == 5

    @pytest.mark.asyncio
    async def test_execute(self):
        tool = EchoTool()
        result = await tool.execute("hello")
        data = json.loads(result)
        assert data["result"] == "hello"
        assert data["tool"] == "echo"

    @pytest.mark.asyncio
    async def test_execute_empty(self):
        tool = EchoTool()
        result = await tool.execute("")
        data = json.loads(result)
        assert data["result"] == ""

    @pytest.mark.asyncio
    async def test_execute_unicode(self):
        tool = EchoTool()
        result = await tool.execute("ETH > $10k?")
        data = json.loads(result)
        assert data["result"] == "ETH > $10k?"

    @pytest.mark.asyncio
    async def test_valory_compat(self):
        tool = EchoTool()
        resp = await tool.execute_valory(prompt="test")
        assert len(resp) == 5
        assert isinstance(resp[0], str)
        assert resp[1] == "test"

    @pytest.mark.asyncio
    async def test_execute_with_timeout(self):
        tool = EchoTool()
        result = await tool.execute_with_timeout("hello")
        data = json.loads(result)
        assert data["result"] == "hello"


class TestTimeoutEnforcement:
    @pytest.mark.asyncio
    async def test_slow_tool_times_out(self):
        tool = SlowTool()
        with pytest.raises(ToolExecutionError, match="Timed out"):
            await tool.execute_with_timeout("test")

    @pytest.mark.asyncio
    async def test_failing_tool_propagates_error(self):
        tool = FailingTool()
        with pytest.raises(RuntimeError, match="intentional failure"):
            await tool.execute_with_timeout("test")


class TestToolRegistry:
    def test_register_and_get(self):
        reg = ToolRegistry()
        tool = DummyTool()
        reg.register(tool)
        assert reg.get("dummy") is tool

    def test_get_nonexistent_raises(self):
        reg = ToolRegistry()
        with pytest.raises(ToolNotFoundError, match="nonexistent"):
            reg.get("nonexistent")

    def test_get_optional(self):
        reg = ToolRegistry()
        assert reg.get_optional("nope") is None
        reg.register(DummyTool())
        assert reg.get_optional("dummy") is not None

    def test_has(self):
        reg = ToolRegistry()
        assert reg.has("dummy") is False
        reg.register(DummyTool())
        assert reg.has("dummy") is True

    def test_list_tools(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(EchoTool())
        tools = reg.list_tools()
        assert len(tools) == 2

    def test_tool_ids(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(EchoTool())
        assert set(reg.tool_ids) == {"dummy", "echo"}

    def test_overwrite_warns(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(DummyTool())
        assert len(reg.list_tools()) == 1

    def test_load_builtins(self):
        reg = ToolRegistry()
        reg.load_builtins()
        assert reg.has("echo")


class TestValoryCompat:
    @pytest.mark.asyncio
    async def test_load_and_execute(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "test_tool"
        tool_dir.mkdir()

        (tool_dir / "component.yaml").write_text(
            "name: test_valory\n"
            "entry_point: test_valory.py\n"
            "callable: run\n"
            "version: 0.2.0\n"
            "description: Test Valory tool\n"
        )

        (tool_dir / "test_valory.py").write_text(
            'ALLOWED_TOOLS = ["test_valory"]\n'
            "\n"
            "def run(**kwargs):\n"
            '    prompt = kwargs.get("prompt", "")\n'
            '    return f"valory:{prompt}", prompt, None, None, None\n'
        )

        tool = load_valory_tool(tool_dir)
        assert tool is not None
        assert tool.metadata.id == "test-valory"
        assert tool.metadata.version == "0.2.0"

        result = await tool.execute("hello")
        assert "valory:hello" in result

    def test_missing_yaml(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool = load_valory_tool(tmp_path / "nonexistent")
        assert tool is None

    def test_missing_entry_point(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "bad_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: bad_tool\nentry_point: missing.py\n")
        tool = load_valory_tool(tool_dir)
        assert tool is None

    def test_missing_callable(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "no_callable"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: no_callable\nentry_point: no_callable.py\ncallable: missing_fn\n"
        )
        (tool_dir / "no_callable.py").write_text("x = 1\n")
        tool = load_valory_tool(tool_dir)
        assert tool is None

    def test_invalid_name_rejected(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "evil"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: ../../etc\n")
        tool = load_valory_tool(tool_dir)
        assert tool is None

    @pytest.mark.asyncio
    async def test_valory_full_tuple(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "full_tuple"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: full_tuple\nentry_point: full_tuple.py\ncallable: run\n"
        )
        (tool_dir / "full_tuple.py").write_text(
            'def run(**kwargs):\n    return "result", "prompt", {"tx": "0x1"}, "artifact", None\n'
        )

        tool = load_valory_tool(tool_dir)
        assert tool is not None
        resp = await tool.execute_valory(prompt="test")
        assert resp[0] == "result"
        assert resp[1] == "prompt"
        assert resp[2] == {"tx": "0x1"}
        assert resp[3] == "artifact"
