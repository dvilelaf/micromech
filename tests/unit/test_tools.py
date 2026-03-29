"""Tests for the tool system."""

import json
from pathlib import Path

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

    def test_valory_run(self):
        reg = ToolRegistry()
        reg.load_builtins()
        tool = reg.get("echo")
        resp = tool.run(prompt="test", tool="echo")
        assert len(resp) == 4
        assert resp[1] == "test"
        data = json.loads(resp[0])
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

    def test_get_optional(self):
        reg = ToolRegistry()
        assert reg.get_optional("nope") is None
        reg.register(_make_tool())
        assert reg.get_optional("dummy") is not None

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


class TestValoryCompat:
    @pytest.mark.asyncio
    async def test_load_and_execute(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "test_tool"
        tool_dir.mkdir()

        (tool_dir / "component.yaml").write_text(
            "name: test_valory\nentry_point: test_valory.py\ncallable: run\nversion: 0.2.0\n"
        )

        (tool_dir / "test_valory.py").write_text(
            'ALLOWED_TOOLS = ["test_valory"]\n'
            "\n"
            "def run(**kwargs):\n"
            '    prompt = kwargs.get("prompt", "")\n'
            '    return f"valory:{prompt}", prompt, None, None\n'
        )

        tool = load_valory_tool(tool_dir)
        assert tool is not None
        assert tool.metadata.id == "test-valory"

        result = await tool.execute("hello")
        assert "valory:hello" in result

    def test_missing_yaml(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        assert load_valory_tool(tmp_path / "nonexistent") is None

    def test_missing_entry_point(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "bad_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: bad_tool\nentry_point: missing.py\n")
        assert load_valory_tool(tool_dir) is None

    def test_invalid_name(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "evil"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: ../../etc\n")
        assert load_valory_tool(tool_dir) is None

    def test_bad_yaml(self, tmp_path: Path):
        """Invalid YAML returns None."""
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "bad_yaml"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("{{invalid yaml: [")
        assert load_valory_tool(tool_dir) is None

    def test_exec_module_failure(self, tmp_path: Path):
        """Module that raises on import returns None."""
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "import_fail"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: import_fail\nentry_point: import_fail.py\n")
        (tool_dir / "import_fail.py").write_text("raise RuntimeError('bad import')")
        assert load_valory_tool(tool_dir) is None

    def test_no_callable(self, tmp_path: Path):
        """Module without the expected callable returns None."""
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "no_run"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: no_run\nentry_point: no_run.py\ncallable: run\n"
        )
        (tool_dir / "no_run.py").write_text("x = 42\n")
        assert load_valory_tool(tool_dir) is None

    def test_spec_from_file_returns_none(self, tmp_path: Path):
        """When spec_from_file_location returns None, returns None."""
        from unittest.mock import patch

        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "spec_none"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text("name: spec_none\nentry_point: spec_none.py\n")
        (tool_dir / "spec_none.py").write_text("def run(**kw): pass\n")

        with patch(
            "micromech.tools.valory_compat.importlib.util.spec_from_file_location",
            return_value=None,
        ):
            assert load_valory_tool(tool_dir) is None

    def test_valory_run_direct(self, tmp_path: Path):
        from micromech.tools.valory_compat import load_valory_tool

        tool_dir = tmp_path / "direct"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: direct_tool\nentry_point: direct_tool.py\ncallable: run\n"
        )
        (tool_dir / "direct_tool.py").write_text(
            'def run(**kwargs):\n    return "ok", "p", {"tx": "0x1"}, None\n'
        )

        tool = load_valory_tool(tool_dir)
        assert tool is not None
        resp = tool.run(prompt="test")
        assert resp[0] == "ok"
        assert resp[2] == {"tx": "0x1"}
