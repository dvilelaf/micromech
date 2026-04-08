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
        assert "result" in data
        assert data["result"] == "hello"

    @pytest.mark.asyncio
    async def test_execute_with_timeout(self):
        reg = ToolRegistry()
        reg.load_builtins()
        tool = reg.get("echo")
        result = await tool.execute_with_timeout("hello")
        data = json.loads(result)
        assert data["result"] == "hello"


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

    def test_all_builtin_tool_ids_resolve(self):
        """Every ALLOWED_TOOLS ID (including aliases) must resolve in the registry."""
        reg = ToolRegistry()
        reg.load_builtins()
        # Collect all ALLOWED_TOOLS from all loaded tool packages
        expected_ids: set[str] = set()
        for tool in reg.list_tools():
            for tool_id in tool.ALLOWED_TOOLS:
                expected_ids.add(tool_id)
        # Every ID must resolve to a Tool
        for tool_id in expected_ids:
            assert reg.has(tool_id), f"Tool ID '{tool_id}' not found in registry"
            tool = reg.get(tool_id)
            assert tool is not None

    def test_list_packages(self):
        reg = ToolRegistry()
        reg.load_builtins()
        packages = reg.list_packages()
        assert len(packages) >= 1
        for pkg in packages:
            assert "name" in pkg
            assert "version" in pkg
            assert "tools" in pkg
            assert len(pkg["tools"]) >= 1
            assert pkg["origin"] == "builtin"

    def test_builtins_have_origin_builtin(self):
        reg = ToolRegistry()
        reg.load_builtins()
        for tool in reg.list_tools():
            assert tool.metadata.origin == "builtin"


class TestCustomTools:
    """Test custom tool loading from external directory."""

    def test_load_custom_empty_dir(self, tmp_path: Path):
        """Loading from empty directory does nothing."""
        reg = ToolRegistry()
        reg.load_custom(tmp_path)
        assert len(reg.list_tools()) == 0

    def test_load_custom_nonexistent_dir(self, tmp_path: Path):
        """Loading from nonexistent directory does nothing."""
        reg = ToolRegistry()
        reg.load_custom(tmp_path / "nonexistent")
        assert len(reg.list_tools()) == 0

    def test_load_custom_tool(self, tmp_path: Path):
        """Load a custom tool from a directory with component.yaml + .py."""
        tool_dir = tmp_path / "my_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: my_tool\nversion: 1.0.0\nentry_point: my_tool.py\n"
        )
        (tool_dir / "my_tool.py").write_text(
            'ALLOWED_TOOLS = ["my-custom"]\n'
            'def run(**kwargs):\n'
            '    return kwargs.get("prompt", ""), None, None, None\n'
        )
        (tool_dir / "__init__.py").write_text("")

        reg = ToolRegistry()
        reg.load_custom(tmp_path)
        assert reg.has("my-custom")
        tool = reg.get("my-custom")
        assert tool.metadata.origin == "custom"
        assert tool.metadata.name == "my_tool"

    def test_custom_cannot_override_builtin(self, tmp_path: Path):
        """Custom tool with same ID as builtin is rejected."""
        tool_dir = tmp_path / "echo_clone"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: echo_clone\nversion: 1.0.0\nentry_point: echo_clone.py\n"
        )
        (tool_dir / "echo_clone.py").write_text(
            'ALLOWED_TOOLS = ["echo"]\n'
            'def run(**kwargs):\n'
            '    return "custom", None, None, None\n'
        )
        (tool_dir / "__init__.py").write_text("")

        reg = ToolRegistry()
        reg.load_builtins()
        builtin_echo = reg.get("echo")
        reg.load_custom(tmp_path)
        # Builtin should win
        assert reg.get("echo") is builtin_echo

    def test_custom_disabled(self, tmp_path: Path):
        """Disabled custom tools are skipped."""
        tool_dir = tmp_path / "skipped_tool"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: skipped_tool\nversion: 1.0.0\nentry_point: skipped_tool.py\n"
        )
        (tool_dir / "skipped_tool.py").write_text(
            'ALLOWED_TOOLS = ["skip-me"]\n'
            'def run(**kwargs):\n'
            '    return "", None, None, None\n'
        )
        (tool_dir / "__init__.py").write_text("")

        reg = ToolRegistry()
        reg.load_custom(tmp_path, disabled={"skipped_tool"})
        assert not reg.has("skip-me")

    def test_list_packages_shows_origin(self, tmp_path: Path):
        """list_packages includes origin field for both builtin and custom."""
        tool_dir = tmp_path / "custom_pkg"
        tool_dir.mkdir()
        (tool_dir / "component.yaml").write_text(
            "name: custom_pkg\nversion: 0.2.0\nentry_point: custom_pkg.py\n"
        )
        (tool_dir / "custom_pkg.py").write_text(
            'ALLOWED_TOOLS = ["custom-id"]\n'
            'def run(**kwargs):\n'
            '    return "", None, None, None\n'
        )
        (tool_dir / "__init__.py").write_text("")

        reg = ToolRegistry()
        reg.load_builtins()
        reg.load_custom(tmp_path)
        packages = reg.list_packages()
        origins = {p["name"]: p["origin"] for p in packages}
        assert origins.get("echo_tool") == "builtin"
        assert origins.get("custom_pkg") == "custom"
