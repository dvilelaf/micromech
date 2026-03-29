"""micromech CLI — management and runtime commands."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from loguru import logger

from micromech.core.config import DEFAULT_CONFIG_PATH, MicromechConfig

app = typer.Typer(
    name="micromech",
    help="Lightweight OLAS mech runtime.",
    no_args_is_help=True,
)


def _load_config(config_path: Optional[Path]) -> MicromechConfig:
    """Load config, using default path if not specified."""
    path = config_path or DEFAULT_CONFIG_PATH
    return MicromechConfig.load(path)


@app.command()
def init(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Initialize config file with defaults."""
    path = config_path or DEFAULT_CONFIG_PATH
    if path.exists():
        typer.echo(f"Config already exists at {path}")
        raise typer.Exit(1)
    config = MicromechConfig()
    config.save(path)
    typer.echo(f"Config created at {path}")


@app.command()
def config(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show current configuration."""
    cfg = _load_config(config_path)
    import yaml

    typer.echo(yaml.dump(cfg.model_dump(mode="json"), default_flow_style=False, sort_keys=False))


@app.command()
def run(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    no_http: bool = typer.Option(False, "--no-http", help="Disable HTTP server"),
) -> None:
    """Run the mech server (listener + executor + delivery + HTTP)."""
    cfg = _load_config(config_path)
    logger.info("Starting micromech server...")

    from micromech.runtime.server import MechServer

    # Try to load iwa bridge
    bridge = None
    try:
        from micromech.integration.iwa_bridge import IwaBridge

        bridge = IwaBridge(chain_name=cfg.mech.chain)
        logger.info("iwa bridge loaded for chain: {}", cfg.mech.chain)
    except Exception as e:
        logger.warning("iwa bridge not available: {}. Running without chain access.", e)

    server = MechServer(cfg, bridge=bridge)
    try:
        asyncio.run(server.run(with_http=not no_http))
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


@app.command()
def status(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show queue status from the database."""
    cfg = _load_config(config_path)
    from micromech.core.persistence import PersistentQueue

    queue = PersistentQueue(cfg.persistence.db_path)
    counts = queue.count_by_status()
    recent = queue.get_recent(limit=5)
    queue.close()

    typer.echo("Queue Status:")
    for status_name, count in counts.items():
        typer.echo(f"  {status_name}: {count}")

    if recent:
        typer.echo("\nRecent Requests:")
        for r in recent:
            typer.echo(
                f"  [{r.request.status}] {r.request.request_id[:20]}... "
                f"tool={r.request.tool} prompt={r.request.prompt[:40]}"
            )


@app.command(name="tools")
def list_tools() -> None:
    """List available tools."""
    from micromech.tools.registry import ToolRegistry

    reg = ToolRegistry()
    reg.load_builtins()

    typer.echo("Available Tools:")
    for tool in reg.list_tools():
        m = tool.metadata
        typer.echo(f"  {m.id} (v{m.version}) — {m.description or m.name} [timeout={m.timeout}s]")


@app.command(name="test-tool")
def test_tool(
    tool_id: str = typer.Argument(help="Tool ID to test"),
    prompt: str = typer.Argument(help="Prompt to send"),
) -> None:
    """Test a tool interactively."""
    from micromech.tools.registry import ToolRegistry

    reg = ToolRegistry()
    reg.load_builtins()

    if not reg.has(tool_id):
        typer.echo(f"Tool '{tool_id}' not found. Available: {reg.tool_ids}")
        raise typer.Exit(1)

    tool = reg.get(tool_id)
    typer.echo(f"Running {tool_id}...")

    result = asyncio.run(tool.execute_with_timeout(prompt))
    typer.echo(f"Result:\n{result}")


@app.command()
def cleanup(
    days: int = typer.Option(30, "--days", "-d"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Clean up old delivered/failed requests from the database."""
    cfg = _load_config(config_path)
    from micromech.core.persistence import PersistentQueue

    queue = PersistentQueue(cfg.persistence.db_path)
    deleted = queue.cleanup(days=days)
    queue.close()
    typer.echo(f"Cleaned up {deleted} records older than {days} days")


@app.command()
def web(
    port: int = typer.Option(8000, "--port", "-p"),
    host: str = typer.Option("0.0.0.0", "--host"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Launch the web dashboard."""
    import uvicorn

    from micromech.core.persistence import PersistentQueue
    from micromech.tools.registry import ToolRegistry
    from micromech.web.app import create_web_app

    cfg = _load_config(config_path)
    queue = PersistentQueue(cfg.persistence.db_path)
    reg = ToolRegistry()
    reg.load_builtins()

    async def noop_on_request(req):
        pass  # Web UI is read-only for now

    def get_status():
        return {
            "status": "dashboard",
            "queue": queue.count_by_status(),
            "tools": reg.tool_ids,
            "delivered_total": 0,
        }

    def get_recent(limit):
        return queue.get_recent(limit)

    def get_tools():
        return [{"id": t.metadata.id, "version": t.metadata.version} for t in reg.list_tools()]

    web_app = create_web_app(get_status, get_recent, get_tools, noop_on_request)
    typer.echo(f"Dashboard at http://{host}:{port}")
    uvicorn.run(web_app, host=host, port=port)


@app.command()
def version() -> None:
    """Show version."""
    from micromech import __version__

    typer.echo(f"micromech {__version__}")
