"""micromech CLI — management and runtime commands."""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import typer
from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.constants import (
    CHAIN_DEFAULTS,
    DB_PATH,
    MIN_NATIVE_WEI,
    MIN_OLAS_WHOLE,
)


def _configure_logging() -> None:
    """Configure loguru with green timestamps, matching triton's format."""
    if hasattr(_configure_logging, "configured"):
        return
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss,SSS}</green> - <level>{level: <8}</level> - {message}"
    )
    logger.add(sys.stderr, format=log_format, level="INFO", colorize=True)

    class _InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno  # type: ignore[assignment]
            frame, depth = logging.currentframe(), 2
            while frame and frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back  # type: ignore[assignment]
                depth += 1
            logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    _configure_logging.configured = True  # type: ignore[attr-defined]


_configure_logging()

app = typer.Typer(
    name="micromech",
    help="Lightweight OLAS mech runtime.",
    no_args_is_help=True,
)

# Available chains for setup wizard
CHAIN_NAMES = list(CHAIN_DEFAULTS.keys())
CHAIN_DISPLAY = {
    "gnosis": "Gnosis (recommended — cheapest gas)",
    "base": "Base",
    "ethereum": "Ethereum (expensive gas)",
    "polygon": "Polygon",
    "optimism": "Optimism",
    "arbitrum": "Arbitrum",
    "celo": "Celo",
}
NATIVE_SYMBOL = {
    "gnosis": "xDAI",
    "base": "ETH",
    "ethereum": "ETH",
    "polygon": "POL",
    "optimism": "ETH",
    "arbitrum": "ETH",
    "celo": "CELO",
}


def _load_config(config_path: Optional[Path] = None) -> MicromechConfig:
    """Load config via iwa plugin system (or fallback file)."""
    from micromech.core.config import register_plugin

    register_plugin()
    return MicromechConfig.load(config_path)


def _print_step(step: int, total: int, msg: str) -> None:
    """Print a wizard step header."""
    typer.echo(f"\n[{step}/{total}] {msg}")
    typer.echo("-" * 40)


def _check_balances(chain_name: str) -> tuple[float, float]:
    """Check native token and OLAS balances. Delegates to core.bridge (cached).

    Returns (0.0, 0.0) if the underlying fetch returned None (unknown).
    CLI callers (init funding loop) treat unknown as "keep waiting".
    """
    from micromech.core.bridge import check_balances

    result = check_balances(chain_name)
    return result if result is not None else (0.0, 0.0)


@app.command()
def init(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    chain: Optional[str] = typer.Option(None, "--chain", help="Chain to deploy on"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Non-interactive mode"),
    skip_funding: bool = typer.Option(False, "--skip-funding-check"),
) -> None:
    """Setup wizard — wallet, chain, funding, deploy. Get running in 3 minutes."""
    from micromech.core.config import register_plugin

    register_plugin()
    total_steps = 5

    typer.echo("\nmicromech setup wizard")
    typer.echo("=" * 40)
    typer.echo("Get your mech earning OLAS rewards in 3 minutes.\n")

    # --- Step 1: Wallet ---
    _print_step(1, total_steps, "Wallet")
    wallet_address: Optional[str] = None
    try:
        from iwa.core.wallet import Wallet

        wallet = Wallet()
        wallet_address = wallet.master_account.address
        typer.echo(f"  Wallet found: {wallet_address}")

        # Show mnemonic if just created
        try:
            mnemonic = wallet.key_storage.get_pending_mnemonic()
            if mnemonic:
                typer.echo("\n  NEW WALLET — write down your recovery phrase:")
                typer.echo(f"\n  {mnemonic}\n")
                if not yes:
                    typer.confirm("  Have you saved your recovery phrase?", abort=True)
        except Exception:
            pass

    except ImportError:
        typer.echo("  iwa not installed. Install with: pip install micromech[chain]")
        raise typer.Exit(1)
    except Exception as e:
        err = str(e).lower()
        if "password" in err or "none" in err or "secret" in err:
            typer.echo("  Wallet found but locked. Set wallet_password env var:")
            typer.echo("    export wallet_password=YOUR_PASSWORD")
            typer.echo("  Then re-run: micromech init")
        else:
            typer.echo(f"  Wallet error: {e}")
            typer.echo("  Re-run: micromech init")
        raise typer.Exit(1)

    # --- Step 2: Chain Selection ---
    _print_step(2, total_steps, "Chain Selection")
    if chain and chain in CHAIN_NAMES:
        selected_chain = chain
        typer.echo(f"  Using: {selected_chain}")
    elif yes:
        selected_chain = "gnosis"
        typer.echo(f"  Using default: {selected_chain}")
    else:
        for i, name in enumerate(CHAIN_NAMES, 1):
            label = CHAIN_DISPLAY.get(name, name)
            typer.echo(f"    {i}. {label}")
        while True:
            choice = typer.prompt("  Select chain", default="1")
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(CHAIN_NAMES):
                    selected_chain = CHAIN_NAMES[idx]
                    break
            except ValueError:
                if choice in CHAIN_NAMES:
                    selected_chain = choice
                    break
            typer.echo("  Invalid choice.")
    typer.echo(f"  Selected: {selected_chain}")

    # --- Step 3: Funding Check ---
    _print_step(3, total_steps, "Fund Your Wallet")
    native_sym = NATIVE_SYMBOL.get(selected_chain, "ETH")
    min_native = MIN_NATIVE_WEI.get(selected_chain, 100_000_000_000_000_000) / 1e18

    typer.echo(f"  Your mech needs funds on {selected_chain}:")
    typer.echo(f"    - ~{min_native} {native_sym} for gas")
    typer.echo(f"    - {MIN_OLAS_WHOLE:,} OLAS for staking bond")
    typer.echo(f"\n  Send funds to: {wallet_address}")

    if not skip_funding:
        typer.echo("\n  Checking balances...")
        funded = False
        attempts = 0
        max_wait = 600  # 10 minutes
        while not funded and attempts * 15 < max_wait:
            native_bal, olas_bal = _check_balances(selected_chain)
            native_ok = native_bal >= min_native
            olas_ok = olas_bal >= MIN_OLAS_WHOLE

            native_status = "OK" if native_ok else "waiting"
            olas_status = "OK" if olas_ok else "waiting"

            sys.stdout.write(
                f"\r    {native_sym}: {native_bal:.4f} [{native_status}]  "
                f"OLAS: {olas_bal:,.0f} [{olas_status}]    "
            )
            sys.stdout.flush()

            if native_ok and olas_ok:
                funded = True
                typer.echo("\n  Wallet funded!")
                break

            if attempts == 0 and not yes:
                typer.echo(
                    "\n\n  Waiting for funds... (Ctrl+C to cancel, --skip-funding-check to skip)"
                )

            attempts += 1
            time.sleep(15)

        if not funded:
            typer.echo("\n  Timed out waiting for funds.")
            typer.echo(f"  Fund {wallet_address} and re-run: micromech init --skip-funding-check")
            raise typer.Exit(1)
    else:
        typer.echo("  Skipping funding check (--skip-funding-check)")

    # --- Step 4: Tools ---
    _print_step(4, total_steps, "Tools")
    typer.echo("  Default tools enabled:")
    typer.echo("    [x] echo — Test tool (default prediction)")
    typer.echo("    [x] llm — Local LLM (Qwen 0.5B, CPU)")
    typer.echo("  (Edit data/micromech.yaml to customize later)")

    # --- Step 5: Deploy ---
    _print_step(5, total_steps, "Deploy to OLAS Protocol")

    # Build or load config
    chain_defaults = CHAIN_DEFAULTS.get(selected_chain, {})
    from micromech.core.config import ChainConfig

    chain_cfg = ChainConfig(
        chain=selected_chain,
        marketplace_address=chain_defaults.get("marketplace", ""),
        factory_address=chain_defaults.get("factory", ""),
        staking_address=chain_defaults.get("staking", ""),
    )

    # Check existing state
    config = MicromechConfig.load()
    if selected_chain in config.chains:
        chain_cfg = config.chains[selected_chain]
    else:
        config.chains[selected_chain] = chain_cfg

    state = chain_cfg.detect_setup_state()
    if chain_cfg.setup_complete:
        typer.echo("  Service already fully deployed!")
        typer.echo(f"    mech: {chain_cfg.mech_address}")
        typer.echo("\n  Start with: micromech run")
        config.save()
        return

    if state != "needs_create":
        typer.echo(f"  Resuming from state: {state}")

    def _on_progress(step: int | str, total: int, msg: str, success: bool = True) -> None:
        icon = "+" if success else "!"
        typer.echo(f"  [{icon}] [{step}/{total}] {msg}")

    try:
        from micromech.management import MechLifecycle

        lc = MechLifecycle(config, chain_name=selected_chain)
        result = lc.full_deploy(on_progress=_on_progress)

        # Update config with deployment results
        chain_cfg.apply_deploy_result(result)
        config.chains[selected_chain] = chain_cfg
        config.save()

        typer.echo("\n  Config saved.")
        typer.echo("\n  Your mech is ready! Start with:\n")
        typer.echo("    micromech run")
        typer.echo("\n  Dashboard: http://localhost:8090")

    except RuntimeError as e:
        typer.echo(f"\n  Deployment failed: {e}")
        typer.echo("  Fix the issue and re-run: micromech init (it will resume)")
        # Save partial state
        config.chains[selected_chain] = chain_cfg
        config.save()
        raise typer.Exit(1)
    except ImportError:
        typer.echo("  iwa not installed. Install with: pip install micromech[chain]")
        raise typer.Exit(1)


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
    host: str = typer.Option("127.0.0.1", "--host", help="HTTP server bind address"),
) -> None:
    """Run the mech server (listener + executor + delivery + HTTP)."""
    cfg = _load_config(config_path)
    logger.info("Starting micromech server...")

    from micromech.core.bridge import create_bridges
    from micromech.runtime.server import MechServer

    bridges = create_bridges(cfg)

    server = MechServer(cfg, bridges=bridges, host=host)

    async def _run_all():
        """Run server + optionally Telegram bot."""
        from micromech.tasks.notifications import NotificationService

        bot_app = None
        notification = NotificationService()

        # Start Telegram bot first — same pattern as triton.
        # Bot is initialized here so notification_service shares the same
        # bot instance used for polling (not a separate standalone Bot).
        try:
            from micromech.secrets import secrets

            if secrets.telegram_token and secrets.telegram_chat_id:
                from micromech.bot.app import create_application

                bot_app = create_application(
                    config=cfg,
                    runtime_manager=None,
                    queue=server.queue,
                    metrics=server.metrics,
                    bridges=bridges,
                )
                await bot_app.initialize()
                await bot_app.start()

                # Register bot command menu (blue button) — same pattern as triton
                from telegram import BotCommand

                await bot_app.bot.set_my_commands(
                    [
                        BotCommand("status", "Mech status per chain"),
                        BotCommand("wallet", "Wallet addresses and balances"),
                        BotCommand("claim", "Claim staking rewards"),
                        BotCommand("checkpoint", "Call staking checkpoint"),
                        BotCommand("manage", "Stake/unstake per chain"),
                        BotCommand("contracts", "Staking contract info"),
                        BotCommand("last_rewards", "Accrued rewards this epoch"),
                        BotCommand("queue", "Request queue status"),
                        BotCommand("info", "Version and runtime info"),
                        BotCommand("logs", "Download last 24h logs"),
                        BotCommand("settings", "Toggle features and edit values"),
                        BotCommand("update", "Check for updates"),
                        BotCommand("restart", "Restart runtime"),
                        BotCommand("help", "Show all commands"),
                    ]
                )
                logger.info("Bot command menu registered")

                await bot_app.updater.start_polling(drop_pending_updates=True)
                # Wire the initialized bot into the notification service
                notification = NotificationService(
                    bot=bot_app.bot,
                    chat_id=secrets.telegram_chat_id,
                )
                logger.info("Telegram bot started")
        except ImportError:
            logger.debug("Telegram bot not available (python-telegram-bot not installed)")
        except Exception as e:
            logger.warning("Telegram bot failed to start: {}", e)

        try:
            await server.run(with_http=not no_http, notification=notification)
        finally:
            if bot_app:
                await bot_app.updater.stop()
                await bot_app.stop()
                await bot_app.shutdown()

    try:
        asyncio.run(_run_all())
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


@app.command()
def status(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show queue status from the database."""
    _load_config(config_path)
    from micromech.core.persistence import PersistentQueue

    queue = PersistentQueue(DB_PATH)
    counts = queue.count_by_status()
    by_chain = queue.count_by_chain()
    recent = queue.get_recent(limit=5)
    queue.close()

    typer.echo("Queue Status:")
    for status_name, count in counts.items():
        typer.echo(f"  {status_name}: {count}")

    if by_chain:
        typer.echo("\nBy Chain:")
        for chain_name, count in by_chain.items():
            typer.echo(f"  {chain_name}: {count}")

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
    _load_config(config_path)
    from micromech.core.persistence import PersistentQueue

    queue = PersistentQueue(DB_PATH)
    deleted = queue.cleanup(days=days)
    queue.close()
    typer.echo(f"Cleaned up {deleted} records older than {days} days")


@app.command()
def web(
    port: int = typer.Option(8090, "--port", "-p"),
    host: str = typer.Option("127.0.0.1", "--host"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
    no_runtime: bool = typer.Option(False, "--no-runtime", help="Dashboard only, no mech runtime"),
) -> None:
    """Launch micromech with web dashboard + optional auto-start runtime.

    If a service is deployed, the mech runtime starts automatically.
    Use --no-runtime for dashboard-only mode (monitoring/setup).
    """
    import uvicorn

    from micromech.core.persistence import PersistentQueue
    from micromech.runtime.manager import RuntimeManager
    from micromech.tools.registry import ToolRegistry
    from micromech.web.app import create_web_app

    cfg = _load_config(config_path)
    queue = PersistentQueue(DB_PATH)
    from micromech.core.constants import CUSTOM_TOOLS_DIR

    reg = ToolRegistry()
    disabled = set(cfg.disabled_tools) if cfg.disabled_tools else None
    reg.load_builtins(disabled=disabled)
    reg.load_custom(CUSTOM_TOOLS_DIR, disabled=disabled)
    mgr = RuntimeManager(cfg)

    async def noop_on_request(req):
        pass

    def get_status():
        # Use runtime status if running, otherwise basic dashboard status
        if mgr.state == "running":
            return mgr.get_status()
        return {
            "status": mgr.state,
            "queue": queue.count_by_status(),
            "queue_by_chain": queue.count_by_chain(),
            "chains": list(cfg.enabled_chains.keys()),
            "tools": reg.tool_ids,
            "delivered_total": 0,
        }

    def get_recent(limit, chain=None):
        return queue.get_recent(limit, chain=chain)

    def get_tools():
        return reg.list_packages()

    from micromech.metadata_manager import _BUILTIN_TOOLS_DIR, MetadataManager

    mm = MetadataManager(cfg, tools_dirs=[_BUILTIN_TOOLS_DIR, CUSTOM_TOOLS_DIR])

    async def _reload_tools() -> list[str]:
        """Hot-reload delegator for the ``web`` command path.

        The RuntimeManager lazily creates the MechServer inside start(),
        so at web-app construction time ``mgr._server`` may be None. When
        the runtime is not running, there is no in-memory registry to
        swap — the disabled_tools change was already persisted to
        config.yaml by the UI and will take effect on the next start().
        """
        server = getattr(mgr, "_server", None)
        if server is None:
            # Refresh the standalone registry used by the dashboard when
            # the runtime is stopped, so the Tools tab reflects any new
            # packages dropped into data/tools/.
            from micromech.tools.registry import ToolRegistry

            fresh_cfg = MicromechConfig.load()
            disabled = set(fresh_cfg.disabled_tools) if fresh_cfg.disabled_tools else None
            new_reg = ToolRegistry()
            new_reg.load_builtins(disabled=disabled)
            new_reg.load_custom(CUSTOM_TOOLS_DIR, disabled=disabled)
            reg.swap_contents(new_reg)
            return reg.tool_ids
        return await server.reload_tools()

    web_app = create_web_app(
        get_status,
        get_recent,
        get_tools,
        noop_on_request,
        queue=queue,
        runtime_manager=mgr,
        metadata_manager=mm,
        reload_tools=_reload_tools,
    )

    # Auto-start runtime if service is deployed and not --no-runtime
    has_deployed = any(c.setup_complete for c in cfg.chains.values())
    if has_deployed and not no_runtime:
        typer.echo("Service deployed — runtime will auto-start")

        # TODO: migrate from deprecated on_event("startup") to lifespan
        # context manager — requires restructuring the web command.
        @web_app.on_event("startup")
        async def _auto_start_runtime():
            ok = await mgr.start()
            if ok:
                logger.info("Runtime auto-started")
            else:
                logger.warning("Runtime auto-start failed: {}", mgr.error)

    typer.echo(f"\n  Dashboard: http://{host}:{port}\n")
    uvicorn.run(web_app, host=host, port=port)


@app.command()
def fingerprint() -> None:
    """Compute and write fingerprints for all built-in tool packages."""
    from micromech.ipfs.metadata import fingerprint_all_builtins

    results = fingerprint_all_builtins()
    for tool_name, fps in results.items():
        typer.echo(f"\n{tool_name}:")
        for path, cid in fps.items():
            typer.echo(f"  {path}: {cid}")
    typer.echo(f"\nFingerprinted {len(results)} tool(s)")


@app.command(name="metadata-build")
def metadata_build() -> None:
    """Build metadata.json from registered tools."""
    import json

    from micromech.ipfs.metadata import (
        build_metadata,
        build_tools_to_package_hash,
        scan_tool_packages,
    )

    tools_dir = Path(__file__).parent / "tools"
    tools = scan_tool_packages(tools_dir)
    metadata = build_metadata(tools)
    tools_hash = build_tools_to_package_hash(tools)

    typer.echo(json.dumps(metadata, indent=2))
    typer.echo(f"\nTOOLS_TO_PACKAGE_HASH:\n{json.dumps(tools_hash, indent=2)}")


@app.command(name="metadata-push")
def metadata_push(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Build metadata and push to IPFS."""

    from micromech.ipfs.metadata import (
        build_metadata,
        compute_onchain_hash,
        scan_tool_packages,
    )

    tools_dir = Path(__file__).parent / "tools"
    tools = scan_tool_packages(tools_dir)
    metadata = build_metadata(tools)

    onchain_hash = compute_onchain_hash(metadata)
    typer.echo(f"On-chain hash: {onchain_hash}")

    try:
        from micromech.ipfs.client import push_json_to_ipfs

        cid, cid_hex = asyncio.run(push_json_to_ipfs(metadata))
        typer.echo(f"IPFS CID: {cid}")
        typer.echo(f"CID hex: {cid_hex}")
    except Exception as e:
        typer.echo(f"IPFS push failed: {e}")
        typer.echo("Use the on-chain hash above to update manually.")


@app.command(name="metadata-publish")
def metadata_publish(
    chain: Optional[str] = typer.Option(None, "--chain", help="Chain to update (default: all)"),
    skip_onchain: bool = typer.Option(False, "--skip-onchain", help="Push IPFS only"),
) -> None:
    """Publish tool metadata: scan → IPFS → on-chain (all-in-one)."""
    from micromech.core.config import MicromechConfig, register_plugin
    from micromech.core.constants import CUSTOM_TOOLS_DIR
    from micromech.metadata_manager import _BUILTIN_TOOLS_DIR, MetadataManager

    register_plugin()
    config = MicromechConfig.load()
    mm = MetadataManager(config, tools_dirs=[_BUILTIN_TOOLS_DIR, CUSTOM_TOOLS_DIR])

    def on_progress(step: str, msg: str) -> None:
        typer.echo(f"  [{step}] {msg}")

    typer.echo("Publishing tool metadata...")
    result = asyncio.run(
        mm.publish(
            update_onchain=not skip_onchain,
            on_progress=on_progress,
        )
    )

    if result.success:
        typer.echo(f"\nIPFS CID: {result.ipfs_cid}")
        typer.echo(f"On-chain hash: {result.onchain_hash}")
        for ch, tx in result.chain_txs.items():
            typer.echo(f"  {ch}: tx {tx}")
    else:
        typer.echo(f"\nFailed: {result.error}")
        raise typer.Exit(1)


@app.command(name="create-service")
def create_service(
    agent_id: int = typer.Option(40, help="Agent ID for the service"),
    bond: int = typer.Option(
        5000, help="Bond amount in OLAS (= minStakingDeposit for Supply Alpha)"
    ),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Create a new OLAS service on-chain."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    service_id = lc.create_service(agent_id=agent_id, bond_olas=bond)
    if service_id:
        typer.echo(f"Service created: {service_id}")
    else:
        typer.echo("Failed to create service")
        raise typer.Exit(1)


@app.command(name="deploy-mech")
def deploy_mech(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Deploy mech: activate → register → deploy Safe → create mech on marketplace."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)

    typer.echo("Activating registration...")
    if not lc.activate(service_key):
        typer.echo("Activation failed")
        raise typer.Exit(1)

    typer.echo("Registering agent...")
    if not lc.register_agent(service_key):
        typer.echo("Registration failed")
        raise typer.Exit(1)

    typer.echo("Deploying Safe...")
    multisig = lc.deploy(service_key)
    if not multisig:
        typer.echo("Deploy failed")
        raise typer.Exit(1)
    typer.echo(f"Safe deployed: {multisig}")

    typer.echo("Creating mech on marketplace...")
    mech_addr = lc.create_mech(service_key)
    if mech_addr:
        typer.echo(f"Mech created: {mech_addr}")
    else:
        typer.echo("Mech creation failed")
        raise typer.Exit(1)


@app.command(name="stake")
def stake_cmd(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    contract: Optional[str] = typer.Option(None, "--contract", help="Staking contract address"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Stake the service in a supply staking contract."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    if lc.stake(service_key, staking_contract=contract):
        typer.echo("Staked successfully")
    else:
        typer.echo("Staking failed")
        raise typer.Exit(1)


@app.command(name="unstake")
def unstake_cmd(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    contract: Optional[str] = typer.Option(None, "--contract"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Unstake the service."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    if lc.unstake(service_key, staking_contract=contract):
        typer.echo("Unstaked successfully")
    else:
        typer.echo("Unstaking failed")
        raise typer.Exit(1)


@app.command(name="claim")
def claim_cmd(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Claim staking rewards."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    if lc.claim_rewards(service_key):
        typer.echo("Rewards claimed")
    else:
        typer.echo("Claim failed")
        raise typer.Exit(1)


@app.command(name="mech-status")
def mech_status_cmd(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show mech service and staking status."""
    import json

    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    status = lc.get_status(service_key)
    if status:
        typer.echo(json.dumps(status, indent=2))
    else:
        typer.echo("Failed to get status")


@app.command(name="metadata-update")
def metadata_update(
    service_key: str = typer.Argument(help="Service key from iwa config"),
    metadata_hash: str = typer.Argument(help="0x-prefixed hash from metadata-push"),
    chain: str = typer.Option("gnosis", "--chain", help="Target chain"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Update mech metadata hash on-chain."""
    from micromech.management import MechLifecycle

    cfg = _load_config(config_path)
    lc = MechLifecycle(cfg, chain_name=chain)
    tx = lc.update_metadata_onchain(service_key, metadata_hash)
    if tx:
        typer.echo(f"Metadata updated: {tx}")
    else:
        typer.echo("Update failed")
        raise typer.Exit(1)


@app.command(name="add-tool")
def add_tool(
    name: str = typer.Argument(help="Name for the new tool (e.g. 'my_tool')"),
) -> None:
    """Scaffold a new custom tool package under tools/custom/<name>/."""
    import re

    if not re.match(r"^[a-z][a-z0-9_]*$", name):
        typer.echo(f"Invalid tool name '{name}'. Use lowercase letters, digits, and underscores.")
        raise typer.Exit(1)

    tools_dir = Path(__file__).parent.parent.parent / "tools" / "custom"
    tool_dir = tools_dir / name

    if tool_dir.exists():
        typer.echo(f"Tool directory already exists: {tool_dir}")
        raise typer.Exit(1)

    tool_dir.mkdir(parents=True, exist_ok=True)

    # __init__.py
    (tool_dir / "__init__.py").write_text(f'"""{name} tool package."""\n')

    # component.yaml
    component_yaml = (
        f"name: {name}\n"
        f"author: micromech\n"
        f"version: 0.1.0\n"
        f"type: custom\n"
        f"description: Custom tool — {name}\n"
        f"license: Apache-2.0\n"
        f"aea_version: '>=1.0.0, <2.0.0'\n"
        f"entry_point: {name}.py\n"
        f"callable: run\n"
        f"dependencies: {{}}\n"
        f"fingerprint: {{}}\n"
    )
    (tool_dir / "component.yaml").write_text(component_yaml)

    # <name>.py with ALLOWED_TOOLS + run() template
    tool_py = (
        f'"""{name} — custom micromech tool.\n'
        f"\n"
        f"Valory-compatible: ALLOWED_TOOLS + run(**kwargs) -> MechResponse.\n"
        f'"""\n'
        f"\n"
        f"import json\n"
        f"from typing import Any, Optional\n"
        f"\n"
        f'ALLOWED_TOOLS = ["{name}"]\n'
        f"\n"
        f"\n"
        f"def run(\n"
        f"    **kwargs: Any,\n"
        f") -> tuple[Optional[str], Optional[str], Optional[dict[str, Any]], Any]:\n"
        f'    """Valory-compatible entry point."""\n'
        f'    prompt = kwargs.get("prompt", "")\n'
        f'    counter_callback = kwargs.get("counter_callback")\n'
        f"\n"
        f"    # TODO: implement your tool logic here\n"
        f"    result = json.dumps({{\n"
        f'        "result": "not implemented",\n'
        f'        "prompt": prompt,\n'
        f"    }})\n"
        f"\n"
        f"    return result, prompt, None, counter_callback\n"
    )
    (tool_dir / f"{name}.py").write_text(tool_py)

    typer.echo(f"Created tool package: {tool_dir}")
    typer.echo(f"  {name}/__init__.py")
    typer.echo(f"  {name}/component.yaml")
    typer.echo(f"  {name}/{name}.py")
    typer.echo(f"\nEdit {tool_dir / name}.py to implement your tool logic.")


@app.command()
def doctor(
    config_path: Optional[Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Diagnose common issues — wallet, RPCs, service state, tools."""
    from micromech.core.config import register_plugin

    register_plugin()
    issues = 0
    warnings = 0

    def ok(msg: str) -> None:
        typer.echo(f"  [OK] {msg}")

    def warn(msg: str) -> None:
        nonlocal warnings
        warnings += 1
        typer.echo(f"  [!!] {msg}")

    def fail(msg: str) -> None:
        nonlocal issues
        issues += 1
        typer.echo(f"  [FAIL] {msg}")

    typer.echo("\nmicromech doctor")
    typer.echo("=" * 40)

    # 1. Config
    typer.echo("\nConfig:")
    try:
        cfg = MicromechConfig.load()
        ok("Config loaded")
        ok(f"Chains configured: {list(cfg.chains.keys())}")
    except Exception as e:
        fail(f"Config load error: {e}")
        cfg = MicromechConfig()

    # 2. Wallet
    typer.echo("\nWallet:")
    try:
        from iwa.core.wallet import Wallet

        wallet = Wallet()
        ok(f"Address: {wallet.master_account.address}")

        # Verify key decryption and mnemonic integrity
        try:
            from eth_account import Account as EthAccount
            from iwa.core.models import StoredSafeAccount

            storage = wallet.key_storage
            success = 0
            for acct in storage.accounts.values():
                if isinstance(acct, StoredSafeAccount):
                    continue
                priv = acct.decrypt_private_key()
                derived = EthAccount.from_key(priv)
                if derived.address.lower() == acct.address.lower():
                    success += 1
                else:
                    fail(f"Address mismatch: stored={acct.address} derived={derived.address}")
            if success > 0:
                ok(f"Key decryption verified ({success} account(s))")

            if storage.encrypted_mnemonic:
                from iwa.core.mnemonic import EncryptedMnemonic
                from iwa.core.secrets import secrets as iwa_secrets

                enc = EncryptedMnemonic(**storage.encrypted_mnemonic)
                pwd = iwa_secrets.wallet_password.get_secret_value()
                phrase = enc.decrypt(pwd)
                words = len(phrase.split())
                if words in (12, 15, 18, 21, 24):
                    ok(f"Mnemonic decrypted ({words} words)")
                else:
                    warn(f"Mnemonic decrypted but unusual word count: {words}")
            else:
                warn("No mnemonic in wallet.json — backup not possible without it")
        except Exception as e:
            fail(f"Key integrity check failed: {e}")

    except ImportError:
        fail("iwa not installed (pip install micromech[chain])")
    except Exception as e:
        fail(f"Wallet error: {e}")

    # 3. Chain connectivity
    typer.echo("\nChain RPCs:")
    try:
        from iwa.core.chain import ChainInterfaces

        interfaces = ChainInterfaces()
        for chain_name in cfg.chains:
            ci = interfaces.get(chain_name)
            if ci:
                try:
                    block = ci.web3.eth.block_number
                    ok(f"{chain_name}: block #{block}")
                except Exception as e:
                    fail(f"{chain_name}: {e}")
            else:
                fail(f"{chain_name}: no chain interface")
    except ImportError:
        warn("Cannot check RPCs (iwa not installed)")

    # 4. Service state
    typer.echo("\nService State:")
    for chain_name, chain_cfg in cfg.chains.items():
        state = chain_cfg.detect_setup_state()
        if chain_cfg.setup_complete:
            ok(f"{chain_name}: complete (mech: {chain_cfg.mech_address})")
        else:
            warn(f"{chain_name}: {state}")

    # 5. Tools
    typer.echo("\nTools:")
    try:
        from micromech.tools.registry import ToolRegistry

        reg = ToolRegistry()
        reg.load_builtins()
        tool_list = reg.tool_ids
        if tool_list:
            ok(f"Available: {', '.join(tool_list)}")
        else:
            warn("No tools loaded")
    except Exception as e:
        fail(f"Tool registry error: {e}")

    # 6. Database
    typer.echo("\nDatabase:")
    if DB_PATH.exists():
        size_kb = DB_PATH.stat().st_size / 1024
        ok(f"DB exists: {DB_PATH} ({size_kb:.1f} KB)")
    else:
        warn("No database yet (will be created on first run)")

    # Summary
    typer.echo(f"\n{'=' * 40}")
    if issues == 0 and warnings == 0:
        typer.echo("All checks passed!")
    elif issues == 0:
        typer.echo(f"{warnings} warning(s), 0 failures")
    else:
        typer.echo(f"{issues} failure(s), {warnings} warning(s)")


@app.command()
def version() -> None:
    """Show version."""
    from micromech import __version__

    typer.echo(f"micromech {__version__}")
