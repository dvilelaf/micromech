"""MechServer — main runtime orchestrator.

Coordinates: event listeners, request queue, tool executor, delivery managers, HTTP server.
Supports multiple chains simultaneously — one listener + delivery manager per enabled chain.
On startup, recovers any pending/interrupted requests from the database.
"""

import asyncio
import signal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from micromech.tasks.notifications import NotificationService

from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.constants import (
    DB_PATH,
    DEFAULT_HOST,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_PORT,
    STATUS_PENDING,
)
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager
from micromech.runtime.executor import ToolExecutor
from micromech.runtime.listener import EventListener
from micromech.runtime.metrics import MetricsCollector
from micromech.tools.registry import ToolRegistry


class MechServer:
    """Main micromech runtime server.

    Lifecycle:
    1. Initialize components (queue, registry, executor, per-chain listeners/deliveries)
    2. Start processor loop
    3. Recover interrupted requests from DB
    4. Run all loops concurrently (event listeners, processor, deliveries, HTTP)
    5. Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        config: MicromechConfig,
        bridges: Optional[dict[str, Any]] = None,
        host: str = DEFAULT_HOST,
    ):
        self.config = config
        self.bridges = bridges or {}
        self.host = host

        # Shared components
        self.queue = PersistentQueue(DB_PATH)
        self.registry = ToolRegistry()
        self.metrics = MetricsCollector()
        self.executor = ToolExecutor(
            registry=self.registry,
            queue=self.queue,
            max_concurrent=DEFAULT_MAX_CONCURRENT,
            metrics=self.metrics,
        )

        # Per-chain components
        self.listeners: dict[str, EventListener] = {}
        self.deliveries: dict[str, DeliveryManager] = {}

        for chain_name, chain_cfg in config.enabled_chains.items():
            bridge = self.bridges.get(chain_name)
            self.listeners[chain_name] = EventListener(config, chain_cfg, bridge)
            self.deliveries[chain_name] = DeliveryManager(
                config, chain_cfg, self.queue, bridge, metrics=self.metrics
            )

        # Unbounded queue — backpressure is handled by the executor semaphore
        self._request_queue: asyncio.Queue[MechRequest] = asyncio.Queue()
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._executor_tasks: set[asyncio.Task] = set()
        # Dedup set to prevent double execution of the same request
        self._queued_ids: set[str] = set()
        # Serializes reload_tools() calls — lazy-init on first async use
        # because MechServer() can be constructed outside an event loop.
        self._reload_lock: Optional[asyncio.Lock] = None

    def _load_tools(self) -> None:
        """Load tools (builtins + custom from data/tools/, respecting disabled list)."""
        from micromech.core.constants import CUSTOM_TOOLS_DIR

        disabled = set(self.config.disabled_tools) if self.config.disabled_tools else None
        self.registry.load_builtins(disabled=disabled)
        self.registry.load_custom(CUSTOM_TOOLS_DIR, disabled=disabled)
        logger.info("Loaded tools: {}", self.registry.tool_ids)

    async def reload_tools(self) -> list[str]:
        """Hot-reload tools without restarting the server.

        Re-reads ``disabled_tools`` from config on disk, rebuilds the registry
        from scratch (builtins + custom) **in a worker thread** (so the event
        loop stays responsive during filesystem + importlib work), then
        atomically swaps the registry contents via
        :meth:`ToolRegistry.swap_contents`.

        A lock serializes concurrent reload calls. The config is only
        mutated **after** the rebuild succeeds, so a failed reload leaves
        server state unchanged.

        In-flight executor lookups via ``registry.get()`` either see the
        old or the new map — never a half-populated state.
        """
        if self._reload_lock is None:
            self._reload_lock = asyncio.Lock()

        async with self._reload_lock:
            # Build the new registry off the event loop — YAML parsing,
            # importlib, rglob() are all blocking I/O.
            fresh_cfg, new_registry = await asyncio.to_thread(
                self._build_reloaded_registry,
            )
            # Only mutate shared state after a successful rebuild.
            self.config.disabled_tools = fresh_cfg.disabled_tools
            self.registry.swap_contents(new_registry)
            logger.info("Tools hot-reloaded: {}", self.registry.tool_ids)
            return self.registry.tool_ids

    def _build_reloaded_registry(self) -> tuple[MicromechConfig, ToolRegistry]:
        """Synchronous worker for :meth:`reload_tools` — runs in a thread.

        Returns the freshly-loaded config and a fully-populated registry.
        Any exception aborts the reload with no side effects on the server.
        """
        from micromech.core.constants import CUSTOM_TOOLS_DIR

        fresh = MicromechConfig.load()
        disabled = set(fresh.disabled_tools) if fresh.disabled_tools else None
        new_registry = ToolRegistry()
        new_registry.load_builtins(disabled=disabled)
        new_registry.load_custom(CUSTOM_TOOLS_DIR, disabled=disabled)
        return fresh, new_registry

    async def _prefetch_llm_model(self) -> None:
        """Download the default LLM model in the background on first startup.

        Runs in a thread so it doesn't block the event loop. Silently skipped
        if llama-cpp-python is not installed (non-Docker / dev environments).
        The model is cached in data/models/ — subsequent startups are instant.
        """
        def _download():
            try:
                from micromech.tools.local_llm.local_llm import _get_llm

                _get_llm()
                logger.info("LLM model ready")
            except ImportError:
                pass  # llm extra not installed — skip silently
            except Exception as e:
                logger.warning("LLM model prefetch failed: {}", e)

        from micromech.core.constants import DEFAULT_LLM_FILE

        model_path = Path("data") / "models" / DEFAULT_LLM_FILE
        if model_path.exists():
            return  # Already downloaded

        logger.info("Downloading default LLM model in background (this may take a few minutes)...")
        await asyncio.to_thread(_download)

    async def _recover(self) -> None:
        """Recover interrupted requests from DB on startup.

        Called AFTER the processor loop starts, so queue consumption is active.
        """
        # Requests stuck in 'executing' were interrupted by crash — re-queue
        executing = self.queue.get_executing()
        for record in executing:
            req_id = record.request.request_id
            if req_id not in self._queued_ids:
                logger.info("Recovering interrupted request: {}", req_id)
                self._queued_ids.add(req_id)
                await self._request_queue.put(record.request)

        # Pending requests also need processing
        pending = self.queue.get_pending()
        for record in pending:
            req_id = record.request.request_id
            if req_id not in self._queued_ids:
                logger.info("Recovering pending request: {}", req_id)
                self._queued_ids.add(req_id)
                await self._request_queue.put(record.request)

        total = len(executing) + len(pending)
        if total:
            logger.info("Recovered {} requests from previous session", total)

    async def _on_new_request(self, request: MechRequest) -> None:
        """Callback for new requests (from listener or HTTP).

        Deduplicates: skips if request is already queued or processed.
        """
        req_id = request.request_id

        # Fast dedup: already in-flight
        if req_id in self._queued_ids:
            logger.debug("Skipping duplicate request {}", req_id)
            return

        # DB dedup: already processed
        existing = self.queue.get_by_id(req_id)
        if existing and existing.request.status != STATUS_PENDING:
            logger.debug("Skipping already-processed request {}", req_id)
            return

        self.queue.add_request(request)
        self._queued_ids.add(req_id)
        self.metrics.record_request_received(
            req_id, request.tool, request.is_offchain, chain=request.chain
        )
        await self._request_queue.put(request)

    async def _processor_loop(self) -> None:
        """Process requests from the internal queue."""
        logger.info("Request processor started")
        while self._running:
            try:
                request = await asyncio.wait_for(self._request_queue.get(), timeout=1.0)
                # Track the task for graceful shutdown
                task = asyncio.create_task(self._execute_and_cleanup(request))
                self._executor_tasks.add(task)
                task.add_done_callback(self._executor_tasks.discard)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Processor error: {}", e)

    async def _execute_and_cleanup(self, request: MechRequest) -> None:
        """Execute a request and remove from dedup set when done."""
        try:
            await self.executor.execute(request)
        finally:
            self._queued_ids.discard(request.request_id)

    def get_status(self) -> dict:
        """Get current server status."""
        return {
            "status": "running" if self._running else "stopped",
            "queue": self.queue.count_by_status(),
            "queue_by_chain": self.queue.count_by_chain(),
            "chains": list(self.config.enabled_chains.keys()),
            "tools": self.registry.tool_ids,
            "delivered_total": sum(d.delivered_count for d in self.deliveries.values()),
            "metrics": self.metrics.get_live_snapshot(),
        }

    async def run(
        self,
        with_http: bool = True,
        register_signals: bool = True,
        notification: "Optional[NotificationService]" = None,
    ) -> None:
        """Run the server with all components."""
        self._running = True
        self._load_tools()

        # Register signal handlers (skip when embedded in another process)
        if register_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._handle_signal)

        chains = list(self.config.enabled_chains.keys())
        logger.info("MechServer starting on chains: {}", chains)

        # Start processor FIRST so recovery queue is consumed
        self._tasks = [
            asyncio.create_task(self._processor_loop()),
            # Prefetch default LLM model in background so it's ready before first use.
            # Added to self._tasks so stop() cancels it cleanly on shutdown.
            asyncio.create_task(self._prefetch_llm_model()),
        ]

        # Start per-chain delivery managers
        for chain_name, delivery in self.deliveries.items():
            self._tasks.append(asyncio.create_task(delivery.run()))

        # Recover after processor is running
        await self._recover()

        # Start per-chain listeners (only where bridge + mech_address are available)
        for chain_name, listener in self.listeners.items():
            bridge = self.bridges.get(chain_name)
            cc = self.config.enabled_chains.get(chain_name)
            if bridge and cc and cc.mech_address:
                self._tasks.append(asyncio.create_task(listener.run(self._on_new_request)))
                logger.info("Listener started for chain: {}", chain_name)
            elif bridge and (not cc or not cc.mech_address):
                logger.warning("Listener skipped for {} — no mech_address configured", chain_name)

        if with_http:
            self._tasks.append(asyncio.create_task(self._run_http()))

        # Start task scheduler (checkpoint, rewards, fund, alerts, etc.)
        try:
            from micromech.tasks.notifications import NotificationService
            from micromech.tasks.scheduler import TaskScheduler

            # Use the injected notification service (wired with the Telegram bot
            # from cli.py, same pattern as triton). Fall back to log-only if none.
            if notification is None:
                notification = NotificationService()

            self._task_scheduler = TaskScheduler(
                self.config,
                self.bridges,
                notification,
                queue=self.queue,
            )
            self._task_scheduler.start()
            logger.info("TaskScheduler started")

            from micromech.tasks.watchdog import watchdog_loop

            self._tasks.append(asyncio.create_task(watchdog_loop(notification)))
        except Exception as e:
            logger.warning("TaskScheduler failed to start: {}", e)

        logger.info(
            "MechServer running (chains={}, tools={}, max_concurrent={})",
            len(chains),
            len(self.registry.tool_ids),
            DEFAULT_MAX_CONCURRENT,
        )

        # Fire-and-forget startup notification — must NOT be in self._tasks because
        # a notification failure would propagate through asyncio.gather and crash
        # the server. Exceptions are caught inside notification.send() already.
        try:
            from micromech import __version__
            from micromech.core.bridge import get_service_info

            chain_lines = []
            for chain_name in chains:
                svc = get_service_info(chain_name)
                chain_cfg = self.config.chains.get(chain_name)
                mech = (chain_cfg.mech_address or "?") if chain_cfg else "?"
                svc_id = svc.get("service_id", "?")
                chain_lines.append(f"  {chain_name}: svc={svc_id} mech={str(mech)[:12]}…")
            details = "\n".join(chain_lines) if chain_lines else "  (no chains)"
            asyncio.ensure_future(
                notification.send(
                    "Micromech started",
                    f"v{__version__} | tools: {len(self.registry.tool_ids)}\n{details}",
                )
            )
        except Exception as e:
            logger.warning("Startup notification failed: {}", e)

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Server tasks cancelled")

    async def _run_http(self) -> None:
        """Run the HTTP server with both runtime API and web dashboard."""
        import uvicorn

        from micromech.runtime.http import create_app

        app = create_app(
            on_request=self._on_new_request,
            get_status=self.get_status,
            get_result=self.queue.get_by_id,
        )

        # Mount the web dashboard on the same server
        from micromech.web.app import create_web_app

        def get_tools():
            return self.registry.list_packages()

        from micromech.core.constants import CUSTOM_TOOLS_DIR
        from micromech.metadata_manager import _BUILTIN_TOOLS_DIR, MetadataManager

        mm = MetadataManager(self.config, tools_dirs=[_BUILTIN_TOOLS_DIR, CUSTOM_TOOLS_DIR])

        web_app = create_web_app(
            get_status=self.get_status,
            get_recent=self.queue.get_recent,
            get_tools=get_tools,
            on_request=self._on_new_request,
            queue=self.queue,
            metrics=self.metrics,
            metadata_manager=mm,
            reload_tools=self.reload_tools,
        )
        app.mount("/dashboard", web_app)

        config = uvicorn.Config(
            app,
            host=self.host,
            port=DEFAULT_PORT,
            log_level="info",
        )
        server = uvicorn.Server(config)
        logger.info(
            "HTTP server on {}:{} (dashboard at /dashboard)",
            self.host,
            DEFAULT_PORT,
        )
        await server.serve()

    def _handle_signal(self) -> None:
        logger.info("Shutdown signal received")
        self.stop()

    def stop(self) -> None:
        """Gracefully stop the server."""
        self._running = False
        if hasattr(self, "_task_scheduler"):
            self._task_scheduler.shutdown()
        for listener in self.listeners.values():
            listener.stop()
        for delivery in self.deliveries.values():
            delivery.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._executor_tasks:
            task.cancel()
        logger.info(
            "MechServer stopping ({} in-flight tasks cancelled)",
            len(self._executor_tasks),
        )

    def shutdown(self) -> None:
        """Final cleanup."""
        self.queue.close()
        logger.info("MechServer shut down")
