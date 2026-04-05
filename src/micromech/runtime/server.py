"""MechServer — main runtime orchestrator.

Coordinates: event listeners, request queue, tool executor, delivery managers, HTTP server.
Supports multiple chains simultaneously — one listener + delivery manager per enabled chain.
On startup, recovers any pending/interrupted requests from the database.
"""

import asyncio
import signal
from typing import Any, Optional

from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.constants import STATUS_PENDING
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
    ):
        self.config = config
        self.bridges = bridges or {}

        # Shared components
        self.queue = PersistentQueue(config.persistence.db_path)
        self.registry = ToolRegistry()
        self.metrics = MetricsCollector()
        self.executor = ToolExecutor(
            registry=self.registry,
            queue=self.queue,
            max_concurrent=config.runtime.max_concurrent,
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

    def _load_tools(self) -> None:
        """Load tools based on config."""
        self.registry.load_builtins()

        for tool_cfg in self.config.tools:
            if not tool_cfg.enabled:
                continue
            if not self.registry.has(tool_cfg.id):
                logger.warning("Tool '{}' not found in registry", tool_cfg.id)

        logger.info("Loaded tools: {}", self.registry.tool_ids)

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
        self, with_http: bool = True, register_signals: bool = True,
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
        if self.config.tasks.enabled:
            try:
                from micromech.tasks.scheduler import TaskScheduler
                from micromech.tasks.notifications import NotificationService

                notification = NotificationService()
                self._task_scheduler = TaskScheduler(
                    self.config, self.bridges, notification,
                )
                self._task_scheduler.start()
                logger.info("TaskScheduler started")

                # Start watchdog loop
                from micromech.tasks.watchdog import watchdog_loop
                self._tasks.append(asyncio.create_task(
                    watchdog_loop(notification)
                ))
            except Exception as e:
                logger.warning("TaskScheduler failed to start: {}", e)

        logger.info(
            "MechServer running (chains={}, tools={}, max_concurrent={})",
            len(chains),
            len(self.registry.tool_ids),
            self.config.runtime.max_concurrent,
        )

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
            return [
                {"id": t.metadata.id, "version": t.metadata.version}
                for t in self.registry.list_tools()
            ]

        web_app = create_web_app(
            get_status=self.get_status,
            get_recent=self.queue.get_recent,
            get_tools=get_tools,
            on_request=self._on_new_request,
            queue=self.queue,
            metrics=self.metrics,
        )
        app.mount("/dashboard", web_app)

        config = uvicorn.Config(
            app,
            host=self.config.runtime.host,
            port=self.config.runtime.port,
            log_level=self.config.runtime.log_level.lower(),
        )
        server = uvicorn.Server(config)
        logger.info(
            "HTTP server on {}:{} (dashboard at /dashboard)",
            self.config.runtime.host,
            self.config.runtime.port,
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
