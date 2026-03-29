"""MechServer — main runtime orchestrator.

Coordinates: event listener, request queue, tool executor, delivery manager, HTTP server.
On startup, recovers any pending/interrupted requests from the database.
"""

import asyncio
import signal
from typing import Optional

from loguru import logger

from micromech.core.config import MicromechConfig
from micromech.core.constants import STATUS_PENDING
from micromech.core.models import MechRequest
from micromech.core.persistence import PersistentQueue
from micromech.runtime.delivery import DeliveryManager
from micromech.runtime.executor import ToolExecutor
from micromech.runtime.listener import EventListener
from micromech.tools.registry import ToolRegistry


class MechServer:
    """Main micromech runtime server.

    Lifecycle:
    1. Initialize components (queue, registry, executor, listener, delivery)
    2. Recover interrupted requests from DB
    3. Run all loops concurrently (event listener, processor, delivery, HTTP)
    4. Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        config: MicromechConfig,
        bridge: Optional[object] = None,
    ):
        self.config = config
        self.bridge = bridge

        # Core components
        self.queue = PersistentQueue(config.persistence.db_path)
        self.registry = ToolRegistry()
        self.executor = ToolExecutor(
            registry=self.registry,
            queue=self.queue,
            max_concurrent=config.runtime.max_concurrent,
        )
        self.listener = EventListener(config, bridge)
        self.delivery = DeliveryManager(config, self.queue, bridge)

        # Internal queue with backpressure (maxsize = 2x concurrency)
        max_queued = config.runtime.max_concurrent * 2
        self._request_queue: asyncio.Queue[MechRequest] = asyncio.Queue(maxsize=max_queued)
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._executor_tasks: set[asyncio.Task] = set()

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
        """Recover interrupted requests from DB on startup."""
        # Requests stuck in 'executing' were interrupted by crash — re-queue
        executing = self.queue.get_executing()
        for record in executing:
            logger.info("Recovering interrupted request: {}", record.request.request_id)
            await self._request_queue.put(record.request)

        # Pending requests also need processing
        pending = self.queue.get_pending()
        for record in pending:
            logger.info("Recovering pending request: {}", record.request.request_id)
            await self._request_queue.put(record.request)

        total = len(executing) + len(pending)
        if total:
            logger.info("Recovered {} requests from previous session", total)

    async def _on_new_request(self, request: MechRequest) -> None:
        """Callback for new requests (from listener or HTTP).

        Deduplicates: if request_id already exists with non-pending status, skip.
        """
        existing = self.queue.get_by_id(request.request_id)
        if existing and existing.request.status != STATUS_PENDING:
            logger.debug("Skipping duplicate request {}", request.request_id)
            return

        self.queue.add_request(request)
        await self._request_queue.put(request)

    async def _processor_loop(self) -> None:
        """Process requests from the internal queue."""
        logger.info("Request processor started")
        while self._running:
            try:
                request = await asyncio.wait_for(self._request_queue.get(), timeout=1.0)
                # Track the task for graceful shutdown
                task = asyncio.create_task(self.executor.execute(request))
                self._executor_tasks.add(task)
                task.add_done_callback(self._executor_tasks.discard)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Processor error: {}", e)

    def get_status(self) -> dict:
        """Get current server status."""
        return {
            "status": "running" if self._running else "stopped",
            "queue": self.queue.count_by_status(),
            "tools": self.registry.tool_ids,
            "delivered_total": self.delivery.delivered_count,
        }

    async def run(self, with_http: bool = True) -> None:
        """Run the server with all components."""
        self._running = True
        self._load_tools()
        await self._recover()

        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal)

        logger.info("MechServer starting...")

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._processor_loop()),
            asyncio.create_task(self.delivery.run()),
        ]

        if self.bridge:
            self._tasks.append(asyncio.create_task(self.listener.run(self._on_new_request)))

        if with_http:
            self._tasks.append(asyncio.create_task(self._run_http()))

        logger.info(
            "MechServer running (tools={}, max_concurrent={})",
            len(self.registry.tool_ids),
            self.config.runtime.max_concurrent,
        )

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Server tasks cancelled")

    async def _run_http(self) -> None:
        """Run the HTTP server."""
        import uvicorn

        from micromech.runtime.http import create_app

        app = create_app(
            on_request=self._on_new_request,
            get_status=self.get_status,
        )

        config = uvicorn.Config(
            app,
            host=self.config.runtime.host,
            port=self.config.runtime.port,
            log_level=self.config.runtime.log_level.lower(),
        )
        server = uvicorn.Server(config)
        logger.info(
            "HTTP server on {}:{}",
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
        self.listener.stop()
        self.delivery.stop()
        # Cancel main loop tasks
        for task in self._tasks:
            task.cancel()
        # Cancel in-flight executor tasks
        for task in self._executor_tasks:
            task.cancel()
        logger.info("MechServer stopping ({} in-flight tasks cancelled)", len(self._executor_tasks))

    def shutdown(self) -> None:
        """Final cleanup."""
        self.queue.close()
        logger.info("MechServer shut down")
