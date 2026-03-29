"""MechServer — main runtime orchestrator.

Coordinates: event listener, request queue, tool executor, delivery manager, HTTP server.
On startup, recovers any pending/interrupted requests from the database.

All synchronous DB calls run on the single-threaded asyncio event loop.
Since Peewee SQLite calls are non-yielding (no await between them),
they are safe from concurrent access within the event loop. The
asyncio.Semaphore in the executor provides concurrency control for
tool execution, while DB writes are inherently serialized by the GIL
and single-threaded event loop.
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
    2. Start processor loop
    3. Recover interrupted requests from DB
    4. Run all loops concurrently (event listener, processor, delivery, HTTP)
    5. Graceful shutdown on SIGTERM/SIGINT
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
            "tools": self.registry.tool_ids,
            "delivered_total": self.delivery.delivered_count,
        }

    async def run(self, with_http: bool = True) -> None:
        """Run the server with all components."""
        self._running = True
        self._load_tools()

        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal)

        logger.info("MechServer starting...")

        # Start processor FIRST so recovery queue is consumed
        self._tasks = [
            asyncio.create_task(self._processor_loop()),
            asyncio.create_task(self.delivery.run()),
        ]

        # Recover after processor is running
        await self._recover()

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
