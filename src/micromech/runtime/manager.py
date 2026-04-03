"""RuntimeManager — controls MechServer lifecycle from within the web process.

Allows the mech runtime to be started, stopped, and restarted dynamically
without requiring a process restart. The web server (uvicorn) owns the event
loop; the RuntimeManager runs MechServer as an asyncio task within it.
"""

import asyncio
from typing import Any, Optional

from loguru import logger

from micromech.core.config import MicromechConfig


class RuntimeManager:
    """Manages MechServer start/stop/restart within a running event loop."""

    def __init__(self, config: MicromechConfig):
        self.config = config
        self._server: Optional[Any] = None
        self._task: Optional[asyncio.Task] = None
        self._state: str = "stopped"  # stopped | starting | running | stopping | error
        self._error: Optional[str] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> str:
        return self._state

    @property
    def error(self) -> Optional[str]:
        return self._error

    def _create_bridges(self) -> dict:
        """Create IwaBridge instances for all enabled chains."""
        from micromech.core.bridge import create_bridges
        return create_bridges(self.config)

    async def start(self) -> bool:
        """Start the MechServer runtime (without HTTP — web app already serves it)."""
        async with self._lock:
            if self._state == "running":
                return True

            self._state = "starting"
            self._error = None
            try:
                from micromech.runtime.server import MechServer

                bridges = self._create_bridges()
                self._server = MechServer(self.config, bridges=bridges)
                self._task = asyncio.create_task(self._run_and_monitor())

                # Wait briefly for startup to fail fast
                await asyncio.sleep(0.5)
                if self._state == "error":
                    return False

                self._state = "running"
                logger.info("Runtime started")
                return True
            except Exception as e:
                self._state = "error"
                self._error = str(e)
                logger.error("Runtime failed to start: {}", e)
                return False

    async def _run_and_monitor(self) -> None:
        """Run server and handle crashes."""
        try:
            await self._server.run(with_http=False, register_signals=False)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Runtime crashed: {}", e)
            self._state = "error"
            self._error = str(e)

    async def stop(self) -> bool:
        """Stop the runtime gracefully."""
        async with self._lock:
            if self._state not in ("running", "error", "starting"):
                return True

            self._state = "stopping"
            logger.info("Stopping runtime...")

            if self._server:
                self._server.stop()
            if self._task:
                self._task.cancel()
                try:
                    await asyncio.wait_for(self._task, timeout=10)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            if self._server:
                self._server.shutdown()

            self._server = None
            self._task = None
            self._state = "stopped"
            self._error = None
            logger.info("Runtime stopped")
            return True

    async def restart(self) -> bool:
        """Stop, reload config, and start again."""
        await self.stop()
        self.config = MicromechConfig.load()
        return await self.start()

    def get_status(self) -> dict:
        """Get runtime status for API responses."""
        result: dict[str, Any] = {"state": self._state}
        if self._error:
            result["error"] = self._error
        if self._server and self._state == "running":
            server_status = self._server.get_status()
            result.update(server_status)
        return result
