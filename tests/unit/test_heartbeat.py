"""Tests for the heartbeat loop in micromech cli._run_all."""

import inspect
from pathlib import Path

CLI_PY = Path(__file__).parent.parent.parent / "src" / "micromech" / "cli.py"


class TestHeartbeatStructural:
    """Source-level checks — _heartbeat_loop is nested so we inspect the source."""

    def _src(self) -> str:
        return CLI_PY.read_text()

    def test_sleep_before_touch_in_loop(self):
        """await asyncio.sleep(30) must appear before hb.touch() inside the while loop."""
        src = self._src()
        # Find the while True: block inside _heartbeat_loop
        while_idx = src.find("while True:\n                await asyncio.sleep(30)")
        assert while_idx != -1, "sleep-first pattern not found in heartbeat while loop"

    def test_oserror_logs_warning(self):
        """OSError must be caught and logged, not silently swallowed."""
        src = self._src()
        assert "except OSError as e:" in src
        assert 'logger.warning("Heartbeat touch failed' in src

    def test_task_created_at_start_of_run_all(self):
        """create_task must appear before MechServer init in _run_all."""
        src = self._src()
        task_idx = src.find("asyncio.create_task(_heartbeat_loop())")
        server_idx = src.find("server.run(")
        assert task_idx != -1, "create_task(_heartbeat_loop()) not found"
        assert task_idx < server_idx, "heartbeat task must be created before server.run()"

    def test_task_cancelled_in_finally(self):
        """Heartbeat task must be cancelled in the finally block of _run_all."""
        src = self._src()
        assert "heartbeat_task.cancel()" in src, "heartbeat_task.cancel() not found"
        cancel_idx = src.find("heartbeat_task.cancel()")
        # Walk backward from cancel() to the nearest "finally:" — must be present
        preceding = src[:cancel_idx]
        assert "finally:" in preceding, "cancel() must appear after a finally: block"

    def test_compose_healthcheck_override_absent(self):
        """docker-compose.yml must NOT have a healthcheck: block that overrides the Dockerfile."""
        compose = Path(__file__).parent.parent.parent / "docker-compose.yml"
        src = compose.read_text()
        assert "healthcheck:" not in src, (
            "docker-compose.yml defines healthcheck: which overrides the Dockerfile HEALTHCHECK "
            "and makes the heartbeat loop dead code"
        )
