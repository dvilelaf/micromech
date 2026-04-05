"""Task scheduler module."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from apscheduler.events import EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from micromech.core.bridge import IwaBridge
from micromech.core.config import MicromechConfig
from micromech.management import MechLifecycle
from micromech.secrets import secrets
from micromech.tasks.checkpoint import checkpoint_task
from micromech.tasks.fund import fund_task
from micromech.tasks.health import health_task
from micromech.tasks.low_balance_alert import low_balance_alert_task
from micromech.tasks.notifications import NotificationService
from micromech.tasks.rewards import rewards_task
from micromech.tasks.update_check import (
    AUTO_UPDATE_POLL_MINUTES,
    auto_update_poll_task,
    update_check_task,
)
from micromech.tasks.watchdog import record_task_success


class TaskScheduler:
    """Manages periodic tasks for micromech."""

    def __init__(
        self,
        config: MicromechConfig,
        bridges: dict[str, IwaBridge],
        notification_service: NotificationService,
    ):
        """Initialize scheduler."""
        self.scheduler = AsyncIOScheduler(timezone=timezone.utc)
        self.config = config
        self.bridges = bridges
        self.notification_service = notification_service

        # Create MechLifecycle per enabled chain
        self.lifecycles: dict[str, MechLifecycle] = {}
        for chain_name in config.enabled_chains:
            try:
                self.lifecycles[chain_name] = MechLifecycle(config, chain_name)
            except Exception as e:
                logger.warning(f"Failed to create MechLifecycle for {chain_name}: {e}")

    def start(self) -> None:
        """Start the scheduler and add jobs."""
        tasks_config = self.config.tasks
        misfire_grace_time = 600
        startup_delay = 5

        # Health Task
        if secrets.health_url:
            self.scheduler.add_job(
                health_task,
                "interval",
                seconds=tasks_config.health_interval_seconds,
                id="health_task",
                replace_existing=True,
                misfire_grace_time=misfire_grace_time,
                max_instances=1,
                coalesce=True,
                next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=startup_delay),
            )
            startup_delay += 20

        # Checkpoint Task
        self.scheduler.add_job(
            checkpoint_task,
            "interval",
            minutes=tasks_config.checkpoint_interval_minutes,
            args=[self.lifecycles, self.notification_service, self.config],
            id="checkpoint_task",
            replace_existing=True,
            misfire_grace_time=misfire_grace_time,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=startup_delay),
        )
        startup_delay += 20

        # Rewards Task
        self.scheduler.add_job(
            rewards_task,
            "interval",
            minutes=tasks_config.claim_interval_minutes,
            args=[self.lifecycles, self.notification_service, self.config],
            id="rewards_task",
            replace_existing=True,
            misfire_grace_time=misfire_grace_time,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=startup_delay),
        )
        startup_delay += 20

        # Fund Task
        if tasks_config.fund_enabled:
            self.scheduler.add_job(
                fund_task,
                "interval",
                minutes=tasks_config.fund_interval_minutes,
                args=[self.bridges, self.notification_service, self.config],
                id="fund_task",
                replace_existing=True,
                misfire_grace_time=misfire_grace_time,
                max_instances=1,
                coalesce=True,
                next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=startup_delay),
            )
            startup_delay += 20

        # Low Balance Alert Task
        if tasks_config.low_balance_alert_enabled:
            self.scheduler.add_job(
                low_balance_alert_task,
                "interval",
                hours=tasks_config.low_balance_alert_interval_hours,
                args=[self.lifecycles, self.bridges, self.notification_service, self.config],
                id="low_balance_alert_task",
                replace_existing=True,
                misfire_grace_time=misfire_grace_time,
                max_instances=1,
                coalesce=True,
                next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=startup_delay),
            )
            startup_delay += 20

        # Update Check Task (daily at 8 AM local time)
        local_tz = ZoneInfo("Europe/Madrid")
        if tasks_config.update_check_enabled:
            self.scheduler.add_job(
                update_check_task,
                "cron",
                hour=8,
                timezone=local_tz,
                args=[self.notification_service, self.config],
                id="update_check_task",
                replace_existing=True,
            )

            # Auto-update poll
            if tasks_config.auto_update_enabled:
                self.scheduler.add_job(
                    auto_update_poll_task,
                    "interval",
                    minutes=AUTO_UPDATE_POLL_MINUTES,
                    args=[self.notification_service],
                    id="auto_update_poll_task",
                    replace_existing=True,
                    misfire_grace_time=misfire_grace_time,
                    max_instances=1,
                    coalesce=True,
                )

        # Record successful task completions for the watchdog
        self.scheduler.add_listener(lambda event: record_task_success(), EVENT_JOB_EXECUTED)

        self.scheduler.start()
        logger.info("TaskScheduler started.")

    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("TaskScheduler shut down.")
        else:
            logger.warning("TaskScheduler shutdown called but it was not running.")
