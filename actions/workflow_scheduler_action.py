"""Workflow Scheduler Action Module.

Schedule and manage workflow executions with cron and interval support.
"""

from __future__ import annotations

import asyncio
import croniter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from .automation_scheduler_action import Schedule, ScheduleType


@dataclass
class ScheduledWorkflow:
    """Scheduled workflow definition."""
    schedule_id: str
    workflow_fn: Callable
    schedule: Schedule
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    enabled: bool = True
    description: str | None = None
    last_run: datetime | None = None
    next_run: datetime | None = None
    run_count: int = 0
    error_count: int = 0


class WorkflowScheduler:
    """Schedule and manage workflow executions."""

    def __init__(self) -> None:
        self._scheduled: dict[str, ScheduledWorkflow] = {}
        self._running = False
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def schedule_workflow(
        self,
        schedule_id: str,
        workflow_fn: Callable,
        schedule: Schedule,
        *args,
        description: str | None = None,
        **kwargs
    ) -> ScheduledWorkflow:
        """Schedule a workflow."""
        sw = ScheduledWorkflow(
            schedule_id=schedule_id,
            workflow_fn=workflow_fn,
            schedule=schedule,
            args=args,
            kwargs=kwargs,
            description=description
        )
        sw.next_run = self._calculate_next_run(sw)
        async with self._lock:
            self._scheduled[schedule_id] = sw
        return sw

    def _calculate_next_run(self, sw: ScheduledWorkflow) -> datetime | None:
        """Calculate next run time."""
        now = datetime.now(timezone.utc)
        stype = sw.schedule.schedule_type
        if stype == ScheduleType.IMMEDIATE:
            return now
        if stype == ScheduleType.ONCE and sw.schedule.run_at:
            return sw.schedule.run_at
        if stype == ScheduleType.INTERVAL and sw.schedule.interval_seconds:
            if sw.last_run:
                from datetime import timedelta
                return sw.last_run + timedelta(seconds=sw.schedule.interval_seconds)
            return now
        if stype == ScheduleType.CRON and sw.schedule.cron_expression:
            try:
                cron = croniter.croniter(sw.schedule.cron_expression, now)
                return cron.get_next(datetime)
            except Exception:
                return None
        return None

    async def unschedule(self, schedule_id: str) -> bool:
        """Remove a scheduled workflow."""
        async with self._lock:
            if schedule_id in self._scheduled:
                del self._scheduled[schedule_id]
                return True
            return False

    async def enable(self, schedule_id: str) -> bool:
        """Enable a scheduled workflow."""
        async with self._lock:
            sw = self._scheduled.get(schedule_id)
            if sw:
                sw.enabled = True
                sw.next_run = self._calculate_next_run(sw)
                return True
            return False

    async def disable(self, schedule_id: str) -> bool:
        """Disable a scheduled workflow."""
        async with self._lock:
            sw = self._scheduled.get(schedule_id)
            if sw:
                sw.enabled = False
                return True
            return False

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = datetime.now(timezone.utc)
            async with self._lock:
                for sw in list(self._scheduled.values()):
                    if not sw.enabled:
                        continue
                    if sw.next_run and now >= sw.next_run:
                        asyncio.create_task(self._execute_workflow(sw))
                        sw.last_run = now
                        sw.next_run = self._calculate_next_run(sw)
            await asyncio.sleep(1)

    async def _execute_workflow(self, sw: ScheduledWorkflow) -> None:
        """Execute a scheduled workflow."""
        try:
            if asyncio.iscoroutinefunction(sw.workflow_fn):
                await sw.workflow_fn(*sw.args, **sw.kwargs)
            else:
                await asyncio.to_thread(sw.workflow_fn, *sw.args, **sw.kwargs)
            sw.run_count += 1
        except Exception:
            sw.error_count += 1

    def get_scheduled_workflows(self) -> list[ScheduledWorkflow]:
        """Get all scheduled workflows."""
        return list(self._scheduled.values())

    def get_pending_workflows(self) -> list[ScheduledWorkflow]:
        """Get workflows ready to run."""
        now = datetime.now(timezone.utc)
        return [
            sw for sw in self._scheduled.values()
            if sw.enabled and sw.next_run and now >= sw.next_run
        ]
