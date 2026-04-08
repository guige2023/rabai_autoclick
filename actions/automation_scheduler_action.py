# Copyright (c) 2024. coded by claude
"""Automation Scheduler Action Module.

Schedules and manages automation task execution with support for
cron expressions, intervals, and priority-based queuing.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
from croniter import croniter

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"
    DAILY = "daily"
    WEEKLY = "weekly"


@dataclass
class ScheduleConfig:
    name: str
    schedule_type: ScheduleType
    enabled: bool = True
    cron_expression: Optional[str] = None
    interval_seconds: Optional[float] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    max_runs: Optional[int] = None


@dataclass
class ScheduledTask:
    task_id: str
    config: ScheduleConfig
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0


@dataclass
class SchedulerEvent:
    task_id: str
    event_type: str
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)


class AutomationScheduler:
    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._scheduler_task: Optional[asyncio.Task] = None
        self._task_handlers: Dict[str, Callable] = {}
        self._event_listeners: List[Callable] = []

    def add_task(self, task: ScheduledTask, handler: Callable) -> None:
        self._tasks[task.task_id] = task
        self._task_handlers[task.task_id] = handler
        self._calculate_next_run(task)

    def remove_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            del self._tasks[task_id]
            if task_id in self._task_handlers:
                del self._task_handlers[task_id]
            return True
        return False

    def enable_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            self._tasks[task_id].config.enabled = True
            return True
        return False

    def disable_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            self._tasks[task_id].config.enabled = False
            return True
        return False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._scheduler_task = asyncio.create_task(self._run_scheduler())

    async def stop(self) -> None:
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    async def _run_scheduler(self) -> None:
        while self._running:
            now = datetime.now()
            for task in self._tasks.values():
                if not task.config.enabled:
                    continue
                if task.next_run and now >= task.next_run:
                    await self._execute_task(task)
            await asyncio.sleep(1)

    async def _execute_task(self, task: ScheduledTask) -> None:
        if task.task_id not in self._task_handlers:
            return
        handler = self._task_handlers[task.task_id]
        try:
            result = handler()
            if asyncio.iscoroutine(result):
                await result
            task.last_run = datetime.now()
            task.run_count += 1
            self._emit_event(task.task_id, "executed", {"run_count": task.run_count})
        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            self._emit_event(task.task_id, "failed", {"error": str(e)})
        self._calculate_next_run(task)

    def _calculate_next_run(self, task: ScheduledTask) -> None:
        config = task.config
        if config.schedule_type == ScheduleType.INTERVAL and config.interval_seconds:
            if task.last_run:
                task.next_run = task.last_run + timedelta(seconds=config.interval_seconds)
            else:
                task.next_run = datetime.now()
        elif config.schedule_type == ScheduleType.CRON and config.cron_expression:
            cron = croniter(config.cron_expression, datetime.now())
            task.next_run = cron.get_next(datetime)
        elif config.schedule_type == ScheduleType.ONCE and config.start_time:
            task.next_run = config.start_time
        else:
            task.next_run = None

    def _emit_event(self, task_id: str, event_type: str, data: Dict[str, Any]) -> None:
        event = SchedulerEvent(task_id=task_id, event_type=event_type, timestamp=datetime.now(), data=data)
        for listener in self._event_listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Event listener failed: {e}")

    def add_event_listener(self, listener: Callable) -> None:
        self._event_listeners.append(listener)

    def get_task_status(self) -> Dict[str, Any]:
        return {
            task_id: {
                "enabled": task.config.enabled,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "run_count": task.run_count,
            }
            for task_id, task in self._tasks.items()
        }
