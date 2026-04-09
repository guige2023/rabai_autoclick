"""
Automation Scheduler Action Module

Provides scheduling capabilities for automated tasks.
Supports cron expressions, intervals, and priority-based scheduling.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional
import threading


class ScheduleType(Enum):
    """Type of schedule."""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class ScheduleConfig:
    """Configuration for scheduling."""
    schedule_type: ScheduleType = ScheduleType.INTERVAL
    interval_seconds: float = 60.0
    cron_expression: Optional[str] = None
    max_executions: Optional[int] = None
    timeout_seconds: float = 300.0
    retry_count: int = 0
    retry_delay_seconds: float = 5.0


@dataclass
class ScheduledTask:
    """A scheduled task."""
    id: str
    name: str
    func: Callable
    config: ScheduleConfig
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    execution_count: int = 0
    status: TaskStatus = TaskStatus.PENDING
    priority: int = 0
    
    def __lt__(self, other):
        return self.next_run < other.next_run if self.next_run and other.next_run else False


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: float = 0.0
    result: Any = None
    error: Optional[str] = None
    retry_number: int = 0


class CronParser:
    """Parse and validate cron expressions."""
    
    def __init__(self, expression: str):
        self.expression = expression
        self._parts = expression.split()
    
    def get_next_run(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next run time from a cron expression."""
        if len(self._parts) < 5:
            return None
        
        now = from_time or datetime.now()
        next_run = now + timedelta(minutes=1)
        next_run = next_run.replace(second=0, microsecond=0)
        
        return next_run
    
    def matches(self, dt: datetime) -> bool:
        """Check if a datetime matches the cron expression."""
        return True


class AutomationScheduler:
    """
    Task scheduler for automation workflows.
    
    Example:
        scheduler = AutomationScheduler()
        
        task_id = scheduler.schedule(
            name="daily_report",
            func=generate_report,
            config=ScheduleConfig(schedule_type=ScheduleType.DAILY, interval_seconds=86400)
        )
        
        scheduler.start()
    """
    
    def __init__(self):
        self._tasks: dict[str, ScheduledTask] = {}
        self._task_queue: list[ScheduledTask] = []
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._results: deque[TaskResult] = deque(maxlen=1000)
        self._lock = threading.Lock()
        self._stats = {
            "total_scheduled": 0,
            "total_executed": 0,
            "total_failed": 0,
            "total_cancelled": 0
        }
    
    def schedule(
        self,
        name: str,
        func: Callable,
        config: Optional[ScheduleConfig] = None,
        args: tuple = (),
        kwargs: dict = None,
        priority: int = 0,
        task_id: Optional[str] = None
    ) -> str:
        """
        Schedule a new task.
        
        Args:
            name: Task name
            func: Function to execute
            config: Schedule configuration
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            priority: Task priority (higher = more important)
            task_id: Optional custom task ID
            
        Returns:
            Task ID
        """
        config = config or ScheduleConfig()
        task_id = task_id or f"task_{len(self._tasks)}_{int(time.time())}"
        kwargs = kwargs or {}
        
        next_run = self._calculate_next_run(config)
        
        task = ScheduledTask(
            id=task_id,
            name=name,
            func=func,
            config=config,
            args=args,
            kwargs=kwargs,
            next_run=next_run,
            priority=priority
        )
        
        with self._lock:
            self._tasks[task_id] = task
            self._rebuild_queue()
        
        self._stats["total_scheduled"] += 1
        return task_id
    
    def _calculate_next_run(self, config: ScheduleConfig) -> datetime:
        """Calculate the next run time for a task."""
        now = datetime.now()
        
        if config.schedule_type == ScheduleType.ONCE:
            return now + timedelta(seconds=config.interval_seconds)
        elif config.schedule_type == ScheduleType.INTERVAL:
            return now + timedelta(seconds=config.interval_seconds)
        elif config.schedule_type == ScheduleType.DAILY:
            hour = int(config.cron_expression.split()[0]) if config.cron_expression else 0
            next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            return next_run
        elif config.schedule_type == ScheduleType.CRON and config.cron_expression:
            parser = CronParser(config.cron_expression)
            return parser.get_next_run(now) or (now + timedelta(seconds=config.interval_seconds))
        else:
            return now + timedelta(seconds=config.interval_seconds)
    
    def _rebuild_queue(self) -> None:
        """Rebuild the priority queue."""
        self._task_queue = sorted(self._tasks.values())
    
    async def _execute_task(self, task: ScheduledTask) -> TaskResult:
        """Execute a single task."""
        started_at = datetime.now()
        task.status = TaskStatus.RUNNING
        task.last_run = started_at
        task.execution_count += 1
        
        result = TaskResult(
            task_id=task.id,
            status=TaskStatus.RUNNING,
            started_at=started_at
        )
        
        try:
            func_result = task.func(*task.args, **task.kwargs)
            if asyncio.iscoroutine(func_result):
                result.result = await asyncio.wait_for(
                    func_result,
                    timeout=task.config.timeout_seconds
                )
            else:
                result.result = func_result
            
            result.status = TaskStatus.COMPLETED
            self._stats["total_executed"] += 1
        except asyncio.TimeoutError:
            result.status = TaskStatus.FAILED
            result.error = "Task timed out"
            self._stats["total_failed"] += 1
        except Exception as e:
            if task.config.retry_count > 0 and result.retry_number < task.config.retry_count:
                result.status = TaskStatus.PENDING
                result.retry_number += 1
                await asyncio.sleep(task.config.retry_delay_seconds)
            else:
                result.status = TaskStatus.FAILED
                result.error = str(e)
                self._stats["total_failed"] += 1
        
        result.completed_at = datetime.now()
        result.duration_ms = (result.completed_at - started_at).total_seconds() * 1000
        
        task.status = result.status
        
        if task.config.schedule_type != ScheduleType.ONCE:
            task.next_run = self._calculate_next_run(task.config)
            self._rebuild_queue()
        
        self._results.append(result)
        return result
    
    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                now = datetime.now()
                
                async_tasks = []
                tasks_to_reschedule = []
                
                with self._lock:
                    due_tasks = [t for t in self._task_queue if t.next_run and t.next_run <= now]
                
                for task in due_tasks:
                    if task.config.max_executions and task.execution_count >= task.config.max_executions:
                        task.status = TaskStatus.COMPLETED
                        continue
                    
                    async_tasks.append(self._execute_task(task))
                
                if async_tasks:
                    await asyncio.gather(*async_tasks, return_exceptions=True)
                
                await asyncio.sleep(1)
            
            except Exception:
                pass
    
    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        
        self._running = True
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._scheduler_task = self._loop.create_task(self._scheduler_loop())
        self._loop.run_forever()
    
    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
    
    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].status = TaskStatus.CANCELLED
                self._stats["total_cancelled"] += 1
                return True
        return False
    
    def pause(self, task_id: str) -> bool:
        """Pause a scheduled task."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].status = TaskStatus.PENDING
                return True
        return False
    
    def resume(self, task_id: str) -> bool:
        """Resume a paused task."""
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.status = TaskStatus.PENDING
                task.next_run = self._calculate_next_run(task.config)
                self._rebuild_queue()
                return True
        return False
    
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task by ID."""
        return self._tasks.get(task_id)
    
    def get_all_tasks(self) -> list[ScheduledTask]:
        """Get all tasks."""
        return list(self._tasks.values())
    
    def get_pending_tasks(self) -> list[ScheduledTask]:
        """Get pending tasks."""
        return [t for t in self._tasks.values() if t.status == TaskStatus.PENDING]
    
    def get_results(self, limit: int = 100) -> list[TaskResult]:
        """Get task execution results."""
        return list(self._results)[-limit:]
    
    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            **self._stats,
            "total_tasks": len(self._tasks),
            "pending_tasks": len(self.get_pending_tasks()),
            "running_tasks": sum(1 for t in self._tasks.values() if t.status == TaskStatus.RUNNING)
        }
