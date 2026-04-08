"""Automation Scheduler v2 Action.

Advanced task scheduler with cron, intervals, and priority queues.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import re


@dataclass
class ScheduledTask:
    task_id: str
    name: str
    fn: Callable
    interval_sec: Optional[float] = None
    cron_expr: Optional[str] = None
    priority: int = 5
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0
    error_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationSchedulerV2Action:
    """Advanced automation scheduler with multiple scheduling strategies."""

    CRON_RE = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$')

    def __init__(self) -> None:
        self.tasks: Dict[str, ScheduledTask] = {}
        self._running: Dict[str, bool] = {}

    def add_interval_task(
        self,
        task_id: str,
        name: str,
        fn: Callable,
        interval_sec: float,
        priority: int = 5,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ScheduledTask:
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            fn=fn,
            interval_sec=interval_sec,
            priority=priority,
            next_run=time.time() + interval_sec,
            metadata=metadata or {},
        )
        self.tasks[task_id] = task
        return task

    def add_cron_task(
        self,
        task_id: str,
        name: str,
        fn: Callable,
        cron_expr: str,
        priority: int = 5,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ScheduledTask:
        if not self.CRON_RE.match(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            fn=fn,
            cron_expr=cron_expr,
            priority=priority,
            next_run=time.time(),
            metadata=metadata or {},
        )
        self.tasks[task_id] = task
        return task

    def enable(self, task_id: str) -> bool:
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True
            return True
        return False

    def disable(self, task_id: str) -> bool:
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False
            return True
        return False

    def get_due_tasks(self, limit: int = 10) -> List[ScheduledTask]:
        now = time.time()
        due = [t for t in self.tasks.values() if t.enabled and t.next_run and t.next_run <= now]
        due.sort(key=lambda t: (-t.priority, t.next_run))
        return due[:limit]

    def run_task(self, task: ScheduledTask) -> Any:
        if self._running.get(task.task_id, False):
            return None
        self._running[task.task_id] = True
        task.last_run = time.time()
        task.run_count += 1
        try:
            result = task.fn()
            if task.interval_sec:
                task.next_run = time.time() + task.interval_sec
            return result
        except Exception as e:
            task.error_count += 1
            raise
        finally:
            self._running[task.task_id] = False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_tasks": len(self.tasks),
            "enabled": sum(1 for t in self.tasks.values() if t.enabled),
            "total_runs": sum(t.run_count for t in self.tasks.values()),
            "total_errors": sum(t.error_count for t in self.tasks.values()),
        }
