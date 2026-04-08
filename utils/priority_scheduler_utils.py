"""
Priority Scheduler Utilities

Provides utilities for priority-based task scheduling
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass, field
import heapq


@dataclass
class ScheduledTask:
    """A scheduled task with priority."""
    priority: int
    task: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    run_at: float = 0.0


class PriorityScheduler:
    """
    Schedules tasks by priority.
    
    Uses a heap for efficient priority queue
    operations.
    """

    def __init__(self) -> None:
        self._tasks: list[ScheduledTask] = []
        self._task_id = 0

    def schedule(
        self,
        priority: int,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> int:
        """
        Schedule a task with priority.
        
        Args:
            priority: Higher number = higher priority.
            task: Task callable.
            
        Returns:
            Task ID.
        """
        self._task_id += 1
        scheduled = ScheduledTask(
            priority=priority,
            task=task,
            args=args,
            kwargs=kwargs,
            run_at=float(self._task_id),
        )
        heapq.heappush(self._tasks, scheduled)
        return self._task_id

    def get_next(self) -> ScheduledTask | None:
        """Get the highest priority task without removing it."""
        if self._tasks:
            return self._tasks[0]
        return None

    def execute_next(self) -> Any | None:
        """Execute and remove highest priority task."""
        if not self._tasks:
            return None
        task = heapq.heappop(self._tasks)
        return task.task(*task.args, **task.kwargs)

    def size(self) -> int:
        """Get number of scheduled tasks."""
        return len(self._tasks)

    def clear(self) -> None:
        """Clear all scheduled tasks."""
        self._tasks.clear()
