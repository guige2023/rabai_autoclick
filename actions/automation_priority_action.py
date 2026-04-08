"""
Automation Priority Action Module.

Priority-based task execution with dynamic reordering,
preemption support, and weighted fair scheduling.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar
from dataclasses import dataclass, field
from enum import IntEnum
import logging
import asyncio
import time

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TaskPriority(IntEnum):
    """Task priority levels (higher = more important)."""
    CRITICAL = 100
    HIGH = 75
    NORMAL = 50
    LOW = 25
    IDLE = 10


@dataclass
class PrioritizedTask:
    """A task with associated priority and metadata."""
    priority: int
    task_id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    weight: float = 1.0
    deadline: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class AutomationPriorityAction:
    """
    Priority-driven task execution with preemption.

    Supports priority levels, weighted fair queuing,
    deadline-based scheduling, and task preemption.

    Example:
        scheduler = AutomationPriorityAction()
        scheduler.schedule(my_task, priority=TaskPriority.HIGH)
        result = await scheduler.execute_next()
    """

    def __init__(
        self,
        allow_preemption: bool = True,
        weighted_fair: bool = True,
    ) -> None:
        self.allow_preemption = allow_preemption
        self.weighted_fair = weighted_fair
        self._tasks: list[PrioritizedTask] = []
        self._completed: dict[str, Any] = {}
        self._failed: dict[str, str] = {}
        self._executing: Optional[PrioritizedTask] = None

    def schedule(
        self,
        func: Callable[..., T],
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        task_id: Optional[str] = None,
        weight: float = 1.0,
        deadline: Optional[float] = None,
        **kwargs: Any,
    ) -> str:
        """Schedule a task with priority."""
        task_id = task_id or f"task_{time.time()}_{id(func)}"

        task = PrioritizedTask(
            priority=priority,
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            weight=weight,
            deadline=deadline,
        )

        self._tasks.append(task)
        self._tasks.sort(key=lambda t: (-t.priority, t.created_at))

        logger.debug("Scheduled task %s with priority %d", task_id, priority)
        return task_id

    def execute_next(self) -> Optional[Any]:
        """Execute highest priority task synchronously."""
        task = self._pop_next()
        if task is None:
            return None

        self._executing = task

        try:
            result = task.func(*task.args, **task.kwargs)
            self._completed[task.task_id] = result
            return result
        except Exception as e:
            self._failed[task.task_id] = str(e)
            raise
        finally:
            self._executing = None

    async def execute_next_async(self) -> Any:
        """Execute highest priority task asynchronously."""
        task = self._pop_next()
        if task is None:
            return None

        self._executing = task

        try:
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*task.args, **task.kwargs)
            else:
                result = await asyncio.to_thread(
                    task.func, *task.args, **task.kwargs
                )
            self._completed[task.task_id] = result
            return result
        except Exception as e:
            self._failed[task.task_id] = str(e)
            raise
        finally:
            self._executing = None

    def cancel(self, task_id: str) -> bool:
        """Cancel a pending task."""
        for i, task in enumerate(self._tasks):
            if task.task_id == task_id:
                del self._tasks[i]
                logger.info("Cancelled task %s", task_id)
                return True
        return False

    def reschedule(
        self,
        task_id: str,
        new_priority: TaskPriority,
    ) -> bool:
        """Change priority of a pending task."""
        for task in self._tasks:
            if task.task_id == task_id:
                task.priority = new_priority
                self._tasks.sort(key=lambda t: (-t.priority, t.created_at))
                logger.info(
                    "Rescheduled task %s to priority %d",
                    task_id, new_priority
                )
                return True
        return False

    def _pop_next(self) -> Optional[PrioritizedTask]:
        """Get the highest priority task that is ready."""
        if not self._tasks:
            return None

        now = time.time()

        for i, task in enumerate(self._tasks):
            if task.deadline is not None and task.deadline < now:
                return self._tasks.pop(i)

        if self.weighted_fair:
            return self._pop_weighted()
        else:
            return self._tasks.pop(0) if self._tasks else None

    def _pop_weighted(self) -> Optional[PrioritizedTask]:
        """Select task using weighted fair queuing."""
        if not self._tasks:
            return None

        now = time.time()
        deadline_tasks = [
            (i, t) for i, t in enumerate(self._tasks)
            if t.deadline is not None and t.deadline <= now
        ]

        if deadline_tasks:
            idx, _ = deadline_tasks[0]
            return self._tasks.pop(idx)

        weights = [
            t.priority * t.weight for t in self._tasks
        ]
        total_weight = sum(weights)

        if total_weight <= 0:
            return self._tasks.pop(0)

        import random
        r = random.uniform(0, total_weight)

        cumulative = 0
        for i, w in enumerate(weights):
            cumulative += w
            if r <= cumulative:
                return self._tasks.pop(i)

        return self._tasks.pop()

    def get_pending(self) -> list[dict[str, Any]]:
        """Get all pending tasks."""
        return [
            {
                "task_id": t.task_id,
                "priority": t.priority,
                "created_at": t.created_at,
                "deadline": t.deadline,
                "age_seconds": time.time() - t.created_at,
            }
            for t in self._tasks
        ]

    @property
    def queue_size(self) -> int:
        """Number of pending tasks."""
        return len(self._tasks)
