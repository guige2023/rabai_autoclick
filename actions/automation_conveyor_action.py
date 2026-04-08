"""Automation Conveyor Action.

Conveyor belt pattern for sequential task processing.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class TaskStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ConveyorTask:
    task_id: str
    name: str
    fn: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.QUEUED
    result: Any = None
    error: Optional[str] = None
    enqueued_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


class AutomationConveyorAction:
    """Conveyor belt for sequential task processing."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.queue: List[ConveyorTask] = []
        self._current: Optional[ConveyorTask] = None

    def enqueue(
        self,
        task_id: str,
        name: str,
        fn: Callable,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> ConveyorTask:
        task = ConveyorTask(
            task_id=task_id,
            name=name,
            fn=fn,
            args=args or (),
            kwargs=kwargs or {},
        )
        self.queue.append(task)
        return task

    def process_next(self) -> Optional[Any]:
        if not self.queue:
            return None
        if self._current and self._current.status == TaskStatus.PROCESSING:
            return None
        task = self.queue[0]
        task.status = TaskStatus.PROCESSING
        task.started_at = time.time()
        self._current = task
        try:
            result = task.fn(*task.args, **task.kwargs)
            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = time.time()
            self.queue.pop(0)
            return result
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            self.queue.pop(0)
            return None

    def process_all(self) -> List[Any]:
        results = []
        while self.queue:
            result = self.process_next()
            if result is not None:
                results.append(result)
        return results

    def get_queue_status(self) -> Dict[str, Any]:
        return {
            "queue_length": len(self.queue),
            "current": {
                "task_id": self._current.task_id if self._current else None,
                "status": self._current.status.value if self._current else None,
            } if self._current else None,
            "queued": [t.task_id for t in self.queue],
        }
