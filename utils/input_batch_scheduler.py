"""Input batch scheduler for scheduling and coordinating input batches."""
from typing import List, Dict, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import threading


class BatchPriority(Enum):
    """Priority levels for input batches."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class InputBatch:
    """A scheduled batch of input actions."""
    batch_id: str
    actions: List[Dict]
    priority: BatchPriority = BatchPriority.NORMAL
    scheduled_time: Optional[float] = None
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class InputBatchScheduler:
    """Schedules and coordinates batches of input actions.
    
    Manages queuing, prioritization, and execution of input batches
    with support for timing constraints and dependencies.
    
    Example:
        scheduler = InputBatchScheduler()
        batch = InputBatch(batch_id="batch1", actions=[{"type": "click", "x": 100, "y": 200}])
        scheduler.schedule(batch)
        scheduler.execute_pending()
    """

    def __init__(self) -> None:
        self._pending: List[InputBatch] = []
        self._executing: Dict[str, InputBatch] = {}
        self._completed: Dict[str, InputBatch] = {}
        self._lock = threading.RLock()
        self._executors: Dict[BatchPriority, Callable] = {}

    def schedule(self, batch: InputBatch) -> bool:
        """Schedule a batch for execution."""
        with self._lock:
            if batch.scheduled_time is None:
                batch.scheduled_time = time.time()
            self._pending.append(batch)
            self._pending.sort(key=lambda b: b.priority.value, reverse=True)
            return True

    def cancel(self, batch_id: str) -> bool:
        """Cancel a pending batch."""
        with self._lock:
            for i, batch in enumerate(self._pending):
                if batch.batch_id == batch_id:
                    self._pending.pop(i)
                    return True
            return batch_id in self._executing

    def execute_pending(self) -> List[str]:
        """Execute all pending batches that are ready."""
        with self._lock:
            ready = []
            now = time.time()
            for batch in self._pending[:]:
                if batch.scheduled_time <= now:
                    self._pending.remove(batch)
                    self._executing[batch.batch_id] = batch
                    ready.append(batch.batch_id)
            return ready

    def complete(self, batch_id: str) -> bool:
        """Mark a batch as completed."""
        with self._lock:
            if batch_id in self._executing:
                batch = self._executing.pop(batch_id)
                self._completed[batch_id] = batch
                return True
            return False

    def get_status(self, batch_id: str) -> Optional[str]:
        """Get execution status of a batch."""
        if batch_id in self._pending:
            return "pending"
        if batch_id in self._executing:
            return "executing"
        if batch_id in self._completed:
            return "completed"
        return None

    def list_pending(self) -> List[InputBatch]:
        """List all pending batches."""
        return self._pending.copy()

    def clear_completed(self) -> int:
        """Clear completed batch records."""
        with self._lock:
            count = len(self._completed)
            self._completed.clear()
            return count
