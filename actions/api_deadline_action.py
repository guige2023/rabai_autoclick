"""API Deadline Action.

Manages API request deadlines with timeout and priority handling.
"""
from typing import Any, Callable, Dict, List, Optional, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import time


class Priority(Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


class DeadlineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class APIDeadline:
    deadline_id: str
    priority: Priority
    created_at: float
    deadline_at: float
    status: DeadlineStatus = DeadlineStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


T = TypeVar("T")


class APIDeadlineAction(Generic[T]):
    """Manages API deadlines with timeout and priority queues."""

    def __init__(
        self,
        default_timeout: float = 30.0,
        max_concurrent: int = 10,
    ) -> None:
        self.default_timeout = default_timeout
        self.max_concurrent = max_concurrent
        self.deadlines: Dict[str, APIDeadline] = {}
        self._running = 0

    def create(
        self,
        priority: Priority = Priority.NORMAL,
        timeout: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> APIDeadline:
        deadline_id = f"dl_{int(time.time()*1000)}"
        deadline = APIDeadline(
            deadline_id=deadline_id,
            priority=priority,
            created_at=time.time(),
            deadline_at=time.time() + (timeout or self.default_timeout),
            metadata=metadata or {},
        )
        self.deadlines[deadline_id] = deadline
        return deadline

    def start(self, deadline_id: str) -> bool:
        dl = self.deadlines.get(deadline_id)
        if not dl or dl.status != DeadlineStatus.PENDING:
            return False
        if self._running >= self.max_concurrent:
            return False
        dl.status = DeadlineStatus.RUNNING
        dl.started_at = time.time()
        self._running += 1
        return True

    def complete(self, deadline_id: str, result: Any = None) -> bool:
        dl = self.deadlines.get(deadline_id)
        if not dl or dl.status != DeadlineStatus.RUNNING:
            return False
        dl.status = DeadlineStatus.COMPLETED
        dl.completed_at = time.time()
        dl.result = result
        self._running = max(0, self._running - 1)
        return True

    def fail(self, deadline_id: str, error: str) -> bool:
        dl = self.deadlines.get(deadline_id)
        if not dl or dl.status != DeadlineStatus.RUNNING:
            return False
        dl.status = DeadlineStatus.COMPLETED
        dl.completed_at = time.time()
        dl.error = error
        self._running = max(0, self._running - 1)
        return False

    def check_timeouts(self) -> List[APIDeadline]:
        now = time.time()
        timed_out = []
        for dl in self.deadlines.values():
            if dl.status == DeadlineStatus.RUNNING and now >= dl.deadline_at:
                dl.status = DeadlineStatus.TIMEOUT
                dl.completed_at = now
                self._running = max(0, self._running - 1)
                timed_out.append(dl)
        return timed_out

    def get_due(self, limit: int = 10) -> List[APIDeadline]:
        due = [dl for dl in self.deadlines.values() if dl.status == DeadlineStatus.PENDING]
        due.sort(key=lambda d: (d.deadline_at, -d.priority.value))
        return due[:limit]

    def get_stats(self) -> Dict[str, Any]:
        counts = {}
        for dl in self.deadlines.values():
            counts[dl.status.value] = counts.get(dl.status.value, 0) + 1
        return {
            "total": len(self.deadlines),
            "running": self._running,
            "max_concurrent": self.max_concurrent,
            "by_status": counts,
        }
