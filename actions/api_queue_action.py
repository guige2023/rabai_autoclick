"""API Queue Action Module.

Implements a persistent request queue with retry logic, priority ordering,
and backpressure handling for high-throughput API client scenarios.
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from enum import IntEnum
from collections import deque
import threading

logger = logging.getLogger(__name__)


class Priority(IntEnum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass(order=True)
class QueuedRequest:
    priority: int
    timestamp: float = field(compare=True)
    request_id: str = field(compare=False, default="")
    url: str = field(compare=False, default="")
    method: str = field(compare=False, default="GET")
    headers: Dict[str, str] = field(compare=False, default_factory=dict)
    body: Optional[bytes] = field(compare=False, default=None)
    retry_count: int = field(compare=False, default=0)
    max_retries: int = field(compare=False, default=3)
    on_success: Optional[Callable] = field(compare=False, default=None)
    on_failure: Optional[Callable] = field(compare=False, default=None)


class APIQueueAction:
    """Persistent async API request queue with priority and backpressure."""

    def __init__(
        self,
        max_size: int = 10000,
        max_concurrent: int = 10,
        default_timeout: float = 30.0,
        backpressure_threshold: float = 0.9,
    ) -> None:
        self.max_size = max_size
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.backpressure_threshold = backpressure_threshold
        self._queue: List[QueuedRequest] = []
        self._active = 0
        self._lock = threading.RLock()
        self._stats = {
            "enqueued": 0,
            "dequeued": 0,
            "completed": 0,
            "failed": 0,
            "rejected": 0,
        }

    def enqueue(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        priority: Priority = Priority.NORMAL,
        request_id: Optional[str] = None,
        max_retries: int = 3,
        on_success: Optional[Callable] = None,
        on_failure: Optional[Callable] = None,
    ) -> bool:
        if len(self._queue) >= self.max_size:
            logger.warning("Queue at capacity, rejecting request")
            self._stats["rejected"] += 1
            return False
        req = QueuedRequest(
            priority=int(priority),
            timestamp=time.time(),
            request_id=request_id or f"req_{self._stats['enqueued']}",
            url=url,
            method=method,
            headers=headers or {},
            body=body,
            max_retries=max_retries,
            on_success=on_success,
            on_failure=on_failure,
        )
        with self._lock:
            self._queue.append(req)
            self._queue.sort(reverse=True)
            self._stats["enqueued"] += 1
        return True

    def dequeue(self) -> Optional[QueuedRequest]:
        with self._lock:
            if not self._queue:
                return None
            self._stats["dequeued"] += 1
            return self._queue.pop(0)

    def requeue(self, request: QueuedRequest) -> bool:
        if request.retry_count >= request.max_retries:
            logger.debug(f"Max retries reached for {request.request_id}")
            return False
        request.retry_count += 1
        request.timestamp = time.time()
        with self._lock:
            self._queue.append(request)
            self._queue.sort(reverse=True)
        return True

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._stats,
                "queue_size": len(self._queue),
                "active": self._active,
                "utilization": self._active / self.max_concurrent
                if self.max_concurrent > 0
                else 0,
            }

    def is_backpressured(self) -> bool:
        with self._lock:
            return len(self._queue) >= int(self.max_size * self.backpressure_threshold)

    def clear(self) -> int:
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count

    def peek(self, n: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "priority": r.priority,
                    "timestamp": r.timestamp,
                    "request_id": r.request_id,
                    "url": r.url,
                    "method": r.method,
                    "retry_count": r.retry_count,
                }
                for r in self._queue[:n]
            ]
