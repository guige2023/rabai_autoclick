"""API request scheduler with priority and throttling.

This module provides request scheduling:
- Priority queues
- Rate throttling
- Delayed execution
- Request batching

Example:
    >>> from actions.api_scheduler_action import RequestScheduler
    >>> scheduler = RequestScheduler(rate_limit=100)
    >>> scheduler.schedule(make_request, priority=1)
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

logger = logging.getLogger(__name__)


class RequestPriority(Enum):
    """Request priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class ScheduledRequest:
    """A scheduled API request."""
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: RequestPriority = RequestPriority.NORMAL
    scheduled_at: float = field(default_factory=time.time)
    execute_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3


@dataclass
class ScheduleResult:
    """Result of scheduled request execution."""
    request_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    executed_at: float = field(default_factory=time.time)


class RequestScheduler:
    """Schedule API requests with priority and rate limiting.

    Example:
        >>> scheduler = RequestScheduler(rate_limit=100, max_concurrent=5)
        >>> scheduler.schedule(api_call, priority=RequestPriority.HIGH)
        >>> scheduler.start()
    """

    def __init__(
        self,
        rate_limit: int = 100,
        window: float = 1.0,
        max_concurrent: int = 10,
    ) -> None:
        self.rate_limit = rate_limit
        self.window = window
        self.max_concurrent = max_concurrent
        self._queue: deque[ScheduledRequest] = deque()
        self._priority_queues: dict[RequestPriority, deque[ScheduledRequest]] = {
            p: deque() for p in RequestPriority
        }
        self._lock = threading.RLock()
        self._running = False
        self._executing: int = 0
        self._request_times: deque[float] = deque()
        self._results: dict[str, ScheduleResult] = {}

    def schedule(
        self,
        func: Callable[..., Any],
        *args: Any,
        priority: RequestPriority = RequestPriority.NORMAL,
        delay: float = 0.0,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> str:
        """Schedule a request for execution.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            priority: Request priority.
            delay: Delay before execution (seconds).
            max_retries: Maximum retry attempts.
            **kwargs: Keyword arguments.

        Returns:
            Request ID.
        """
        import uuid
        request_id = str(uuid.uuid4())[:8]
        request = ScheduledRequest(
            id=request_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            execute_at=time.time() + delay,
            max_retries=max_retries,
        )
        with self._lock:
            self._priority_queues[priority].append(request)
        logger.debug(f"Scheduled request {request_id} with priority {priority.name}")
        return request_id

    def start(self) -> None:
        """Start the scheduler processing loop."""
        self._running = True
        self._process_loop()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

    def _process_loop(self) -> None:
        """Main processing loop."""
        while self._running:
            self._process_requests()
            time.sleep(0.01)

    def _process_requests(self) -> None:
        """Process ready requests within rate limits."""
        with self._lock:
            if self._executing >= self.max_concurrent:
                return
            now = time.time()
            self._clean_request_times()
            available_capacity = self.max_concurrent - self._executing
            rate_available = len(self._request_times) < self.rate_limit
            if not rate_available:
                return
            to_execute: list[ScheduledRequest] = []
            for priority in reversed(RequestPriority):
                queue = self._priority_queues[priority]
                while queue and len(to_execute) < available_capacity:
                    if rate_available:
                        request = queue[0]
                        if request.execute_at <= now:
                            queue.popleft()
                            to_execute.append(request)
                            self._request_times.append(now)
                    else:
                        break
            for request in to_execute:
                self._execute_async(request)

    def _execute_async(self, request: ScheduledRequest) -> None:
        """Execute a request asynchronously."""
        self._executing += 1

        def execute() -> None:
            try:
                result = request.func(*request.args, **request.kwargs)
                self._results[request.id] = ScheduleResult(
                    request_id=request.id,
                    success=True,
                    result=result,
                )
            except Exception as e:
                if request.retries < request.max_retries:
                    request.retries += 1
                    request.execute_at = time.time() + (2 ** request.retries)
                    with self._lock:
                        self._priority_queues[request.priority].append(request)
                else:
                    self._results[request.id] = ScheduleResult(
                        request_id=request.id,
                        success=False,
                        error=str(e),
                    )
            finally:
                self._executing -= 1
        thread = threading.Thread(target=execute)
        thread.start()

    def _clean_request_times(self) -> None:
        """Clean old request timestamps from rate tracking."""
        cutoff = time.time() - self.window
        while self._request_times and self._request_times[0] < cutoff:
            self._request_times.popleft()

    def get_result(self, request_id: str) -> Optional[ScheduleResult]:
        """Get the result of a scheduled request."""
        return self._results.get(request_id)

    def get_queue_size(self, priority: Optional[RequestPriority] = None) -> int:
        """Get the current queue size."""
        with self._lock:
            if priority:
                return len(self._priority_queues[priority])
            return sum(len(q) for q in self._priority_queues.values())

    def cancel(self, request_id: str) -> bool:
        """Cancel a scheduled request."""
        with self._lock:
            for queue in self._priority_queues.values():
                for i, req in enumerate(queue):
                    if req.id == request_id:
                        queue.pop(i)
                        return True
        return False


class PriorityQueue:
    """Thread-safe priority queue implementation."""

    def __init__(self) -> None:
        self._queues: dict[RequestPriority, deque[ScheduledRequest]] = {
            p: deque() for p in RequestPriority
        }
        self._lock = threading.Lock()

    def enqueue(self, request: ScheduledRequest) -> None:
        """Add a request to the queue."""
        with self._lock:
            self._queues[request.priority].append(request)

    def dequeue(self) -> Optional[ScheduledRequest]:
        """Get the highest priority request."""
        with self._lock:
            for priority in reversed(RequestPriority):
                if self._queues[priority]:
                    return self._queues[priority].popleft()
        return None

    def size(self) -> int:
        """Get total queue size."""
        with self._lock:
            return sum(len(q) for q in self._queues.values())

    def clear(self) -> None:
        """Clear all queues."""
        with self._lock:
            for queue in self._queues.values():
                queue.clear()
