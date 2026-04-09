"""
API Request Queue Action Module.

Priority queue for API requests with fairness and ordering.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from heapq import heappush, heappop
from typing import Any, Callable, Coroutine, Dict, Optional


class Priority(IntEnum):
    """Request priority levels."""
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass(order=True)
class QueuedRequest:
    """A queued API request."""
    priority: int
    created_at: float
    request_id: str = field(compare=False)
    func: Callable[..., Coroutine[Any, Any, Any]] = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: Dict[str, Any] = field(compare=False)


@dataclass
class QueueStats:
    """Queue statistics."""
    total_enqueued: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_rejected: int = 0
    current_size: int = 0


class ApiRequestQueueAction:
    """
    Priority queue for API requests with controlled concurrency.

    Supports priority levels, fairness, and deadline tracking.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        max_queue_size: int = 1000,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self.timeout_seconds = timeout_seconds
        self._queue: list[QueuedRequest] = []
        self._active: Dict[str, asyncio.Task] = {}
        self._stats = QueueStats()
        self._paused = False

    async def enqueue(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        priority: Priority = Priority.NORMAL,
        request_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """
        Add a request to the queue.

        Args:
            func: Async function to execute
            *args: Positional arguments
            priority: Request priority
            request_id: Optional custom ID
            **kwargs: Keyword arguments

        Returns:
            Request ID

        Raises:
            asyncio.QueueFull: If queue is full
        """
        if len(self._queue) >= self.max_queue_size:
            self._stats.total_rejected += 1
            raise asyncio.QueueFull(f"Queue full ({self.max_queue_size})")

        request_id = request_id or str(uuid.uuid4())

        request = QueuedRequest(
            priority=priority.value,
            created_at=time.time(),
            request_id=request_id,
            func=func,
            args=args,
            kwargs=kwargs,
        )

        heappush(self._queue, request)
        self._stats.total_enqueued += 1
        self._stats.current_size = len(self._queue)

        asyncio.create_task(self._process_next())

        return request_id

    async def _process_next(self) -> None:
        """Process next request if slots available."""
        if self._paused:
            return

        if not self._queue or len(self._active) >= self.max_concurrent:
            return

        while self._queue and len(self._active) < self.max_concurrent:
            request = heappop(self._queue)
            self._stats.current_size = len(self._queue)

            task = asyncio.create_task(self._execute(request))
            self._active[request.request_id] = task
            task.add_done_callback(
                lambda t, rid=request.request_id: self._active.pop(rid, None)
            )

    async def _execute(self, request: QueuedRequest) -> Any:
        """Execute a queued request."""
        try:
            result = await asyncio.wait_for(
                request.func(*request.args, **request.kwargs),
                timeout=self.timeout_seconds,
            )
            self._stats.total_completed += 1
            return result
        except asyncio.TimeoutError:
            self._stats.total_failed += 1
            raise
        except Exception:
            self._stats.total_failed += 1
            raise
        finally:
            asyncio.create_task(self._process_next())

    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "total_enqueued": self._stats.total_enqueued,
            "total_completed": self._stats.total_completed,
            "total_failed": self._stats.total_failed,
            "total_rejected": self._stats.total_rejected,
            "current_queue_size": len(self._queue),
            "current_active": len(self._active),
            "max_concurrent": self.max_concurrent,
            "max_queue_size": self.max_queue_size,
            "paused": self._paused,
        }

    def pause(self) -> None:
        """Pause queue processing."""
        self._paused = True

    def resume(self) -> None:
        """Resume queue processing."""
        self._paused = False
        asyncio.create_task(self._process_next())

    def clear(self) -> int:
        """Clear all pending requests."""
        count = len(self._queue)
        self._queue.clear()
        self._stats.current_size = 0
        return count

    def cancel(self, request_id: str) -> bool:
        """Cancel a specific request."""
        for i, req in enumerate(self._queue):
            if req.request_id == request_id:
                self._queue.pop(i)
                self._stats.current_size = len(self._queue)
                return True
        return False
