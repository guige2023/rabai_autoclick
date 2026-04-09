"""Load Shedding Action Module.

Protect systems from overload with adaptive load shedding.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class LoadSheddingStrategy(Enum):
    """Load shedding strategies."""
    RANDOM = "random"
    PRIORITY = "priority"
    OLDEST_FIRST = "oldest_first"
    LATENCY_BASED = "latency_based"


@dataclass
class LoadMetric:
    """System load metric."""
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    request_queue_size: int = 0
    active_connections: int = 0
    timestamp: float = 0.0


@dataclass
class RequestPriority:
    """Request priority level."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class LoadShedder(Generic[T]):
    """Load shedder with adaptive strategies."""

    def __init__(
        self,
        strategy: LoadSheddingStrategy = LoadSheddingStrategy.PRIORITY,
        cpu_threshold: float = 80.0,
        queue_threshold: int = 1000
    ) -> None:
        self.strategy = strategy
        self.cpu_threshold = cpu_threshold
        self.queue_threshold = queue_threshold
        self._pending: list[tuple[int, T, float]] = []
        self._lock = asyncio.Lock()
        self._load_history: list[LoadMetric] = []

    async def submit(
        self,
        request: T,
        priority: int = RequestPriority.NORMAL
    ) -> tuple[bool, float | None]:
        """Submit request for processing.

        Returns: (accepted, estimated_wait_time)
        """
        async with self._lock:
            load = self._get_current_load()
            if self._should_shed(load):
                return False, None
            wait_time = self._estimate_wait_time()
            self._pending.append((priority, request, time.time()))
            self._pending.sort(key=lambda x: (-x[0], x[2]))
            return True, wait_time

    async def get_next(self) -> T | None:
        """Get next request to process."""
        async with self._lock:
            if not self._pending:
                return None
            _, request, _ = self._pending.pop(0)
            return request

    def _should_shed(self, load: LoadMetric) -> bool:
        """Determine if load should be shed."""
        if load.cpu_percent > self.cpu_threshold:
            return True
        if load.request_queue_size > self.queue_threshold:
            return True
        return False

    def _get_current_load(self) -> LoadMetric:
        """Get current system load."""
        import psutil
        return LoadMetric(
            cpu_percent=psutil.cpu_percent(),
            memory_percent=psutil.virtual_memory().percent,
            request_queue_size=len(self._pending),
            timestamp=time.time()
        )

    def _estimate_wait_time(self) -> float:
        """Estimate wait time for next request."""
        if not self._pending:
            return 0.0
        return len(self._pending) * 0.01

    async def record_completion(self, request: T, latency_ms: float) -> None:
        """Record request completion."""
        async with self._lock:
            self._load_history.append(LoadMetric(
                request_queue_size=len(self._pending),
                timestamp=time.time()
            ))
            if len(self._load_history) > 100:
                self._load_history = self._load_history[-50:]

    def get_load_stats(self) -> dict[str, Any]:
        """Get load statistics."""
        return {
            "pending_requests": len(self._pending),
            "strategy": self.strategy.value,
            "cpu_threshold": self.cpu_threshold,
            "queue_threshold": self.queue_threshold,
        }
