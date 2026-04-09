"""Load shedding action module.

Provides load shedding functionality to protect systems from overload
by rejecting requests when capacity thresholds are exceeded.
"""

from __future__ import annotations

import time
import threading
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import logging

logger = logging.getLogger(__name__)


class RejectionStrategy(Enum):
    """Load shedding rejection strategies."""
    REJECT_ALL = "reject_all"
    REJECT_HALF = "reject_half"
    QUEUE_BASED = "queue_based"
    PRIORITY_BASED = "priority_based"


@dataclass
class LoadMetric:
    """System load metric snapshot."""
    timestamp: float
    active_requests: int
    queue_depth: int
    cpu_usage: float
    memory_usage: float
    avg_response_time: float


@dataclass
class SheddingConfig:
    """Configuration for load shedding."""
    max_concurrent_requests: int = 1000
    max_queue_depth: int = 500
    cpu_threshold: float = 80.0
    memory_threshold: float = 85.0
    response_time_threshold: float = 1000.0
    min_available_capacity: float = 0.1
    strategy: RejectionStrategy = RejectionStrategy.REJECT_HALF
    window_size: int = 60


class LoadShedder:
    """Load shedding handler."""

    def __init__(self, config: SheddingConfig):
        """Initialize load shedder.

        Args:
            config: Shedding configuration
        """
        self.config = config
        self._metrics: deque[LoadMetric] = deque(maxlen=config.window_size)
        self._lock = threading.Lock()
        self._active_requests = 0
        self._total_requests = 0
        self._rejected_requests = 0
        self._request_times: deque[float] = deque(maxlen=1000)

    def should_shed(self) -> bool:
        """Check if request should be shed.

        Returns:
            True if request should be rejected
        """
        with self._lock:
            self._update_metrics()

            if self._active_requests >= self.config.max_concurrent_requests:
                return True

            if self._get_avg_queue_depth() >= self.config.max_queue_depth:
                return True

            if self._get_avg_cpu_usage() >= self.config.cpu_threshold:
                return True

            if self._get_avg_memory_usage() >= self.config.memory_threshold:
                return True

            if self._get_avg_response_time() >= self.config.response_time_threshold:
                return True

            return False

    def check_load(self) -> dict[str, Any]:
        """Get current load status.

        Returns:
            Dictionary with load metrics
        """
        with self._lock:
            self._update_metrics()
            return {
                "active_requests": self._active_requests,
                "total_requests": self._total_requests,
                "rejected_requests": self._rejected_requests,
                "rejection_rate": self._rejected_requests / max(1, self._total_requests),
                "avg_response_time": self._get_avg_response_time(),
                "cpu_usage": self._get_avg_cpu_usage(),
                "memory_usage": self._get_avg_memory_usage(),
                "queue_depth": self._get_avg_queue_depth(),
            }

    def record_request_start(self) -> bool:
        """Record start of request processing.

        Returns:
            True if request accepted, False if shed
        """
        with self._lock:
            self._total_requests += 1
            self._active_requests += 1
            self._request_times.append(time.time())

            if self.should_shed():
                self._active_requests -= 1
                self._rejected_requests += 1
                return False

            return True

    def record_request_end(self, response_time: float = 0.0) -> None:
        """Record end of request processing.

        Args:
            response_time: Request processing time in ms
        """
        with self._lock:
            self._active_requests = max(0, self._active_requests - 1)
            if response_time > 0:
                self._request_times.append(time.time())

    def _update_metrics(self) -> None:
        """Update load metrics snapshot."""
        metric = LoadMetric(
            timestamp=time.time(),
            active_requests=self._active_requests,
            queue_depth=self._get_current_queue_depth(),
            cpu_usage=self._get_current_cpu_usage(),
            memory_usage=self._get_current_memory_usage(),
            avg_response_time=self._get_avg_response_time(),
        )
        self._metrics.append(metric)

    def _get_avg_response_time(self) -> float:
        """Get average response time from recent requests."""
        if not self._request_times:
            return 0.0
        current_time = time.time()
        recent_times = [t for t in self._request_times if current_time - t < 60]
        if not recent_times:
            return 0.0
        return len(recent_times) / len(self._request_times) * 100

    def _get_avg_cpu_usage(self) -> float:
        """Get average CPU usage."""
        if not self._metrics:
            return 0.0
        return sum(m.cpu_usage for m in self._metrics) / len(self._metrics)

    def _get_avg_memory_usage(self) -> float:
        """Get average memory usage."""
        if not self._metrics:
            return 0.0
        return sum(m.memory_usage for m in self._metrics) / len(self._metrics)

    def _get_avg_queue_depth(self) -> float:
        """Get average queue depth."""
        if not self._metrics:
            return 0.0
        return sum(m.queue_depth for m in self._metrics) / len(self._metrics)

    def _get_current_queue_depth(self) -> int:
        """Get current queue depth (mock implementation)."""
        return min(self._active_requests, self.config.max_queue_depth)

    def _get_current_cpu_usage(self) -> float:
        """Get current CPU usage (mock implementation)."""
        return min(50.0 + (self._active_requests / self.config.max_concurrent_requests) * 50, 100)

    def _get_current_memory_usage(self) -> float:
        """Get current memory usage (mock implementation)."""
        return min(40.0 + (self._active_requests / self.config.max_concurrent_requests) * 50, 100)

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._active_requests = 0
            self._total_requests = 0
            self._rejected_requests = 0
            self._request_times.clear()


class LoadSheddingMiddleware:
    """Middleware for automatic load shedding."""

    def __init__(self, shedder: LoadShedder):
        """Initialize middleware.

        Args:
            shedder: LoadShedder instance
        """
        self.shedder = shedder

    def should_reject_request(self, priority: int = 0) -> bool:
        """Check if request should be rejected.

        Args:
            priority: Request priority (higher = more important)

        Returns:
            True if request should be rejected
        """
        if self.shedder.config.strategy == RejectionStrategy.REJECT_ALL:
            return self.shedder.should_shed()

        elif self.shedder.config.strategy == RejectionStrategy.REJECT_HALF:
            if priority > 5:
                return False
            return self.shedder.should_shed()

        elif self.shedder.config.strategy == RejectionStrategy.PRIORITY_BASED:
            if priority > 7:
                return False
            return self.shedder.should_shed()

        return self.shedder.should_shed()


def create_load_shedder(
    max_concurrent_requests: int = 1000,
    strategy: RejectionStrategy = RejectionStrategy.REJECT_HALF,
) -> LoadShedder:
    """Create load shedder instance.

    Args:
        max_concurrent_requests: Maximum concurrent requests
        strategy: Rejection strategy

    Returns:
        LoadShedder instance
    """
    config = SheddingConfig(
        max_concurrent_requests=max_concurrent_requests,
        strategy=strategy,
    )
    return LoadShedder(config)
