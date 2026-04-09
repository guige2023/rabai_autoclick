"""API monitoring and metrics collection.

This module provides comprehensive API monitoring including:
- Request/response timing
- Error rate tracking
- Latency percentiles
- Health checks

Example:
    >>> from actions.api_monitor_action import APIMonitor
    >>> monitor = APIMonitor()
    >>> with monitor.track("get_user"):
    ...     response = get_user(123)
"""

from __future__ import annotations

import time
import threading
import logging
import statistics
from dataclasses import dataclass, field
from typing import Any, Optional
from collections import deque
from enum import Enum

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """API health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class RequestMetrics:
    """Metrics for a single API request."""
    name: str
    duration: float
    status_code: Optional[int] = None
    success: bool = True
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AggregatedMetrics:
    """Aggregated metrics for an endpoint."""
    name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_duration: float = 0.0
    min_duration: float = float("inf")
    max_duration: float = 0.0
    durations: list[float] = field(default_factory=list)
    error_codes: dict[int, int] = field(default_factory=dict)

    @property
    def avg_duration(self) -> float:
        return self.total_duration / self.total_requests if self.total_requests else 0.0

    @property
    def p50_duration(self) -> float:
        return statistics.median(self.durations) if self.durations else 0.0

    @property
    def p95_duration(self) -> float:
        if not self.durations:
            return 0.0
        sorted_durations = sorted(self.durations)
        index = int(len(sorted_durations) * 0.95)
        return sorted_durations[min(index, len(sorted_durations) - 1)]

    @property
    def p99_duration(self) -> float:
        if not self.durations:
            return 0.0
        sorted_durations = sorted(self.durations)
        index = int(len(sorted_durations) * 0.99)
        return sorted_durations[min(index, len(sorted_durations) - 1)]

    @property
    def error_rate(self) -> float:
        return self.failed_requests / self.total_requests if self.total_requests else 0.0

    @property
    def success_rate(self) -> float:
        return self.successful_requests / self.total_requests if self.total_requests else 0.0


class APIMonitor:
    """API monitoring and metrics collector.

    Attributes:
        window_size: Number of recent requests to keep per endpoint.
    """

    def __init__(
        self,
        window_size: int = 1000,
        error_threshold: float = 0.05,
        latency_threshold_ms: float = 1000.0,
    ) -> None:
        self.window_size = window_size
        self.error_threshold = error_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self._metrics: dict[str, AggregatedMetrics] = {}
        self._recent_requests: dict[str, deque[RequestMetrics]] = {}
        self._lock = threading.RLock()
        self._health_status: dict[str, HealthStatus] = {}
        self._overall_status = HealthStatus.HEALTHY

    def track(
        self,
        name: str,
        status_code: Optional[int] = None,
        error: Optional[str] = None,
    ) -> RequestMetrics:
        """Track a request start.

        Args:
            name: Endpoint or operation name.
            status_code: HTTP status code.
            error: Error message if request failed.

        Returns:
            RequestMetrics object to complete the tracking.
        """
        return RequestMetrics(name=name, status_code=status_code, error=error)

    def record(self, metrics: RequestMetrics) -> None:
        """Record completed request metrics.

        Args:
            metrics: The completed request metrics.
        """
        with self._lock:
            if metrics.name not in self._metrics:
                self._metrics[metrics.name] = AggregatedMetrics(name=metrics.name)
                self._recent_requests[metrics.name] = deque(maxlen=self.window_size)

            agg = self._metrics[metrics.name]
            agg.total_requests += 1
            agg.total_duration += metrics.duration
            agg.min_duration = min(agg.min_duration, metrics.duration)
            agg.max_duration = max(agg.max_duration, metrics.duration)
            agg.durations.append(metrics.duration)

            if not metrics.success:
                agg.failed_requests += 1
                if metrics.status_code:
                    agg.error_codes[metrics.status_code] = (
                        agg.error_codes.get(metrics.status_code, 0) + 1
                    )
            else:
                agg.successful_requests += 1

            self._recent_requests[metrics.name].append(metrics)
            self._update_health(metrics.name)

    def get_metrics(self, name: str) -> Optional[AggregatedMetrics]:
        """Get aggregated metrics for an endpoint.

        Args:
            name: Endpoint name.

        Returns:
            AggregatedMetrics or None if not found.
        """
        with self._lock:
            return self._metrics.get(name)

    def get_all_metrics(self) -> dict[str, AggregatedMetrics]:
        """Get all endpoint metrics."""
        with self._lock:
            return dict(self._metrics)

    def get_health_status(self, name: str) -> HealthStatus:
        """Get health status for an endpoint.

        Args:
            name: Endpoint name.

        Returns:
            HealthStatus for the endpoint.
        """
        with self._lock:
            return self._health_status.get(name, HealthStatus.HEALTHY)

    def get_overall_status(self) -> HealthStatus:
        """Get overall API health status."""
        with self._lock:
            if any(s == HealthStatus.UNHEALTHY for s in self._health_status.values()):
                return HealthStatus.UNHEALTHY
            if any(s == HealthStatus.DEGRADED for s in self._health_status.values()):
                return HealthStatus.DEGRADED
            return HealthStatus.HEALTHY

    def get_recent_requests(self, name: str, limit: int = 100) -> list[RequestMetrics]:
        """Get recent requests for an endpoint.

        Args:
            name: Endpoint name.
            limit: Maximum number of requests to return.

        Returns:
            List of recent RequestMetrics.
        """
        with self._lock:
            if name not in self._recent_requests:
                return []
            return list(self._recent_requests[name])[-limit:]

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of all monitoring data.

        Returns:
            Dictionary containing monitoring summary.
        """
        with self._lock:
            return {
                "overall_status": self.get_overall_status().value,
                "endpoint_count": len(self._metrics),
                "endpoints": {
                    name: {
                        "status": self._health_status.get(name, HealthStatus.HEALTHY).value,
                        "total_requests": agg.total_requests,
                        "error_rate": f"{agg.error_rate:.2%}",
                        "avg_duration_ms": f"{agg.avg_duration * 1000:.2f}",
                        "p99_duration_ms": f"{agg.p99_duration * 1000:.2f}",
                    }
                    for name, agg in self._metrics.items()
                },
            }

    def reset(self, name: Optional[str] = None) -> None:
        """Reset metrics for an endpoint or all endpoints.

        Args:
            name: Optional endpoint name. If None, reset all.
        """
        with self._lock:
            if name:
                if name in self._metrics:
                    del self._metrics[name]
                if name in self._recent_requests:
                    del self._recent_requests[name]
                if name in self._health_status:
                    del self._health_status[name]
            else:
                self._metrics.clear()
                self._recent_requests.clear()
                self._health_status.clear()

    def _update_health(self, name: str) -> None:
        """Update health status for an endpoint."""
        agg = self._metrics[name]
        status = HealthStatus.HEALTHY
        if agg.total_requests < 10:
            status = HealthStatus.HEALTHY
        elif agg.error_rate > self.error_threshold:
            status = HealthStatus.UNHEALTHY
        elif agg.p95_duration > self.latency_threshold_ms / 1000:
            status = HealthStatus.DEGRADED
        self._health_status[name] = status


class MetricsTracker:
    """Context manager for tracking request duration."""

    def __init__(
        self,
        monitor: APIMonitor,
        name: str,
        status_code: Optional[int] = None,
    ) -> None:
        self.monitor = monitor
        self.name = name
        self.status_code = status_code
        self.metrics: Optional[RequestMetrics] = None
        self.start_time: float = 0.0

    def __enter__(self) -> MetricsTracker:
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        duration = time.time() - self.start_time
        success = exc_type is None
        error_msg = str(exc_val) if exc_val else None
        self.metrics = RequestMetrics(
            name=self.name,
            duration=duration,
            status_code=self.status_code,
            success=success,
            error=error_msg,
        )
        self.monitor.record(self.metrics)
