"""API metrics collection and monitoring action module.

Tracks API call latency, error rates, status codes, and payload sizes.
Provides percentile calculations and alerting thresholds.
"""

from __future__ import annotations

import time
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class APIMetric:
    """A single API metric data point."""
    endpoint: str
    method: str
    status_code: int
    latency_ms: float
    payload_size: int
    timestamp: float = field(default_factory=lambda: time.time())
    error: Optional[str] = None


@dataclass
class EndpointStats:
    """Aggregated statistics for an endpoint."""
    endpoint: str
    method: str
    total_requests: int = 0
    total_errors: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    payload_sizes: List[int] = field(default_factory=list)
    status_codes: Dict[int, int] = field(default_factory=dict)
    last_request_at: Optional[float] = None

    @property
    def avg_latency_ms(self) -> float:
        """Average latency in milliseconds."""
        if self.total_requests == 0:
            return 0.0
        return self.total_latency_ms / self.total_requests

    @property
    def error_rate(self) -> float:
        """Error rate as a fraction."""
        if self.total_requests == 0:
            return 0.0
        return self.total_errors / self.total_requests

    def percentile_latency(self, p: float) -> float:
        """Calculate percentile latency (p = 0.0-1.0)."""
        if not self.payload_sizes:
            return 0.0
        sorted_sizes = sorted(self.payload_sizes)
        idx = int(len(sorted_sizes) * p)
        return sorted_sizes[min(idx, len(sorted_sizes) - 1)]


class APIMetricsAction:
    """API metrics collector with in-memory storage.

    Tracks per-endpoint latency, error rates, and payload sizes.
    Computes rolling percentiles and provides alerting thresholds.

    Example:
        metrics = APIMetricsAction(window_size=1000)
        metrics.record(endpoint="/api/users", method="GET", status_code=200, latency_ms=45.2)
        stats = metrics.get_stats("/api/users", "GET")
    """

    def __init__(
        self,
        window_size: int = 1000,
        alert_threshold_ms: float = 5000.0,
        alert_error_rate: float = 0.05,
    ) -> None:
        """Initialize metrics collector.

        Args:
            window_size: Number of recent requests to track per endpoint.
            alert_threshold_ms: Latency threshold for alerts (ms).
            alert_error_rate: Error rate threshold for alerts (0.0-1.0).
        """
        self.window_size = window_size
        self.alert_threshold_ms = alert_threshold_ms
        self.alert_error_rate = alert_error_rate
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self._stats: Dict[str, EndpointStats] = {}
        self._alerts: List[Dict[str, Any]] = []

    def record(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency_ms: float,
        payload_size: int = 0,
        error: Optional[str] = None,
    ) -> None:
        """Record an API call metric.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            status_code: Response status code.
            latency_ms: Latency in milliseconds.
            payload_size: Response payload size in bytes.
            error: Optional error message string.
        """
        metric = APIMetric(
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            latency_ms=latency_ms,
            payload_size=payload_size,
            error=error,
        )

        key = f"{method}:{endpoint}"
        self._metrics[key].append(metric)
        self._update_stats(key, metric)
        self._check_alerts(key)

    def get_stats(
        self,
        endpoint: str,
        method: str,
    ) -> Optional[EndpointStats]:
        """Get aggregated statistics for an endpoint.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.

        Returns:
            EndpointStats or None if no data.
        """
        return self._stats.get(f"{method}:{endpoint}")

    def get_recent_metrics(
        self,
        endpoint: str,
        method: str,
        limit: int = 100,
    ) -> List[APIMetric]:
        """Get recent metrics for an endpoint.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            limit: Maximum number of metrics to return.

        Returns:
            List of APIMetric objects, newest first.
        """
        key = f"{method}:{endpoint}"
        metrics = list(self._metrics.get(key, []))
        return metrics[-limit:][::-1]

    def get_all_stats(self) -> Dict[str, EndpointStats]:
        """Get statistics for all tracked endpoints."""
        return dict(self._stats)

    def get_p99_latency(self, endpoint: str, method: str) -> float:
        """Get P99 latency for an endpoint.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.

        Returns:
            P99 latency in milliseconds.
        """
        key = f"{method}:{endpoint}"
        metrics = self._metrics.get(key, [])
        if not metrics:
            return 0.0

        latencies = sorted(m.latency_ms for m in metrics)
        idx = int(len(latencies) * 0.99)
        return latencies[min(idx, len(latencies) - 1)]

    def get_error_rate(
        self,
        endpoint: str,
        method: str,
        window: Optional[int] = None,
    ) -> float:
        """Calculate error rate for an endpoint.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            window: Only consider last N requests (None = all).

        Returns:
            Error rate as a fraction (0.0-1.0).
        """
        key = f"{method}:{endpoint}"
        metrics = self._metrics.get(key, [])
        if not metrics:
            return 0.0

        if window:
            metrics = list(metrics)[-window:]

        errors = sum(1 for m in metrics if m.status_code >= 400 or m.error)
        return errors / len(metrics)

    def get_throughput(
        self,
        endpoint: str,
        method: str,
        window_seconds: int = 60,
    ) -> float:
        """Calculate requests per second over a time window.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            window_seconds: Time window in seconds.

        Returns:
            Requests per second.
        """
        key = f"{method}:{endpoint}"
        metrics = self._metrics.get(key, [])
        if not metrics:
            return 0.0

        cutoff = time.time() - window_seconds
        recent = [m for m in metrics if m.timestamp >= cutoff]
        if not recent:
            return 0.0

        time_span = recent[-1].timestamp - recent[0].timestamp
        if time_span <= 0:
            return 0.0
        return len(recent) / time_span

    def get_alerts(self) -> List[Dict[str, Any]]:
        """Get current active alerts."""
        return list(self._alerts)

    def clear_alert(self, endpoint: str, method: str, alert_type: str) -> bool:
        """Clear a specific alert.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            alert_type: Type of alert (latency, error_rate).

        Returns:
            True if alert was cleared.
        """
        key = f"{method}:{endpoint}"
        initial_len = len(self._alerts)
        self._alerts = [
            a for a in self._alerts
            if not (a["endpoint"] == key and a["type"] == alert_type)
        ]
        return len(self._alerts) < initial_len

    def summary(self) -> Dict[str, Any]:
        """Get a complete metrics summary.

        Returns:
            Dict with overall stats, per-endpoint stats, and active alerts.
        """
        total_requests = sum(s.total_requests for s in self._stats.values())
        total_errors = sum(s.total_errors for s in self._stats.values())
        overall_p99 = []
        for key, metrics in self._metrics.items():
            if metrics:
                latencies = sorted(m.latency_ms for m in metrics)
                idx = int(len(latencies) * 0.99)
                overall_p99.append(latencies[min(idx, len(latencies) - 1)])

        return {
            "total_requests": total_requests,
            "total_errors": total_errors,
            "overall_error_rate": total_errors / total_requests if total_requests else 0.0,
            "overall_p99_latency_ms": max(overall_p99) if overall_p99 else 0.0,
            "endpoints": {
                key: {
                    "requests": s.total_requests,
                    "error_rate": s.error_rate,
                    "avg_latency_ms": s.avg_latency_ms,
                    "p99_latency_ms": self.get_p99_latency(key.split(":")[1], key.split(":")[0]),
                }
                for key, s in self._stats.items()
            },
            "alerts": self._alerts,
        }

    def _update_stats(self, key: str, metric: APIMetric) -> None:
        """Update rolling statistics for a metric."""
        stats = self._stats.get(key)
        if stats is None:
            parts = key.split(":", 1)
            stats = EndpointStats(endpoint=parts[1], method=parts[0])
            self._stats[key] = stats

        stats.total_requests += 1
        stats.total_latency_ms += metric.latency_ms
        stats.min_latency_ms = min(stats.min_latency_ms, metric.latency_ms)
        stats.max_latency_ms = max(stats.max_latency_ms, metric.latency_ms)
        stats.payload_sizes.append(metric.latency_ms)
        if len(stats.payload_sizes) > self.window_size:
            stats.payload_sizes = stats.payload_sizes[-self.window_size:]
        stats.status_codes[metric.status_code] = stats.status_codes.get(metric.status_code, 0) + 1
        stats.last_request_at = metric.timestamp

        if metric.status_code >= 400 or metric.error:
            stats.total_errors += 1

    def _check_alerts(self, key: str) -> None:
        """Check if any alert thresholds are exceeded."""
        stats = self._stats.get(key)
        if stats is None or stats.total_requests < 10:
            return

        alerts_to_add = []

        if stats.avg_latency_ms > self.alert_threshold_ms:
            alerts_to_add.append({
                "endpoint": key,
                "type": "latency",
                "message": f"High latency: {stats.avg_latency_ms:.1f}ms (threshold: {self.alert_threshold_ms}ms)",
                "severity": "warning",
                "timestamp": time.time(),
            })

        if stats.error_rate > self.alert_error_rate:
            alerts_to_add.append({
                "endpoint": key,
                "type": "error_rate",
                "message": f"High error rate: {stats.error_rate:.2%} (threshold: {self.alert_error_rate:.2%})",
                "severity": "critical",
                "timestamp": time.time(),
            })

        for alert in alerts_to_add:
            existing = [a for a in self._alerts if a["endpoint"] == key and a["type"] == alert["type"]]
            if not existing:
                self._alerts.append(alert)
                logger.warning("API alert: %s - %s", key, alert["message"])
