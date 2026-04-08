"""
API Metrics Action Module.

Collects and aggregates API metrics including latency, throughput,
 error rates, and provides alerting on anomalies.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import statistics
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Type of metric."""
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    AVAILABILITY = "availability"


@dataclass
class APIMetric:
    """A single API metric observation."""
    endpoint: str
    method: str
    metric_type: MetricType
    value: float
    status_code: int
    timestamp: float = field(default_factory=time.time)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class EndpointMetrics:
    """Aggregated metrics for an endpoint."""
    endpoint: str
    method: str
    total_requests: int = 0
    error_count: int = 0
    avg_latency_ms: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    min_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    error_rate: float = 0.0
    requests_per_second: float = 0.0


@dataclass
class MetricsAlert:
    """Alert triggered by metric threshold."""
    endpoint: str
    metric_type: MetricType
    threshold: float
    current_value: float
    message: str


class APIMetricsAction:
    """
    API metrics collection and alerting system.

    Tracks request metrics, computes percentiles, detects anomalies,
    and triggers alerts when thresholds are exceeded.

    Example:
        metrics = APIMetricsAction()
        metrics.record_request("/api/users", "GET", 45.2, 200)
        metrics.add_alert_threshold("p95_latency", 500.0, "error")
        alerts = metrics.check_alerts()
    """

    def __init__(
        self,
        window_size: int = 10000,
        alert_callback: Optional[Callable[[MetricsAlert], None]] = None,
    ) -> None:
        self.window_size = window_size
        self.alert_callback = alert_callback
        self._metrics: deque = deque(maxlen=window_size)
        self._alert_thresholds: list[dict[str, Any]] = []
        self._endpoint_metrics: dict[tuple[str, str], deque] = {}

    def record_request(
        self,
        endpoint: str,
        method: str,
        latency_ms: float,
        status_code: int,
        tags: Optional[dict[str, str]] = None,
    ) -> APIMetric:
        """Record a single API request."""
        metric = APIMetric(
            endpoint=endpoint,
            method=method,
            metric_type=MetricType.LATENCY,
            value=latency_ms,
            status_code=status_code,
            tags=tags or {},
        )

        self._metrics.append(metric)

        key = (endpoint, method)
        if key not in self._endpoint_metrics:
            self._endpoint_metrics[key] = deque(maxlen=self.window_size)
        self._endpoint_metrics[key].append(metric)

        return metric

    def add_alert_threshold(
        self,
        metric_name: str,
        threshold: float,
        severity: str = "warning",
        comparison: str = "gt",
    ) -> "APIMetricsAction":
        """Add an alert threshold for a metric."""
        self._alert_thresholds.append({
            "metric": metric_name,
            "threshold": threshold,
            "severity": severity,
            "comparison": comparison,
        })
        return self

    def get_endpoint_metrics(
        self,
        endpoint: str,
        method: str,
        window_seconds: Optional[float] = None,
    ) -> EndpointMetrics:
        """Get aggregated metrics for an endpoint."""
        now = time.time()
        window = window_seconds or 300

        key = (endpoint, method)
        metrics = self._endpoint_metrics.get(key, [])

        recent = [
            m for m in metrics
            if (now - m.timestamp) <= window
        ]

        if not recent:
            return EndpointMetrics(endpoint=endpoint, method=method)

        latencies = [m.value for m in recent if m.metric_type == MetricType.LATENCY]
        errors = [m for m in recent if m.status_code >= 400]

        sorted_latencies = sorted(latencies)
        total_requests = len(recent)

        return EndpointMetrics(
            endpoint=endpoint,
            method=method,
            total_requests=total_requests,
            error_count=len(errors),
            avg_latency_ms=statistics.mean(latencies) if latencies else 0,
            p50_latency_ms=sorted_latencies[int(len(sorted_latencies) * 0.5)] if sorted_latencies else 0,
            p95_latency_ms=sorted_latencies[int(len(sorted_latencies) * 0.95)] if sorted_latencies else 0,
            p99_latency_ms=sorted_latencies[int(len(sorted_latencies) * 0.99)] if sorted_latencies else 0,
            min_latency_ms=min(latencies) if latencies else 0,
            max_latency_ms=max(latencies) if latencies else 0,
            error_rate=len(errors) / total_requests if total_requests > 0 else 0,
            requests_per_second=total_requests / window if window > 0 else 0,
        )

    def check_alerts(
        self,
        endpoint: Optional[str] = None,
    ) -> list[MetricsAlert]:
        """Check for any metric alerts."""
        alerts: list[MetricsAlert] = []

        endpoints = [(endpoint, "*")] if endpoint else list(self._endpoint_metrics.keys())

        for ep, method in endpoints:
            metrics = self.get_endpoint_metrics(ep, method)

            for threshold_config in self._alert_thresholds:
                metric_name = threshold_config["metric"]
                threshold = threshold_config["threshold"]
                comparison = threshold_config.get("comparison", "gt")

                current_value = getattr(metrics, metric_name, 0)

                triggered = False
                if comparison == "gt" and current_value > threshold:
                    triggered = True
                elif comparison == "lt" and current_value < threshold:
                    triggered = True
                elif comparison == "eq" and abs(current_value - threshold) < 0.001:
                    triggered = True

                if triggered:
                    alert = MetricsAlert(
                        endpoint=ep,
                        metric_type=MetricType.LATENCY,
                        threshold=threshold,
                        current_value=current_value,
                        message=f"{metric_name} {comparison} {threshold}: current={current_value:.2f}",
                    )
                    alerts.append(alert)

                    if self.alert_callback:
                        self.alert_callback(alert)

        return alerts

    def get_all_metrics(
        self,
        window_seconds: Optional[float] = None,
    ) -> dict[str, EndpointMetrics]:
        """Get metrics for all endpoints."""
        result: dict[str, EndpointMetrics] = {}

        for (endpoint, method) in self._endpoint_metrics.keys():
            metrics = self.get_endpoint_metrics(endpoint, method)
            key = f"{method} {endpoint}"
            result[key] = metrics

        return result

    def export_prometheus_format(self) -> str:
        """Export metrics in Prometheus exposition format."""
        lines: list[str] = []

        for (endpoint, method), metrics_deque in self._endpoint_metrics.items():
            endpoint_clean = endpoint.replace(".", "_").replace("/", "_")

            recent = list(metrics_deque)[-100:]
            if not recent:
                continue

            latencies = [m.value for m in recent]
            errors = sum(1 for m in recent if m.status_code >= 400)

            lines.append(f"# HELP api_requests_total Total API requests")
            lines.append(f"# TYPE api_requests_total counter")
            lines.append(f'api_requests_total{{endpoint="{endpoint}",method="{method}"}} {len(recent)}')

            lines.append(f"# HELP api_latency_ms API request latency")
            lines.append(f"# TYPE api_latency_ms gauge")
            lines.append(f'api_latency_ms{{endpoint="{endpoint}",method="{method}",quantile="avg"}} {statistics.mean(latencies) if latencies else 0}')
            lines.append(f'api_latency_ms{{endpoint="{endpoint}",method="{method}",quantile="p95"}} {sorted(latencies)[int(len(latencies)*0.95)] if latencies else 0}')

            lines.append(f"# HELP api_errors_total Total API errors")
            lines.append(f"# TYPE api_errors_total counter")
            lines.append(f'api_errors_total{{endpoint="{endpoint}",method="{method}"}} {errors}')

        return "\n".join(lines)
