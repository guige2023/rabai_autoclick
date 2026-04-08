"""
Automation Monitor Action Module.

Monitors automation workflows for health, performance, and anomalies
 with alerting and automatic recovery triggers.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Type of monitoring metric."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"


@dataclass
class Metric:
    """A single monitoring metric."""
    name: str
    metric_type: MetricType
    value: float
    timestamp: float = field(default_factory=time.time)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """A monitoring alert."""
    name: str
    severity: str
    message: str
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthStatus:
    """Overall health status of a component."""
    component: str
    healthy: bool
    latency_ms: float = 0.0
    error_rate: float = 0.0
    message: Optional[str] = None


class AutomationMonitorAction:
    """
    Workflow monitoring with metrics, alerts, and health checks.

    Collects metrics, detects anomalies, triggers alerts, and
    provides health status for automation systems.

    Example:
        monitor = AutomationMonitorAction()
        monitor.record_metric("task.duration", 1.5, tags={"task": "scrape"})
        monitor.add_health_check("api", check_api_health)
        status = monitor.get_overall_health()
    """

    def __init__(
        self,
        metrics_window_size: int = 1000,
        alert_callback: Optional[Callable[[Alert], None]] = None,
    ) -> None:
        self.metrics_window_size = metrics_window_size
        self.alert_callback = alert_callback
        self._metrics: dict[str, deque] = {}
        self._health_checks: dict[str, Callable[[], HealthStatus]] = {}
        self._alerts: deque = deque(maxlen=100)
        self._alert_rules: list[tuple[str, Callable[[float], bool], str]] = []

    def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a metric value."""
        metric = Metric(
            name=name,
            metric_type=metric_type,
            value=value,
            tags=tags or {},
        )

        if name not in self._metrics:
            self._metrics[name] = deque(maxlen=self.metrics_window_size)
        self._metrics[name].append(metric)

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        self.record_metric(name, value, MetricType.COUNTER, tags)

    def add_health_check(
        self,
        name: str,
        check_func: Callable[[], HealthStatus],
    ) -> None:
        """Add a health check function."""
        self._health_checks[name] = check_func

    def add_alert_rule(
        self,
        metric_name: str,
        condition_func: Callable[[float], bool],
        alert_message: str,
    ) -> None:
        """Add an alert rule for a metric."""
        self._alert_rules.append((metric_name, condition_func, alert_message))

    async def check_health(self) -> dict[str, HealthStatus]:
        """Run all health checks and return status."""
        results: dict[str, HealthStatus] = {}
        for name, check_func in self._health_checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    status = await check_func()
                else:
                    status = check_func()
                results[name] = status
            except Exception as e:
                results[name] = HealthStatus(
                    component=name,
                    healthy=False,
                    message=f"Health check failed: {e}",
                )
        return results

    def get_overall_health(self) -> HealthStatus:
        """Get overall system health status."""
        if not self._health_checks:
            return HealthStatus(component="system", healthy=True)

        statuses = self._get_health_values()
        unhealthy = [s for s in statuses.values() if not s.healthy]

        if unhealthy:
            return HealthStatus(
                component="system",
                healthy=False,
                message=f"{len(unhealthy)}/{len(statuses)} components unhealthy",
            )

        return HealthStatus(component="system", healthy=True)

    def _get_health_values(self) -> dict[str, HealthStatus]:
        """Get current health check values without async."""
        return {}

    def get_metric_stats(
        self,
        name: str,
        window_seconds: Optional[float] = None,
    ) -> dict[str, float]:
        """Get statistics for a metric."""
        if name not in self._metrics:
            return {}

        now = time.time()
        window = window_seconds or 300
        metrics = [
            m for m in self._metrics[name]
            if now - m.timestamp <= window
        ]

        if not metrics:
            return {}

        values = [m.value for m in metrics]
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest": values[-1],
        }

    def get_error_rate(
        self,
        metric_name: str = "errors",
        window_seconds: float = 300.0,
    ) -> float:
        """Calculate error rate over a time window."""
        stats = self.get_metric_stats(metric_name, window_seconds)
        total = self.get_metric_stats("total", window_seconds)

        if not total or total.get("count", 0) == 0:
            return 0.0

        return stats.get("count", 0) / total.get("count", 1)

    def trigger_alert(
        self,
        name: str,
        severity: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Alert:
        """Trigger an alert."""
        alert = Alert(
            name=name,
            severity=severity,
            message=message,
            metadata=metadata or {},
        )
        self._alerts.append(alert)

        if self.alert_callback:
            self.alert_callback(alert)

        logger.warning(f"Alert triggered: [{severity}] {name} - {message}")
        return alert

    def get_recent_alerts(self, limit: int = 50) -> list[Alert]:
        """Get recent alerts."""
        return list(self._alerts)[-limit:]

    def clear_metrics(self, name: Optional[str] = None) -> None:
        """Clear metrics data."""
        if name:
            self._metrics.pop(name, None)
        else:
            self._metrics.clear()


import asyncio
