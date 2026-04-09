"""
API Observability Action Module

Provides comprehensive observability for API services including metrics collection,
distributed tracing, health monitoring, and alerting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertState(Enum):
    """Alert states."""

    INACTIVE = "inactive"
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"


@dataclass
class Metric:
    """A metric data point."""

    metric_id: str
    name: str
    metric_type: MetricType
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class MetricSeries:
    """A time series of metrics."""

    name: str
    metric_type: MetricType
    labels: Dict[str, str]
    values: List[tuple[float, float]] = field(default_factory=list)
    counter_value: float = 0.0


@dataclass
class Alert:
    """An observability alert."""

    alert_id: str
    name: str
    condition: str
    threshold: float
    severity: str
    state: AlertState = AlertState.INACTIVE
    firing_value: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    fired_at: Optional[float] = None


@dataclass
class HealthCheck:
    """A health check definition."""

    check_id: str
    name: str
    checker: Callable[[], bool]
    interval_seconds: float = 30.0
    timeout_seconds: float = 5.0
    enabled: bool = True


@dataclass
class ObservabilityConfig:
    """Configuration for observability."""

    metrics_retention_seconds: float = 3600.0
    health_check_interval: float = 30.0
    alert_evaluation_interval: float = 10.0
    enable_alerting: bool = True
    enable_health_checks: bool = True


class MetricsCollector:
    """Collects and stores metrics."""

    def __init__(self, config: Optional[ObservabilityConfig] = None):
        self.config = config or ObservabilityConfig()
        self._series: Dict[str, MetricSeries] = {}

    def record_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a counter metric."""
        key = self._make_key(name, labels or {})
        if key not in self._series:
            self._series[key] = MetricSeries(
                name=name,
                metric_type=MetricType.COUNTER,
                labels=labels or {},
            )

        series = self._series[key]
        series.counter_value += value
        series.values.append((time.time(), series.counter_value))

    def record_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a gauge metric."""
        key = self._make_key(name, labels or {})
        if key not in self._series:
            self._series[key] = MetricSeries(
                name=name,
                metric_type=MetricType.GAUGE,
                labels=labels or {},
            )

        series = self._series[key]
        series.values.append((time.time(), value))

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a histogram metric."""
        key = self._make_key(name, labels or {})
        if key not in self._series:
            self._series[key] = MetricSeries(
                name=name,
                metric_type=MetricType.HISTOGRAM,
                labels=labels or {},
            )

        self._series[key].values.append((time.time(), value))

    def get_series(self, name: str) -> List[MetricSeries]:
        """Get all series for a metric name."""
        return [s for s in self._series.values() if s.name == name]

    @staticmethod
    def _make_key(name: str, labels: Dict[str, str]) -> str:
        """Create a unique key for a metric with labels."""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class AlertManager:
    """Manages alerting rules and evaluation."""

    def __init__(self, config: Optional[ObservabilityConfig] = None):
        self.config = config or ObservabilityConfig()
        self._alerts: Dict[str, Alert] = {}
        self._alert_handlers: List[Callable[[Alert], None]] = []

    def define_alert(
        self,
        name: str,
        condition: str,
        threshold: float,
        severity: str = "warning",
    ) -> Alert:
        """Define a new alert."""
        alert_id = f"alert_{uuid.uuid4().hex[:8]}"
        alert = Alert(
            alert_id=alert_id,
            name=name,
            condition=condition,
            threshold=threshold,
            severity=severity,
        )
        self._alerts[alert_id] = alert
        return alert

    def evaluate_alerts(self, metrics: MetricsCollector) -> List[Alert]:
        """Evaluate all alerts against current metrics."""
        firing_alerts = []

        for alert in self._alerts.values():
            series_list = metrics.get_series(alert.name)

            if not series_list:
                continue

            series = series_list[0]
            if not series.values:
                continue

            current_value = series.values[-1][1]

            should_fire = False
            if alert.condition == "above" and current_value > alert.threshold:
                should_fire = True
            elif alert.condition == "below" and current_value < alert.threshold:
                should_fire = True
            elif alert.condition == "equals" and abs(current_value - alert.threshold) < 0.001:
                should_fire = True

            if should_fire:
                if alert.state != AlertState.FIRING:
                    alert.state = AlertState.FIRING
                    alert.firing_value = current_value
                    alert.fired_at = time.time()
                    firing_alerts.append(alert)
                    for handler in self._alert_handlers:
                        handler(alert)
            else:
                if alert.state == AlertState.FIRING:
                    alert.state = AlertState.RESOLVED

        return firing_alerts

    def register_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register an alert handler."""
        self._alert_handlers.append(handler)


class APIObservabilityAction:
    """
    Observability action for API services.

    Features:
    - Multi-type metrics collection (counter, gauge, histogram)
    - Alert definition and evaluation
    - Health check monitoring
    - Time series data retention
    - Label-based metric organization
    - Alert state management

    Usage:
        obs = APIObservabilityAction(config)
        
        obs.record_counter("api_requests_total", labels={"method": "GET"})
        obs.record_histogram("api_request_duration_ms", 45.2)
        
        obs.define_alert("error_rate", "above", 0.05, severity="critical")
    """

    def __init__(self, config: Optional[ObservabilityConfig] = None):
        self.config = config or ObservabilityConfig()
        self._metrics = MetricsCollector(self.config)
        self._alert_manager = AlertManager(self.config)
        self._health_checks: Dict[str, HealthCheck] = {}
        self._health_status: Dict[str, bool] = {}
        self._stats = {
            "metrics_recorded": 0,
            "alerts_fired": 0,
            "health_checks_run": 0,
        }

    def record_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a counter metric."""
        self._metrics.record_counter(name, value, labels)
        self._stats["metrics_recorded"] += 1

    def record_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a gauge metric."""
        self._metrics.record_gauge(name, value, labels)
        self._stats["metrics_recorded"] += 1

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a histogram metric."""
        self._metrics.record_histogram(name, value, labels)
        self._stats["metrics_recorded"] += 1

    def define_alert(
        self,
        name: str,
        condition: str,
        threshold: float,
        severity: str = "warning",
    ) -> Alert:
        """Define a new alert rule."""
        return self._alert_manager.define_alert(name, condition, threshold, severity)

    def register_health_check(
        self,
        name: str,
        checker: Callable[[], bool],
        interval_seconds: float = 30.0,
    ) -> HealthCheck:
        """Register a health check."""
        check_id = f"health_{uuid.uuid4().hex[:8]}"
        check = HealthCheck(
            check_id=check_id,
            name=name,
            checker=checker,
            interval_seconds=interval_seconds,
        )
        self._health_checks[check_id] = check
        return check

    async def run_health_checks(self) -> Dict[str, bool]:
        """Run all health checks."""
        results = {}
        for check_id, check in self._health_checks.items():
            if not check.enabled:
                continue

            try:
                if asyncio.iscoroutinefunction(check.checker):
                    result = await asyncio.wait_for(
                        check.checker(),
                        timeout=check.timeout_seconds,
                    )
                else:
                    result = check.checker()

                results[check.name] = result
                self._health_status[check_id] = result
            except Exception as e:
                logger.error(f"Health check {check.name} failed: {e}")
                results[check.name] = False
                self._health_status[check_id] = False

            self._stats["health_checks_run"] += 1

        return results

    def get_health_status(self) -> Dict[str, bool]:
        """Get current health status of all checks."""
        return {check.name: self._health_status.get(check.check_id, False)
                for check in self._health_checks.values()}

    def get_metric_series(self, name: str) -> List[MetricSeries]:
        """Get time series for a metric."""
        return self._metrics.get_series(name)

    def evaluate_alerts(self) -> List[Alert]:
        """Evaluate all alert rules."""
        firing = self._alert_manager.evaluate_alerts(self._metrics)
        self._stats["alerts_fired"] += len(firing)
        return firing

    def get_stats(self) -> Dict[str, Any]:
        """Get observability statistics."""
        return {
            **self._stats.copy(),
            "total_metrics_series": len(self._metrics._series),
            "total_alerts": len(self._alert_manager._alerts),
            "total_health_checks": len(self._health_checks),
        }


async def demo_observability():
    """Demonstrate observability."""
    config = ObservabilityConfig()
    obs = APIObservabilityAction(config)

    obs.record_counter("api_requests_total", labels={"method": "GET", "status": "200"})
    obs.record_counter("api_requests_total", labels={"method": "POST", "status": "201"})
    obs.record_histogram("api_request_duration_ms", 45.2, labels={"endpoint": "/users"})
    obs.record_gauge("active_connections", 150)

    obs.define_alert("error_rate", "above", 0.05, severity="critical")

    alerts = obs.evaluate_alerts()
    print(f"Alerts firing: {len(alerts)}")

    obs.define_alert("latency_p99", "above", 500, severity="warning")

    print(f"Stats: {obs.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_observability())
