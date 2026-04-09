"""
Automation Monitor Action Module.

Real-time monitoring and observability for automation
workflows with metrics, alerts, and health checks.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Metric:
    """
    Metric definition.

    Attributes:
        name: Metric name.
        metric_type: Type of metric.
        value: Current value.
        labels: Label/tag dictionary.
        timestamp: Last update time.
    """
    name: str
    metric_type: MetricType
    value: float = 0.0
    labels: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time, init=False)


@dataclass
class Alert:
    """
    Alert definition.

    Attributes:
        name: Alert name.
        condition: Callable that returns True when alert fires.
        severity: Alert severity level.
        message: Alert message template.
        cooldown: Cooldown period in seconds.
    """
    name: str
    condition: Callable[[], bool]
    severity: str = "warning"
    message: str = ""
    cooldown: float = 300.0
    _last_fired: float = field(default=0.0, init=False)


@dataclass
class HealthCheck:
    """Health check definition."""
    name: str
    check_func: Callable[[], bool]
    interval: float = 60.0
    timeout: float = 10.0


@dataclass
class MonitorStats:
    """Monitoring statistics."""
    uptime: float
    metrics_count: int
    alerts_triggered: int
    health_checks_passed: int
    health_checks_failed: int


class AutomationMonitorAction:
    """
    Real-time monitoring for automation workflows.

    Example:
        monitor = AutomationMonitorAction()
        monitor.increment_counter("tasks_completed")
        monitor.set_gauge("active_workers", 5)
        monitor.record_histogram("task_duration", 1.234)
        stats = monitor.get_stats()
    """

    def __init__(self, name: str = "automation"):
        """
        Initialize automation monitor.

        Args:
            name: Monitor identifier.
        """
        self.name = name
        self._metrics: dict[str, Metric] = {}
        self._counters: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._alerts: list[Alert] = []
        self._health_checks: list[HealthCheck] = {}
        self._start_time = time.time()
        self._alert_history: list[dict] = []

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict] = None
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name.
            value: Increment value.
            labels: Optional labels.
        """
        key = self._metric_key(name, labels)
        self._counters[key] = self._counters.get(key, 0.0) + value

        self._metrics[key] = Metric(
            name=name,
            metric_type=MetricType.COUNTER,
            value=self._counters[key],
            labels=labels or {}
        )

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None
    ) -> None:
        """
        Set a gauge metric value.

        Args:
            name: Metric name.
            value: Gauge value.
            labels: Optional labels.
        """
        key = self._metric_key(name, labels)

        self._metrics[key] = Metric(
            name=name,
            metric_type=MetricType.GAUGE,
            value=value,
            labels=labels or {}
        )

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None
    ) -> None:
        """
        Record a histogram value.

        Args:
            name: Metric name.
            value: Value to record.
            labels: Optional labels.
        """
        key = self._metric_key(name, labels)

        if key not in self._histograms:
            self._histograms[key] = []

        self._histograms[key].append(value)

        metric = self._metrics.get(key)
        if metric:
            metric.value = value
        else:
            self._metrics[key] = Metric(
                name=name,
                metric_type=MetricType.HISTOGRAM,
                value=value,
                labels=labels or {}
            )

    def start_timer(self, name: str, labels: Optional[dict] = None) -> float:
        """
        Start a timer and return start time.

        Args:
            name: Metric name.
            labels: Optional labels.

        Returns:
            Start timestamp.
        """
        return time.time()

    def stop_timer(
        self,
        name: str,
        start_time: float,
        labels: Optional[dict] = None
    ) -> None:
        """
        Stop a timer and record duration.

        Args:
            name: Metric name.
            start_time: Start timestamp from start_timer.
            labels: Optional labels.
        """
        duration = time.time() - start_time
        self.record_histogram(f"{name}_duration", duration, labels)

    def register_alert(
        self,
        name: str,
        condition: Callable[[], bool],
        severity: str = "warning",
        message: str = "",
        cooldown: float = 300.0
    ) -> None:
        """
        Register an alert.

        Args:
            name: Alert name.
            condition: Function that returns True when alert fires.
            severity: Alert severity (info/warning/error/critical).
            message: Alert message template.
            cooldown: Minimum time between alerts.
        """
        alert = Alert(
            name=name,
            condition=condition,
            severity=severity,
            message=message,
            cooldown=cooldown
        )
        self._alerts.append(alert)
        logger.debug(f"Registered alert: {name}")

    def register_health_check(
        self,
        name: str,
        check_func: Callable[[], bool],
        interval: float = 60.0,
        timeout: float = 10.0
    ) -> None:
        """
        Register a health check.

        Args:
            name: Health check name.
            check_func: Function returning True if healthy.
            interval: Check interval in seconds.
            timeout: Check timeout in seconds.
        """
        self._health_checks[name] = HealthCheck(
            name=name,
            check_func=check_func,
            interval=interval,
            timeout=timeout
        )
        logger.debug(f"Registered health check: {name}")

    async def check_alerts(self) -> list[dict]:
        """
        Evaluate all registered alerts.

        Returns:
            List of fired alerts with details.
        """
        fired_alerts = []
        now = time.time()

        for alert in self._alerts:
            try:
                should_fire = alert.condition()

                if should_fire and (now - alert._last_fired) > alert.cooldown:
                    alert_record = {
                        "name": alert.name,
                        "severity": alert.severity,
                        "message": alert.message,
                        "timestamp": now
                    }

                    fired_alerts.append(alert_record)
                    self._alert_history.append(alert_record)
                    alert._last_fired = now

                    logger.warning(f"Alert fired: {alert.name} - {alert.message}")

            except Exception as e:
                logger.error(f"Alert check failed for {alert.name}: {e}")

        return fired_alerts

    async def check_health(self) -> dict:
        """
        Execute all health checks.

        Returns:
            Dict with health check results.
        """
        results = {
            "healthy": True,
            "checks": {}
        }

        for name, check in self._health_checks.items():
            try:
                start = time.time()

                if asyncio.iscoroutinefunction(check.check_func):
                    result = await asyncio.wait_for(
                        check.check_func(),
                        timeout=check.timeout
                    )
                else:
                    result = check.check_func()

                duration = time.time() - start

                results["checks"][name] = {
                    "status": "passed" if result else "failed",
                    "duration": duration
                }

                if not result:
                    results["healthy"] = False

            except asyncio.TimeoutError:
                results["checks"][name] = {
                    "status": "timeout",
                    "duration": check.timeout
                }
                results["healthy"] = False

            except Exception as e:
                results["checks"][name] = {
                    "status": "error",
                    "error": str(e)
                }
                results["healthy"] = False

        return results

    def get_metric(
        self,
        name: str,
        labels: Optional[dict] = None
    ) -> Optional[Metric]:
        """Get metric by name."""
        key = self._metric_key(name, labels)
        return self._metrics.get(key)

    def get_all_metrics(self) -> list[Metric]:
        """Get all registered metrics."""
        return list(self._metrics.values())

    def get_histogram_stats(self, name: str, labels: Optional[dict] = None) -> dict:
        """
        Get histogram statistics.

        Args:
            name: Histogram name.
            labels: Optional labels.

        Returns:
            Dict with min, max, mean, p50, p95, p99.
        """
        key = self._metric_key(name, labels)
        values = self._histograms.get(key, [])

        if not values:
            return {}

        sorted_values = sorted(values)
        count = len(sorted_values)

        return {
            "count": count,
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "mean": sum(sorted_values) / count,
            "p50": sorted_values[int(count * 0.50)],
            "p95": sorted_values[int(count * 0.95)] if count > 1 else sorted_values[0],
            "p99": sorted_values[int(count * 0.99)] if count > 1 else sorted_values[0],
        }

    def get_stats(self) -> MonitorStats:
        """Get monitoring statistics."""
        health_results = asyncio.get_event_loop().run_until_complete(self.check_health())

        passed = sum(1 for r in health_results["checks"].values() if r["status"] == "passed")
        failed = len(health_results["checks"]) - passed

        return MonitorStats(
            uptime=time.time() - self._start_time,
            metrics_count=len(self._metrics),
            alerts_triggered=len(self._alert_history),
            health_checks_passed=passed,
            health_checks_failed=failed
        )

    def _metric_key(self, name: str, labels: Optional[dict]) -> str:
        """Generate metric key from name and labels."""
        if not labels:
            return name

        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def clear_metrics(self) -> None:
        """Clear all metrics."""
        self._metrics.clear()
        self._counters.clear()
        self._histograms.clear()

    def clear_alert_history(self) -> None:
        """Clear alert history."""
        self._alert_history.clear()
