"""
API Monitor Action Module.

Provides request/response monitoring, metrics collection,
health checks, and alerting capabilities.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import statistics


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Metric:
    """Single metric."""
    name: str
    metric_type: MetricType
    value: float = 0.0
    count: int = 0
    min_value: float = float("inf")
    max_value: float = float("-inf")
    sum_values: float = 0.0
    labels: dict = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)


@dataclass
class HealthCheck:
    """Health check definition."""
    name: str
    check_func: Callable[[], bool]
    interval: float = 60.0
    timeout: float = 5.0
    critical: bool = False


@dataclass
class HealthStatus:
    """Health check status."""
    name: str
    healthy: bool
    latency: float = 0.0
    error: Optional[str] = None
    last_check: float = field(default_factory=time.time)


@dataclass
class MonitorConfig:
    """Monitor configuration."""
    enable_metrics: bool = True
    enable_health_checks: bool = True
    metrics_window: int = 300
    alert_threshold: float = 0.95


class MetricsCollector:
    """Collects and manages metrics."""

    def __init__(self, window_size: int = 300):
        self.window_size = window_size
        self._metrics: dict[str, Metric] = {}
        self._lock = asyncio.Lock()

    async def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict] = None
    ) -> None:
        """Increment counter."""
        async with self._lock:
            key = self._make_key(name, labels)
            if key not in self._metrics:
                self._metrics[key] = Metric(
                    name=name,
                    metric_type=MetricType.COUNTER,
                    labels=labels or {}
                )

            metric = self._metrics[key]
            metric.value += value
            metric.count += 1
            metric.last_updated = time.time()

    async def record(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: Optional[dict] = None
    ) -> None:
        """Record metric value."""
        async with self._lock:
            key = self._make_key(name, labels)
            if key not in self._metrics:
                self._metrics[key] = Metric(
                    name=name,
                    metric_type=metric_type,
                    labels=labels or {}
                )

            metric = self._metrics[key]
            metric.value = value
            metric.count += 1
            metric.last_updated = time.time()

            if metric_type == MetricType.HISTOGRAM:
                metric.min_value = min(metric.min_value, value)
                metric.max_value = max(metric.max_value, value)
                metric.sum_values += value

    async def get_metric(self, name: str, labels: Optional[dict] = None) -> Optional[Metric]:
        """Get metric by name."""
        async with self._lock:
            key = self._make_key(name, labels)
            return self._metrics.get(key)

    async def get_all_metrics(self) -> list[Metric]:
        """Get all metrics."""
        async with self._lock:
            return list(self._metrics.values())

    def _make_key(self, name: str, labels: Optional[dict]) -> str:
        """Make metric key."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class HealthChecker:
    """Performs health checks."""

    def __init__(self, checks: list[HealthCheck]):
        self.checks = checks
        self._statuses: dict[str, HealthStatus] = {}
        self._running = False
        self._lock = asyncio.Lock()

    async def check_health(self, check: HealthCheck) -> HealthStatus:
        """Perform single health check."""
        start = time.monotonic()
        status = HealthStatus(name=check.name, healthy=False, latency=0.0)

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(check.check_func),
                timeout=check.timeout
            )
            status.healthy = result
            status.latency = time.monotonic() - start
        except asyncio.TimeoutError:
            status.error = f"Timeout after {check.timeout}s"
            status.latency = check.timeout
        except Exception as e:
            status.error = str(e)
            status.latency = time.monotonic() - start

        status.last_check = time.time()
        return status

    async def run_checks(self) -> dict[str, HealthStatus]:
        """Run all health checks."""
        async with self._lock:
            tasks = [self.check_health(check) for check in self.checks]
            results = await asyncio.gather(*tasks)

            for status in results:
                self._statuses[status.name] = status

            return self._statuses.copy()

    async def start_monitoring(self) -> None:
        """Start continuous health monitoring."""
        self._running = True
        while self._running:
            await self.run_checks()
            await asyncio.sleep(1.0)

    def stop_monitoring(self) -> None:
        """Stop monitoring."""
        self._running = False


class AlertManager:
    """Manages alerts."""

    def __init__(self, threshold: float = 0.95):
        self.threshold = threshold
        self._alerts: list[dict] = []
        self._handlers: list[Callable] = []

    def add_handler(self, handler: Callable[[dict], None]) -> None:
        """Add alert handler."""
        self._handlers.append(handler)

    async def trigger_alert(
        self,
        metric_name: str,
        message: str,
        severity: str = "warning"
    ) -> None:
        """Trigger an alert."""
        alert = {
            "metric": metric_name,
            "message": message,
            "severity": severity,
            "timestamp": time.time()
        }
        self._alerts.append(alert)

        for handler in self._handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(alert)
                else:
                    handler(alert)
            except Exception:
                pass

    def get_alerts(self, since: Optional[float] = None) -> list[dict]:
        """Get alerts since timestamp."""
        if since is None:
            return self._alerts.copy()
        return [a for a in self._alerts if a["timestamp"] >= since]

    def clear_alerts(self) -> None:
        """Clear all alerts."""
        self._alerts.clear()


class APIMonitorAction:
    """
    API monitoring with metrics and health checks.

    Example:
        monitor = APIMonitorAction()

        monitor.record_request("GET", "/api/users", 200, 45.2)
        monitor.record_request("POST", "/api/orders", 201, 123.4)

        health = await monitor.check_health()
        metrics = monitor.get_metrics()
    """

    def __init__(self, config: Optional[MonitorConfig] = None):
        self.config = config or MonitorConfig()
        self._metrics = MetricsCollector(self.config.metrics_window)
        self._health_checks: list[HealthCheck] = []
        self._health_checker: Optional[HealthChecker] = None
        self._alert_manager = AlertManager(self.config.alert_threshold)

    def add_health_check(
        self,
        name: str,
        check_func: Callable[[], bool],
        interval: float = 60.0,
        critical: bool = False
    ) -> None:
        """Add health check."""
        check = HealthCheck(
            name=name,
            check_func=check_func,
            interval=interval,
            critical=critical
        )
        self._health_checks.append(check)
        if self._health_checker:
            self._health_checker.checks = self._health_checks

    async def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float
    ) -> None:
        """Record API request."""
        labels = {"method": method, "path": path, "status": str(status_code)}
        await self._metrics.increment("api_requests_total", 1.0, labels)
        await self._metrics.record(
            "api_request_duration_ms",
            latency_ms,
            MetricType.HISTOGRAM,
            labels
        )

        if status_code >= 500:
            await self._alert_manager.trigger_alert(
                "api_requests_total",
                f"High 5xx error rate: {status_code}",
                "critical"
            )

    async def record_response_size(
        self,
        path: str,
        size_bytes: int
    ) -> None:
        """Record response size."""
        await self._metrics.record(
            "api_response_size_bytes",
            float(size_bytes),
            MetricType.HISTOGRAM,
            {"path": path}
        )

    async def get_metrics(self) -> dict[str, Any]:
        """Get all metrics."""
        metrics = await self._metrics.get_all_metrics()
        return {
            "metrics": [
                {
                    "name": m.name,
                    "type": m.metric_type.value,
                    "value": m.value,
                    "count": m.count,
                    "labels": m.labels
                }
                for m in metrics
            ]
        }

    async def get_metric_summary(self, name: str) -> Optional[dict]:
        """Get metric summary."""
        metric = await self._metrics.get_metric(name)
        if not metric:
            return None

        result = {
            "name": metric.name,
            "count": metric.count,
            "value": metric.value
        }

        if metric.metric_type == MetricType.HISTOGRAM:
            if metric.count > 0:
                avg = metric.sum_values / metric.count
                result.update({
                    "min": metric.min_value,
                    "max": metric.max_value,
                    "avg": avg,
                    "p50": metric.min_value,
                    "p95": metric.max_value * 0.95,
                    "p99": metric.max_value * 0.99
                })

        return result

    async def check_health(self) -> dict[str, HealthStatus]:
        """Run health checks."""
        if not self._health_checker:
            self._health_checker = HealthChecker(self._health_checks)
        return await self._health_checker.run_checks()

    def add_alert_handler(self, handler: Callable) -> None:
        """Add alert handler."""
        self._alert_manager.add_handler(handler)
