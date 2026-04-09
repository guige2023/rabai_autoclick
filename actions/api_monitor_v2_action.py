"""API Monitor v2 with metrics, alerting, and health tracking.

This module provides comprehensive API monitoring with:
- Real-time metrics collection
- Multi-level health checks
- Configurable alerting
- Latency percentile tracking
- Error rate monitoring
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class HealthStatus(Enum):
    """Health check status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class MetricPoint:
    """A single metric data point."""

    timestamp: float
    value: float
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class LatencyPercentiles:
    """Latency percentile values."""

    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    p999: float = 0.0


@dataclass
class Alert:
    """An alert notification."""

    level: AlertLevel
    message: str
    metric_name: str
    current_value: float
    threshold: float
    timestamp: float = field(default_factory=time.time)
    resolved: bool = False
    resolved_at: float | None = None


@dataclass
class HealthCheck:
    """A health check configuration."""

    name: str
    check_func: Callable[[], Awaitable[bool]]
    timeout: float = 5.0
    interval: float = 60.0
    consecutive_failures: int = 0
    failure_threshold: int = 3


@dataclass
class APIMetrics:
    """API call metrics."""

    endpoint: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeout_requests: int = 0
    total_latency: float = 0.0
    min_latency: float = float("inf")
    max_latency: float = 0.0
    latency_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    status_codes: dict[int, int] = field(default_factory=dict)
    error_codes: dict[str, int] = field(default_factory=dict)
    last_request_time: float = 0.0
    last_error: str | None = None

    def record_request(
        self,
        latency: float,
        status_code: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Record a request result."""
        self.total_requests += 1
        self.total_latency += latency
        self.min_latency = min(self.min_latency, latency)
        self.max_latency = max(self.max_latency, latency)
        self.latency_history.append(latency)
        self.last_request_time = time.time()

        if status_code:
            self.status_codes[status_code] = self.status_codes.get(status_code, 0) + 1

        if 200 <= (status_code or 0) < 300:
            self.successful_requests += 1
        elif status_code == 408 or isinstance(error, asyncio.TimeoutError):
            self.timeout_requests += 1
            self.failed_requests += 1
        elif status_code and status_code >= 400:
            self.failed_requests += 1
            if error:
                self.error_codes[str(error)] = self.error_codes.get(str(error), 0) + 1

        if error:
            self.last_error = str(error)

    def get_percentiles(self) -> LatencyPercentiles:
        """Calculate latency percentiles."""
        if not self.latency_history:
            return LatencyPercentiles()

        sorted_latencies = sorted(self.latency_history)
        n = len(sorted_latencies)

        def percentile(p: float) -> float:
            idx = int(n * p)
            if idx >= n:
                idx = n - 1
            return sorted_latencies[idx]

        return LatencyPercentiles(
            p50=percentile(0.50),
            p90=percentile(0.90),
            p95=percentile(0.95),
            p99=percentile(0.99),
            p999=percentile(0.999),
        )

    def get_success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    def get_error_rate(self) -> float:
        """Calculate error rate."""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests

    def get_avg_latency(self) -> float:
        """Calculate average latency."""
        if self.total_requests == 0:
            return 0.0
        return self.total_latency / self.total_requests


class AlertManager:
    """Manage and dispatch alerts."""

    def __init__(self):
        """Initialize alert manager."""
        self.alerts: dict[str, Alert] = {}
        self.handlers: list[Callable[[Alert], Awaitable[None]]] = []

    def add_handler(self, handler: Callable[[Alert], Awaitable[None]]) -> None:
        """Add an alert handler.

        Args:
            handler: Async function to handle alerts
        """
        self.handlers.append(handler)

    async def trigger(
        self,
        name: str,
        level: AlertLevel,
        message: str,
        metric_name: str,
        current_value: float,
        threshold: float,
    ) -> None:
        """Trigger an alert.

        Args:
            name: Alert identifier
            level: Alert severity
            message: Alert message
            metric_name: Name of the triggering metric
            current_value: Current metric value
            threshold: Threshold that was exceeded
        """
        existing = self.alerts.get(name)

        if existing and not existing.resolved:
            # Already alerted, don't repeat
            return

        alert = Alert(
            level=level,
            message=message,
            metric_name=metric_name,
            current_value=current_value,
            threshold=threshold,
        )

        self.alerts[name] = alert
        logger.log(
            self._log_level(level),
            f"ALERT [{level.value}] {name}: {message} (current={current_value}, threshold={threshold})",
        )

        # Dispatch to handlers
        for handler in self.handlers:
            try:
                await handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")

    async def resolve(self, name: str) -> None:
        """Resolve an alert.

        Args:
            name: Alert identifier
        """
        if name in self.alerts:
            self.alerts[name].resolved = True
            self.alerts[name].resolved_at = time.time()
            logger.info(f"Alert resolved: {name}")

    def get_active_alerts(self) -> list[Alert]:
        """Get all active (unresolved) alerts."""
        return [a for a in self.alerts.values() if not a.resolved]

    def _log_level(self, level: AlertLevel) -> int:
        """Convert alert level to logging level."""
        return {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.ERROR: logging.ERROR,
            AlertLevel.CRITICAL: logging.CRITICAL,
        }[level]


class HealthChecker:
    """Perform health checks on services."""

    def __init__(self):
        """Initialize health checker."""
        self.checks: dict[str, HealthCheck] = {}
        self.results: dict[str, HealthStatus] = {}
        self.last_check: dict[str, float] = {}

    def register_check(
        self,
        name: str,
        check_func: Callable[[], Awaitable[bool]],
        timeout: float = 5.0,
        interval: float = 60.0,
    ) -> None:
        """Register a health check.

        Args:
            name: Check name
            check_func: Async function returning True if healthy
            timeout: Check timeout
            interval: Check interval
        """
        self.checks[name] = HealthCheck(
            name=name,
            check_func=check_func,
            timeout=timeout,
            interval=interval,
        )

    async def run_check(self, name: str) -> HealthStatus:
        """Run a single health check.

        Args:
            name: Check name

        Returns:
            Health status
        """
        check = self.checks.get(name)
        if not check:
            return HealthStatus.UNHEALTHY

        try:
            result = await asyncio.wait_for(
                check.check_func(),
                timeout=check.timeout,
            )

            if result:
                check.consecutive_failures = 0
                self.results[name] = HealthStatus.HEALTHY
                return HealthStatus.HEALTHY
            else:
                check.consecutive_failures += 1
                if check.consecutive_failures >= check.failure_threshold:
                    self.results[name] = HealthStatus.UNHEALTHY
                else:
                    self.results[name] = HealthStatus.DEGRADED
                return self.results[name]

        except asyncio.TimeoutError:
            check.consecutive_failures += 1
            self.results[name] = HealthStatus.UNHEALTHY
            logger.warning(f"Health check timeout: {name}")

        except Exception as e:
            check.consecutive_failures += 1
            self.results[name] = HealthStatus.UNHEALTHY
            logger.warning(f"Health check failed: {name}: {e}")

        return self.results.get(name, HealthStatus.UNHEALTHY)

    async def run_all_checks(self) -> HealthStatus:
        """Run all health checks.

        Returns:
            Overall health status
        """
        results = []
        for name in self.checks:
            status = await self.run_check(name)
            results.append(status)
            self.last_check[name] = time.time()

        if all(r == HealthStatus.HEALTHY for r in results):
            return HealthStatus.HEALTHY
        elif any(r == HealthStatus.UNHEALTHY for r in results):
            return HealthStatus.UNHEALTHY
        return HealthStatus.DEGRADED

    def get_status(self) -> dict[str, Any]:
        """Get health check status."""
        return {
            name: {
                "status": self.results.get(name, HealthStatus.UNHEALTHY).value,
                "last_check": self.last_check.get(name),
                "consecutive_failures": self.checks[name].consecutive_failures if name in self.checks else 0,
            }
            for name in self.checks
        }


class APIMonitorV2:
    """Comprehensive API monitoring system."""

    def __init__(
        self,
        error_rate_threshold: float = 0.05,
        latency_p99_threshold: float = 1000.0,
        success_rate_threshold: float = 0.95,
    ):
        """Initialize API monitor.

        Args:
            error_rate_threshold: Error rate threshold for alerts
            latency_p99_threshold: P99 latency threshold in ms
            success_rate_threshold: Minimum success rate
        """
        self.error_rate_threshold = error_rate_threshold
        self.latency_p99_threshold = latency_p99_threshold
        self.success_rate_threshold = success_rate_threshold

        self.metrics: dict[str, APIMetrics] = {}
        self.alert_manager = AlertManager()
        self.health_checker = HealthChecker()
        self._alerting_tasks: dict[str, asyncio.Task] = {}

    def track_endpoint(self, endpoint: str) -> APIMetrics:
        """Get or create metrics for an endpoint.

        Args:
            endpoint: API endpoint path

        Returns:
            APIMetrics for the endpoint
        """
        if endpoint not in self.metrics:
            self.metrics[endpoint] = APIMetrics(endpoint=endpoint)
        return self.metrics[endpoint]

    async def record_call(
        self,
        endpoint: str,
        latency: float,
        status_code: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Record an API call.

        Args:
            endpoint: API endpoint
            latency: Call latency in ms
            status_code: HTTP status code
            error: Exception if call failed
        """
        metrics = self.track_endpoint(endpoint)
        metrics.record_request(latency, status_code, error)

        # Check alert conditions
        await self._check_alerts(endpoint, metrics)

    async def _check_alerts(self, endpoint: str, metrics: APIMetrics) -> None:
        """Check and trigger alerts based on metrics.

        Args:
            endpoint: API endpoint
            metrics: Endpoint metrics
        """
        alert_name = f"error_rate_{endpoint}"
        error_rate = metrics.get_error_rate()

        if error_rate > self.error_rate_threshold:
            await self.alert_manager.trigger(
                name=alert_name,
                level=AlertLevel.ERROR,
                message=f"High error rate on {endpoint}",
                metric_name="error_rate",
                current_value=error_rate,
                threshold=self.error_rate_threshold,
            )
        else:
            await self.alert_manager.resolve(alert_name)

        # Latency alert
        p99 = metrics.get_percentiles().p99
        latency_alert = f"latency_p99_{endpoint}"

        if p99 > self.latency_p99_threshold > 0:
            await self.alert_manager.trigger(
                name=latency_alert,
                level=AlertLevel.WARNING,
                message=f"High P99 latency on {endpoint}",
                metric_name="latency_p99",
                current_value=p99,
                threshold=self.latency_p99_threshold,
            )
        else:
            await self.alert_manager.resolve(latency_alert)

        # Success rate alert
        success_alert = f"success_rate_{endpoint}"
        success_rate = metrics.get_success_rate()

        if success_rate < self.success_rate_threshold:
            await self.alert_manager.trigger(
                name=success_alert,
                level=AlertLevel.ERROR,
                message=f"Low success rate on {endpoint}",
                metric_name="success_rate",
                current_value=success_rate,
                threshold=self.success_rate_threshold,
            )
        else:
            await self.alert_manager.resolve(success_alert)

    def register_health_check(
        self,
        name: str,
        check_func: Callable[[], Awaitable[bool]],
        timeout: float = 5.0,
        interval: float = 60.0,
    ) -> None:
        """Register a health check.

        Args:
            name: Check name
            check_func: Async function returning True if healthy
            timeout: Check timeout
            interval: Check interval
        """
        self.health_checker.register_check(name, check_func, timeout, interval)

    async def get_health_status(self) -> dict[str, Any]:
        """Get overall health status.

        Returns:
            Health status dictionary
        """
        health_status = await self.health_checker.run_all_checks()

        return {
            "overall": health_status.value,
            "endpoint_health": {
                endpoint: {
                    "success_rate": m.get_success_rate(),
                    "error_rate": m.get_error_rate(),
                    "avg_latency": m.get_avg_latency(),
                    "p99_latency": m.get_percentiles().p99,
                    "total_requests": m.total_requests,
                }
                for endpoint, m in self.metrics.items()
            },
            "health_checks": self.health_checker.get_status(),
            "active_alerts": len(self.alert_manager.get_active_alerts()),
        }

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get metrics summary for all endpoints.

        Returns:
            Metrics summary dictionary
        """
        return {
            endpoint: {
                "total_requests": m.total_requests,
                "successful_requests": m.successful_requests,
                "failed_requests": m.failed_requests,
                "timeout_requests": m.timeout_requests,
                "success_rate": m.get_success_rate(),
                "error_rate": m.get_error_rate(),
                "avg_latency": m.get_avg_latency(),
                "min_latency": m.min_latency if m.min_latency != float("inf") else 0,
                "max_latency": m.max_latency,
                "p50": m.get_percentiles().p50,
                "p90": m.get_percentiles().p90,
                "p95": m.get_percentiles().p95,
                "p99": m.get_percentiles().p99,
                "status_codes": m.status_codes,
                "last_request_time": m.last_request_time,
            }
            for endpoint, m in self.metrics.items()
        }

    def get_active_alerts(self) -> list[Alert]:
        """Get all active alerts."""
        return self.alert_manager.get_active_alerts()


def create_monitor(
    error_rate_threshold: float = 0.05,
    latency_threshold: float = 1000.0,
) -> APIMonitorV2:
    """Create a configured API monitor.

    Args:
        error_rate_threshold: Error rate threshold
        latency_threshold: Latency threshold in ms

    Returns:
        Configured APIMonitorV2 instance
    """
    return APIMonitorV2(
        error_rate_threshold=error_rate_threshold,
        latency_p99_threshold=latency_threshold,
    )
