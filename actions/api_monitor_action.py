"""
API Monitor Action Module

API endpoint monitoring, health checks, latency tracking,
alerting, and SLA reporting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    endpoint: str
    status: HealthStatus
    response_time_ms: float
    status_code: Optional[int] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SLAMetric:
    """SLA metric data point."""

    timestamp: datetime
    uptime_percent: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate_percent: float
    total_requests: int


@dataclass
class Alert:
    """Alert notification."""

    level: AlertLevel
    message: str
    endpoint: str
    triggered_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EndpointConfig:
    """Configuration for an endpoint monitor."""

    name: str
    url: str
    method: str = "GET"
    expected_status: int = 200
    timeout_seconds: float = 30.0
    check_interval_seconds: float = 60.0
    latency_threshold_ms: float = 1000.0
    error_rate_threshold_percent: float = 5.0
    consecutive_failures_threshold: int = 3
    headers: Dict[str, str] = field(default_factory=dict)


class LatencyTracker:
    """Tracks latency metrics with rolling window."""

    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self._latencies: deque = deque(maxlen=window_size)

    def add(self, latency_ms: float) -> None:
        """Add a latency measurement."""
        self._latencies.append(latency_ms)

    def get_stats(self) -> Dict[str, float]:
        """Get latency statistics."""
        if not self._latencies:
            return {"min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}

        sorted_latencies = sorted(self._latencies)
        n = len(sorted_latencies)

        return {
            "min": sorted_latencies[0],
            "max": sorted_latencies[-1],
            "avg": sum(sorted_latencies) / n,
            "p50": sorted_latencies[int(n * 0.5)],
            "p95": sorted_latencies[int(n * 0.95)],
            "p99": sorted_latencies[min(int(n * 0.99), n - 1)],
        }


class HealthChecker:
    """Performs health checks on endpoints."""

    def __init__(self, config: EndpointConfig):
        self.config = config
        self._latency_tracker = LatencyTracker()

    async def check(self) -> HealthCheckResult:
        """Perform health check."""
        import httpx

        start_time = time.time()

        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=self.config.method,
                    url=self.config.url,
                    headers=self.config.headers,
                    timeout=self.config.timeout_seconds,
                )

            response_time_ms = (time.time() - start_time) * 1000
            self._latency_tracker.add(response_time_ms)

            is_healthy = response.status_code == self.config.expected_status
            status = HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY

            return HealthCheckResult(
                endpoint=self.config.name,
                status=status,
                response_time_ms=response_time_ms,
                status_code=response.status_code,
            )

        except asyncio.TimeoutError:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                endpoint=self.config.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time_ms,
                error="Request timeout",
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                endpoint=self.config.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time_ms,
                error=f"{type(e).__name__}: {str(e)}",
            )

    def get_latency_stats(self) -> Dict[str, float]:
        """Get latency statistics."""
        return self._latency_tracker.get_stats()


class AlertManager:
    """Manages alerts and notifications."""

    def __init__(self):
        self._handlers: List[Callable[[Alert], None]] = []
        self._alert_history: deque = deque(maxlen=100)
        self._active_alerts: Dict[str, Alert] = {}

    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add an alert handler."""
        self._handlers.append(handler)

    def trigger_alert(
        self,
        level: AlertLevel,
        message: str,
        endpoint: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Trigger an alert."""
        alert = Alert(
            level=level,
            message=message,
            endpoint=endpoint,
            metadata=metadata or {},
        )

        self._active_alerts[endpoint] = alert
        self._alert_history.append(alert)

        for handler in self._handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")

    def resolve_alert(self, endpoint: str) -> None:
        """Resolve an active alert."""
        if endpoint in self._active_alerts:
            del self._active_alerts[endpoint]

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return list(self._active_alerts.values())

    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get alert history."""
        return list(self._alert_history)[-limit:]


class SLATracker:
    """Tracks SLA metrics over time."""

    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes
        self._requests: deque = deque()
        self._window_start = datetime.now()

    def record_request(
        self,
        success: bool,
        latency_ms: float,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Record a request."""
        self._requests.append({
            "success": success,
            "latency_ms": latency_ms,
            "timestamp": timestamp or datetime.now(),
        })

    def get_sla_metrics(self) -> SLAMetric:
        """Calculate SLA metrics."""
        now = datetime.now()
        cutoff = now - timedelta(minutes=self.window_minutes)

        # Filter requests in window
        requests = [
            r for r in self._requests
            if r["timestamp"] >= cutoff
        ]

        if not requests:
            return SLAMetric(
                timestamp=now,
                uptime_percent=100.0,
                avg_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                error_rate_percent=0.0,
                total_requests=0,
            )

        # Calculate metrics
        total = len(requests)
        successful = sum(1 for r in requests if r["success"])
        latencies = sorted(r["latency_ms"] for r in requests)
        errors = total - successful

        uptime = (successful / total * 100) if total > 0 else 100.0
        error_rate = (errors / total * 100) if total > 0 else 0.0

        n = len(latencies)

        return SLAMetric(
            timestamp=now,
            uptime_percent=uptime,
            avg_latency_ms=sum(latencies) / n,
            p95_latency_ms=latencies[int(n * 0.95)],
            p99_latency_ms=latencies[min(int(n * 0.99), n - 1)],
            error_rate_percent=error_rate,
            total_requests=total,
        )


class APIMonitorAction:
    """
    Main action class for API monitoring.

    Features:
    - Health checks with configurable intervals
    - Latency tracking and statistics
    - Error rate monitoring
    - Alert management
    - SLA reporting
    - Historical data retention
    """

    def __init__(self):
        self._endpoints: Dict[str, EndpointConfig] = {}
        self._health_checkers: Dict[str, HealthChecker] = {}
        self._alert_manager = AlertManager()
        self._sla_trackers: Dict[str, SLATracker] = {}
        self._stats = {
            "checks_performed": 0,
            "checks_passed": 0,
            "checks_failed": 0,
            "alerts_triggered": 0,
        }

    def add_endpoint(self, config: EndpointConfig) -> "APIMonitorAction":
        """Add an endpoint to monitor."""
        self._endpoints[config.name] = config
        self._health_checkers[config.name] = HealthChecker(config)
        self._sla_trackers[config.name] = SLATracker()
        return self

    async def check_endpoint(self, name: str) -> HealthCheckResult:
        """Perform health check on an endpoint."""
        if name not in self._health_checkers:
            raise ValueError(f"Unknown endpoint: {name}")

        checker = self._health_checkers[name]
        result = await checker.check()

        self._stats["checks_performed"] += 1

        if result.status == HealthStatus.HEALTHY:
            self._stats["checks_passed"] += 1
            self._sla_trackers[name].record_request(True, result.response_time_ms)
            self._alert_manager.resolve_alert(name)
        else:
            self._stats["checks_failed"] += 1
            self._sla_trackers[name].record_request(False, result.response_time_ms)
            self._handle_failure(name, result)

        return result

    async def check_all_endpoints(self) -> Dict[str, HealthCheckResult]:
        """Check all registered endpoints."""
        results = {}
        for name in self._endpoints:
            results[name] = await self.check_endpoint(name)
        return results

    def _handle_failure(self, name: str, result: HealthCheckResult) -> None:
        """Handle endpoint failure."""
        config = self._endpoints[name]

        if result.response_time_ms > config.latency_threshold_ms:
            self._alert_manager.trigger_alert(
                AlertLevel.WARNING,
                f"High latency: {result.response_time_ms:.2f}ms",
                name,
                {"latency_ms": result.response_time_ms},
            )
            self._stats["alerts_triggered"] += 1

        if result.error:
            self._alert_manager.trigger_alert(
                AlertLevel.ERROR,
                f"Endpoint error: {result.error}",
                name,
                {"error": result.error},
            )
            self._stats["alerts_triggered"] += 1

    def get_endpoint_health(self, name: str) -> Dict[str, Any]:
        """Get health status for an endpoint."""
        if name not in self._health_checkers:
            raise ValueError(f"Unknown endpoint: {name}")

        checker = self._health_checkers[name]
        sla = self._sla_trackers[name].get_sla_metrics()

        return {
            "endpoint": name,
            "latency_stats": checker.get_latency_stats(),
            "sla_metrics": {
                "uptime_percent": sla.uptime_percent,
                "avg_latency_ms": sla.avg_latency_ms,
                "p95_latency_ms": sla.p95_latency_ms,
                "error_rate_percent": sla.error_rate_percent,
            },
            "active_alerts": [
                a for a in self._alert_manager.get_active_alerts()
                if a.endpoint == name
            ],
        }

    def get_all_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status for all endpoints."""
        return {name: self.get_endpoint_health(name) for name in self._endpoints}

    def add_alert_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add an alert handler."""
        self._alert_manager.add_handler(handler)

    def get_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics."""
        return {
            **self._stats,
            "endpoints_monitored": len(self._endpoints),
            "active_alerts": len(self._alert_manager.get_active_alerts()),
        }


async def demo_monitor():
    """Demonstrate API monitoring."""
    import httpx

    action = APIMonitorAction()

    action.add_endpoint(EndpointConfig(
        name="api_health",
        url="https://httpbin.org/status/200",
        expected_status=200,
        latency_threshold_ms=2000,
    ))

    # Check endpoint
    result = await action.check_endpoint("api_health")
    print(f"Health check: {result.status.value}, latency={result.response_time_ms:.2f}ms")

    # Check all
    results = await action.check_all_endpoints()
    for name, r in results.items():
        print(f"{name}: {r.status.value}")


if __name__ == "__main__":
    asyncio.run(demo_monitor())
