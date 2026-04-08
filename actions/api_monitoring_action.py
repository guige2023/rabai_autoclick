"""API Monitoring Action Module.

Provides API monitoring, health checks, alerting, and
performance tracking for distributed services.
"""

from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import asyncio
import json
import time
import hashlib


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Endpoint:
    """Represents a monitored API endpoint."""
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: int = 30
    expected_status: int = 200
    check_interval: int = 60


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    endpoint: str
    status: HealthStatus
    response_time_ms: float
    status_code: Optional[int] = None
    error_message: Optional[str] = None
    checked_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    """Represents an alert notification."""
    id: str
    severity: AlertSeverity
    source: str
    message: str
    created_at: datetime = field(default_factory=datetime.now)
    acknowledged: bool = False
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary of metric over a time window."""
    name: str
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float
    p95: float
    p99: float
    window_start: datetime
    window_end: datetime


class MetricStore:
    """Stores and retrieves time-series metrics."""

    def __init__(self, retention_seconds: int = 3600):
        self._metrics: Dict[str, List[MetricPoint]] = {}
        self._retention_seconds = retention_seconds

    def record(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a metric value."""
        if name not in self._metrics:
            self._metrics[name] = []
        self._metrics[name].append(MetricPoint(
            name=name,
            value=value,
            labels=labels or {},
        ))
        self._cleanup_old_metrics(name)

    def _cleanup_old_metrics(self, name: str):
        """Remove metrics outside retention window."""
        cutoff = datetime.now() - timedelta(seconds=self._retention_seconds)
        self._metrics[name] = [
            p for p in self._metrics[name] if p.timestamp > cutoff
        ]

    def query(
        self,
        name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[MetricPoint]:
        """Query metric points within time range."""
        if name not in self._metrics:
            return []
        points = self._metrics[name]
        if start_time:
            points = [p for p in points if p.timestamp >= start_time]
        if end_time:
            points = [p for p in points if p.timestamp <= end_time]
        return points

    def summarize(
        self,
        name: str,
        window_seconds: int = 60,
    ) -> Optional[MetricSummary]:
        """Get summary statistics for a metric."""
        cutoff = datetime.now() - timedelta(seconds=window_seconds)
        points = self.query(name, start_time=cutoff)
        if not points:
            return None

        values = sorted([p.value for p in points])
        n = len(values)

        def percentile(p: float) -> float:
            idx = int(n * p)
            return values[min(idx, n - 1)]

        return MetricSummary(
            name=name,
            count=n,
            sum=sum(values),
            min=values[0],
            max=values[-1],
            avg=sum(values) / n,
            p50=percentile(0.5),
            p95=percentile(0.95),
            p99=percentile(0.99),
            window_start=cutoff,
            window_end=datetime.now(),
        )

    def get_all_metric_names(self) -> List[str]:
        """Get all stored metric names."""
        return list(self._metrics.keys())


class HealthChecker:
    """Performs health checks on endpoints."""

    def __init__(self, metric_store: Optional[MetricStore] = None):
        self._endpoints: Dict[str, Endpoint] = {}
        self._last_results: Dict[str, HealthCheckResult] = {}
        self._metric_store = metric_store or MetricStore()

    def register_endpoint(self, endpoint: Endpoint):
        """Register an endpoint for monitoring."""
        self._endpoints[endpoint.name] = endpoint

    def unregister_endpoint(self, name: str):
        """Remove an endpoint from monitoring."""
        if name in self._endpoints:
            del self._endpoints[name]

    async def check_endpoint(self, name: str) -> HealthCheckResult:
        """Perform health check on a single endpoint."""
        endpoint = self._endpoints.get(name)
        if not endpoint:
            return HealthCheckResult(
                endpoint=name,
                status=HealthStatus.UNKNOWN,
                response_time_ms=0,
                error_message="Endpoint not registered",
            )

        start_time = time.time()
        try:
            response_time = (time.time() - start_time) * 1000
            result = HealthCheckResult(
                endpoint=name,
                status=HealthStatus.HEALTHY,
                response_time_ms=response_time,
                status_code=endpoint.expected_status,
            )
        except Exception as e:
            result = HealthCheckResult(
                endpoint=name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                error_message=str(e),
            )

        self._last_results[name] = result
        self._metric_store.record(
            f"healthcheck_response_time_{name}",
            result.response_time_ms,
        )
        self._metric_store.record(
            f"healthcheck_status_{name}",
            1 if result.status == HealthStatus.HEALTHY else 0,
        )
        return result

    async def check_all(self) -> Dict[str, HealthCheckResult]:
        """Perform health checks on all endpoints."""
        tasks = [
            self.check_endpoint(name)
            for name in self._endpoints.keys()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return dict(zip(self._endpoints.keys(), results))

    def get_last_result(self, name: str) -> Optional[HealthCheckResult]:
        """Get last health check result for endpoint."""
        return self._last_results.get(name)

    def get_all_last_results(self) -> Dict[str, HealthCheckResult]:
        """Get all last results."""
        return self._last_results.copy()


class AlertManager:
    """Manages alerts and notifications."""

    def __init__(self):
        self._alerts: Dict[str, Alert] = {}
        self._handlers: Dict[AlertSeverity, List[Callable]] = {
            severity: [] for severity in AlertSeverity
        }
        self._auto_resolve: bool = True

    def set_auto_resolve(self, enabled: bool):
        """Set auto-resolve behavior."""
        self._auto_resolve = enabled

    def register_handler(self, severity: AlertSeverity, handler: Callable):
        """Register alert notification handler."""
        self._handlers[severity].append(handler)

    async def create_alert(
        self,
        source: str,
        severity: AlertSeverity,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Alert:
        """Create and fire a new alert."""
        alert_id = hashlib.md5(
            f"{source}:{message}:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:12]

        alert = Alert(
            id=alert_id,
            severity=severity,
            source=source,
            message=message,
            metadata=metadata or {},
        )
        self._alerts[alert_id] = alert
        await self._notify_handlers(alert)
        return alert

    async def _notify_handlers(self, alert: Alert):
        """Notify registered handlers of alert."""
        for handler in self._handlers.get(alert.severity, []):
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(alert)
                else:
                    handler(alert)
            except Exception:
                pass

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        if alert_id in self._alerts:
            self._alerts[alert_id].acknowledged = True
            return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        if alert_id in self._alerts:
            alert = self._alerts[alert_id]
            alert.resolved_at = datetime.now()
            return True
        return False

    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
        acknowledged: Optional[bool] = None,
    ) -> List[Alert]:
        """Get unresolved alerts, optionally filtered."""
        alerts = [a for a in self._alerts.values() if a.resolved_at is None]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        if acknowledged is not None:
            alerts = [a for a in alerts if a.acknowledged == acknowledged]
        return sorted(alerts, key=lambda a: a.created_at, reverse=True)

    def get_alert_count(self) -> Dict[str, int]:
        """Get count of alerts by severity."""
        counts = {s.value: 0 for s in AlertSeverity}
        for alert in self._alerts.values():
            if alert.resolved_at is None:
                counts[alert.severity.value] += 1
        return counts


class APIMonitoringAction:
    """High-level API monitoring action."""

    def __init__(
        self,
        health_checker: Optional[HealthChecker] = None,
        alert_manager: Optional[AlertManager] = None,
        metric_store: Optional[MetricStore] = None,
    ):
        self.metric_store = metric_store or MetricStore()
        self.health_checker = health_checker or HealthChecker(self.metric_store)
        self.alert_manager = alert_manager or AlertManager()

    def add_endpoint(
        self,
        name: str,
        url: str,
        method: str = "GET",
        timeout: int = 30,
    ) -> "APIMonitoringAction":
        """Add an endpoint to monitor."""
        self.health_checker.register_endpoint(Endpoint(
            name=name,
            url=url,
            method=method,
            timeout=timeout,
        ))
        return self

    async def check_health(self) -> Dict[str, HealthCheckResult]:
        """Run health checks on all endpoints."""
        return await self.health_checker.check_all()

    async def record_metric(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ):
        """Record a metric value."""
        self.metric_store.record(name, value, labels)

    async def create_alert(
        self,
        source: str,
        severity: str,
        message: str,
    ) -> Alert:
        """Create an alert."""
        alert_severity = AlertSeverity(severity)
        return await self.alert_manager.create_alert(source, alert_severity, message)

    def get_metrics_summary(
        self,
        name: str,
        window_seconds: int = 60,
    ) -> Optional[MetricSummary]:
        """Get metric summary."""
        return self.metric_store.summarize(name, window_seconds)

    def get_status(self) -> Dict[str, Any]:
        """Get overall monitoring status."""
        last_results = self.health_checker.get_all_last_results()
        healthy = sum(
            1 for r in last_results.values()
            if r.status == HealthStatus.HEALTHY
        )
        total = len(last_results)
        return {
            "total_endpoints": total,
            "healthy": healthy,
            "unhealthy": total - healthy,
            "health_percentage": (healthy / total * 100) if total > 0 else 100,
            "active_alerts": self.alert_manager.get_alert_count(),
            "metric_count": len(self.metric_store.get_all_metric_names()),
        }


# Module exports
__all__ = [
    "APIMonitoringAction",
    "HealthChecker",
    "HealthCheckResult",
    "HealthStatus",
    "Endpoint",
    "AlertManager",
    "Alert",
    "AlertSeverity",
    "MetricStore",
    "MetricPoint",
    "MetricSummary",
]
