"""
Automation Monitor Action Module.

Provides monitoring, alerting, and observability for automation workflows
including metrics collection, health checks, and incident management.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status values."""
    FIRING = "firing"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"
    SUPPRESSED = "suppressed"


class HealthStatus(Enum):
    """Health status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class MetricValue:
    """Single metric value with timestamp."""
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "labels": self.labels,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class Alert:
    """Alert instance."""
    alert_id: str
    name: str
    description: str
    severity: AlertSeverity
    status: AlertStatus = AlertStatus.FIRING
    fired_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    summary: Optional[str] = None

    def acknowledge(self, by: str):
        """Acknowledge the alert."""
        self.status = AlertStatus.ACKNOWLEDGED
        self.acknowledged_at = datetime.now()
        self.acknowledged_by = by

    def resolve(self):
        """Resolve the alert."""
        self.status = AlertStatus.RESOLVED
        self.resolved_at = datetime.now()

    def suppress(self, duration: Optional[timedelta] = None):
        """Suppress the alert."""
        self.status = AlertStatus.SUPPRESSED


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    component: str
    status: HealthStatus
    message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=datetime.now)
    response_time: Optional[float] = None

    @property
    def is_healthy(self) -> bool:
        """Check if health status is healthy."""
        return self.status == HealthStatus.HEALTHY


@dataclass
class Incident:
    """Incident record."""
    incident_id: str
    title: str
    description: str
    severity: AlertSeverity
    status: str = "open"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    assignee: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    related_alerts: List[str] = field(default_factory=list)

    def resolve(self):
        """Resolve the incident."""
        self.status = "resolved"
        self.resolved_at = datetime.now()
        self.updated_at = datetime.now()

    def assign(self, assignee: str):
        """Assign incident."""
        self.assignee = assignee
        self.updated_at = datetime.now()


@dataclass
class DashboardWidget:
    """Dashboard widget definition."""
    widget_id: str
    title: str
    widget_type: str
    query: str
    refresh_interval: int = 60
    position: Tuple[int, int] = (0, 0)
    size: Tuple[int, int] = (1, 1)


@dataclass
class Dashboard:
    """Monitoring dashboard."""
    dashboard_id: str
    name: str
    widgets: List[DashboardWidget] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class MetricsCollector:
    """Collects and stores metrics."""

    def __init__(self, retention_period: int = 3600):
        self.retention_period = retention_period
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)

    def record(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a metric value."""
        metric = MetricValue(name=name, value=value, labels=labels or {})
        self._metrics[name].append(metric)

        cutoff = datetime.now() - timedelta(seconds=self.retention_period)
        while self._metrics[name] and self._metrics[name][0].timestamp < cutoff:
            self._metrics[name].popleft()

    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter."""
        key = self._make_key(name, labels)
        self._counters[key] += value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge value."""
        key = self._make_key(name, labels)
        self._gauges[key] = value

    def observe(self, name: str, value: float):
        """Observe a value for histogram."""
        self._histograms[name].append(value)

    def get_series(
        self,
        name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[MetricValue]:
        """Get metric series."""
        series = list(self._metrics.get(name, []))

        if start_time:
            series = [m for m in series if m.timestamp >= start_time]
        if end_time:
            series = [m for m in series if m.timestamp <= end_time]

        return series

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)

    def get_histogram_stats(
        self,
        name: str
    ) -> Dict[str, float]:
        """Get histogram statistics."""
        values = self._histograms.get(name, [])
        if not values:
            return {}

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        return {
            "count": n,
            "sum": sum(values),
            "mean": sum(values) / n,
            "min": min(values),
            "max": max(values),
            "p50": sorted_vals[n // 2],
            "p90": sorted_vals[int(n * 0.9)],
            "p95": sorted_vals[int(n * 0.95)],
            "p99": sorted_vals[int(n * 0.99)]
        }

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Make composite key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class AlertManager:
    """Manages alerts and notifications."""

    def __init__(self):
        self.alerts: Dict[str, Alert] = {}
        self.alert_rules: List[Dict[str, Any]] = []
        self._handlers: Dict[AlertSeverity, List[Callable]] = defaultdict(list)
        self._notification_channels: List[Callable] = []

    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        severity: AlertSeverity,
        description: str,
        labels: Optional[Dict[str, str]] = None
    ):
        """Add alert rule."""
        self.alert_rules.append({
            "name": name,
            "condition": condition,
            "severity": severity,
            "description": description,
            "labels": labels or {}
        })

    def fire_alert(
        self,
        name: str,
        description: str,
        severity: AlertSeverity,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None
    ) -> Alert:
        """Fire a new alert."""
        alert_id = str(uuid.uuid4())
        alert = Alert(
            alert_id=alert_id,
            name=name,
            description=description,
            severity=severity,
            labels=labels or {},
            annotations=annotations or {}
        )

        self.alerts[alert_id] = alert

        for handler in self._handlers.get(severity, []):
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")

        for channel in self._notification_channels:
            try:
                channel(alert)
            except Exception as e:
                logger.error(f"Notification channel error: {e}")

        logger.warning(f"Alert fired: {name} ({severity.value})")
        return alert

    def resolve_alert(self, alert_id: str):
        """Resolve an alert."""
        alert = self.alerts.get(alert_id)
        if alert:
            alert.resolve()
            logger.info(f"Alert resolved: {alert.name}")

    def acknowledge_alert(self, alert_id: str, by: str):
        """Acknowledge an alert."""
        alert = self.alerts.get(alert_id)
        if alert:
            alert.acknowledge(by)

    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
        status: Optional[AlertStatus] = None
    ) -> List[Alert]:
        """Get active alerts."""
        alerts = self.alerts.values()

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        if status:
            alerts = [a for a in alerts if a.status == status]
        else:
            alerts = [a for a in alerts if a.status == AlertStatus.FIRING]

        return sorted(alerts, key=lambda a: a.fired_at, reverse=True)

    def register_handler(self, severity: AlertSeverity, handler: Callable):
        """Register alert handler."""
        self._handlers[severity].append(handler)

    def add_notification_channel(self, channel: Callable):
        """Add notification channel."""
        self._notification_channels.append(channel)


class HealthChecker:
    """Performs health checks on components."""

    def __init__(self):
        self.checks: Dict[str, Callable] = {}
        self.results: Dict[str, HealthCheckResult] = {}

    def register_check(self, component: str, check_func: Callable):
        """Register a health check function."""
        self.checks[component] = check_func

    async def check(self, component: str) -> HealthCheckResult:
        """Perform health check for component."""
        if component not in self.checks:
            return HealthCheckResult(
                component=component,
                status=HealthStatus.UNKNOWN,
                message="No health check registered"
            )

        start_time = time.time()
        check_func = self.checks[component]

        try:
            if asyncio.iscoroutinefunction(check_func):
                result = await check_func()
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(check_func),
                    timeout=30.0
                )

            response_time = time.time() - start_time

            if isinstance(result, HealthCheckResult):
                result.response_time = response_time
                self.results[component] = result
                return result

            status = HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY
            health_result = HealthCheckResult(
                component=component,
                status=status,
                response_time=response_time
            )

        except Exception as e:
            health_result = HealthCheckResult(
                component=component,
                status=HealthStatus.UNHEALTHY,
                message=str(e)
            )

        self.results[component] = health_result
        return health_result

    async def check_all(self) -> Dict[str, HealthCheckResult]:
        """Perform all health checks."""
        tasks = [self.check(component) for component in self.checks.keys()]
        results = await asyncio.gather(*tasks)
        return {r.component: r for r in results}

    def get_status(self) -> HealthStatus:
        """Get overall health status."""
        if not self.results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in self.results.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        return HealthStatus.DEGRADED


class IncidentManager:
    """Manages incidents."""

    def __init__(self):
        self.incidents: Dict[str, Incident] = {}

    def create_incident(
        self,
        title: str,
        description: str,
        severity: AlertSeverity,
        tags: Optional[Set[str]] = None
    ) -> Incident:
        """Create new incident."""
        incident = Incident(
            incident_id=str(uuid.uuid4()),
            title=title,
            description=description,
            severity=severity,
            tags=tags or set()
        )
        self.incidents[incident.incident_id] = incident
        return incident

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Get incident by ID."""
        return self.incidents.get(incident_id)

    def get_open_incidents(
        self,
        severity: Optional[AlertSeverity] = None
    ) -> List[Incident]:
        """Get open incidents."""
        incidents = [i for i in self.incidents.values() if i.status == "open"]
        if severity:
            incidents = [i for i in incidents if i.severity == severity]
        return sorted(incidents, key=lambda i: i.created_at, reverse=True)


class MonitoringDashboard:
    """Monitoring dashboard aggregator."""

    def __init__(self):
        self.dashboards: Dict[str, Dashboard] = {}
        self.metrics_collector = MetricsCollector()
        self.alert_manager = AlertManager()
        self.health_checker = HealthChecker()
        self.incident_manager = IncidentManager()

    def create_dashboard(self, name: str) -> Dashboard:
        """Create new dashboard."""
        dashboard = Dashboard(
            dashboard_id=str(uuid.uuid4()),
            name=name
        )
        self.dashboards[dashboard.dashboard_id] = dashboard
        return dashboard

    def get_summary(self) -> Dict[str, Any]:
        """Get monitoring summary."""
        return {
            "dashboards": len(self.dashboards),
            "active_alerts": len(self.alert_manager.get_active_alerts()),
            "health_status": self.health_checker.get_status().value,
            "open_incidents": len(self.incident_manager.get_open_incidents()),
            "metrics_collected": sum(
                len(q) for q in self.metrics_collector._metrics.values()
            )
        }


async def demo_health_check() -> HealthCheckResult:
    """Demo health check function."""
    await asyncio.sleep(0.1)
    return HealthCheckResult(
        component="demo",
        status=HealthStatus.HEALTHY,
        message="All systems operational"
    )


async def main():
    """Demonstrate monitoring capabilities."""
    dashboard = MonitoringDashboard()

    dashboard.health_checker.register_check("api", demo_health_check)
    health = await dashboard.health_checker.check("api")
    print(f"Health check: {health.status.value}")

    dashboard.metrics_collector.increment("requests_total", 1)
    dashboard.metrics_collector.set_gauge("active_connections", 42)
    dashboard.metrics_collector.observe("request_duration", 0.15)

    alert = dashboard.alert_manager.fire_alert(
        name="HighErrorRate",
        description="Error rate exceeded threshold",
        severity=AlertSeverity.ERROR
    )
    print(f"Alert fired: {alert.alert_id}")

    print(f"Summary: {dashboard.get_summary()}")


if __name__ == "__main__":
    asyncio.run(main())
