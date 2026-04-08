"""
API Monitoring Action Module

Provides API monitoring, metrics collection, alerting, and observability.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import asyncio


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)
    unit: str = ""


@dataclass
class Alert:
    """An alert definition."""
    alert_id: str
    name: str
    severity: AlertSeverity
    condition: Callable[[dict], bool]
    message: str
    cooldown_seconds: float = 60.0
    last_triggered: Optional[datetime] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertEvent:
    """An alert firing event."""
    alert: Alert
    fired_at: datetime
    current_value: float
    threshold: float
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class HealthStatus:
    """Health check status."""
    component: str
    healthy: bool
    latency_ms: float
    message: Optional[str] = None
    last_check: datetime = field(default_factory=datetime.now)


@dataclass
class MonitoringSnapshot:
    """A snapshot of monitoring data."""
    timestamp: datetime
    metrics: dict[str, float]
    active_alerts: int
    health_status: dict[str, bool]


class MetricsCollector:
    """Collects and stores metrics."""
    
    def __init__(self, retention_minutes: int = 60):
        self.retention_minutes = retention_minutes
        self._metrics: dict[str, list[MetricPoint]] = defaultdict(list)
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = defaultdict(list)
    
    def record_metric(self, metric: MetricPoint):
        """Record a metric point."""
        self._metrics[metric.name].append(metric)
        self._prune_metrics(metric.name)
    
    def increment_counter(self, name: str, value: int = 1, labels: Optional[dict] = None):
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        self._counters[key] += value
    
    def set_gauge(self, name: str, value: float, labels: Optional[dict] = None):
        """Set a gauge metric."""
        key = self._make_key(name, labels)
        self._gauges[key] = value
    
    def record_histogram(self, name: str, value: float, labels: Optional[dict] = None):
        """Record a histogram value."""
        key = self._make_key(name, labels)
        self._histograms[key].append(value)
        
        # Keep only recent values
        max_values = 1000
        if len(self._histograms[key]) > max_values:
            self._histograms[key] = self._histograms[key][-max_values:]
    
    def _make_key(self, name: str, labels: Optional[dict]) -> str:
        """Create a unique key for metric with labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def _prune_metrics(self, name: str):
        """Remove old metric points."""
        cutoff = datetime.now() - timedelta(minutes=self.retention_minutes)
        self._metrics[name] = [
            m for m in self._metrics[name]
            if m.timestamp > cutoff
        ]
    
    def get_metric_history(
        self,
        name: str,
        duration: timedelta = timedelta(minutes=5)
    ) -> list[MetricPoint]:
        """Get metric history for a time period."""
        cutoff = datetime.now() - duration
        return [m for m in self._metrics.get(name, []) if m.timestamp > cutoff]
    
    def get_percentile(self, name: str, percentile: float) -> Optional[float]:
        """Calculate percentile for a histogram."""
        values = self._histograms.get(name, [])
        if not values:
            return None
        
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(idx, len(sorted_values) - 1)]


class ApiMonitoringAction:
    """Main API monitoring action handler."""
    
    def __init__(self):
        self._collector = MetricsCollector()
        self._alerts: dict[str, Alert] = {}
        self._alert_events: list[AlertEvent] = []
        self._health_checks: dict[str, Callable] = {}
        self._health_status: dict[str, HealthStatus] = {}
        self._stats: dict[str, Any] = defaultdict(int)
    
    def register_alert(self, alert: Alert) -> "ApiMonitoringAction":
        """Register an alert."""
        self._alerts[alert.alert_id] = alert
        return self
    
    def register_health_check(
        self,
        component: str,
        check: Callable[[], Awaitable[HealthStatus]]
    ) -> "ApiMonitoringAction":
        """Register a health check."""
        self._health_checks[component] = check
        return self
    
    async def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
        labels: Optional[dict] = None
    ):
        """Record an API request metric."""
        labels = labels or {}
        labels["method"] = method
        labels["path"] = path
        labels["status_code"] = str(status_code)
        
        self._collector.record_metric(MetricPoint(
            name="api_requests_total",
            value=1,
            timestamp=datetime.now(),
            labels=labels
        ))
        
        self._collector.record_histogram(
            "api_request_duration_ms",
            latency_ms,
            labels
        )
        
        if status_code >= 500:
            self._collector.increment_counter("api_errors_total", 1, labels)
            self._stats["error_requests"] += 1
        elif status_code >= 400:
            self._collector.increment_counter("api_client_errors_total", 1, labels)
        
        self._stats["total_requests"] += 1
    
    async def record_custom_metric(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
        unit: str = ""
    ):
        """Record a custom metric."""
        self._collector.record_metric(MetricPoint(
            name=name,
            value=value,
            timestamp=datetime.now(),
            labels=labels or {},
            unit=unit
        ))
    
    async def increment_counter(
        self,
        name: str,
        value: int = 1,
        labels: Optional[dict] = None
    ):
        """Increment a counter."""
        self._collector.increment_counter(name, value, labels)
    
    async def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None
    ):
        """Set a gauge value."""
        self._collector.set_gauge(name, value, labels)
    
    async def check_alerts(self) -> list[AlertEvent]:
        """Evaluate all alerts and return firing alerts."""
        firing_alerts = []
        now = datetime.now()
        
        for alert in self._alerts.values():
            # Check cooldown
            if alert.last_triggered:
                cooldown = timedelta(seconds=alert.cooldown_seconds)
                if now - alert.last_triggered < cooldown:
                    continue
            
            # Evaluate alert condition
            try:
                context = {
                    "metrics": dict(self._collector._gauges),
                    "counters": dict(self._collector._counters),
                    "timestamp": now
                }
                
                should_fire = alert.condition(context)
                
                if should_fire:
                    event = AlertEvent(
                        alert=alert,
                        fired_at=now,
                        current_value=context["metrics"].get(alert.name, 0),
                        threshold=0
                    )
                    
                    alert.last_triggered = now
                    firing_alerts.append(event)
                    self._alert_events.append(event)
                    self._stats["alerts_fired"] += 1
                
            except Exception as e:
                self._stats["alert_check_errors"] += 1
        
        return firing_alerts
    
    async def check_health(self) -> dict[str, HealthStatus]:
        """Run all health checks."""
        results = {}
        
        for component, check in self._health_checks.items():
            try:
                status = await asyncio.wait_for(
                    check(),
                    timeout=5.0
                )
                results[component] = status
                self._health_status[component] = status
                
                if not status.healthy:
                    self._stats["unhealthy_checks"] += 1
                
            except asyncio.TimeoutError:
                results[component] = HealthStatus(
                    component=component,
                    healthy=False,
                    latency_ms=5000,
                    message="Health check timed out"
                )
                self._stats["health_check_timeouts"] += 1
                
            except Exception as e:
                results[component] = HealthStatus(
                    component=component,
                    healthy=False,
                    latency_ms=0,
                    message=str(e)
                )
                self._stats["health_check_errors"] += 1
        
        self._stats["health_checks_run"] += 1
        return results
    
    async def get_snapshot(self) -> MonitoringSnapshot:
        """Get a snapshot of current monitoring state."""
        firing_alerts = await self.check_alerts()
        
        return MonitoringSnapshot(
            timestamp=datetime.now(),
            metrics=dict(self._collector._gauges),
            active_alerts=len(firing_alerts),
            health_status={
                c: s.healthy for c, s in self._health_status.items()
            }
        )
    
    async def get_metrics_summary(
        self,
        duration: timedelta = timedelta(minutes=5)
    ) -> dict[str, Any]:
        """Get summary of collected metrics."""
        return {
            "requests": {
                "total": self._collector._counters.get("api_requests_total", 0),
                "errors": self._collector._counters.get("api_errors_total", 0),
                "p50_latency_ms": self._collector.get_percentile("api_request_duration_ms", 50),
                "p95_latency_ms": self._collector.get_percentile("api_request_duration_ms", 95),
                "p99_latency_ms": self._collector.get_percentile("api_request_duration_ms", 99)
            },
            "gauges": dict(self._collector._gauges),
            "alerts": {
                "registered": len(self._alerts),
                "fired": self._stats["alerts_fired"]
            },
            "health": {
                "checks_run": self._stats["health_checks_run"],
                "unhealthy": self._stats["unhealthy_checks"]
            }
        }
    
    def get_stats(self) -> dict[str, Any]:
        """Get monitoring statistics."""
        return dict(self._stats)
    
    def list_alerts(self, active_only: bool = False) -> list[dict[str, Any]]:
        """List registered alerts."""
        alerts = list(self._alerts.values())
        
        if active_only:
            now = datetime.now()
            alerts = [
                a for a in alerts
                if a.last_triggered and
                (now - a.last_triggered).total_seconds() < a.cooldown_seconds
            ]
        
        return [
            {
                "id": a.alert_id,
                "name": a.name,
                "severity": a.severity.value,
                "message": a.message,
                "last_triggered": a.last_triggered.isoformat() if a.last_triggered else None
            }
            for a in alerts
        ]
