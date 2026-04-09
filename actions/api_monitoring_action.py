"""
API Monitoring Action Module.

Provides API monitoring capabilities including metrics collection,
health checks, alerting, and performance tracking for API services.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
import statistics


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    RATE = "rate"


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Metric:
    """Represents a metric data point."""
    name: str
    metric_type: MetricType
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthStatus:
    """Health status of a service."""
    service_name: str
    healthy: bool
    latency_ms: float
    timestamp: datetime
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    """Represents an alert."""
    id: str
    name: str
    level: AlertLevel
    message: str
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    """
    Collects and stores metrics.
    
    Example:
        collector = MetricsCollector()
        collector.increment("requests_total", labels={"method": "GET"})
        collector.record("response_time", 0.125)
        
        stats = collector.get_stats("requests_total")
    """
    
    def __init__(self, retention_minutes: int = 60):
        self.retention_minutes = retention_minutes
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.counters: Dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()
    
    def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None
    ):
        """Increment a counter metric."""
        with self._lock:
            self.counters[name] += value
            
            metric = Metric(
                name=name,
                metric_type=MetricType.COUNTER,
                value=self.counters[name],
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric)
    
    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ):
        """Record a gauge metric."""
        with self._lock:
            metric = Metric(
                name=name,
                metric_type=MetricType.GAUGE,
                value=value,
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric)
    
    def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ):
        """Record a histogram metric."""
        with self._lock:
            metric = Metric(
                name=name,
                metric_type=MetricType.HISTOGRAM,
                value=value,
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric)
    
    def timer(self, name: str, duration_ms: float, labels: Optional[Dict[str, str]] = None):
        """Record a timer metric."""
        self.histogram(f"{name}.duration_ms", duration_ms, labels)
    
    def get_stats(
        self,
        name: str,
        window_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get statistics for a metric."""
        with self._lock:
            if name not in self.metrics:
                return {}
            
            cutoff = datetime.now() - timedelta(seconds=window_seconds or 60)
            recent = [
                m.value for m in self.metrics[name]
                if m.timestamp >= cutoff
            ]
            
            if not recent:
                return {"count": 0}
            
            return {
                "count": len(recent),
                "sum": sum(recent),
                "avg": statistics.mean(recent),
                "min": min(recent),
                "max": max(recent),
                "p50": statistics.median(recent),
                "p95": self._percentile(recent, 0.95),
                "p99": self._percentile(recent, 0.99)
            }
    
    def get_all_metrics(self) -> Dict[str, List[Metric]]:
        """Get all metrics."""
        with self._lock:
            return {name: list(metrics) for name, metrics in self.metrics.items()}
    
    def _percentile(self, data: List[float], p: float) -> float:
        """Calculate percentile."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p)
        return sorted_data[min(idx, len(sorted_data) - 1)]


class HealthChecker:
    """
    Performs health checks on services.
    
    Example:
        checker = HealthChecker()
        checker.add_check("api", lambda: check_api_health())
        
        status = checker.check_all()
    """
    
    def __init__(self):
        self.checks: Dict[str, Callable[[], HealthStatus]] = {}
        self._lock = threading.RLock()
    
    def add_check(
        self,
        name: str,
        check_fn: Callable[[], HealthStatus]
    ) -> "HealthChecker":
        """Add a health check."""
        with self._lock:
            self.checks[name] = check_fn
        return self
    
    def check(self, name: str) -> Optional[HealthStatus]:
        """Check health of a specific service."""
        with self._lock:
            check_fn = self.checks.get(name)
        
        if not check_fn:
            return None
        
        try:
            return check_fn()
        except Exception as e:
            return HealthStatus(
                service_name=name,
                healthy=False,
                latency_ms=0,
                timestamp=datetime.now(),
                message=str(e)
            )
    
    def check_all(self) -> Dict[str, HealthStatus]:
        """Check health of all services."""
        results = {}
        for name in list(self.checks.keys()):
            status = self.check(name)
            if status:
                results[name] = status
        return results


class AlertManager:
    """
    Manages alerts based on metrics and conditions.
    
    Example:
        manager = AlertManager()
        manager.add_rule("high_error_rate", lambda: error_rate > 0.05, AlertLevel.ERROR)
        
        alerts = manager.evaluate(metrics)
    """
    
    def __init__(self):
        self.rules: Dict[str, tuple] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
    
    def add_rule(
        self,
        name: str,
        condition_fn: Callable[[Dict], bool],
        level: AlertLevel,
        message_template: str = ""
    ) -> "AlertManager":
        """Add an alert rule."""
        with self._lock:
            self.rules[name] = (condition_fn, level, message_template)
        return self
    
    def evaluate(self, metrics: Dict[str, Any]) -> List[Alert]:
        """Evaluate all rules and trigger alerts."""
        triggered = []
        
        with self._lock:
            for name, (condition_fn, level, template) in self.rules.items():
                try:
                    should_alert = condition_fn(metrics)
                    
                    if should_alert and name not in self.active_alerts:
                        alert = Alert(
                            id=f"alert_{name}_{int(time.time())}",
                            name=name,
                            level=level,
                            message=template.format(**metrics),
                            triggered_at=datetime.now()
                        )
                        self.active_alerts[name] = alert
                        triggered.append(alert)
                        self.alert_history.append(alert)
                    
                    elif not should_alert and name in self.active_alerts:
                        # Resolve alert
                        alert = self.active_alerts[name]
                        alert.resolved_at = datetime.now()
                        del self.active_alerts[name]
                
                except Exception:
                    continue
        
        return triggered
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        with self._lock:
            return list(self.active_alerts.values())
    
    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get alert history."""
        with self._lock:
            return list(self.alert_history)[-limit:]


class APIMonitor:
    """
    Complete API monitoring system.
    
    Example:
        monitor = APIMonitor()
        monitor.record_request(method="GET", path="/api/users", status=200, duration_ms=50)
        
        stats = monitor.get_stats()
    """
    
    def __init__(self, service_name: str = "api"):
        self.service_name = service_name
        self.collector = MetricsCollector()
        self.health_checker = HealthChecker()
        self.alert_manager = AlertManager()
        self._lock = threading.Lock()
        
        # Setup default alert rules
        self._setup_default_alerts()
    
    def _setup_default_alerts(self):
        """Setup default alert rules."""
        self.alert_manager.add_rule(
            "high_error_rate",
            lambda m: m.get("error_rate", 0) > 0.05,
            AlertLevel.ERROR,
            "Error rate is above 5%: {error_rate}"
        )
        
        self.alert_manager.add_rule(
            "high_latency",
            lambda m: m.get("p99_latency", 0) > 1000,
            AlertLevel.WARNING,
            "P99 latency is above 1s: {p99_latency}ms"
        )
    
    def record_request(
        self,
        method: str,
        path: str,
        status: int,
        duration_ms: float,
        labels: Optional[Dict[str, str]] = None
    ):
        """Record an API request."""
        labels = labels or {}
        labels.update({"method": method, "path": path, "status": str(status)})
        
        self.collector.increment("requests_total", labels=labels)
        self.collector.histogram("response_time_ms", duration_ms, labels)
        
        # Record errors
        if status >= 400:
            self.collector.increment("errors_total", labels=labels)
    
    def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: Optional[Dict[str, str]] = None
    ):
        """Record a custom metric."""
        if metric_type == MetricType.COUNTER:
            self.collector.increment(name, value, labels)
        else:
            self.collector.gauge(name, value, labels)
    
    def check_health(self) -> Dict[str, HealthStatus]:
        """Check health of monitored services."""
        return self.health_checker.check_all()
    
    def evaluate_alerts(self) -> List[Alert]:
        """Evaluate alert rules."""
        stats = self.get_stats()
        return self.alert_manager.evaluate(stats)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics."""
        request_stats = self.collector.get_stats("requests_total")
        error_stats = self.collector.get_stats("errors_total")
        latency_stats = self.collector.get_stats("response_time_ms")
        
        error_rate = 0.0
        if request_stats.get("count", 0) > 0:
            error_count = error_stats.get("count", 0)
            total_count = request_stats.get("count", 0)
            error_rate = error_count / total_count if total_count > 0 else 0
        
        return {
            "requests_total": request_stats.get("count", 0),
            "errors_total": error_stats.get("count", 0),
            "error_rate": error_rate,
            "avg_latency_ms": latency_stats.get("avg", 0),
            "p50_latency_ms": latency_stats.get("p50", 0),
            "p95_latency_ms": latency_stats.get("p95", 0),
            "p99_latency_ms": latency_stats.get("p99", 0),
        }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APIMonitoringAction(BaseAction):
    """
    API monitoring action for observability.
    
    Parameters:
        operation: Operation type (record/check/stats/alerts)
        metric_name: Name of the metric
        value: Metric value
        method: HTTP method (for request recording)
        path: API path (for request recording)
        status: HTTP status code
    
    Example:
        action = APIMonitoringAction()
        result = action.execute({}, {
            "operation": "record",
            "metric_name": "requests_total",
            "value": 1
        })
    """
    
    _monitor: Optional[APIMonitor] = None
    _lock = threading.Lock()
    
    def _get_monitor(self) -> APIMonitor:
        """Get or create monitor."""
        with self._lock:
            if self._monitor is None:
                self._monitor = APIMonitor()
            return self._monitor
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute monitoring operation."""
        operation = params.get("operation", "record")
        monitor = self._get_monitor()
        
        if operation == "record":
            metric_name = params.get("metric_name")
            value = params.get("value", 1)
            labels = params.get("labels", {})
            
            if metric_name:
                monitor.record_metric(metric_name, value, labels=labels)
            
            return {
                "success": True,
                "operation": "record",
                "metric_name": metric_name,
                "value": value
            }
        
        elif operation == "record_request":
            method = params.get("method", "GET")
            path = params.get("path", "/")
            status = params.get("status", 200)
            duration_ms = params.get("duration_ms", 0)
            
            monitor.record_request(method, path, status, duration_ms)
            
            return {
                "success": True,
                "operation": "record_request",
                "method": method,
                "path": path,
                "status": status
            }
        
        elif operation == "stats":
            stats = monitor.get_stats()
            return {
                "success": True,
                "operation": "stats",
                "stats": stats
            }
        
        elif operation == "alerts":
            alerts = monitor.evaluate_alerts()
            active = monitor.alert_manager.get_active_alerts()
            
            return {
                "success": True,
                "operation": "alerts",
                "triggered_count": len(alerts),
                "active_count": len(active),
                "alerts": [
                    {"name": a.name, "level": a.level.value, "message": a.message}
                    for a in active
                ]
            }
        
        elif operation == "health":
            health = monitor.check_health()
            return {
                "success": True,
                "operation": "health",
                "health": [
                    {
                        "service": h.service_name,
                        "healthy": h.healthy,
                        "latency_ms": h.latency_ms,
                        "message": h.message
                    }
                    for h in health.values()
                ]
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
