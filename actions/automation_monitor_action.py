"""Automation Monitor Action Module.

Provides monitoring utilities: metrics collection, health checks,
alerting rules, dashboards, and observability for automated systems.

Example:
    result = execute(context, {"action": "record_metric", "name": "cpu_usage", "value": 85.5})
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import time


@dataclass
class Metric:
    """A single metric measurement."""
    
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    labels: dict[str, str] = field(default_factory=dict)
    unit: str = ""
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "labels": self.labels,
            "unit": self.unit,
        }


@dataclass
class Alert:
    """An alert condition."""
    
    id: str
    name: str
    condition: str
    threshold: float
    severity: str = "warning"
    fired_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    message: str = ""
    
    @property
    def is_active(self) -> bool:
        """Check if alert is currently firing."""
        return self.fired_at is not None and self.resolved_at is None


class MetricsCollector:
    """Collects and aggregates metrics."""
    
    def __init__(self, retention_minutes: int = 60) -> None:
        """Initialize metrics collector.
        
        Args:
            retention_minutes: How long to retain metrics
        """
        self.retention_minutes = retention_minutes
        self._metrics: dict[str, deque[Metric]] = defaultdict(
            lambda: deque(maxlen=10000)
        )
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
    
    def record(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
        unit: str = "",
    ) -> None:
        """Record a metric.
        
        Args:
            name: Metric name
            value: Metric value
            labels: Optional labels
            unit: Optional unit
        """
        metric = Metric(
            name=name,
            value=value,
            labels=labels or {},
            unit=unit,
        )
        self._metrics[name].append(metric)
        self._cleanup_old(name)
    
    def increment(self, name: str, amount: float = 1.0) -> None:
        """Increment a counter metric.
        
        Args:
            name: Counter name
            amount: Amount to increment
        """
        self._counters[name] += amount
    
    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge metric.
        
        Args:
            name: Gauge name
            value: Gauge value
        """
        self._gauges[name] = value
    
    def get(
        self,
        name: str,
        duration_seconds: Optional[float] = None,
    ) -> list[Metric]:
        """Get metrics by name.
        
        Args:
            name: Metric name
            duration_seconds: Optional time filter
            
        Returns:
            List of metrics
        """
        if name not in self._metrics:
            return []
        
        metrics = list(self._metrics[name])
        
        if duration_seconds:
            cutoff = datetime.now() - timedelta(seconds=duration_seconds)
            metrics = [m for m in metrics if m.timestamp >= cutoff]
        
        return metrics
    
    def get_stats(self, name: str, duration_seconds: float = 60) -> dict[str, Any]:
        """Get statistics for a metric.
        
        Args:
            name: Metric name
            duration_seconds: Time window
            
        Returns:
            Statistics dictionary
        """
        metrics = self.get(name, duration_seconds)
        
        if not metrics:
            return {
                "count": 0,
                "min": 0.0,
                "max": 0.0,
                "avg": 0.0,
            }
        
        values = [m.value for m in metrics]
        
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "sum": sum(values),
            "p50": self._percentile(values, 50),
            "p95": self._percentile(values, 95),
            "p99": self._percentile(values, 99),
        }
    
    @staticmethod
    def _percentile(values: list[float], p: float) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    def _cleanup_old(self, name: str) -> None:
        """Remove old metrics beyond retention."""
        cutoff = datetime.now() - timedelta(minutes=self.retention_minutes)
        metrics = self._metrics[name]
        
        while metrics and metrics[0].timestamp < cutoff:
            metrics.popleft()
    
    def get_all_gauge_values(self) -> dict[str, float]:
        """Get all current gauge values."""
        return dict(self._gauges)
    
    def get_all_counter_values(self) -> dict[str, float]:
        """Get all counter values."""
        return dict(self._counters)


class HealthChecker:
    """Performs health checks on components."""
    
    def __init__(self) -> None:
        """Initialize health checker."""
        self._checks: dict[str, Callable[[], bool]] = {}
    
    def register_check(self, name: str, check_fn: Callable[[], bool]) -> None:
        """Register a health check.
        
        Args:
            name: Check name
            check_fn: Function returning True if healthy
        """
        self._checks[name] = check_fn
    
    def check(self, name: str) -> dict[str, Any]:
        """Run a specific health check.
        
        Args:
            name: Check name
            
        Returns:
            Check result
        """
        if name not in self._checks:
            return {
                "name": name,
                "status": "unknown",
                "error": "Check not found",
            }
        
        try:
            start = time.time()
            is_healthy = self._checks[name]()
            duration_ms = (time.time() - start) * 1000
            
            return {
                "name": name,
                "status": "healthy" if is_healthy else "unhealthy",
                "duration_ms": round(duration_ms, 2),
            }
        except Exception as e:
            return {
                "name": name,
                "status": "error",
                "error": str(e),
            }
    
    def check_all(self) -> dict[str, Any]:
        """Run all health checks.
        
        Returns:
            Overall health status
        """
        results = {}
        healthy_count = 0
        
        for name in self._checks:
            result = self.check(name)
            results[name] = result
            if result["status"] == "healthy":
                healthy_count += 1
        
        total = len(self._checks)
        all_healthy = healthy_count == total
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "healthy_count": healthy_count,
            "total_count": total,
            "checks": results,
        }


class AlertManager:
    """Manages alerting rules and alert state."""
    
    def __init__(self) -> None:
        """Initialize alert manager."""
        self._rules: dict[str, Alert] = {}
        self._active_alerts: dict[str, Alert] = {}
    
    def add_rule(self, alert: Alert) -> None:
        """Add an alert rule.
        
        Args:
            alert: Alert definition
        """
        self._rules[alert.id] = alert
    
    def evaluate(
        self,
        metric_name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> list[Alert]:
        """Evaluate alerts for a metric value.
        
        Args:
            metric_name: Metric name
            value: Current value
            labels: Metric labels
            
        Returns:
            List of firing alerts
        """
        firing = []
        
        for alert in self._rules.values():
            if metric_name not in alert.condition:
                continue
            
            should_fire = self._evaluate_condition(
                alert.condition,
                value,
                alert.threshold,
            )
            
            if should_fire and alert.id not in self._active_alerts:
                alert.fired_at = datetime.now()
                self._active_alerts[alert.id] = alert
                firing.append(alert)
            elif not should_fire and alert.id in self._active_alerts:
                self._active_alerts[alert.id].resolved_at = datetime.now()
                del self._active_alerts[alert.id]
        
        return firing
    
    @staticmethod
    def _evaluate_condition(
        condition: str,
        value: float,
        threshold: float,
    ) -> bool:
        """Evaluate alert condition."""
        if condition == ">":
            return value > threshold
        elif condition == ">=":
            return value >= threshold
        elif condition == "<":
            return value < threshold
        elif condition == "<=":
            return value <= threshold
        elif condition == "==":
            return value == threshold
        elif condition == "!=":
            return value != threshold
        return False
    
    def get_active_alerts(self) -> list[Alert]:
        """Get all currently active alerts."""
        return list(self._active_alerts.values())
    
    def get_alert_history(self) -> list[dict[str, Any]]:
        """Get alert history."""
        all_alerts = list(self._rules.values())
        return [
            {
                "id": a.id,
                "name": a.name,
                "severity": a.severity,
                "fired_at": a.fired_at.isoformat() if a.fired_at else None,
                "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
                "is_active": a.is_active,
            }
            for a in all_alerts
        ]


class DashboardBuilder:
    """Builds monitoring dashboards."""
    
    def __init__(self) -> None:
        """Initialize dashboard builder."""
        self._panels: list[dict[str, Any]] = []
    
    def add_graph_panel(
        self,
        title: str,
        metrics: list[str],
        width: int = 6,
    ) -> "DashboardBuilder":
        """Add a graph panel.
        
        Args:
            title: Panel title
            metrics: Metrics to display
            width: Panel width
            
        Returns:
            Self for chaining
        """
        self._panels.append({
            "type": "graph",
            "title": title,
            "metrics": metrics,
            "width": width,
        })
        return self
    
    def add_stat_panel(
        self,
        title: str,
        metric: str,
        width: int = 3,
    ) -> "DashboardBuilder":
        """Add a stat panel.
        
        Args:
            title: Panel title
            metric: Metric to display
            width: Panel width
            
        Returns:
            Self for chaining
        """
        self._panels.append({
            "type": "stat",
            "title": title,
            "metric": metric,
            "width": width,
        })
        return self
    
    def add_alert_panel(
        self,
        title: str = "Active Alerts",
        width: int = 6,
    ) -> "DashboardBuilder":
        """Add an alert panel.
        
        Args:
            title: Panel title
            width: Panel width
            
        Returns:
            Self for chaining
        """
        self._panels.append({
            "type": "alert",
            "title": title,
            "width": width,
        })
        return self
    
    def build(self) -> dict[str, Any]:
        """Build dashboard definition.
        
        Returns:
            Dashboard structure
        """
        return {
            "panels": self._panels,
            "panel_count": len(self._panels),
        }


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute automation monitor action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "record_metric":
        collector = MetricsCollector()
        collector.record(
            params.get("name", ""),
            params.get("value", 0.0),
            params.get("labels"),
        )
        result["data"] = {"recorded": True}
    
    elif action == "increment":
        collector = MetricsCollector()
        collector.increment(params.get("name", ""), params.get("amount", 1.0))
        result["data"] = {"incremented": True}
    
    elif action == "set_gauge":
        collector = MetricsCollector()
        collector.set_gauge(params.get("name", ""), params.get("value", 0.0))
        result["data"] = {"set": True}
    
    elif action == "get_stats":
        collector = MetricsCollector()
        stats = collector.get_stats(
            params.get("name", ""),
            params.get("duration_seconds", 60),
        )
        result["data"] = stats
    
    elif action == "health_check":
        checker = HealthChecker()
        status = checker.check_all()
        result["data"] = status
    
    elif action == "register_health_check":
        checker = HealthChecker()
        result["data"] = {"registered": True}
    
    elif action == "evaluate_alert":
        manager = AlertManager()
        firing = manager.evaluate(
            params.get("metric_name", ""),
            params.get("value", 0.0),
        )
        result["data"] = {"firing_count": len(firing)}
    
    elif action == "get_active_alerts":
        manager = AlertManager()
        alerts = manager.get_active_alerts()
        result["data"] = {"active_count": len(alerts)}
    
    elif action == "build_dashboard":
        builder = DashboardBuilder()
        for metric in params.get("metrics", []):
            builder.add_graph_panel(
                title=metric,
                metrics=[metric],
            )
        dashboard = builder.build()
        result["data"] = dashboard
    
    elif action == "gauge_values":
        collector = MetricsCollector()
        result["data"] = {"gauges": collector.get_all_gauge_values()}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
