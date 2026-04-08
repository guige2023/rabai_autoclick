"""
Observability Action Module.

Provides comprehensive observability capabilities including metrics collection,
distributed tracing, health checks, alerting, and SLO tracking.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    RATE = "rate"


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "type": self.metric_type.value,
            "labels": self.labels,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SLOTarget:
    """SLO target definition."""
    id: str
    name: str
    target_percentage: float
    window: str  # "30d", "7d", "1d"
    good_events: int = 0
    total_events: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def current_percentage(self) -> float:
        if self.total_events == 0:
            return 100.0
        return (self.good_events / self.total_events) * 100

    @property
    def is_healthy(self) -> bool:
        return self.current_percentage >= self.target_percentage


@dataclass
class Alert:
    """An observability alert."""
    id: str
    name: str
    severity: AlertSeverity
    message: str
    metric_name: str
    current_value: float
    threshold: float
    fired_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    labels: Dict[str, str] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.resolved_at is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "severity": self.severity.value,
            "message": self.message,
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "fired_at": self.fired_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "is_active": self.is_active,
            "labels": self.labels,
        }


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    component: str
    healthy: bool
    latency_ms: Optional[float] = None
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "healthy": self.healthy,
            "latency_ms": self.latency_ms,
            "message": self.message,
            "details": self.details,
        }


class MetricsCollector:
    """In-memory metrics collector with aggregation."""

    def __init__(self, retention_seconds: int = 3600):
        self.retention_seconds = retention_seconds
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._series: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._labels_index: Dict[str, List[MetricPoint]] = defaultdict(list)

    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        self._counters[name] += value
        point = MetricPoint(name=name, value=self._counters[name], metric_type=MetricType.COUNTER, labels=labels or {})
        self._add_to_series(name, point)

    def gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        self._gauges[name] = value
        point = MetricPoint(name=name, value=value, metric_type=MetricType.GAUGE, labels=labels or {})
        self._add_to_series(name, point)

    def histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value."""
        self._histograms[name].append(value)
        point = MetricPoint(name=name, value=value, metric_type=MetricType.HISTOGRAM, labels=labels or {})
        self._add_to_series(name, point)

    def get(self, name: str) -> Optional[float]:
        """Get current value of a metric."""
        if name in self._counters:
            return self._counters[name]
        if name in self._gauges:
            return self._gauges[name]
        return None

    def get_histogram_stats(self, name: str) -> Dict[str, float]:
        """Get histogram statistics."""
        values = self._histograms.get(name, [])
        if not values:
            return {}
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        return {
            "count": n,
            "sum": sum(sorted_vals),
            "mean": sum(sorted_vals) / n,
            "min": sorted_vals[0],
            "max": sorted_vals[-1],
            "p50": sorted_vals[int(n * 0.5)],
            "p95": sorted_vals[int(n * 0.95)],
            "p99": sorted_vals[int(n * 0.99)],
        }

    def get_rate(self, name: str, window_seconds: float = 60.0) -> float:
        """Calculate rate of change for a counter."""
        points = list(self._series.get(name, []))
        if len(points) < 2:
            return 0.0
        now = datetime.now()
        cutoff = now - timedelta(seconds=window_seconds)
        recent = [p for p in points if p.timestamp >= cutoff]
        if len(recent) < 2:
            return 0.0
        recent.sort(key=lambda p: p.timestamp)
        time_diff = (recent[-1].timestamp - recent[0].timestamp).total_seconds()
        if time_diff == 0:
            return 0.0
        return (recent[-1].value - recent[0].value) / time_diff

    def _add_to_series(self, name: str, point: MetricPoint) -> None:
        """Add point to time series."""
        self._series[name].append(point)
        if point.labels:
            label_key = json.dumps(point.labels, sort_keys=True)
            self._labels_index[f"{name}:{label_key}"].append(point)

    def get_all_metrics(self) -> Dict[str, Any]:
        """Export all metrics."""
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                name: self.get_histogram_stats(name)
                for name in self._histograms
            },
            "series_count": {name: len(s) for name, s in self._series.items()},
        }


class DistributedTracer:
    """Simple distributed tracing implementation."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._traces: Dict[str, List[Dict]] = defaultdict(list)
        self._active_spans: Dict[str, Dict] = {}

    def start_span(
        self,
        name: str,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> str:
        """Start a new trace span."""
        span_id = str(uuid.uuid4())[:16]
        trace_id = trace_id or str(uuid.uuid4())

        span = {
            "span_id": span_id,
            "trace_id": trace_id,
            "parent_span_id": parent_span_id,
            "name": name,
            "service": self.service_name,
            "start_time": datetime.now().isoformat(),
            "labels": labels or {},
            "status": "active",
        }
        self._active_spans[span_id] = span
        return span_id

    def end_span(self, span_id: str, status: str = "ok", error: Optional[str] = None) -> None:
        """End a span."""
        if span_id not in self._active_spans:
            return
        span = self._active_spans[span_id]
        span["end_time"] = datetime.now().isoformat()
        span["status"] = status
        span["error"] = error

        trace_id = span["trace_id"]
        self._traces[trace_id].append(span)
        del self._active_spans[span_id]

    def get_trace(self, trace_id: str) -> List[Dict]:
        """Get all spans for a trace."""
        return self._traces.get(trace_id, [])


class AlertingEngine:
    """Alerting engine with threshold and anomaly detection."""

    def __init__(self):
        self._rules: Dict[str, Dict[str, Any]] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: List[Alert] = []
        self._handlers: List[Callable] = []

    def add_rule(
        self,
        name: str,
        metric_name: str,
        condition: str,  # "gt", "lt", "gte", "lte"
        threshold: float,
        severity: str = "medium",
        labels: Optional[Dict[str, str]] = None,
    ) -> str:
        """Add an alerting rule."""
        rule_id = str(uuid.uuid4())
        self._rules[rule_id] = {
            "name": name,
            "metric_name": metric_name,
            "condition": condition,
            "threshold": threshold,
            "severity": AlertSeverity(severity),
            "labels": labels or {},
        }
        return rule_id

    def register_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register an alert handler."""
        self._handlers.append(handler)

    def evaluate(self, metric_name: str, value: float) -> List[Alert]:
        """Evaluate metric against all rules."""
        fired = []
        for rule_id, rule in self._rules.items():
            if rule["metric_name"] != metric_name:
                continue

            condition = rule["condition"]
            threshold = rule["threshold"]

            should_fire = False
            if condition == "gt":
                should_fire = value > threshold
            elif condition == "lt":
                should_fire = value < threshold
            elif condition == "gte":
                should_fire = value >= threshold
            elif condition == "lte":
                should_fire = value <= threshold

            if should_fire:
                alert = Alert(
                    id=str(uuid.uuid4()),
                    name=rule["name"],
                    severity=rule["severity"],
                    message=f"{metric_name} {condition} {threshold}: current={value}",
                    metric_name=metric_name,
                    current_value=value,
                    threshold=threshold,
                    labels=rule["labels"],
                )
                self._active_alerts[rule_id] = alert
                self._alert_history.append(alert)
                fired.append(alert)

                for handler in self._handlers:
                    try:
                        handler(alert)
                    except Exception:
                        pass

        return fired

    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get currently active alerts."""
        alerts = [a for a in self._active_alerts.values() if a.is_active]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts

    def resolve_alert(self, rule_id: str) -> bool:
        """Mark an alert as resolved."""
        if rule_id in self._active_alerts:
            self._active_alerts[rule_id].resolved_at = datetime.now()
            return True
        return False


class ObservabilityEngine:
    """
    Unified observability platform combining metrics, tracing, and alerting.

    Provides end-to-end observability for distributed systems with
    health checks, SLO tracking, and automated alerting.

    Example:
        >>> obs = ObservabilityEngine(service_name="api-service")
        >>> obs.metrics.increment("requests_total", labels={"method": "GET"})
        >>> obs.metrics.gauge("cpu_usage", 45.2)
        >>> obs.check_health([health_check_fn])
        >>> report = obs.get_status_report()
    """

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.metrics = MetricsCollector()
        self.tracer = DistributedTracer(service_name)
        self.alerting = AlertingEngine()
        self._health_checks: Dict[str, Callable] = {}
        self._slo_targets: Dict[str, SLOTarget] = {}
        self._start_time = datetime.now()

    def add_health_check(self, name: str, check_fn: Callable[[], HealthCheckResult]) -> None:
        """Register a health check function."""
        self._health_checks[name] = check_fn

    def check_health(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = []
        overall_healthy = True

        for name, check_fn in self._health_checks.items():
            start = time.time()
            try:
                result = check_fn()
                result.latency_ms = (time.time() - start) * 1000
                if not result.healthy:
                    overall_healthy = False
                results.append(result)
            except Exception as e:
                results.append(HealthCheckResult(
                    component=name,
                    healthy=False,
                    latency_ms=(time.time() - start) * 1000,
                    message=str(e),
                ))
                overall_healthy = False

        return {
            "service": self.service_name,
            "healthy": overall_healthy,
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
            "checks": [r.to_dict() for r in results],
        }

    def define_slo(
        self,
        name: str,
        target_percentage: float,
        window: str = "30d",
    ) -> str:
        """Define an SLO target."""
        slo_id = str(uuid.uuid4())
        self._slo_targets[slo_id] = SLOTarget(
            id=slo_id,
            name=name,
            target_percentage=target_percentage,
            window=window,
        )
        return slo_id

    def record_slo_event(self, slo_id: str, good: bool) -> None:
        """Record an SLO event (good or bad)."""
        if slo_id not in self._slo_targets:
            return
        slo = self._slo_targets[slo_id]
        slo.total_events += 1
        if good:
            slo.good_events += 1

    def get_slo_status(self) -> Dict[str, Any]:
        """Get current SLO status."""
        return {
            "slos": {
                slo_id: {
                    "name": slo.name,
                    "target": slo.target_percentage,
                    "current": slo.current_percentage,
                    "is_healthy": slo.is_healthy,
                    "window": slo.window,
                    "good_events": slo.good_events,
                    "total_events": slo.total_events,
                }
                for slo_id, slo in self._slo_targets.items()
            }
        }

    def get_status_report(self) -> Dict[str, Any]:
        """Get comprehensive status report."""
        return {
            "service": self.service_name,
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": (datetime.now() - self._start_time).total_seconds(),
            "health": self.check_health(),
            "metrics": self.metrics.get_all_metrics(),
            "slo_status": self.get_slo_status(),
            "active_alerts": [a.to_dict() for a in self.alerting.get_active_alerts()],
        }


def create_observability_engine(service_name: str) -> ObservabilityEngine:
    """Factory to create observability engine."""
    return ObservabilityEngine(service_name=service_name)
