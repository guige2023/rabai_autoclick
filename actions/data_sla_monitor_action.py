"""
Data SLA Monitor Action.

Monitors data processing metrics against defined SLA thresholds,
providing real-time alerts and compliance reporting.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class SLAStatus(Enum):
    """SLA compliance status."""
    PASS = auto()
    WARNING = auto()
    BREACH = auto()
    UNKNOWN = auto()


class MetricType(Enum):
    """Types of SLA metrics."""
    LATENCY = auto()
    THROUGHPUT = auto()
    ERROR_RATE = auto()
    AVAILABILITY = auto()
    FRESHNESS = auto()
    VOLUME = auto()


@dataclass
class SLAThreshold:
    """A single SLA threshold definition."""
    name: str
    metric_type: MetricType
    warning_at: float
    breach_at: float
    operator: str = "gt"  # gt, lt, gte, lte, eq
    window_seconds: Optional[int] = None

    def evaluate(self, value: float) -> SLAStatus:
        ops = {
            "gt": lambda v, t: v > t,
            "lt": lambda v, t: v < t,
            "gte": lambda v, t: v >= t,
            "lte": lambda v, t: v <= t,
            "eq": lambda v, t: v == t,
        }
        op = ops.get(self.operator, lambda v, t: v > t)
        if op(value, self.breach_at):
            return SLAStatus.BREACH
        if op(value, self.warning_at):
            return SLAStatus.WARNING
        return SLAStatus.PASS


@dataclass
class MetricSnapshot:
    """A single metric measurement."""
    metric_name: str
    value: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SLAViolation:
    """Record of an SLA breach or warning."""
    threshold_name: str
    status: SLAStatus
    metric_value: float
    threshold_value: float
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float = 0.0
    description: str = ""


@dataclass
class SLAReport:
    """SLA compliance report."""
    period_start: datetime
    period_end: datetime
    total_measurements: int = 0
    sla_metrics: Dict[str, List[SLAViolation]] = field(default_factory=dict)
    uptime_percent: float = 100.0
    avg_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    error_rate_percent: float = 0.0

    def overall_status(self) -> SLAStatus:
        all_violations = [v for violations in self.sla_metrics.values() for v in violations]
        if any(v.status == SLAStatus.BREACH for v in all_violations):
            return SLAStatus.BREACH
        if any(v.status == SLAStatus.WARNING for v in all_violations):
            return SLAStatus.WARNING
        return SLAStatus.PASS


class DataSLAMonitor:
    """
    Monitor data processing against SLA thresholds.

    Example:
        monitor = DataSLAMonitor()
        monitor.define_threshold("p99_latency", MetricType.LATENCY, warning_at=200, breach_at=500, operator="gt")
        monitor.define_threshold("error_rate", MetricType.ERROR_RATE, warning_at=1.0, breach_at=5.0, operator="gt")
        monitor.record("p99_latency", 320.5)
        report = monitor.generate_report()
    """

    def __init__(self) -> None:
        self._thresholds: Dict[str, SLAThreshold] = {}
        self._measurements: Dict[str, List[MetricSnapshot]] = {}
        self._violations: List[SLAViolation] = []
        self._active_breaches: Dict[str, SLAViolation] = {}
        self._breach_callbacks: List[Callable[[SLAViolation], None]] = []
        self._window_size = 3600  # 1 hour default window

    def define_threshold(
        self,
        name: str,
        metric_type: MetricType,
        warning_at: float,
        breach_at: float,
        operator: str = "gt",
        window_seconds: Optional[int] = None,
    ) -> Self:
        """Define an SLA threshold."""
        self._thresholds[name] = SLAThreshold(
            name=name,
            metric_type=metric_type,
            warning_at=warning_at,
            breach_at=breach_at,
            operator=operator,
            window_seconds=window_seconds or self._window_size,
        )
        return self

    def record(self, metric_name: str, value: float,
               tags: Optional[Dict[str, str]] = None) -> Optional[SLAViolation]:
        """Record a metric measurement and check against thresholds."""
        snapshot = MetricSnapshot(
            metric_name=metric_name,
            value=value,
            tags=tags or {},
        )
        if metric_name not in self._measurements:
            self._measurements[metric_name] = []
        self._measurements[metric_name].append(snapshot)

        # Check threshold
        threshold = self._thresholds.get(metric_name)
        if threshold:
            status = threshold.evaluate(value)
            if status != SLAStatus.PASS:
                violation = SLAViolation(
                    threshold_name=metric_name,
                    status=status,
                    metric_value=value,
                    threshold_value=threshold.breach_at if status == SLAStatus.BREACH else threshold.warning_at,
                    description=f"{metric_name}={value} ({status.name})",
                )
                self._violations.append(violation)
                if status == SLAStatus.BREACH:
                    self._active_breaches[metric_name] = violation
                    for cb in self._breach_callbacks:
                        cb(violation)
                return violation
        return None

    def on_breach(self, callback: Callable[[SLAViolation], None]) -> None:
        """Register a callback for SLA breach events."""
        self._breach_callbacks.append(callback)

    def get_status(self, metric_name: str) -> SLAStatus:
        """Get current SLA status for a metric."""
        if metric_name not in self._measurements:
            return SLAStatus.UNKNOWN
        threshold = self._thresholds.get(metric_name)
        if not threshold:
            return SLAStatus.UNKNOWN
        snapshots = self._measurements[metric_name]
        if not snapshots:
            return SLAStatus.UNKNOWN
        latest = max(snapshots, key=lambda s: s.timestamp)
        return threshold.evaluate(latest.value)

    def get_violations(
        self,
        since: Optional[datetime] = None,
        status: Optional[SLAStatus] = None,
    ) -> List[SLAViolation]:
        """Get violations, optionally filtered by time and status."""
        violations = self._violations
        if since:
            violations = [v for v in violations if v.detected_at >= since]
        if status:
            violations = [v for v in violations if v.status == status]
        return violations

    def generate_report(
        self,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
    ) -> SLAReport:
        """Generate an SLA compliance report for a time period."""
        period_end = period_end or datetime.now(timezone.utc)
        period_start = period_start or (period_end - timedelta(hours=1))

        report = SLAReport(period_start=period_start, period_end=period_end)

        for metric_name, snapshots in self._measurements.items():
            window_snaps = [s for s in snapshots
                          if period_start <= s.timestamp <= period_end]
            report.total_measurements += len(window_snaps)

            if window_snaps:
                values = [s.value for s in window_snaps]
                report.avg_latency_ms = sum(values) / len(values)
                sorted_values = sorted(values)
                report.p99_latency_ms = sorted_values[int(len(sorted_values) * 0.99)]

            violations = self.get_violations(
                since=period_start,
                status=SLAStatus.BREACH,
            )
            if metric_name in self._thresholds:
                report.sla_metrics[metric_name] = [
                    v for v in violations if v.threshold_name == metric_name
                ]

        return report

    def clear_old_data(self, older_than_seconds: int = 86400) -> int:
        """Clear measurement data older than specified seconds. Returns count cleared."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
        total = 0
        for metric_name in list(self._measurements.keys()):
            before = len(self._measurements[metric_name])
            self._measurements[metric_name] = [
                s for s in self._measurements[metric_name] if s.timestamp > cutoff
            ]
            total += before - len(self._measurements[metric_name])
        return total
