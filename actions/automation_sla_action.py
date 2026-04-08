"""
Automation SLA Action Module.

Provides SLA monitoring and compliance tracking
for automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class SLAStatus(Enum):
    """SLA status."""
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    UNKNOWN = "unknown"


@dataclass
class SLAConfig:
    """SLA configuration."""
    name: str
    metric: str
    target_value: float
    window_seconds: float
    warning_threshold: float = 0.8
    critical_threshold: float = 0.95


@dataclass
class SLAMetric:
    """SLA metric snapshot."""
    timestamp: datetime
    metric: str
    value: float
    target: float


@dataclass
class SLAReport:
    """SLA compliance report."""
    sla_name: str
    status: SLAStatus
    compliance_rate: float
    total_samples: int
    breached_samples: int
    current_value: float
    target_value: float
    generated_at: datetime = field(default_factory=datetime.now)


class SLATracker:
    """Tracks SLA metrics."""

    def __init__(self, config: SLAConfig):
        self.config = config
        self.metrics: List[SLAMetric] = []
        self.breaches: List[SLAMetric] = []

    def record(self, value: float):
        """Record a metric value."""
        metric = SLAMetric(
            timestamp=datetime.now(),
            metric=self.config.metric,
            value=value,
            target=self.config.target_value
        )
        self.metrics.append(metric)

        if not self._is_compliant(value):
            self.breaches.append(metric)

        self._cleanup_old_metrics()

    def _is_compliant(self, value: float) -> bool:
        """Check if value meets SLA."""
        if self.config.metric in ("latency", "duration", "time"):
            return value <= self.config.target_value
        else:
            return value >= self.config.target_value

    def _cleanup_old_metrics(self):
        """Remove old metrics outside window."""
        cutoff = datetime.now() - timedelta(seconds=self.config.window_seconds)
        self.metrics = [m for m in self.metrics if m.timestamp >= cutoff]
        self.breaches = [b for b in self.breaches if b.timestamp >= cutoff]

    def get_status(self) -> SLAStatus:
        """Get current SLA status."""
        if not self.metrics:
            return SLAStatus.UNKNOWN

        self._cleanup_old_metrics()

        if not self.metrics:
            return SLAStatus.UNKNOWN

        recent_breaches = len(self.breaches)
        total = len(self.metrics)
        breach_rate = recent_breaches / total if total > 0 else 0

        if breach_rate >= self.config.critical_threshold:
            return SLAStatus.BREACHED
        elif breach_rate >= self.config.warning_threshold:
            return SLAStatus.AT_RISK
        else:
            return SLAStatus.COMPLIANT

    def get_compliance_rate(self) -> float:
        """Get compliance rate."""
        if not self.metrics:
            return 1.0

        self._cleanup_old_metrics()

        compliant = len(self.metrics) - len(self.breaches)
        return compliant / len(self.metrics) if self.metrics else 1.0

    def get_report(self) -> SLAReport:
        """Generate SLA report."""
        current_value = self.metrics[-1].value if self.metrics else 0

        return SLAReport(
            sla_name=self.config.name,
            status=self.get_status(),
            compliance_rate=self.get_compliance_rate(),
            total_samples=len(self.metrics),
            breached_samples=len(self.breaches),
            current_value=current_value,
            target_value=self.config.target_value
        )


class SLAMonitor:
    """Monitors multiple SLAs."""

    def __init__(self):
        self.slas: Dict[str, SLATracker] = {}
        self.handlers: Dict[SLAStatus, List[Callable]] = {
            status: [] for status in SLAStatus
        }

    def add_sla(self, config: SLAConfig):
        """Add an SLA to monitor."""
        self.slas[config.name] = SLATracker(config)

    def remove_sla(self, name: str) -> bool:
        """Remove an SLA."""
        if name in self.slas:
            del self.slas[name]
            return True
        return False

    def record(self, sla_name: str, value: float):
        """Record metric for SLA."""
        tracker = self.slas.get(sla_name)
        if tracker:
            old_status = tracker.get_status()
            tracker.record(value)
            new_status = tracker.get_status()

            if old_status != new_status:
                self._notify_status_change(sla_name, new_status)

    def get_status(self, sla_name: str) -> SLAStatus:
        """Get status of an SLA."""
        tracker = self.slas.get(sla_name)
        return tracker.get_status() if tracker else SLAStatus.UNKNOWN

    def get_all_statuses(self) -> Dict[str, SLAStatus]:
        """Get statuses of all SLAs."""
        return {name: tracker.get_status() for name, tracker in self.slas.items()}

    def register_handler(self, status: SLAStatus, handler: Callable):
        """Register status change handler."""
        self.handlers[status].append(handler)

    def _notify_status_change(self, sla_name: str, status: SLAStatus):
        """Notify handlers of status change."""
        for handler in self.handlers[status]:
            try:
                handler(sla_name, status)
            except Exception as e:
                logger.error(f"SLA handler error: {e}")

    def generate_reports(self) -> List[SLAReport]:
        """Generate reports for all SLAs."""
        return [tracker.get_report() for tracker in self.slas.values()]


class SLAAlerting:
    """Handles SLA alerting."""

    def __init__(self, monitor: SLAMonitor):
        self.monitor = monitor
        self.alert_history: List[Dict[str, Any]] = []

    def check_and_alert(self, sla_name: str):
        """Check SLA and send alerts if needed."""
        status = self.monitor.get_status(sla_name)

        if status == SLAStatus.BREACHED:
            self._send_alert(sla_name, status, "SLA BREACHED!")
        elif status == SLAStatus.AT_RISK:
            self._send_alert(sla_name, status, "SLA at risk")

    def _send_alert(self, sla_name: str, status: SLAStatus, message: str):
        """Send alert."""
        self.alert_history.append({
            "sla_name": sla_name,
            "status": status,
            "message": message,
            "timestamp": datetime.now()
        })
        logger.warning(f"SLA Alert: {sla_name} - {message}")


def main():
    """Demonstrate SLA monitoring."""
    monitor = SLAMonitor()

    monitor.add_sla(SLAConfig(
        name="api_latency",
        metric="latency",
        target_value=100.0,
        window_seconds=60
    ))

    for i in range(10):
        value = 80.0 + (i % 3) * 20
        monitor.record("api_latency", value)

    status = monitor.get_status("api_latency")
    report = monitor.slas["api_latency"].get_report()

    print(f"SLA Status: {status.value}")
    print(f"Compliance: {report.compliance_rate:.1%}")


if __name__ == "__main__":
    main()
