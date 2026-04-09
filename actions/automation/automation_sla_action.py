"""
Automation SLA Action Module.

SLA (Service Level Agreement) monitoring and enforcement for automation
workflows with availability tracking and breach detection.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class SLAStatus(Enum):
    """SLA compliance status."""
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    UNKNOWN = "unknown"


@dataclass
class SLAMetric:
    """A tracked SLA metric."""
    name: str
    current_value: float = 0.0
    target_value: float = 0.0
    unit: str = ""
    last_updated: float = field(default_factory=time.time)


@dataclass
class SLABreach:
    """An SLA breach event."""
    sla_name: str
    metric_name: str
    expected: float
    actual: float
    severity: str
    timestamp: float


@dataclass
class SLAReport:
    """Complete SLA status report."""
    sla_name: str
    status: SLAStatus
    compliance_percent: float
    metrics: List[SLAMetric]
    breaches: List[SLABreach]
    window_start: float
    window_end: float
    uptime_percent: float


class SLAObserver:
    """Tracks SLA metrics and detects breaches."""

    def __init__(
        self,
        name: str,
        window_seconds: float = 3600.0,
    ) -> None:
        self.name = name
        self.window_seconds = window_seconds
        self._metrics: Dict[str, SLAMetric] = {}
        self._events: List[Dict[str, Any]] = []
        self._breaches: List[SLABreach] = []
        self._window_start = time.time()

    def track_metric(
        self,
        metric_name: str,
        value: float,
        target: float,
        unit: str = "",
    ) -> None:
        """Track a metric value."""
        self._metrics[metric_name] = SLAMetric(
            name=metric_name,
            current_value=value,
            target_value=target,
            unit=unit,
            last_updated=time.time(),
        )

    def record_event(self, event_type: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record an SLA-relevant event."""
        self._events.append({
            "type": event_type,
            "timestamp": time.time(),
            "metadata": metadata or {},
        })

    def check_compliance(self) -> SLAStatus:
        """Check if SLA is currently compliant."""
        if not self._metrics:
            return SLAStatus.UNKNOWN

        for metric in self._metrics.values():
            if metric.current_value < metric.target_value:
                return SLAStatus.AT_RISK

        return SLAStatus.COMPLIANT

    def detect_breach(
        self,
        metric_name: str,
        expected: float,
        actual: float,
        severity: str = "high",
    ) -> None:
        """Record a detected breach."""
        breach = SLABreach(
            sla_name=self.name,
            metric_name=metric_name,
            expected=expected,
            actual=actual,
            severity=severity,
            timestamp=time.time(),
        )
        self._breaches.append(breach)
        logger.warning(f"SLA breach detected: {metric_name} expected={expected} actual={actual}")

    def get_report(self) -> SLAReport:
        """Generate an SLA status report."""
        status = self.check_compliance()

        # Calculate compliance percent
        if self._events:
            window_duration = time.time() - self._window_start
            compliant_events = sum(
                1 for e in self._events
                if e.get("metadata", {}).get("compliant", True)
            )
            compliance = (compliant_events / len(self._events)) * 100
        else:
            compliance = 100.0

        # Calculate uptime
        downtime = sum(
            1 for e in self._events
            if e["type"] == "down"
        )
        uptime = max(0, len(self._events) - downtime)
        uptime_pct = (uptime / max(1, len(self._events))) * 100

        return SLAReport(
            sla_name=self.name,
            status=status,
            compliance_percent=compliance,
            metrics=list(self._metrics.values()),
            breaches=self._breaches.copy(),
            window_start=self._window_start,
            window_end=time.time(),
            uptime_percent=uptime_pct,
        )

    def reset(self) -> None:
        """Reset SLA tracking."""
        self._events = []
        self._breaches = []
        self._window_start = time.time()


class AutomationSLAAction:
    """
    SLA monitoring for automation workflows.

    Tracks service level objectives, monitors compliance,
    and detects/records SLA breaches.

    Example:
        sla_monitor = AutomationSLAAction()

        sla = sla_monitor.create_sla("99.9-uptime", window_seconds=3600)
        sla.track_metric("availability", 99.5, 99.9, "%")
        sla.record_event("heartbeat", {"compliant": True})

        report = sla.get_report()
        print(f"SLA Status: {report.status.value}")
    """

    def __init__(self) -> None:
        self._slas: Dict[str, SLAObserver] = {}
        self._breach_handlers: List[Callable[[SLABreach], None]] = []

    def create_sla(
        self,
        name: str,
        window_seconds: float = 3600.0,
    ) -> SLAObserver:
        """Create a new SLA to monitor."""
        sla = SLAObserver(name, window_seconds)
        self._slas[name] = sla
        return sla

    def get_sla(self, name: str) -> Optional[SLAObserver]:
        """Get an SLA by name."""
        return self._slas.get(name)

    def register_breach_handler(
        self,
        handler: Callable[[SLABreach], None],
    ) -> None:
        """Register a handler for SLA breach events."""
        self._breach_handlers.append(handler)

    async def monitor_loop(
        self,
        interval_seconds: float = 60.0,
    ) -> None:
        """Run continuous SLA monitoring."""
        while True:
            for sla in self._slas.values():
                report = sla.get_report()
                if report.status == SLAStatus.BREACHED:
                    for breach in report.breaches[-1:]:  # Latest breaches
                        for handler in self._breach_handlers:
                            try:
                                handler(breach)
                            except Exception as e:
                                logger.error(f"Breach handler error: {e}")

            await asyncio.sleep(interval_seconds)

    def get_all_reports(self) -> List[SLAReport]:
        """Get reports for all SLAs."""
        return [sla.get_report() for sla in self._slas.values()]

    def get_breaches(
        self,
        since: Optional[float] = None,
    ) -> List[SLABreach]:
        """Get all breaches, optionally filtered by time."""
        all_breaches = []
        for sla in self._slas.values():
            breaches = sla._breaches
            if since:
                breaches = [b for b in breaches if b.timestamp >= since]
            all_breaches.extend(breaches)
        return sorted(all_breaches, key=lambda b: b.timestamp, reverse=True)
