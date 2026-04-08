"""
Automation SLA Action Module.

Provides SLA monitoring, tracking, and enforcement
for automated workflows and services.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class SlaStatus(Enum):
    """SLA status values."""
    MET = "met"
    BREACHED = "breached"
    AT_RISK = "at_risk"
    UNKNOWN = "unknown"


@dataclass
class SlaDefinition:
    """SLA definition."""
    sla_id: str
    name: str
    metric: str
    target_value: float
    window: timedelta
    severity: str = "high"
    description: str = ""


@dataclass
class SlaMeasurement:
    """SLA measurement."""
    sla_id: str
    measurement_time: datetime
    actual_value: float
    target_value: float
    status: SlaStatus
    percentile: Optional[str] = None


@dataclass
class SlaViolation:
    """SLA violation record."""
    violation_id: str
    sla_id: str
    measured_at: datetime
    actual_value: float
    target_value: float
    severity: str
    acknowledged: bool = False
    resolved_at: Optional[datetime] = None


class SlaTracker:
    """Tracks SLA compliance."""

    def __init__(self):
        self.slas: Dict[str, SlaDefinition] = {}
        self.measurements: Dict[str, List[SlaMeasurement]] = {}
        self.violations: List[SlaViolation] = []
        self._handlers: List[Callable] = []

    def add_sla(self, sla: SlaDefinition):
        """Add SLA definition."""
        self.slas[sla.sla_id] = sla
        self.measurements[sla.sla_id] = []

    def remove_sla(self, sla_id: str) -> bool:
        """Remove SLA definition."""
        if sla_id in self.slas:
            del self.slas[sla_id]
            return True
        return False

    def add_handler(self, handler: Callable):
        """Add violation handler."""
        self._handlers.append(handler)

    def measure(
        self,
        sla_id: str,
        actual_value: float,
        percentile: Optional[str] = None
    ) -> SlaMeasurement:
        """Record SLA measurement."""
        if sla_id not in self.slas:
            return None

        sla = self.slas[sla_id]

        if actual_value <= sla.target_value:
            status = SlaStatus.MET
        elif actual_value <= sla.target_value * 1.1:
            status = SlaStatus.AT_RISK
        else:
            status = SlaStatus.BREACHED

        measurement = SlaMeasurement(
            sla_id=sla_id,
            measurement_time=datetime.now(),
            actual_value=actual_value,
            target_value=sla.target_value,
            status=status,
            percentile=percentile
        )

        self.measurements[sla_id].append(measurement)

        if len(self.measurements[sla_id]) > 1000:
            self.measurements[sla_id] = self.measurements[sla_id][-1000:]

        if status == SlaStatus.BREACHED:
            violation = SlaViolation(
                violation_id=str(id(measurement)),
                sla_id=sla_id,
                measured_at=datetime.now(),
                actual_value=actual_value,
                target_value=sla.target_value,
                severity=sla.severity
            )
            self.violations.append(violation)

            for handler in self._handlers:
                try:
                    handler(violation)
                except Exception:
                    pass

        return measurement

    def get_status(self, sla_id: str) -> SlaStatus:
        """Get current SLA status."""
        if sla_id not in self.measurements or not self.measurements[sla_id]:
            return SlaStatus.UNKNOWN

        recent = self.measurements[sla_id][-10:]
        breached_count = sum(1 for m in recent if m.status == SlaStatus.BREACHED)

        if breached_count >= 5:
            return SlaStatus.BREACHED
        elif breached_count >= 2:
            return SlaStatus.AT_RISK

        return SlaStatus.MET

    def get_violations(
        self,
        sla_id: Optional[str] = None,
        since: Optional[datetime] = None,
        unacknowledged_only: bool = False
    ) -> List[SlaViolation]:
        """Get SLA violations."""
        violations = self.violations

        if sla_id:
            violations = [v for v in violations if v.sla_id == sla_id]

        if since:
            violations = [v for v in violations if v.measured_at >= since]

        if unacknowledged_only:
            violations = [v for v in violations if not v.acknowledged]

        return violations

    def acknowledge_violation(self, violation_id: str):
        """Acknowledge a violation."""
        for violation in self.violations:
            if str(id(violation)) == violation_id:
                violation.acknowledged = True

    def resolve_violation(self, violation_id: str):
        """Resolve a violation."""
        for violation in self.violations:
            if str(id(violation)) == violation_id:
                violation.resolved_at = datetime.now()


class SlaReporter:
    """Reports SLA status."""

    def __init__(self, tracker: SlaTracker):
        self.tracker = tracker

    def generate_report(self) -> Dict[str, Any]:
        """Generate SLA report."""
        sla_statuses = {}

        for sla_id, sla in self.tracker.slas.items():
            status = self.tracker.get_status(sla_id)
            recent_measurements = self.tracker.measurements.get(sla_id, [])[-100:]

            met_count = sum(1 for m in recent_measurements if m.status == SlaStatus.MET)
            total_count = len(recent_measurements)

            sla_statuses[sla_id] = {
                "name": sla.name,
                "status": status.value,
                "compliance_rate": met_count / total_count if total_count > 0 else 0,
                "recent_measurements": len(recent_measurements)
            }

        unacknowledged_violations = self.tracker.get_violations(unacknowledged_only=True)

        return {
            "report_time": datetime.now().isoformat(),
            "total_slas": len(self.tracker.slas),
            "slas": sla_statuses,
            "unacknowledged_violations": len(unacknowledged_violations)
        }


class SlaEnforcer:
    """Enforces SLA requirements."""

    def __init__(self, tracker: SlaTracker):
        self.tracker = tracker
        self._pre_handlers: Dict[str, List[Callable]] = {}
        self._post_handlers: Dict[str, List[Callable]] = {}

    def add_pre_handler(self, sla_id: str, handler: Callable):
        """Add pre-execution handler."""
        if sla_id not in self._pre_handlers:
            self._pre_handlers[sla_id] = []
        self._pre_handlers[sla_id].append(handler)

    def add_post_handler(self, sla_id: str, handler: Callable):
        """Add post-execution handler."""
        if sla_id not in self._post_handlers:
            self._post_handlers[sla_id] = []
        self._post_handlers[sla_id].append(handler)

    async def execute_with_sla(
        self,
        sla_id: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute function with SLA monitoring."""
        pre_handlers = self._pre_handlers.get(sla_id, [])
        post_handlers = self._post_handlers.get(sla_id, [])

        for handler in pre_handlers:
            try:
                await handler()
            except Exception as e:
                logger.error(f"Pre-handler error: {e}")

        start_time = datetime.now()
        try:
            result = await func(*args, **kwargs)
        finally:
            duration = (datetime.now() - start_time).total_seconds() * 1000
            self.tracker.measure(sla_id, duration)

        for handler in post_handlers:
            try:
                await handler(result)
            except Exception as e:
                logger.error(f"Post-handler error: {e}")

        return result


async def main():
    """Demonstrate SLA tracking."""
    tracker = SlaTracker()

    sla = SlaDefinition(
        sla_id="sla-1",
        name="API Response Time",
        metric="latency_ms",
        target_value=100.0,
        window=timedelta(hours=1)
    )
    tracker.add_sla(sla)

    tracker.measure("sla-1", 50.0)
    tracker.measure("sla-1", 80.0)
    tracker.measure("sla-1", 120.0)

    status = tracker.get_status("sla-1")
    print(f"SLA Status: {status.value}")

    violations = tracker.get_violations()
    print(f"Violations: {len(violations)}")


if __name__ == "__main__":
    asyncio.run(main())
