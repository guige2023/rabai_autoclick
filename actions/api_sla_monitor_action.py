"""
API SLA Monitor Action Module.

Tracks SLA compliance, availability, latency percentiles,
and generates compliance reports.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)


class SLAMetric(Enum):
    """SLA metric types."""
    AVAILABILITY = "availability"
    LATENCY_P50 = "latency_p50"
    LATENCY_P95 = "latency_p95"
    LATENCY_P99 = "latency_p99"
    ERROR_RATE = "error_rate"


@dataclass
class SLATarget:
    """SLA target definition."""
    metric: SLAMetric
    threshold: float
    window: timedelta = field(default_factory=lambda: timedelta(hours=1))


@dataclass
class SLARecord:
    """Single SLA measurement record."""
    timestamp: datetime
    success: bool
    latency_ms: float
    status_code: int
    endpoint: str = ""


@dataclass
class SLACompliance:
    """SLA compliance report."""
    metric: SLAMetric
    target: float
    actual: float
    compliant: bool
    window_start: datetime
    window_end: datetime
    sample_count: int


class APISLAMonitorAction:
    """
    SLA compliance monitoring and reporting.

    Tracks availability, latency, and error rates against
    configured targets over sliding time windows.

    Example:
        monitor = APISLAMonitorAction()
        monitor.add_target(SLATarget(SLAMetric.AVAILABILITY, 99.9))
        monitor.add_target(SLATarget(SLAMetric.LATENCY_P99, 500.0))
        monitor.record(success=True, latency_ms=120)
        report = monitor.get_compliance()
    """

    def __init__(
        self,
        window: timedelta = timedelta(hours=1),
        storage_size: int = 10000,
    ) -> None:
        self.window = window
        self.storage_size = storage_size
        self._targets: list[SLATarget] = []
        self._records: list[SLARecord] = []
        self._by_endpoint: dict[str, list[SLARecord]] = defaultdict(list)

    def add_target(self, target: SLATarget) -> None:
        """Add an SLA target to monitor."""
        self._targets.append(target)

    def record(
        self,
        success: bool,
        latency_ms: float,
        status_code: int = 200,
        endpoint: str = "",
    ) -> None:
        """Record an API call result."""
        record = SLARecord(
            timestamp=datetime.now(),
            success=success,
            latency_ms=latency_ms,
            status_code=status_code,
            endpoint=endpoint,
        )
        self._records.append(record)
        self._by_endpoint[endpoint].append(record)

        if len(self._records) > self.storage_size:
            self._records = self._records[-self.storage_size:]

    def get_compliance(
        self,
        metric: Optional[SLAMetric] = None,
        endpoint: Optional[str] = None,
    ) -> list[SLACompliance]:
        """Calculate SLA compliance for all or specific metric."""
        records = self._get_window_records(endpoint)
        if not records:
            return []

        window_end = datetime.now()
        window_start = window_end - self.window

        results: list[SLACompliance] = []
        targets = (
            [t for t in self._targets if t.metric == metric]
            if metric else self._targets
        )

        for target in targets:
            actual = self._calculate_metric(target.metric, records)
            compliant = self._check_compliance(target.metric, actual, target.threshold)

            results.append(SLACompliance(
                metric=target.metric,
                target=target.threshold,
                actual=actual,
                compliant=compliant,
                window_start=window_start,
                window_end=window_end,
                sample_count=len(records),
            ))

        return results

    def get_percentile(
        self,
        percentile: float,
        endpoint: Optional[str] = None,
    ) -> float:
        """Get latency percentile."""
        records = self._get_window_records(endpoint)
        if not records:
            return 0.0

        latencies = sorted(r.latency_ms for r in records)
        idx = int(len(latencies) * percentile / 100)
        idx = min(idx, len(latencies) - 1)
        return latencies[idx]

    def get_availability(
        self,
        endpoint: Optional[str] = None,
    ) -> float:
        """Get availability percentage."""
        records = self._get_window_records(endpoint)
        if not records:
            return 100.0

        successful = sum(1 for r in records if r.success and r.status_code < 400)
        return (successful / len(records)) * 100

    def _get_window_records(
        self,
        endpoint: Optional[str] = None,
    ) -> list[SLARecord]:
        """Get records within the monitoring window."""
        cutoff = datetime.now() - self.window
        records = self._records if not endpoint else self._by_endpoint.get(endpoint, [])
        return [r for r in records if r.timestamp >= cutoff]

    def _calculate_metric(
        self,
        metric: SLAMetric,
        records: list[SLARecord],
    ) -> float:
        """Calculate a specific SLA metric."""
        if metric == SLAMetric.AVAILABILITY:
            if not records:
                return 100.0
            successful = sum(1 for r in records if r.success and r.status_code < 400)
            return (successful / len(records)) * 100

        if metric == SLAMetric.ERROR_RATE:
            if not records:
                return 0.0
            errors = sum(1 for r in records if not r.success or r.status_code >= 400)
            return (errors / len(records)) * 100

        latencies = sorted(r.latency_ms for r in records)
        if not latencies:
            return 0.0

        if metric == SLAMetric.LATENCY_P50:
            idx = len(latencies) // 2
        elif metric == SLAMetric.LATENCY_P95:
            idx = int(len(latencies) * 0.95)
        elif metric == SLAMetric.LATENCY_P99:
            idx = int(len(latencies) * 0.99)
        else:
            idx = len(latencies) // 2

        idx = min(idx, len(latencies) - 1)
        return latencies[idx]

    def _check_compliance(
        self,
        metric: SLAMetric,
        actual: float,
        threshold: float,
    ) -> bool:
        """Check if actual value meets the threshold."""
        if metric in (SLAMetric.AVAILABILITY,):
            return actual >= threshold
        return actual <= threshold
