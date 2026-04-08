"""
API Analytics Action Module.

Tracks and analyzes API usage patterns, generates
performance reports and usage dashboards.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class APIcallRecord:
    """Single API call record."""
    endpoint: str
    method: str
    status_code: int
    latency_ms: float
    timestamp: float
    error: Optional[str] = None


@dataclass
class EndpointStats:
    """Statistics for a specific endpoint."""
    endpoint: str
    method: str
    total_calls: int
    success_count: int
    error_count: int
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float


@dataclass
class AnalyticsReport:
    """Complete API analytics report."""
    period_start: float
    period_end: float
    total_calls: int
    total_errors: int
    overall_success_rate: float
    avg_latency_ms: float
    top_endpoints: list[EndpointStats]
    error_breakdown: dict[str, int]


class APIAnalyticsAction:
    """
    API usage analytics and reporting.

    Tracks call volume, latency distributions,
    error rates, and generates analytics reports.

    Example:
        analytics = APIAnalyticsAction()
        analytics.record(endpoint="/api/users", method="GET", status_code=200, latency_ms=45)
        report = analytics.generate_report()
    """

    def __init__(
        self,
        retention_period: timedelta = timedelta(days=7),
        sample_rate: float = 1.0,
    ) -> None:
        self.retention_period = retention_period
        self.sample_rate = sample_rate
        self._records: list[APIcallRecord] = []
        self._endpoint_stats: dict[str, list[float]] = defaultdict(list)
        self._error_counts: dict[str, int] = defaultdict(int)

    def record(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency_ms: float,
        error: Optional[str] = None,
    ) -> None:
        """Record an API call."""
        import random
        if random.random() > self.sample_rate:
            return

        record = APIcallRecord(
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            latency_ms=latency_ms,
            timestamp=time.time(),
            error=error,
        )

        self._records.append(record)

        key = f"{method}:{endpoint}"
        self._endpoint_stats[key].append(latency_ms)

        if status_code >= 400 or error:
            error_key = error or f"HTTP_{status_code}"
            self._error_counts[error_key] += 1

        self._prune_old_records()

    def get_endpoint_stats(
        self,
        endpoint: str,
        method: str,
    ) -> Optional[EndpointStats]:
        """Get statistics for a specific endpoint."""
        key = f"{method}:{endpoint}"
        latencies = self._endpoint_stats.get(key, [])

        if not latencies:
            return None

        success_count = sum(1 for r in self._records
                          if f"{r.method}:{r.endpoint}" == key and r.status_code < 400)
        error_count = sum(1 for r in self._records
                         if f"{r.method}:{r.endpoint}" == key and r.status_code >= 400)

        sorted_latencies = sorted(latencies)

        return EndpointStats(
            endpoint=endpoint,
            method=method,
            total_calls=len(latencies),
            success_count=success_count,
            error_count=error_count,
            avg_latency_ms=sum(latencies) / len(latencies),
            p50_latency_ms=self._percentile(sorted_latencies, 50),
            p95_latency_ms=self._percentile(sorted_latencies, 95),
            p99_latency_ms=self._percentile(sorted_latencies, 99),
            min_latency_ms=min(latencies),
            max_latency_ms=max(latencies),
        )

    def generate_report(
        self,
        since: Optional[float] = None,
    ) -> AnalyticsReport:
        """Generate comprehensive analytics report."""
        records = self._records
        if since:
            records = [r for r in records if r.timestamp >= since]

        if not records:
            now = time.time()
            return AnalyticsReport(
                period_start=now - 3600,
                period_end=now,
                total_calls=0,
                total_errors=0,
                overall_success_rate=100.0,
                avg_latency_ms=0.0,
                top_endpoints=[],
                error_breakdown={},
            )

        total_calls = len(records)
        total_errors = sum(1 for r in records if r.status_code >= 400)
        all_latencies = [r.latency_ms for r in records]

        top_endpoints_dict: dict[str, list[float]] = defaultdict(list)
        for r in records:
            key = f"{r.method}:{r.endpoint}"
            top_endpoints_dict[key].append(r.latency_ms)

        top_stats = []
        for key, latencies in top_endpoints_dict.items():
            method, endpoint = key.split(":", 1)
            top_stats.append(EndpointStats(
                endpoint=endpoint,
                method=method,
                total_calls=len(latencies),
                success_count=0,
                error_count=0,
                avg_latency_ms=sum(latencies) / len(latencies),
                p50_latency_ms=self._percentile(sorted(latencies), 50),
                p95_latency_ms=self._percentile(sorted(latencies), 95),
                p99_latency_ms=self._percentile(sorted(latencies), 99),
                min_latency_ms=min(latencies),
                max_latency_ms=max(latencies),
            ))

        top_stats.sort(key=lambda x: x.total_calls, reverse=True)
        top_stats = top_stats[:10]

        return AnalyticsReport(
            period_start=min(r.timestamp for r in records),
            period_end=max(r.timestamp for r in records),
            total_calls=total_calls,
            total_errors=total_errors,
            overall_success_rate=((total_calls - total_errors) / total_calls * 100) if total_calls > 0 else 100.0,
            avg_latency_ms=sum(all_latencies) / len(all_latencies) if all_latencies else 0.0,
            top_endpoints=top_stats,
            error_breakdown=dict(self._error_counts),
        )

    def get_top_endpoints(
        self,
        limit: int = 10,
        by: str = "calls",
    ) -> list[EndpointStats]:
        """Get top endpoints by calls or latency."""
        endpoint_keys = set()
        for r in self._records:
            endpoint_keys.add(f"{r.method}:{r.endpoint}")

        stats = []
        for key in endpoint_keys:
            method, endpoint = key.split(":", 1)
            stat = self.get_endpoint_stats(endpoint, method)
            if stat:
                stats.append(stat)

        if by == "latency":
            stats.sort(key=lambda x: x.avg_latency_ms, reverse=True)
        else:
            stats.sort(key=lambda x: x.total_calls, reverse=True)

        return stats[:limit]

    def _prune_old_records(self) -> None:
        """Remove records older than retention period."""
        cutoff = time.time() - self.retention_period.total_seconds()
        self._records = [r for r in self._records if r.timestamp >= cutoff]

    @staticmethod
    def _percentile(sorted_data: list[float], percentile: int) -> float:
        """Calculate percentile from sorted data."""
        if not sorted_data:
            return 0.0
        idx = int(len(sorted_data) * percentile / 100)
        idx = min(idx, len(sorted_data) - 1)
        return sorted_data[idx]

    @property
    def total_records(self) -> int:
        """Total number of recorded calls."""
        return len(self._records)
