"""
API Analytics Action Module.

Provides API usage analytics, performance monitoring,
and reporting capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)


@dataclass
class APIUsageMetrics:
    """API usage metrics."""
    endpoint: str
    method: str
    request_count: int = 0
    success_count: int = 0
    error_count: int = 0
    total_latency: float = 0.0
    min_latency: float = float('inf')
    max_latency: float = 0.0
    status_codes: Dict[int, int] = field(default_factory=dict)

    @property
    def avg_latency(self) -> float:
        """Get average latency."""
        if self.request_count == 0:
            return 0.0
        return self.total_latency / self.request_count

    @property
    def error_rate(self) -> float:
        """Get error rate."""
        if self.request_count == 0:
            return 0.0
        return self.error_count / self.request_count


@dataclass
class APIEvent:
    """API event for tracking."""
    event_id: str
    event_type: str
    endpoint: str
    method: str
    status_code: int
    latency: float
    timestamp: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIAnalyticsCollector:
    """Collects API analytics."""

    def __init__(self):
        self.metrics: Dict[str, APIUsageMetrics] = defaultdict(
            lambda: APIUsageMetrics(endpoint="", method="")
        )
        self.events: List[APIEvent] = []
        self.max_events: int = 10000

    def record_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency: float,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Record an API request."""
        key = f"{method}:{endpoint}"

        metrics = self.metrics[key]
        metrics.endpoint = endpoint
        metrics.method = method
        metrics.request_count += 1
        metrics.total_latency += latency
        metrics.min_latency = min(metrics.min_latency, latency)
        metrics.max_latency = max(metrics.max_latency, latency)

        if 200 <= status_code < 300:
            metrics.success_count += 1
        else:
            metrics.error_count += 1

        metrics.status_codes[status_code] = metrics.status_codes.get(status_code, 0) + 1

        event = APIEvent(
            event_id=str(id(self)),
            event_type="request",
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            latency=latency,
            user_id=user_id,
            metadata=metadata or {}
        )

        self.events.append(event)
        if len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]

    def get_metrics(self, endpoint: Optional[str] = None) -> List[APIUsageMetrics]:
        """Get metrics."""
        if endpoint:
            return [self.metrics.get(endpoint, APIUsageMetrics(endpoint=endpoint, method=""))]
        return list(self.metrics.values())

    def get_events(
        self,
        since: Optional[datetime] = None,
        endpoint: Optional[str] = None,
        limit: int = 100
    ) -> List[APIEvent]:
        """Get events."""
        events = self.events

        if since:
            events = [e for e in events if e.timestamp >= since]
        if endpoint:
            events = [e for e in events if e.endpoint == endpoint]

        return events[-limit:]


class PerformanceAnalyzer:
    """Analyzes API performance."""

    def __init__(self, collector: APIAnalyticsCollector):
        self.collector = collector

    def get_slow_endpoints(
        self,
        threshold_ms: float = 1000,
        limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Get slowest endpoints."""
        slow = []

        for metrics in self.collector.get_metrics():
            if metrics.avg_latency > threshold_ms:
                slow.append((metrics.endpoint, metrics.avg_latency))

        slow.sort(key=lambda x: x[1], reverse=True)
        return slow[:limit]

    def get_error_prone_endpoints(
        self,
        threshold_rate: float = 0.05,
        limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Get most error-prone endpoints."""
        error_prone = []

        for metrics in self.collector.get_metrics():
            if metrics.error_rate > threshold_rate:
                error_prone.append((metrics.endpoint, metrics.error_rate))

        error_prone.sort(key=lambda x: x[1], reverse=True)
        return error_prone[:limit]

    def get_usage_trend(
        self,
        endpoint: str,
        window: timedelta = timedelta(hours=1)
    ) -> List[Tuple[datetime, int]]:
        """Get usage trend for endpoint."""
        events = self.collector.get_events(endpoint=endpoint, limit=1000)

        if not events:
            return []

        events.sort(key=lambda e: e.timestamp)

        windows = []
        current = events[0].timestamp.replace(minute=0, second=0, microsecond=0)
        end = events[-1].timestamp.replace(minute=0, second=0, microsecond=0) + window

        while current <= end:
            count = sum(
                1 for e in events
                if current <= e.timestamp < current + window
            )
            windows.append((current, count))
            current += window

        return windows


class UsageReporter:
    """Generates usage reports."""

    def __init__(self, collector: APIAnalyticsCollector):
        self.collector = collector

    def generate_summary(
        self,
        since: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Generate summary report."""
        events = self.collector.get_events(since=since)

        total_requests = len(events)
        success_requests = sum(1 for e in events if 200 <= e.status_code < 300)
        error_requests = sum(1 for e in events if e.status_code >= 400)

        avg_latency = sum(e.latency for e in events) / total_requests if total_requests else 0

        return {
            "period": {
                "since": since.isoformat() if since else None,
                "to": datetime.now().isoformat()
            },
            "totals": {
                "requests": total_requests,
                "success": success_requests,
                "errors": error_requests,
                "success_rate": success_requests / total_requests if total_requests else 0,
                "avg_latency_ms": avg_latency
            },
            "top_endpoints": self._get_top_endpoints(events),
            "status_distribution": self._get_status_distribution(events)
        }

    def _get_top_endpoints(self, events: List[APIEvent], limit: int = 10) -> List[Dict[str, Any]]:
        """Get top endpoints by request count."""
        counter = Counter(f"{e.method}:{e.endpoint}" for e in events)
        top = counter.most_common(limit)

        return [
            {"endpoint": endpoint, "count": count}
            for endpoint, count in top
        ]

    def _get_status_distribution(self, events: List[APIEvent]) -> Dict[int, int]:
        """Get status code distribution."""
        return dict(Counter(e.status_code for e in events))


def main():
    """Demonstrate API analytics."""
    collector = APIAnalyticsCollector()
    collector.record_request("/api/users", "GET", 200, 50.0)
    collector.record_request("/api/users", "GET", 200, 75.0)
    collector.record_request("/api/users", "POST", 400, 30.0)
    collector.record_request("/api/orders", "GET", 500, 200.0)

    analyzer = PerformanceAnalyzer(collector)
    slow = analyzer.get_slow_endpoints()
    print(f"Slow endpoints: {slow}")

    reporter = UsageReporter(collector)
    summary = reporter.generate_summary()
    print(f"Total requests: {summary['totals']['requests']}")


if __name__ == "__main__":
    main()
