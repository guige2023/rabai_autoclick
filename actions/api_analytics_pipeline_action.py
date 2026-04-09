"""
API Analytics Pipeline Action.

Builds and executes analytics pipelines for API usage data,
with support for aggregation, time-series analysis, and alerting.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class AggregationType(Enum):
    """Types of aggregations supported."""
    COUNT = auto()
    SUM = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()
    PERCENTILE = auto()
    RATE = auto()
    UNIQUE = auto()


@dataclass
class APIMetric:
    """A single API metric data point."""
    endpoint: str
    method: str
    status_code: int
    latency_ms: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    request_size_bytes: int = 0
    response_size_bytes: int = 0
    user_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    metric_name: str
    aggregation_type: AggregationType
    value: float
    group_by: Dict[str, str] = field(default_factory=dict)
    window: Optional[str] = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class TimeSeriesPoint:
    """A single point in a time series."""
    timestamp: datetime
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


class TimeWindow:
    """Time window for aggregation."""

    def __init__(self, size_seconds: int, offset_seconds: int = 0) -> None:
        self.size_seconds = size_seconds
        self.offset_seconds = offset_seconds

    def get_bucket(self, ts: datetime) -> datetime:
        """Get the bucket timestamp for a given time."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        total_seconds = int((ts - epoch).total_seconds())
        bucket = ((total_seconds - self.offset_seconds) // self.size_seconds) * self.size_seconds
        return datetime.fromtimestamp(bucket, tz=timezone.utc)


class APIAnalyticsPipeline:
    """
    Build analytics pipelines for API metrics.

    Example:
        pipeline = (APIAnalyticsPipeline()
            .filter_endpoints(["/api/v1/users", "/api/v1/orders"])
            .filter_errors()  # Only 4xx/5xx
            .aggregate_by_endpoint(AggregationType.COUNT)
            .aggregate_by_endpoint(AggregationType.AVG, metric="latency_ms")
            .time_bucket("5m")
            .build())
        results = pipeline.execute(metrics)
    """

    def __init__(self) -> None:
        self._filters: List[Callable[[APIMetric], bool]] = []
        self._aggregations: List[Tuple[str, AggregationType, Optional[str]]] = []  # group_by, agg_type, metric
        self._time_window: Optional[TimeWindow] = None
        self._sort_by: Optional[str] = None
        self._sort_desc: bool = True
        self._limit: Optional[int] = None

    def filter_endpoints(self, endpoints: List[str]) -> Self:
        """Filter metrics by endpoint patterns."""
        self._filters.append(lambda m: m.endpoint in endpoints)
        return self

    def filter_errors(self) -> Self:
        """Filter to only error responses (4xx/5xx)."""
        self._filters.append(lambda m: m.status_code >= 400)
        return self

    def filter_slow_requests(self, threshold_ms: float) -> Self:
        """Filter to only slow requests."""
        self._filters.append(lambda m: m.latency_ms > threshold_ms)
        return self

    def filter_by_method(self, methods: List[str]) -> Self:
        """Filter by HTTP method."""
        self._filters.append(lambda m: m.method in methods)
        return self

    def filter_by_tag(self, key: str, value: str) -> Self:
        """Filter by metric tag."""
        self._filters.append(lambda m: m.tags.get(key) == value)
        return self

    def aggregate_by_endpoint(
        self,
        agg_type: AggregationType,
        metric: Optional[str] = None,
    ) -> Self:
        """Aggregate metrics grouped by endpoint."""
        self._aggregations.append(("endpoint", agg_type, metric))
        return self

    def aggregate_by_status_code(self, agg_type: AggregationType) -> Self:
        """Aggregate metrics grouped by status code."""
        self._aggregations.append(("status_code", agg_type, None))
        return self

    def aggregate_by_user(self, agg_type: AggregationType) -> Self:
        """Aggregate metrics grouped by user."""
        self._aggregations.append(("user_id", agg_type, None))
        return self

    def aggregate_global(
        self,
        agg_type: AggregationType,
        metric: Optional[str] = None,
    ) -> Self:
        """Global aggregation without grouping."""
        self._aggregations.append(("", agg_type, metric))
        return self

    def time_bucket(self, window_spec: str) -> Self:
        """
        Add time bucketing. Format: "5m", "1h", "1d", etc.
        """
        unit = window_spec[-1]
        size = int(window_spec[:-1])
        unit_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        seconds = size * unit_map.get(unit, 60)
        self._time_window = TimeWindow(seconds)
        return self

    def sort_by(self, metric: str, descending: bool = True) -> Self:
        """Sort results by metric."""
        self._sort_by = metric
        self._sort_desc = descending
        return self

    def limit(self, n: int) -> Self:
        """Limit number of results."""
        self._limit = n
        return self

    def build(self) -> Callable[[List[APIMetric]], List[AggregationResult]]:
        """Build the pipeline as an executable function."""

        def execute(metrics: List[APIMetric]) -> List[AggregationResult]:
            # Apply filters
            filtered = [m for m in metrics if all(f(m) for f in self._filters)]

            results: List[AggregationResult] = []

            for group_by, agg_type, metric_name in self._aggregations:
                grouped: Dict[Tuple, List[APIMetric]] = defaultdict(list)

                for m in filtered:
                    if group_by == "endpoint":
                        key = (m.endpoint,)
                    elif group_by == "status_code":
                        key = (m.status_code,)
                    elif group_by == "user_id":
                        key = (m.user_id or "anonymous",)
                    elif group_by == "timestamp":
                        if self._time_window:
                            bucket = self._time_window.get_bucket(m.timestamp)
                            key = (bucket,)
                        else:
                            key = ()
                    else:
                        key = ()

                    grouped[key].append(m)

                for group_keys, group_metrics in grouped.items():
                    group_dict = dict(zip(["endpoint", "status_code", "user_id", "timestamp"], group_keys))

                    if metric_name == "latency_ms" or (metric_name is None and agg_type == AggregationType.AVG):
                        values = [m.latency_ms for m in group_metrics]
                    elif metric_name == "request_size_bytes":
                        values = [float(m.request_size_bytes) for m in group_metrics]
                    elif metric_name == "response_size_bytes":
                        values = [float(m.response_size_bytes) for m in group_metrics]
                    elif agg_type == AggregationType.COUNT:
                        values = [float(len(group_metrics))]
                    else:
                        values = [float(m.latency_ms) for m in group_metrics]

                    value = self._compute_aggregation(values, agg_type)

                    results.append(AggregationResult(
                        metric_name=metric_name or "count",
                        aggregation_type=agg_type,
                        value=value,
                        group_by={k: str(v) for k, v in group_dict.items() if v},
                        window=window_spec if self._time_window else None,
                    ))

            # Sort
            if self._sort_by:
                results.sort(key=lambda r: r.value, reverse=self._sort_desc)

            # Limit
            if self._limit:
                results = results[:self._limit]

            return results

        return execute

    def _compute_aggregation(self, values: List[float], agg_type: AggregationType) -> float:
        if not values:
            return 0.0
        if agg_type == AggregationType.COUNT:
            return float(len(values))
        elif agg_type == AggregationType.SUM:
            return sum(values)
        elif agg_type == AggregationType.AVG:
            return sum(values) / len(values)
        elif agg_type == AggregationType.MIN:
            return min(values)
        elif agg_type == AggregationType.MAX:
            return max(values)
        elif agg_type == AggregationType.PERCENTILE:
            sorted_vals = sorted(values)
            idx = int(len(sorted_vals) * 0.99)
            return sorted_vals[min(idx, len(sorted_vals) - 1)]
        elif agg_type == AggregationType.RATE:
            return sum(values) / max(1, len(values))
        return 0.0


@dataclass
class APIHealthScore:
    """Composite health score for an API."""
    endpoint: str
    score: float  # 0-100
    availability: float  # 0-100
    avg_latency_ms: float
    p99_latency_ms: float
    error_rate: float  # 0-100
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def status(self) -> str:
        if self.score >= 90:
            return "healthy"
        elif self.score >= 70:
            return "degraded"
        else:
            return "unhealthy"


def compute_health_score(metrics: List[APIMetric], endpoint: str) -> APIHealthScore:
    """Compute composite health score for an endpoint."""
    endpoint_metrics = [m for m in metrics if m.endpoint == endpoint]
    if not endpoint_metrics:
        return APIHealthScore(endpoint=endpoint, score=0, availability=0, avg_latency_ms=0, p99_latency_ms=0, error_rate=100)

    latencies = [m.latency_ms for m in endpoint_metrics]
    errors = [m for m in endpoint_metrics if m.status_code >= 400]

    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p99_latency = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
    error_rate = (len(errors) / len(endpoint_metrics)) * 100 if endpoint_metrics else 0

    # Simple weighted score
    latency_score = max(0, 100 - (avg_latency / 10))
    error_score = max(0, 100 - (error_rate * 5))
    availability = max(0, 100 - error_rate)
    score = (latency_score * 0.3) + (error_score * 0.7)

    return APIHealthScore(
        endpoint=endpoint,
        score=score,
        availability=availability,
        avg_latency_ms=avg_latency,
        p99_latency_ms=p99_latency,
        error_rate=error_rate,
    )
