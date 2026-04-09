"""API Analytics Aggregator.

This module provides API analytics aggregation:
- Multi-dimensional analysis
- Time-series aggregation
- Top-N ranking
- Trend detection

Example:
    >>> from actions.api_analytics_action import AnalyticsAggregator
    >>> agg = AnalyticsAggregator()
    >>> agg.record_request("/api/users", method="GET", status=200, latency=45)
    >>> result = agg.get_analytics(group_by="endpoint")
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class RequestRecord:
    """A single request record."""
    timestamp: float
    endpoint: str
    method: str
    status: int
    latency_ms: float
    client_id: str = ""
    error: str = ""


class AnalyticsAggregator:
    """Aggregates and analyzes API analytics data."""

    def __init__(
        self,
        retention_seconds: int = 3600,
        bucket_size: int = 60,
    ) -> None:
        """Initialize the analytics aggregator.

        Args:
            retention_seconds: Data retention period.
            bucket_size: Time bucket size in seconds.
        """
        self._retention = retention_seconds
        self._bucket_size = bucket_size
        self._records: list[RequestRecord] = []
        self._lock = threading.RLock()
        self._stats = {"records": 0, "errors": 0}
        self._dimensions: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict(int))

    def record_request(
        self,
        endpoint: str,
        method: str,
        status: int,
        latency_ms: float,
        client_id: str = "",
        error: str = "",
    ) -> None:
        """Record a request.

        Args:
            endpoint: Request endpoint.
            method: HTTP method.
            status: Response status code.
            latency_ms: Request latency.
            client_id: Client identifier.
            error: Error message if applicable.
        """
        record = RequestRecord(
            timestamp=time.time(),
            endpoint=endpoint,
            method=method,
            status=status,
            latency_ms=latency_ms,
            client_id=client_id,
            error=error,
        )

        with self._lock:
            self._records.append(record)
            self._stats["records"] += 1
            if status >= 400 or error:
                self._stats["errors"] += 1

            cutoff = time.time() - self._retention
            self._records = [r for r in self._records if r.timestamp >= cutoff]

            bucket = int(record.timestamp / self._bucket_size) * self._bucket_size
            self._dimensions[f"{bucket}"]["total"] += 1
            self._dimensions[f"{bucket}"][f"endpoint_{endpoint}"] += 1
            self._dimensions[f"{bucket}"][f"status_{status}"] += 1
            if error:
                self._dimensions[f"{bucket}"]["errors"] += 1

    def get_analytics(
        self,
        group_by: str = "endpoint",
        time_range: Optional[tuple[float, float]] = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Get analytics aggregated by dimension.

        Args:
            group_by: Dimension to group by (endpoint, method, status, client).
            time_range: (start, end) Unix timestamps. None = all.
            limit: Maximum results.

        Returns:
            Analytics results.
        """
        with self._lock:
            records = list(self._records)

        if time_range:
            start, end = time_range
            records = [r for r in records if start <= r.timestamp <= end]

        if not records:
            return {"total": 0, "groups": {}}

        groups: dict[str, list[RequestRecord]] = defaultdict(list)
        for r in records:
            if group_by == "endpoint":
                key = r.endpoint
            elif group_by == "method":
                key = r.method
            elif group_by == "status":
                key = str(r.status)
            elif group_by == "client":
                key = r.client_id or "unknown"
            else:
                key = r.endpoint
            groups[key].append(r)

        result_groups = {}
        for key, recs in groups.items():
            latencies = [r.latency_ms for r in recs]
            result_groups[key] = {
                "count": len(recs),
                "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
                "min_latency_ms": min(latencies) if latencies else 0,
                "max_latency_ms": max(latencies) if latencies else 0,
                "error_count": sum(1 for r in recs if r.status >= 400 or r.error),
                "error_rate": sum(1 for r in recs if r.status >= 400 or r.error) / len(recs) if recs else 0,
            }

        sorted_groups = dict(
            sorted(result_groups.items(), key=lambda x: x[1]["count"], reverse=True)[:limit]
        )

        return {
            "total": len(records),
            "groups": sorted_groups,
            "time_range": time_range,
        }

    def get_top_endpoints(
        self,
        limit: int = 10,
        metric: str = "count",
    ) -> list[dict[str, Any]]:
        """Get top endpoints by metric.

        Args:
            limit: Number of endpoints to return.
            metric: Metric to sort by (count, latency, errors).

        Returns:
            List of endpoint stats.
        """
        analytics = self.get_analytics(group_by="endpoint", limit=limit * 2)

        endpoints = []
        for endpoint, stats in analytics.get("groups", {}).items():
            endpoints.append({
                "endpoint": endpoint,
                **stats,
            })

        if metric == "latency":
            endpoints.sort(key=lambda x: x["avg_latency_ms"], reverse=True)
        elif metric == "errors":
            endpoints.sort(key=lambda x: x["error_count"], reverse=True)
        else:
            endpoints.sort(key=lambda x: x["count"], reverse=True)

        return endpoints[:limit]

    def get_trends(
        self,
        interval_seconds: int = 300,
        window_size: int = 5,
    ) -> list[dict[str, Any]]:
        """Detect trends over time windows.

        Args:
            interval_seconds: Time interval between data points.
            window_size: Number of intervals to compare.

        Returns:
            List of trend data points.
        """
        with self._lock:
            records = list(self._records)

        if not records:
            return []

        now = time.time()
        trends = []

        for i in range(window_size):
            start = now - (i + 1) * interval_seconds
            end = now - i * interval_seconds
            bucket = [r for r in records if start <= r.timestamp < end]

            if bucket:
                latencies = [r.latency_ms for r in bucket]
                trends.append({
                    "timestamp": end,
                    "count": len(bucket),
                    "avg_latency_ms": sum(latencies) / len(latencies),
                    "error_rate": sum(1 for r in bucket if r.status >= 400) / len(bucket),
                })

        trends.reverse()
        if len(trends) >= 2:
            latest = trends[-1]
            prev = trends[-2]
            latest["latency_delta"] = latest["avg_latency_ms"] - prev["avg_latency_ms"]
            latest["count_delta"] = latest["count"] - prev["count"]

        return trends

    def get_percentiles(
        self,
        percentiles: list[float] = None,
    ) -> dict[str, float]:
        """Get latency percentiles.

        Args:
            percentiles: List of percentiles (0-100). Defaults to [50, 90, 95, 99].

        Returns:
            Dict mapping percentile to latency value.
        """
        if percentiles is None:
            percentiles = [50, 90, 95, 99]

        with self._lock:
            latencies = sorted([r.latency_ms for r in self._records])

        if not latencies:
            return {f"p{p}": 0.0 for p in percentiles}

        result = {}
        for p in percentiles:
            idx = int(len(latencies) * p / 100)
            idx = min(idx, len(latencies) - 1)
            result[f"p{p}"] = latencies[idx]

        return result

    def get_stats(self) -> dict[str, int]:
        """Get analytics statistics."""
        with self._lock:
            return {
                **self._stats,
                "stored_records": len(self._records),
            }
