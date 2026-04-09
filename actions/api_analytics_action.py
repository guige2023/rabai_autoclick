"""
API analytics action for request metrics and analysis.

Provides request tracking, latency histograms, and traffic analysis.
"""

from typing import Any, Dict, List, Optional
import time
import threading
from collections import defaultdict, deque


class APIAnalyticsAction:
    """API analytics and metrics collection."""

    def __init__(
        self,
        retention_period: float = 3600.0,
        histogram_buckets: List[float] = None,
        max_requests: int = 100000,
    ) -> None:
        """
        Initialize API analytics.

        Args:
            retention_period: Data retention in seconds
            histogram_buckets: Latency histogram buckets
            max_requests: Maximum requests to track
        """
        self.retention_period = retention_period
        self.histogram_buckets = histogram_buckets or [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self.max_requests = max_requests

        self._requests: deque = deque(maxlen=max_requests)
        self._counters: Dict[str, int] = defaultdict(int)
        self._latencies: Dict[str, List[float]] = defaultdict(list)
        self._errors: Dict[str, int] = defaultdict(int)
        self._status_codes: Dict[int, int] = defaultdict(int)
        self._endpoints: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute analytics operation.

        Args:
            params: Dictionary containing:
                - operation: 'track', 'report', 'histogram', 'top_endpoints'
                - request: Request data to track
                - latency: Request latency in seconds
                - status_code: HTTP status code

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "track")

        if operation == "track":
            return self._track_request(params)
        elif operation == "report":
            return self._generate_report(params)
        elif operation == "histogram":
            return self._get_histogram(params)
        elif operation == "top_endpoints":
            return self._get_top_endpoints(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _track_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Track API request."""
        endpoint = params.get("endpoint", "/unknown")
        method = params.get("method", "GET")
        latency = params.get("latency", 0.0)
        status_code = params.get("status_code", 200)
        error = params.get("error")
        timestamp = params.get("timestamp", time.time())

        request_data = {
            "endpoint": endpoint,
            "method": method,
            "latency": latency,
            "status_code": status_code,
            "timestamp": timestamp,
            "has_error": error is not None,
        }

        with self._lock:
            self._requests.append(request_data)

            self._counters[f"total_requests"] += 1
            self._counters[f"method_{method}"] += 1
            self._counters[f"endpoint_{endpoint}"] += 1

            if status_code >= 400:
                self._counters["errors"] += 1
                self._errors[f"status_{status_code}"] += 1
            elif status_code >= 200 and status_code < 300:
                self._counters["successful"] += 1

            self._status_codes[status_code] += 1

            self._latencies[endpoint].append(latency)

            if endpoint not in self._endpoints:
                self._endpoints[endpoint] = {
                    "method": method,
                    "request_count": 0,
                    "total_latency": 0,
                    "error_count": 0,
                    "last_accessed": timestamp,
                }

            ep_stats = self._endpoints[endpoint]
            ep_stats["request_count"] += 1
            ep_stats["total_latency"] += latency
            ep_stats["last_accessed"] = timestamp
            if error:
                ep_stats["error_count"] += 1

        return {"success": True, "tracked": True}

    def _generate_report(self, params: dict[str, Any]) -> dict[str, Any]:
        """Generate analytics report."""
        period = params.get("period", 3600)
        now = time.time()
        cutoff = now - period

        with self._lock:
            recent_requests = [r for r in self._requests if r["timestamp"] > cutoff]

            total_requests = len(recent_requests)
            error_count = sum(1 for r in recent_requests if r.get("has_error"))
            successful = sum(1 for r in recent_requests if 200 <= r.get("status_code", 0) < 300)

            latencies = [r["latency"] for r in recent_requests if r["latency"] > 0]

            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            min_latency = min(latencies) if latencies else 0
            max_latency = max(latencies) if latencies else 0
            p50_latency = self._percentile(latencies, 50) if latencies else 0
            p95_latency = self._percentile(latencies, 95) if latencies else 0
            p99_latency = self._percentile(latencies, 99) if latencies else 0

            requests_per_second = total_requests / period if period > 0 else 0

            status_distribution = dict(self._status_codes)

        return {
            "success": True,
            "period_seconds": period,
            "total_requests": total_requests,
            "successful_requests": successful,
            "error_count": error_count,
            "error_rate": error_count / total_requests if total_requests > 0 else 0,
            "latency": {
                "avg_ms": round(avg_latency * 1000, 2),
                "min_ms": round(min_latency * 1000, 2),
                "max_ms": round(max_latency * 1000, 2),
                "p50_ms": round(p50_latency * 1000, 2),
                "p95_ms": round(p95_latency * 1000, 2),
                "p99_ms": round(p99_latency * 1000, 2),
            },
            "throughput": {
                "requests_per_second": round(requests_per_second, 2),
            },
            "status_codes": status_distribution,
        }

    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile of data."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]

    def _get_histogram(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get latency histogram."""
        endpoint = params.get("endpoint")
        now = time.time()
        cutoff = now - self.retention_period

        with self._lock:
            if endpoint:
                latencies = [
                    r["latency"] for r in self._requests
                    if r["endpoint"] == endpoint and r["timestamp"] > cutoff and r["latency"] > 0
                ]
            else:
                latencies = [
                    r["latency"] for r in self._requests
                    if r["timestamp"] > cutoff and r["latency"] > 0
                ]

            histogram = {}
            cumulative = 0

            for bucket in self.histogram_buckets:
                count = sum(1 for l in latencies if l <= bucket)
                histogram[f"<=_{bucket}s"] = count
                cumulative = count

            histogram["total"] = len(latencies)

            return {"success": True, "histogram": histogram, "bucket_size": self.histogram_buckets}

    def _get_top_endpoints(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get top endpoints by request count or latency."""
        limit = params.get("limit", 10)
        sort_by = params.get("sort_by", "count")

        with self._lock:
            endpoints = []
            for ep, stats in self._endpoints.items():
                avg_latency = stats["total_latency"] / stats["request_count"] if stats["request_count"] > 0 else 0
                error_rate = stats["error_count"] / stats["request_count"] if stats["request_count"] > 0 else 0

                endpoints.append({
                    "endpoint": ep,
                    "method": stats.get("method", "GET"),
                    "request_count": stats["request_count"],
                    "avg_latency_ms": round(avg_latency * 1000, 2),
                    "error_count": stats["error_count"],
                    "error_rate": round(error_rate * 100, 2),
                    "last_accessed": stats["last_accessed"],
                })

            if sort_by == "latency":
                endpoints.sort(key=lambda x: x["avg_latency_ms"], reverse=True)
            elif sort_by == "errors":
                endpoints.sort(key=lambda x: x["error_count"], reverse=True)
            else:
                endpoints.sort(key=lambda x: x["request_count"], reverse=True)

            return {"success": True, "endpoints": endpoints[:limit]}
