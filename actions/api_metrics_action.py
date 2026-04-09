"""
API metrics collection and monitoring module.

Collects, aggregates, and reports API performance metrics including
latency, throughput, error rates, and status code distributions.

Author: Aito Auto Agent
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional, Callable
import threading


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()
    SUMMARY = auto()


@dataclass
class ApiMetricPoint:
    """Single metric data point."""
    timestamp: float
    value: float
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class LatencyPercentiles:
    """Latency percentile values."""
    p50: float = 0.0
    p75: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    p999: float = 0.0


@dataclass
class ApiMetricsSummary:
    """Summary of API metrics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_rate: float = 0.0
    avg_latency_ms: float = 0.0
    min_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    throughput_rps: float = 0.0
    percentiles: LatencyPercentiles = field(default_factory=LatencyPercentiles)
    status_code_counts: dict[int, int] = field(default_factory=dict)
    method_counts: dict[str, int] = field(default_factory=dict)


class PercentileCalculator:
    """Calculate percentiles from a list of values."""

    @staticmethod
    def calculate(values: list[float], percentiles: list[float]) -> dict[float, float]:
        """
        Calculate percentile values.

        Args:
            values: List of numeric values
            percentiles: List of percentile values (0-100)

        Returns:
            Dict mapping percentile to value
        """
        if not values:
            return {p: 0.0 for p in percentiles}

        sorted_values = sorted(values)
        result = {}

        for p in percentiles:
            if p <= 0:
                result[p] = sorted_values[0] if sorted_values else 0.0
            elif p >= 100:
                result[p] = sorted_values[-1] if sorted_values else 0.0
            else:
                index = (p / 100) * (len(sorted_values) - 1)
                lower = int(index)
                upper = min(lower + 1, len(sorted_values) - 1)
                weight = index - lower
                result[p] = sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

        return result


class ApiMetricsCollector:
    """
    API metrics collection and aggregation.

    Collects request metrics, calculates percentiles, and generates
    summaries for API monitoring and alerting.

    Example:
        collector = ApiMetricsCollector()
        collector.record_request("/api/users", "GET", 200, latency_ms=45.2)
        collector.record_request("/api/users", "POST", 201, latency_ms=120.5)
        summary = collector.get_summary()
    """

    def __init__(self, window_size_seconds: int = 60):
        self._window_size = window_size_seconds
        self._lock = threading.RLock()

        self._request_times: dict[str, list[float]] = defaultdict(list)
        self._latencies: dict[str, list[float]] = defaultdict(list)
        self._status_codes: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self._methods: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._errors: dict[str, int] = defaultdict(int)
        self._total_requests: dict[str, int] = defaultdict(int)
        self._timestamps: dict[str, list[float]] = defaultdict(list)

        self._start_time = time.time()
        self._endpoint_labels: dict[str, str] = {}

    def record_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency_ms: float,
        labels: Optional[dict[str, str]] = None
    ) -> None:
        """
        Record an API request metric.

        Args:
            endpoint: API endpoint path
            method: HTTP method
            status_code: Response status code
            latency_ms: Request latency in milliseconds
            labels: Optional metric labels
        """
        with self._lock:
            timestamp = time.time()
            key = self._normalize_endpoint(endpoint)

            self._request_times[key].append(timestamp)
            self._latencies[key].append(latency_ms)
            self._status_codes[key][status_code] += 1
            self._methods[key][method] += 1
            self._timestamps[key].append(timestamp)
            self._total_requests[key] += 1

            if status_code >= 400:
                self._errors[key] += 1

            self._cleanup_old_data(key)

    def _normalize_endpoint(self, endpoint: str) -> str:
        """Normalize endpoint for consistent grouping."""
        parts = endpoint.split("/")
        normalized = []

        for part in parts:
            if part.isdigit():
                normalized.append("{id}")
            elif self._looks_like_uuid(part):
                normalized.append("{uuid}")
            else:
                normalized.append(part)

        return "/".join(normalized)

    def _looks_like_uuid(self, value: str) -> bool:
        """Check if string looks like a UUID."""
        if len(value) != 36:
            return False
        return value.count("-") == 4

    def _cleanup_old_data(self, key: str) -> None:
        """Remove data points outside the time window."""
        cutoff = time.time() - self._window_size

        valid_indices = [
            i for i, ts in enumerate(self._timestamps[key])
            if ts >= cutoff
        ]

        if len(valid_indices) < len(self._timestamps[key]):
            self._timestamps[key] = [self._timestamps[key][i] for i in valid_indices]
            self._latencies[key] = [self._latencies[key][i] for i in valid_indices]

    def get_summary(self, endpoint: Optional[str] = None) -> ApiMetricsSummary:
        """
        Get metrics summary.

        Args:
            endpoint: Optional specific endpoint, None for all

        Returns:
            ApiMetricsSummary with aggregated metrics
        """
        with self._lock:
            if endpoint:
                keys = [self._normalize_endpoint(endpoint)]
            else:
                keys = list(self._total_requests.keys())

            total_requests = sum(self._total_requests[k] for k in keys)
            total_errors = sum(self._errors[k] for k in keys)
            all_latencies = [l for k in keys for l in self._latencies[k]]

            summary = ApiMetricsSummary()
            summary.total_requests = total_requests
            summary.failed_requests = total_errors
            summary.successful_requests = total_requests - total_errors
            summary.error_rate = total_errors / total_requests if total_requests > 0 else 0.0

            if all_latencies:
                summary.avg_latency_ms = sum(all_latencies) / len(all_latencies)
                summary.min_latency_ms = min(all_latencies)
                summary.max_latency_ms = max(all_latencies)

                percentiles = PercentileCalculator.calculate(all_latencies, [50, 75, 90, 95, 99, 99.9])
                summary.percentiles = LatencyPercentiles(
                    p50=percentiles[50],
                    p75=percentiles[75],
                    p90=percentiles[90],
                    p95=percentiles[95],
                    p99=percentiles[99],
                    p999=percentiles[99.9]
                )

            window_duration = min(
                self._window_size,
                time.time() - self._start_time
            )
            if window_duration > 0:
                summary.throughput_rps = total_requests / window_duration

            for k in keys:
                for code, count in self._status_codes[k].items():
                    summary.status_code_counts[code] = summary.status_code_counts.get(code, 0) + count
                for method, count in self._methods[k].items():
                    summary.method_counts[method] = summary.method_counts.get(method, 0) + count

            return summary

    def get_endpoint_metrics(self) -> dict[str, ApiMetricsSummary]:
        """Get metrics for each endpoint."""
        with self._lock:
            endpoints = list(self._total_requests.keys())
            return {ep: self.get_summary(ep) for ep in endpoints}

    def get_status_code_distribution(self) -> dict[int, int]:
        """Get distribution of status codes across all endpoints."""
        distribution: dict[int, int] = defaultdict(int)
        with self._lock:
            for endpoint_codes in self._status_codes.values():
                for code, count in endpoint_codes.items():
                    distribution[code] += count
        return dict(distribution)

    def get_method_distribution(self) -> dict[str, int]:
        """Get distribution of HTTP methods."""
        distribution: dict[str, int] = defaultdict(int)
        with self._lock:
            for endpoint_methods in self._methods.values():
                for method, count in endpoint_methods.items():
                    distribution[method] += count
        return dict(distribution)

    def get_latency_histogram(
        self,
        endpoint: Optional[str] = None,
        buckets: Optional[list[float]] = None
    ) -> dict[float, int]:
        """
        Get latency histogram buckets.

        Args:
            endpoint: Optional endpoint filter
            buckets: Bucket boundaries in ms

        Returns:
            Dict mapping bucket boundary to count
        """
        if buckets is None:
            buckets = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

        with self._lock:
            if endpoint:
                latencies = self._latencies.get(self._normalize_endpoint(endpoint), [])
            else:
                latencies = [l for ep_latencies in self._latencies.values() for l in ep_latencies]

        histogram: dict[float, int] = {b: 0 for b in buckets}
        histogram[float('inf')] = 0

        for latency in latencies:
            for bucket in buckets:
                if latency <= bucket:
                    histogram[bucket] += 1
                    break
            else:
                histogram[float('inf')] += 1

        return histogram

    def reset(self) -> None:
        """Reset all collected metrics."""
        with self._lock:
            self._request_times.clear()
            self._latencies.clear()
            self._status_codes.clear()
            self._methods.clear()
            self._errors.clear()
            self._total_requests.clear()
            self._timestamps.clear()
            self._start_time = time.time()


class ApiMetricsExporter:
    """
    Export API metrics in various formats.

    Supports Prometheus, JSON, and custom formats.
    """

    def __init__(self, collector: ApiMetricsCollector):
        self._collector = collector

    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus text format."""
        summary = self._collector.get_summary()
        endpoint_metrics = self._collector.get_endpoint_metrics()

        lines = [
            "# HELP api_requests_total Total number of API requests",
            "# TYPE api_requests_total counter",
            f"api_requests_total {summary.total_requests}",
            "",
            "# HELP api_request_duration_seconds API request latency in seconds",
            "# TYPE api_request_duration_seconds histogram",
        ]

        histogram = self._collector.get_latency_histogram()
        for bucket_ms, count in histogram.items():
            bucket_sec = bucket_ms / 1000
            if bucket_sec != float('inf'):
                lines.append(f'api_request_duration_seconds_bucket{{le="{bucket_sec}"}} {count}')
            else:
                lines.append(f'api_request_duration_seconds_bucket{{le="+Inf"}} {count}')

        lines.extend([
            f"api_request_duration_seconds_sum {summary.avg_latency_ms / 1000}",
            f"api_request_duration_seconds_count {summary.total_requests}",
            "",
            "# HELP api_errors_total Total number of API errors",
            "# TYPE api_errors_total counter",
            f"api_errors_total {summary.failed_requests}",
        ])

        return "\n".join(lines)

    def to_json(self) -> dict:
        """Export metrics as JSON-serializable dict."""
        summary = self._collector.get_summary()
        return {
            "summary": {
                "total_requests": summary.total_requests,
                "successful_requests": summary.successful_requests,
                "failed_requests": summary.failed_requests,
                "error_rate": summary.error_rate,
                "avg_latency_ms": summary.avg_latency_ms,
                "min_latency_ms": summary.min_latency_ms,
                "max_latency_ms": summary.max_latency_ms,
                "throughput_rps": summary.throughput_rps,
                "percentiles": {
                    "p50": summary.percentiles.p50,
                    "p75": summary.percentiles.p75,
                    "p90": summary.percentiles.p90,
                    "p95": summary.percentiles.p95,
                    "p99": summary.percentiles.p99,
                    "p999": summary.percentiles.p999,
                },
                "status_codes": summary.status_code_counts,
                "methods": summary.method_counts,
            },
            "endpoints": {
                ep: {
                    "total_requests": m.total_requests,
                    "error_rate": m.error_rate,
                    "avg_latency_ms": m.avg_latency_ms,
                }
                for ep, m in self._collector.get_endpoint_metrics().items()
            },
            "timestamp": datetime.now().isoformat(),
        }

    def to_prometheus_remote_write(
        self,
        remote_url: str,
        headers: Optional[dict[str, str]] = None
    ) -> bool:
        """Export metrics to Prometheus remote write endpoint."""
        import requests

        payload = {
            "timeseries": []
        }

        summary = self._collector.get_summary()

        for code, count in summary.status_code_counts.items():
            payload["timeseries"].append({
                "labels": [
                    {"name": "__name__", "value": "api_requests_total"},
                    {"name": "status_code", "value": str(code)},
                ],
                "samples": [{"value": count, "timestamp": int(time.time() * 1000)}]
            })

        try:
            response = requests.post(
                remote_url,
                json=payload,
                headers=headers or {"Content-Type": "application/json"},
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False


def create_metrics_collector(window_size_seconds: int = 60) -> ApiMetricsCollector:
    """Factory function to create an ApiMetricsCollector."""
    return ApiMetricsCollector(window_size_seconds=window_size_seconds)
