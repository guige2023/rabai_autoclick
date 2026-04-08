# Copyright (c) 2024. coded by claude
"""API Analytics Action Module.

Collects and analyzes API usage patterns including
endpoint popularity, latency distributions, and error rates.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import statistics
import logging

logger = logging.getLogger(__name__)


@dataclass
class EndpointStats:
    endpoint: str
    method: str
    total_calls: int = 0
    total_errors: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    latency_samples: List[float] = field(default_factory=list)


@dataclass
class AnalyticsSummary:
    total_requests: int
    total_errors: int
    error_rate: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    top_endpoints: List[Dict[str, Any]]


class APIAnalytics:
    def __init__(self):
        self._endpoint_stats: Dict[str, EndpointStats] = {}
        self._request_times: List[datetime] = []
        self._lock = None

    def record_request(self, endpoint: str, method: str, latency_ms: float, success: bool) -> None:
        key = f"{method}:{endpoint}"
        if key not in self._endpoint_stats:
            self._endpoint_stats[key] = EndpointStats(endpoint=endpoint, method=method)
        stats = self._endpoint_stats[key]
        stats.total_calls += 1
        stats.total_latency_ms += latency_ms
        stats.latency_samples.append(latency_ms)
        if not success:
            stats.total_errors += 1
        if latency_ms < stats.min_latency_ms:
            stats.min_latency_ms = latency_ms
        if latency_ms > stats.max_latency_ms:
            stats.max_latency_ms = latency_ms

    def get_endpoint_stats(self, endpoint: str, method: str) -> Optional[EndpointStats]:
        key = f"{method}:{endpoint}"
        return self._endpoint_stats.get(key)

    def get_summary(self, limit: int = 10) -> AnalyticsSummary:
        total_requests = sum(s.total_calls for s in self._endpoint_stats.values())
        total_errors = sum(s.total_errors for s in self._endpoint_stats.values())
        error_rate = (total_errors / total_requests * 100) if total_requests > 0 else 0.0
        all_latencies = []
        for stats in self._endpoint_stats.values():
            all_latencies.extend(stats.latency_samples)
        avg_latency = statistics.mean(all_latencies) if all_latencies else 0.0
        sorted_latencies = sorted(all_latencies)
        p95_idx = int(len(sorted_latencies) * 0.95) if sorted_latencies else 0
        p99_idx = int(len(sorted_latencies) * 0.99) if sorted_latencies else 0
        p95_latency = sorted_latencies[p95_idx] if sorted_latencies else 0.0
        p99_latency = sorted_latencies[p99_idx] if sorted_latencies else 0.0
        top_endpoints = sorted(
            [
                {
                    "endpoint": s.endpoint,
                    "method": s.method,
                    "calls": s.total_calls,
                    "error_rate": f"{(s.total_errors / s.total_calls * 100):.2f}%" if s.total_calls > 0 else "0%",
                }
                for s in self._endpoint_stats.values()
            ],
            key=lambda x: x["calls"],
            reverse=True,
        )[:limit]
        return AnalyticsSummary(
            total_requests=total_requests,
            total_errors=total_errors,
            error_rate=error_rate,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            top_endpoints=top_endpoints,
        )

    def reset(self) -> None:
        self._endpoint_stats.clear()
        self._request_times.clear()
