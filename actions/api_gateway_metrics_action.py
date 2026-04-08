"""
API Gateway Metrics Action Module.

Collects and aggregates API gateway metrics: request counts, latencies,
error rates, and bandwidth usage with percentiles.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from actions.base_action import BaseAction


@dataclass
class MetricsSnapshot:
    """A snapshot of gateway metrics."""
    timestamp: float
    request_count: int
    error_count: int
    total_latency_ms: float
    bandwidth_bytes: int


class APIGatewayMetricsAction(BaseAction):
    """Collect and aggregate API gateway metrics."""

    def __init__(self) -> None:
        super().__init__("api_gateway_metrics")
        self._request_counts: dict[str, int] = defaultdict(int)
        self._error_counts: dict[str, int] = defaultdict(int)
        self._latencies: dict[str, list[float]] = defaultdict(list)
        self._bandwidth: dict[str, int] = defaultdict(int)
        self._snapshots: list[MetricsSnapshot] = []

    def execute(self, context: dict, params: dict) -> dict:
        """
        Record or retrieve gateway metrics.

        Args:
            context: Execution context
            params: Parameters:
                - action: record or report
                - endpoint: Endpoint identifier
                - latency_ms: Request latency in milliseconds
                - status_code: HTTP status code
                - response_size: Response size in bytes
                - window_seconds: Aggregation window for reporting

        Returns:
            For record: confirmation
            For report: aggregated metrics
        """
        import time

        action = params.get("action", "record")
        endpoint = params.get("endpoint", "all")
        window_seconds = params.get("window_seconds", 60)

        if action == "record":
            latency_ms = params.get("latency_ms", 0)
            status_code = params.get("status_code", 200)
            response_size = params.get("response_size", 0)

            self._request_counts[endpoint] += 1
            self._latencies[endpoint].append(latency_ms)
            self._bandwidth[endpoint] += response_size
            if status_code >= 400:
                self._error_counts[endpoint] += 1

            return {"recorded": True, "endpoint": endpoint}

        elif action == "report":
            now = time.time()
            cutoff = now - window_seconds

            snapshot = MetricsSnapshot(
                timestamp=now,
                request_count=sum(self._request_counts.values()),
                error_count=sum(self._error_counts.values()),
                total_latency_ms=sum(sum(v) for v in self._latencies.values()),
                bandwidth_bytes=sum(self._bandwidth.values())
            )
            self._snapshots.append(snapshot)

            all_latencies = []
            for lat_list in self._latencies.values():
                all_latencies.extend(lat_list[-100:])

            all_latencies.sort()
            n = len(all_latencies)

            return {
                "period_seconds": window_seconds,
                "total_requests": snapshot.request_count,
                "total_errors": snapshot.error_count,
                "error_rate": snapshot.error_count / snapshot.request_count if snapshot.request_count > 0 else 0.0,
                "avg_latency_ms": snapshot.total_latency_ms / n if n > 0 else 0.0,
                "p50_latency_ms": all_latencies[int(n * 0.5)] if n > 0 else 0.0,
                "p95_latency_ms": all_latencies[int(n * 0.95)] if n > 0 else 0.0,
                "p99_latency_ms": all_latencies[int(n * 0.99)] if n > 0 else 0.0,
                "total_bandwidth_bytes": snapshot.bandwidth_bytes,
                "endpoints": {
                    ep: {
                        "requests": self._request_counts[ep],
                        "errors": self._error_counts[ep],
                        "bandwidth": self._bandwidth[ep]
                    }
                    for ep in self._request_counts
                }
            }

        return {"error": f"Unknown action: {action}"}

    def reset(self) -> None:
        """Reset all metrics."""
        self._request_counts.clear()
        self._error_counts.clear()
        self._latencies.clear()
        self._bandwidth.clear()
        self._snapshots.clear()
