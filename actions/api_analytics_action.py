"""
API Analytics Action Module.

Tracks and analyzes API usage: request volumes, latency distributions,
error rates, top endpoints, and client breakdowns.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from actions.base_action import BaseAction


@dataclass
class APIAnalyticsResult:
    """Analytics summary."""
    total_requests: int
    total_errors: int
    error_rate: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    top_endpoints: list[dict[str, Any]]
    top_clients: list[dict[str, Any]]
    requests_by_hour: dict[str, int]


class APIAnalyticsAction(BaseAction):
    """Track and analyze API usage."""

    def __init__(self) -> None:
        super().__init__("api_analytics")
        self._requests: list[dict[str, Any]] = []
        self._max_records = 10000

    def execute(self, context: dict, params: dict) -> dict:
        """
        Track or analyze API requests.

        Args:
            context: Execution context
            params: Parameters:
                - action: track or analyze
                - request_data: Request metadata for tracking
                - period: Analysis period (hour, day, week)

        Returns:
            For track: confirmation
            For analyze: APIAnalyticsResult
        """
        import time

        action = params.get("action", "track")

        if action == "track":
            request_data = params.get("request_data", {})
            self._requests.append({
                "timestamp": time.time(),
                "endpoint": request_data.get("endpoint", ""),
                "method": request_data.get("method", ""),
                "client_id": request_data.get("client_id", ""),
                "status_code": request_data.get("status_code", 200),
                "latency_ms": request_data.get("latency_ms", 0),
                "error": request_data.get("error")
            })
            if len(self._requests) > self._max_records:
                self._requests = self._requests[-self._max_records:]
            return {"tracked": True, "total_records": len(self._requests)}

        elif action == "analyze":
            period = params.get("period", "day")
            cutoff = self._get_cutoff(period)

            filtered = [r for r in self._requests if r["timestamp"] >= cutoff]
            return self._compute_analytics(filtered, period)

        return {"error": f"Unknown action: {action}"}

    def _get_cutoff(self, period: str) -> float:
        """Get cutoff timestamp for period."""
        import time
        now = time.time()
        if period == "hour":
            return now - 3600
        elif period == "day":
            return now - 86400
        elif period == "week":
            return now - 604800
        return now - 86400

    def _compute_analytics(self, requests: list[dict], period: str) -> dict:
        """Compute analytics from requests."""
        import time

        if not requests:
            return APIAnalyticsResult(
                total_requests=0,
                total_errors=0,
                error_rate=0.0,
                avg_latency_ms=0.0,
                p50_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                top_endpoints=[],
                top_clients=[],
                requests_by_hour={}
            ).__dict__

        total = len(requests)
        errors = sum(1 for r in requests if r.get("error") or (r.get("status_code", 200) >= 400))
        error_rate = errors / total if total > 0 else 0.0

        latencies = sorted([r.get("latency_ms", 0) for r in requests])
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        p50 = latencies[int(len(latencies) * 0.5)] if latencies else 0.0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0.0

        endpoint_counts: dict[str, int] = defaultdict(int)
        client_counts: dict[str, int] = defaultdict(int)
        hour_counts: dict[str, int] = defaultdict(int)

        for r in requests:
            endpoint_counts[r.get("endpoint", "")] += 1
            client_counts[r.get("client_id", "")] += 1
            hour = time.strftime("%Y-%m-%d %H:00", time.localtime(r["timestamp"]))
            hour_counts[hour] += 1

        top_endpoints = sorted([{"endpoint": k, "count": v} for k, v in endpoint_counts.items()], key=lambda x: -x["count"])[:10]
        top_clients = sorted([{"client_id": k, "count": v} for k, v in client_counts.items()], key=lambda x: -x["count"])[:10]

        return APIAnalyticsResult(
            total_requests=total,
            total_errors=errors,
            error_rate=error_rate,
            avg_latency_ms=avg_latency,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p99,
            top_endpoints=top_endpoints,
            top_clients=top_clients,
            requests_by_hour=dict(hour_counts)
        )

    def get_top_errors(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get most common errors."""
        error_counts: dict[str, int] = defaultdict(int)
        for r in self._requests:
            if r.get("error"):
                error_counts[r["error"]] += 1
        return sorted([{"error": k, "count": v} for k, v in error_counts.items()], key=lambda x: -x["count"])[:limit]
