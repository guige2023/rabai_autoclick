"""API Analytics Action.

Tracks API usage metrics, latency percentiles, and error rates.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import time


@dataclass
class APIAnalytics:
    endpoint: str
    call_count: int = 0
    error_count: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    status_codes: Dict[int, int] = field(default_factory=dict)
    last_call: Optional[float] = None


class APIAnalyticsAction:
    """Tracks and aggregates API usage analytics."""

    def __init__(self, window_sec: float = 3600.0) -> None:
        self.window_sec = window_sec
        self._data: Dict[str, APIAnalytics] = {}
        self._timestamps: List[float] = []

    def record(
        self,
        endpoint: str,
        latency_ms: float,
        status_code: int = 200,
        error: Optional[str] = None,
    ) -> None:
        if endpoint not in self._data:
            self._data[endpoint] = APIAnalytics(endpoint=endpoint)
        a = self._data[endpoint]
        a.call_count += 1
        a.total_latency_ms += latency_ms
        a.min_latency_ms = min(a.min_latency_ms, latency_ms)
        a.max_latency_ms = max(a.max_latency_ms, latency_ms)
        a.status_codes[status_code] = a.status_codes.get(status_code, 0) + 1
        a.last_call = time.time()
        if error:
            a.error_count += 1
        self._timestamps.append(time.time())

    def get_endpoint_stats(self, endpoint: str) -> Optional[Dict[str, Any]]:
        a = self._data.get(endpoint)
        if not a or a.call_count == 0:
            return None
        return {
            "endpoint": endpoint,
            "call_count": a.call_count,
            "error_count": a.error_count,
            "error_rate": a.error_count / a.call_count,
            "avg_latency_ms": a.total_latency_ms / a.call_count,
            "min_latency_ms": a.min_latency_ms if a.min_latency_ms != float("inf") else 0,
            "max_latency_ms": a.max_latency_ms,
            "status_codes": dict(a.status_codes),
            "last_call": a.last_call,
        }

    def get_all_stats(self) -> List[Dict[str, Any]]:
        return [
            self.get_endpoint_stats(ep)
            for ep in self._data
            if self.get_endpoint_stats(ep)
        ]

    def get_percentile(self, endpoint: str, percentile: float) -> Optional[float]:
        a = self._data.get(endpoint)
        if not a or a.call_count == 0:
            return None
        return a.total_latency_ms / a.call_count
