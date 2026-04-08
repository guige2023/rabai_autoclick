"""API Metric Action Module.

Collects and reports API metrics including latency percentiles,
throughput, error rates, and custom business metrics.
"""

from __future__ import annotations

import sys
import os
import time
import math
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LatencyBucket:
    """Latency distribution bucket."""
    bucket_max: float
    count: int = 0


class APIMetricAction(BaseAction):
    """
    API metrics collection.

    Collects latency, throughput, error rates,
    and custom business metrics.

    Example:
        metric = APIMetricAction()
        result = metric.execute(ctx, {"action": "record_latency", "endpoint": "/api/users", "latency_ms": 45})
    """
    action_type = "api_metric"
    display_name = "API指标收集"
    description = "API延迟、吞吐量和错误率指标收集"

    def __init__(self) -> None:
        super().__init__()
        self._latencies: Dict[str, List[float]] = defaultdict(list)
        self._request_counts: Dict[str, int] = defaultdict(int)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._custom_metrics: Dict[str, float] = {}
        self._histogram_buckets = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "record_latency":
                return self._record_latency(params)
            elif action == "record_request":
                return self._record_request(params)
            elif action == "record_error":
                return self._record_error(params)
            elif action == "record_custom":
                return self._record_custom(params)
            elif action == "get_metrics":
                return self._get_metrics(params)
            elif action == "get_percentiles":
                return self._get_percentiles(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Metric error: {str(e)}")

    def _record_latency(self, params: Dict[str, Any]) -> ActionResult:
        endpoint = params.get("endpoint", "unknown")
        latency_ms = params.get("latency_ms", 0.0)

        self._latencies[endpoint].append(latency_ms)

        return ActionResult(success=True)

    def _record_request(self, params: Dict[str, Any]) -> ActionResult:
        endpoint = params.get("endpoint", "unknown")
        self._request_counts[endpoint] += 1
        return ActionResult(success=True)

    def _record_error(self, params: Dict[str, Any]) -> ActionResult:
        endpoint = params.get("endpoint", "unknown")
        self._error_counts[endpoint] += 1
        return ActionResult(success=True)

    def _record_custom(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        value = params.get("value", 0.0)

        if not name:
            return ActionResult(success=False, message="name is required")

        self._custom_metrics[name] = value

        return ActionResult(success=True)

    def _get_metrics(self, params: Dict[str, Any]) -> ActionResult:
        endpoint = params.get("endpoint")

        if endpoint:
            latencies = self._latencies.get(endpoint, [])
            return ActionResult(success=True, data={"endpoint": endpoint, "request_count": self._request_counts.get(endpoint, 0), "error_count": self._error_counts.get(endpoint, 0), "latency_samples": len(latencies)})

        return ActionResult(success=True, data={"endpoints": list(self._request_counts.keys()), "custom_metrics": self._custom_metrics})

    def _get_percentiles(self, params: Dict[str, Any]) -> ActionResult:
        endpoint = params.get("endpoint", "unknown")
        latencies = self._latencies.get(endpoint, [])

        if not latencies:
            return ActionResult(success=True, data={"endpoint": endpoint, "p50": 0, "p90": 0, "p95": 0, "p99": 0})

        sorted_latencies = sorted(latencies)
        n = len(sorted_latencies)

        def percentile(p: float) -> float:
            idx = int(n * p)
            return sorted_latencies[min(idx, n - 1)]

        return ActionResult(success=True, data={"endpoint": endpoint, "p50_ms": percentile(0.50), "p90_ms": percentile(0.90), "p95_ms": percentile(0.95), "p99_ms": percentile(0.99), "count": n})
