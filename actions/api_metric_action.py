"""
API Metric Action - Collects API metrics.

This module provides API metrics collection capabilities for
monitoring API performance.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class APIMetric:
    """A single API metric."""
    timestamp: float
    method: str
    url: str
    status_code: int
    duration_ms: float
    response_size: int = 0


@dataclass
class MetricSummary:
    """Summary of API metrics."""
    total_requests: int = 0
    success_count: int = 0
    error_count: int = 0
    avg_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0


class MetricCollector:
    """Collects API metrics."""
    
    def __init__(self, max_metrics: int = 10000) -> None:
        self.max_metrics = max_metrics
        self._metrics: list[APIMetric] = []
    
    def record(self, metric: APIMetric) -> None:
        """Record a metric."""
        self._metrics.append(metric)
        if len(self._metrics) > self.max_metrics:
            self._metrics = self._metrics[-self.max_metrics:]
    
    def get_summary(self) -> MetricSummary:
        """Get metrics summary."""
        if not self._metrics:
            return MetricSummary()
        
        durations = [m.duration_ms for m in self._metrics]
        durations.sort()
        
        p95_idx = int(len(durations) * 0.95)
        p95 = durations[p95_idx] if durations else 0
        
        return MetricSummary(
            total_requests=len(self._metrics),
            success_count=sum(1 for m in self._metrics if m.status_code < 400),
            error_count=sum(1 for m in self._metrics if m.status_code >= 400),
            avg_duration_ms=sum(durations) / len(durations) if durations else 0,
            p95_duration_ms=p95,
        )


class APIMetricAction:
    """API metric action for automation workflows."""
    
    def __init__(self) -> None:
        self.collector = MetricCollector()
    
    def record_request(
        self,
        method: str,
        url: str,
        status_code: int,
        duration_ms: float,
        response_size: int = 0,
    ) -> None:
        """Record an API request."""
        self.collector.record(APIMetric(
            timestamp=time.time(),
            method=method,
            url=url,
            status_code=status_code,
            duration_ms=duration_ms,
            response_size=response_size,
        ))
    
    def get_summary(self) -> MetricSummary:
        """Get metrics summary."""
        return self.collector.get_summary()


__all__ = ["APIMetric", "MetricSummary", "MetricCollector", "APIMetricAction"]
