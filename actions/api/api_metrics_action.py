"""API Metrics Action Module.

Provides comprehensive metrics collection and reporting for API operations,
including request/response metrics, latency tracking, and performance analysis.

Example:
    >>> from actions.api.api_metrics_action import APIMetricsAction
    >>> action = APIMetricsAction()
    >>> action.record_request(endpoint="/api/users", method="GET", duration_ms=45)
    >>> metrics = action.get_metrics()
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import threading


class MetricType(Enum):
    """Types of metrics that can be recorded."""
    REQUEST_COUNT = "request_count"
    REQUEST_DURATION = "request_duration"
    REQUEST_SIZE = "request_size"
    RESPONSE_SIZE = "response_size"
    ERROR_COUNT = "error_count"
    RATE_LIMIT_COUNT = "rate_limit_count"
    TIMEOUT_COUNT = "timeout_count"


class AggregationType(Enum):
    """Aggregation methods for metrics."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE = "percentile"


@dataclass
class APIMetric:
    """Individual API metric record."""
    timestamp: datetime
    metric_type: MetricType
    endpoint: str
    method: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Aggregated metric summary."""
    metric_type: MetricType
    endpoint: str
    method: str
    aggregation: AggregationType
    value: float
    sample_count: int
    time_window: timedelta
    tags: Dict[str, str] = field(default_factory=dict)


class APIMetricsAction:
    """Collects and aggregates API metrics.
    
    Provides thread-safe metrics collection with support for
    multiple aggregation types and time windows.
    
    Attributes:
        metrics: Internal storage for collected metrics
        _lock: Thread lock for concurrent access
    
    Example:
        >>> action = APIMetricsAction()
        >>> action.record_request("GET", "/api/users", 200, 45.2)
        >>> summary = action.get_summary(MetricType.REQUEST_DURATION)
    """
    
    def __init__(
        self,
        time_window: timedelta = timedelta(minutes=5),
        max_metrics: int = 100000,
        enable_percentiles: bool = True,
        percentiles: List[float] = None
    ):
        """Initialize the API metrics action.
        
        Args:
            time_window: Time window for metric aggregation (default 5 minutes)
            max_metrics: Maximum number of metrics to retain in memory
            enable_percentiles: Whether to calculate percentiles
            percentiles: List of percentiles to calculate (default [50, 90, 95, 99])
        """
        self.time_window = time_window
        self.max_metrics = max_metrics
        self.enable_percentiles = enable_percentiles
        self.percentiles = percentiles or [50, 90, 95, 99]
        
        self._metrics: List[APIMetric] = []
        self._counters: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._start_time = datetime.now()
    
    def record_request(
        self,
        method: str,
        endpoint: str,
        status_code: int,
        duration_ms: float,
        request_size: Optional[int] = None,
        response_size: Optional[int] = None,
        tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record an API request metric.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            status_code: HTTP response status code
            duration_ms: Request duration in milliseconds
            request_size: Request body size in bytes
            response_size: Response body size in bytes
            tags: Optional tags for metric categorization
            metadata: Optional additional metadata
        """
        tags = tags or {}
        metadata = metadata or {}
        
        now = datetime.now()
        
        # Record request count
        self._record_metric(APIMetric(
            timestamp=now,
            metric_type=MetricType.REQUEST_COUNT,
            endpoint=endpoint,
            method=method,
            value=1.0,
            tags=tags,
            metadata=metadata
        ))
        
        # Record duration
        self._record_metric(APIMetric(
            timestamp=now,
            metric_type=MetricType.REQUEST_DURATION,
            endpoint=endpoint,
            method=method,
            value=duration_ms,
            tags=tags,
            metadata=metadata
        ))
        
        # Record sizes if provided
        if request_size is not None:
            self._record_metric(APIMetric(
                timestamp=now,
                metric_type=MetricType.REQUEST_SIZE,
                endpoint=endpoint,
                method=method,
                value=float(request_size),
                tags=tags,
                metadata=metadata
            ))
        
        if response_size is not None:
            self._record_metric(APIMetric(
                timestamp=now,
                metric_type=MetricType.RESPONSE_SIZE,
                endpoint=endpoint,
                method=method,
                value=float(response_size),
                tags=tags,
                metadata=metadata
            ))
        
        # Record errors
        if status_code >= 400:
            self._record_metric(APIMetric(
                timestamp=now,
                metric_type=MetricType.ERROR_COUNT,
                endpoint=endpoint,
                method=method,
                value=1.0,
                tags={**tags, "status_code": str(status_code)},
                metadata=metadata
            ))
    
    def record_rate_limit(self, endpoint: str, method: str, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a rate limit event.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            tags: Optional tags for categorization
        """
        self._record_metric(APIMetric(
            timestamp=datetime.now(),
            metric_type=MetricType.RATE_LIMIT_COUNT,
            endpoint=endpoint,
            method=method,
            value=1.0,
            tags=tags or {}
        ))
    
    def record_timeout(self, endpoint: str, method: str, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a timeout event.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            tags: Optional tags for categorization
        """
        self._record_metric(APIMetric(
            timestamp=datetime.now(),
            metric_type=MetricType.TIMEOUT_COUNT,
            endpoint=endpoint,
            method=method,
            value=1.0,
            tags=tags or {}
        ))
    
    def _record_metric(self, metric: APIMetric) -> None:
        """Internal method to record a metric.
        
        Args:
            metric: The metric to record
        """
        with self._lock:
            self._metrics.append(metric)
            
            # Enforce max metrics limit
            if len(self._metrics) > self.max_metrics:
                # Remove oldest 10%
                self._metrics = self._metrics[int(len(self._metrics) * 0.1):]
    
    def get_metrics(
        self,
        metric_type: Optional[MetricType] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        since: Optional[datetime] = None
    ) -> List[APIMetric]:
        """Retrieve metrics matching the given filters.
        
        Args:
            metric_type: Filter by metric type
            endpoint: Filter by endpoint pattern
            method: Filter by HTTP method
            since: Return metrics after this timestamp
        
        Returns:
            List of matching metrics
        """
        with self._lock:
            filtered = self._metrics.copy()
            
            if metric_type is not None:
                filtered = [m for m in filtered if m.metric_type == metric_type]
            
            if endpoint is not None:
                filtered = [m for m in filtered if endpoint in m.endpoint]
            
            if method is not None:
                filtered = [m for m in filtered if m.method == method]
            
            if since is not None:
                filtered = [m for m in filtered if m.timestamp >= since]
            
            return filtered
    
    def get_summary(
        self,
        metric_type: MetricType,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        aggregation: AggregationType = AggregationType.AVG
    ) -> MetricSummary:
        """Get aggregated summary of metrics.
        
        Args:
            metric_type: Type of metric to summarize
            endpoint: Filter by endpoint
            method: Filter by HTTP method
            aggregation: Aggregation method to use
        
        Returns:
            Aggregated metric summary
        """
        cutoff = datetime.now() - self.time_window
        metrics = self.get_metrics(metric_type, endpoint, method, since=cutoff)
        
        if not metrics:
            return MetricSummary(
                metric_type=metric_type,
                endpoint=endpoint or "all",
                method=method or "all",
                aggregation=aggregation,
                value=0.0,
                sample_count=0,
                time_window=self.time_window
            )
        
        values = [m.value for m in metrics]
        
        if aggregation == AggregationType.SUM:
            agg_value = sum(values)
        elif aggregation == AggregationType.AVG:
            agg_value = sum(values) / len(values)
        elif aggregation == AggregationType.MIN:
            agg_value = min(values)
        elif aggregation == AggregationType.MAX:
            agg_value = max(values)
        elif aggregation == AggregationType.COUNT:
            agg_value = float(len(values))
        elif aggregation == AggregationType.PERCENTILE:
            sorted_values = sorted(values)
            idx = int(len(sorted_values) * self.percentiles[0] / 100)
            agg_value = sorted_values[min(idx, len(sorted_values) - 1)]
        else:
            agg_value = sum(values) / len(values)
        
        return MetricSummary(
            metric_type=metric_type,
            endpoint=endpoint or "all",
            method=method or "all",
            aggregation=aggregation,
            value=agg_value,
            sample_count=len(values),
            time_window=self.time_window,
            tags=self._merge_tags(metrics)
        )
    
    def _merge_tags(self, metrics: List[APIMetric]) -> Dict[str, str]:
        """Merge tags from multiple metrics.
        
        Args:
            metrics: List of metrics to merge
        
        Returns:
            Merged tag dictionary
        """
        merged: Dict[str, List[str]] = {}
        for metric in metrics:
            for key, value in metric.tags.items():
                if key not in merged:
                    merged[key] = []
                merged[key].append(value)
        
        return {k: ",".join(set(v)) for k, v in merged.items()}
    
    def get_endpoint_summary(self, endpoint: str) -> Dict[str, Any]:
        """Get comprehensive summary for an endpoint.
        
        Args:
            endpoint: API endpoint path
        
        Returns:
            Dictionary with endpoint metrics summary
        """
        return {
            "endpoint": endpoint,
            "request_count": self.get_summary(
                MetricType.REQUEST_COUNT, endpoint
            ).value,
            "avg_duration_ms": self.get_summary(
                MetricType.REQUEST_DURATION, endpoint, aggregation=AggregationType.AVG
            ).value,
            "max_duration_ms": self.get_summary(
                MetricType.REQUEST_DURATION, endpoint, aggregation=AggregationType.MAX
            ).value,
            "error_count": self.get_summary(
                MetricType.ERROR_COUNT, endpoint
            ).value,
            "error_rate": self._calculate_error_rate(endpoint),
            "p50_ms": self.get_summary(
                MetricType.REQUEST_DURATION, endpoint,
                aggregation=AggregationType.PERCENTILE
            ).value,
            "p95_ms": self.get_summary(
                MetricType.REQUEST_DURATION, endpoint,
                aggregation=AggregationType.PERCENTILE
            ).value,
        }
    
    def _calculate_error_rate(self, endpoint: str) -> float:
        """Calculate error rate for an endpoint.
        
        Args:
            endpoint: API endpoint path
        
        Returns:
            Error rate as a percentage (0-100)
        """
        total = self.get_summary(MetricType.REQUEST_COUNT, endpoint).value
        errors = self.get_summary(MetricType.ERROR_COUNT, endpoint).value
        
        if total == 0:
            return 0.0
        
        return (errors / total) * 100
    
    def reset(self) -> None:
        """Clear all collected metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._start_time = datetime.now()
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics in a serializable format.
        
        Returns:
            Dictionary containing all metrics data
        """
        with self._lock:
            return {
                "export_time": datetime.now().isoformat(),
                "time_window": str(self.time_window),
                "total_metrics": len(self._metrics),
                "start_time": self._start_time.isoformat(),
                "metric_types": [m.value for m in MetricType],
                "metrics": [
                    {
                        "timestamp": m.timestamp.isoformat(),
                        "type": m.metric_type.value,
                        "endpoint": m.endpoint,
                        "method": m.method,
                        "value": m.value,
                        "tags": m.tags,
                        "metadata": m.metadata
                    }
                    for m in self._metrics
                ]
            }
