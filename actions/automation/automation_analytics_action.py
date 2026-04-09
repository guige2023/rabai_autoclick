"""Automation Analytics Action Module.

Provides analytics and metrics collection for automation workflows,
tracking execution patterns, performance metrics, and bottlenecks.

Example:
    >>> from actions.automation.automation_analytics_action import AutomationAnalytics
    >>> analytics = AutomationAnalytics()
    >>> analytics.record_execution(step_id, duration=1.5)
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class MetricType(Enum):
    """Types of metrics collected."""
    DURATION = "duration"
    COUNT = "count"
    ERROR_RATE = "error_rate"
    THROUGHPUT = "throughput"
    AVAILABILITY = "availability"
    LATENCY = "latency"


class Percentile(Enum):
    """Percentile thresholds for analysis."""
    P50 = 50
    P75 = 75
    P90 = 90
    P95 = 95
    P99 = 99


@dataclass
class MetricPoint:
    """Single metric data point.
    
    Attributes:
        timestamp: When the metric was recorded
        value: Metric value
        labels: Optional labels/tags
    """
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric.
    
    Attributes:
        metric_name: Name of the metric
        count: Number of samples
        total: Sum of all values
        mean: Average value
        min: Minimum value
        max: Maximum value
        stddev: Standard deviation
        p50: 50th percentile
        p75: 75th percentile
        p90: 90th percentile
        p95: 95th percentile
        p99: 99th percentile
    """
    metric_name: str
    count: int = 0
    total: float = 0.0
    mean: float = 0.0
    min: float = 0.0
    max: float = 0.0
    stddev: float = 0.0
    p50: float = 0.0
    p75: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0


@dataclass
class WorkflowStats:
    """Statistics for a workflow or step.
    
    Attributes:
        workflow_id: Workflow identifier
        total_executions: Total times executed
        successful: Number of successful runs
        failed: Number of failed runs
        avg_duration: Average execution duration
        last_execution: Last execution timestamp
        error_rate: Current error rate
    """
    workflow_id: str
    total_executions: int = 0
    successful: int = 0
    failed: int = 0
    avg_duration: float = 0.0
    last_execution: Optional[datetime] = None
    error_rate: float = 0.0
    throughput: float = 0.0


@dataclass
class AnalyticsConfig:
    """Configuration for analytics collection.
    
    Attributes:
        retention_period: How long to retain metrics (seconds)
        max_points_per_metric: Maximum data points per metric
        enable_percentiles: Whether to compute percentiles
        flush_interval: Auto-flush interval in seconds
        enable_predictions: Whether to enable trend prediction
    """
    retention_period: float = 3600.0
    max_points_per_metric: int = 10000
    enable_percentiles: bool = True
    flush_interval: float = 60.0
    enable_predictions: bool = True


class AutomationAnalytics:
    """Collects and analyzes automation metrics.
    
    Provides comprehensive analytics for automation workflows,
    including execution times, success rates, and performance trends.
    
    Attributes:
        config: Analytics configuration
    
    Example:
        >>> analytics = AutomationAnalytics()
        >>> analytics.record_execution("step_1", duration=2.5)
        >>> summary = analytics.get_metric_summary("step_1_duration")
    """
    
    def __init__(self, config: Optional[AnalyticsConfig] = None):
        """Initialize the analytics collector.
        
        Args:
            config: Analytics configuration. Uses defaults if not provided.
        """
        self.config = config or AnalyticsConfig()
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.config.max_points_per_metric))
        self._workflows: Dict[str, WorkflowStats] = {}
        self._execution_start: Dict[str, float] = {}
        self._counters: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        self._last_flush = time.time()
    
    def _get_metric_name(self, workflow_id: str, metric_type: MetricType, labels: Optional[Dict[str, str]] = None) -> str:
        """Generate a metric name with labels.
        
        Args:
            workflow_id: Workflow/step identifier
            metric_type: Type of metric
            labels: Optional labels
        
        Returns:
            Metric name string
        """
        name = f"{workflow_id}_{metric_type.value}"
        if labels:
            label_str = "_".join(f"{k}={v}" for k, v in sorted(labels.items()))
            name = f"{name}_{label_str}"
        return name
    
    def record_execution(
        self,
        workflow_id: str,
        duration: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Record a workflow execution.
        
        Args:
            workflow_id: Workflow or step identifier
            duration: Execution duration in seconds
            success: Whether execution succeeded
            error: Optional error message
            labels: Optional metric labels
        """
        now = datetime.now()
        
        with self._lock:
            # Update workflow stats
            if workflow_id not in self._workflows:
                self._workflows[workflow_id] = WorkflowStats(workflow_id=workflow_id)
            
            stats = self._workflows[workflow_id]
            stats.total_executions += 1
            stats.last_execution = now
            
            if success:
                stats.successful += 1
            else:
                stats.failed += 1
            
            stats.error_rate = stats.failed / stats.total_executions if stats.total_executions > 0 else 0.0
            
            # Update duration metrics
            if duration is not None:
                # Running average
                stats.avg_duration = (stats.avg_duration * (stats.total_executions - 1) + duration) / stats.total_executions
                
                # Record metric point
                metric_name = self._get_metric_name(workflow_id, MetricType.DURATION, labels)
                self._metrics[metric_name].append(MetricPoint(timestamp=now, value=duration, labels=labels or {}))
            
            # Record count
            count_name = self._get_metric_name(workflow_id, MetricType.COUNT, labels)
            self._metrics[count_name].append(MetricPoint(timestamp=now, value=1.0, labels=labels or {}))
            
            # Record error rate
            if stats.total_executions > 0:
                error_name = self._get_metric_name(workflow_id, MetricType.ERROR_RATE, labels)
                self._metrics[error_name].append(MetricPoint(
                    timestamp=now,
                    value=stats.error_rate,
                    labels=labels or {}
                ))
    
    def start_execution(self, workflow_id: str) -> None:
        """Mark the start of a workflow execution.
        
        Args:
            workflow_id: Workflow or step identifier
        """
        with self._lock:
            self._execution_start[workflow_id] = time.time()
    
    def end_execution(self, workflow_id: str, success: bool = True, error: Optional[str] = None) -> float:
        """Mark the end of a workflow execution.
        
        Args:
            workflow_id: Workflow or step identifier
            success: Whether execution succeeded
            error: Optional error message
        
        Returns:
            Execution duration in seconds
        """
        duration = 0.0
        
        with self._lock:
            if workflow_id in self._execution_start:
                duration = time.time() - self._execution_start[workflow_id]
                del self._execution_start[workflow_id]
        
        if duration > 0:
            self.record_execution(workflow_id, duration=duration, success=success, error=error)
        
        return duration
    
    def increment_counter(self, counter_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric.
        
        Args:
            counter_name: Name of the counter
            value: Amount to increment
            labels: Optional labels
        """
        now = datetime.now()
        
        with self._lock:
            key = f"{counter_name}_{labels}" if labels else counter_name
            self._counters[key] += value
            
            metric_name = f"{counter_name}_count"
            if labels:
                metric_name = self._get_metric_name(counter_name, MetricType.COUNT, labels)
            
            self._metrics[metric_name].append(MetricPoint(
                timestamp=now,
                value=float(self._counters[key]),
                labels=labels or {}
            ))
    
    def record_value(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Record a generic metric value.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            labels: Optional labels
        """
        now = datetime.now()
        
        with self._lock:
            self._metrics[metric_name].append(MetricPoint(
                timestamp=now,
                value=value,
                labels=labels or {}
            ))
    
    def get_metric_summary(self, metric_name: str, window_seconds: Optional[float] = None) -> Optional[MetricSummary]:
        """Get summary statistics for a metric.
        
        Args:
            metric_name: Name of the metric
            window_seconds: Optional time window to consider
        
        Returns:
            MetricSummary or None if no data
        """
        with self._lock:
            if metric_name not in self._metrics or not self._metrics[metric_name]:
                return None
            
            points = list(self._metrics[metric_name])
        
        # Filter by time window
        if window_seconds:
            cutoff = datetime.now() - timedelta(seconds=window_seconds)
            points = [p for p in points if p.timestamp > cutoff]
        
        if not points:
            return None
        
        values = [p.value for p in points]
        values_sorted = sorted(values)
        count = len(values)
        
        summary = MetricSummary(
            metric_name=metric_name,
            count=count,
            total=sum(values),
            mean=sum(values) / count,
            min=min(values),
            max=max(values)
        )
        
        # Standard deviation
        if count > 1:
            mean = summary.mean
            variance = sum((v - mean) ** 2 for v in values) / (count - 1)
            summary.stddev = variance ** 0.5
        
        # Percentiles
        if self.config.enable_percentiles and count >= 2:
            summary.p50 = self._percentile(values_sorted, Percentile.P50.value)
            summary.p75 = self._percentile(values_sorted, Percentile.P75.value)
            summary.p90 = self._percentile(values_sorted, Percentile.P90.value)
            summary.p95 = self._percentile(values_sorted, Percentile.P95.value)
            summary.p99 = self._percentile(values_sorted, Percentile.P99.value)
        
        return summary
    
    def _percentile(self, sorted_values: List[float], p: int) -> float:
        """Calculate percentile from sorted values.
        
        Args:
            sorted_values: Sorted list of values
            p: Percentile (0-100)
        
        Returns:
            Percentile value
        """
        if not sorted_values:
            return 0.0
        
        n = len(sorted_values)
        idx = (p / 100.0) * (n - 1)
        
        if idx.is_integer():
            return sorted_values[int(idx)]
        
        lower = int(idx)
        upper = lower + 1
        weight = idx - lower
        
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
    
    def get_workflow_stats(self, workflow_id: str) -> Optional[WorkflowStats]:
        """Get statistics for a specific workflow.
        
        Args:
            workflow_id: Workflow identifier
        
        Returns:
            WorkflowStats or None
        """
        with self._lock:
            return self._workflows.get(workflow_id)
    
    def get_all_workflow_stats(self) -> List[WorkflowStats]:
        """Get statistics for all tracked workflows.
        
        Returns:
            List of workflow statistics
        """
        with self._lock:
            return list(self._workflows.values())
    
    def get_trending_metrics(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get metrics with highest recent activity.
        
        Args:
            limit: Maximum number of metrics to return
        
        Returns:
            List of (metric_name, activity_score) tuples
        """
        now = datetime.now()
        cutoff = now - timedelta(seconds=300)  # Last 5 minutes
        
        scores: List[Tuple[str, float]] = []
        
        with self._lock:
            for metric_name, points in self._metrics.items():
                recent = [p for p in points if p.timestamp > cutoff]
                if recent:
                    # Score based on recent activity and variance
                    activity = len(recent)
                    values = [p.value for p in recent]
                    variance = (max(values) - min(values)) if values else 0
                    scores.append((metric_name, activity * (1 + variance)))
        
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:limit]
    
    def detect_anomalies(
        self,
        metric_name: str,
        threshold_stddev: float = 2.0
    ) -> List[MetricPoint]:
        """Detect anomalous metric points.
        
        Args:
            metric_name: Metric to analyze
            threshold_stddev: Number of standard deviations for anomaly
        
        Returns:
            List of anomalous data points
        """
        summary = self.get_metric_summary(metric_name)
        if not summary or summary.count < 3:
            return []
        
        with self._lock:
            points = list(self._metrics.get(metric_name, []))
        
        anomalies = []
        lower = summary.mean - threshold_stddev * summary.stddev
        upper = summary.mean + threshold_stddev * summary.stddev
        
        for point in points:
            if point.value < lower or point.value > upper:
                anomalies.append(point)
        
        return anomalies
    
    def get_throughput(
        self,
        metric_name: str,
        window_seconds: float = 60.0
    ) -> float:
        """Calculate throughput (events per second) for a metric.
        
        Args:
            metric_name: Metric name
            window_seconds: Time window for calculation
        
        Returns:
            Events per second
        """
        summary = self.get_metric_summary(metric_name, window_seconds=window_seconds)
        if not summary:
            return 0.0
        
        return summary.count / window_seconds
    
    def clear_metrics(self, older_than_seconds: Optional[float] = None) -> int:
        """Clear old metric data.
        
        Args:
            older_than_seconds: Clear data older than this. None = clear all.
        
        Returns:
            Number of metrics cleared
        """
        with self._lock:
            if older_than_seconds is None:
                count = sum(len(m) for m in self._metrics.values())
                self._metrics.clear()
                return count
            
            cutoff = datetime.now() - timedelta(seconds=older_than_seconds)
            cleared = 0
            
            for metric_name, points in self._metrics.items():
                original_len = len(points)
                self._metrics[metric_name] = deque(
                    (p for p in points if p.timestamp > cutoff),
                    maxlen=self.config.max_points_per_metric
                )
                cleared += original_len - len(self._metrics[metric_name])
            
            return cleared
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics for external storage.
        
        Returns:
            Dictionary of all metrics
        """
        with self._lock:
            result = {
                "export_time": datetime.now().isoformat(),
                "workflows": {},
                "metrics": {}
            }
            
            for wf_id, stats in self._workflows.items():
                result["workflows"][wf_id] = {
                    "total_executions": stats.total_executions,
                    "successful": stats.successful,
                    "failed": stats.failed,
                    "avg_duration": stats.avg_duration,
                    "error_rate": stats.error_rate,
                    "last_execution": stats.last_execution.isoformat() if stats.last_execution else None
                }
            
            for metric_name, points in self._metrics.items():
                result["metrics"][metric_name] = [
                    {"timestamp": p.timestamp.isoformat(), "value": p.value, "labels": p.labels}
                    for p in points
                ]
            
            return result
