"""Data Pipeline Monitor Action Module.

Monitors data pipeline health, throughput, and performance.
Tracks processing rates, latency, error rates, and resource utilization.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics tracked."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()


@dataclass
class MetricPoint:
    """Single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineStageMetrics:
    """Metrics for a single pipeline stage."""
    name: str
    processed_count: int = 0
    error_count: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    throughput_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    error_samples: deque = field(default_factory=lambda: deque(maxlen=100))


class DataPipelineMonitor:
    """Monitor for data pipeline operations.
    
    Tracks metrics across pipeline stages including:
    - Processing throughput
    - Latency distribution
    - Error rates
    - Queue depths
    """
    
    def __init__(self, name: str, retention_seconds: int = 300):
        self.name = name
        self.retention_seconds = retention_seconds
        self._stages: Dict[str, PipelineStageMetrics] = {}
        self._global_metrics: Dict[str, deque] = {}
        self._start_time = time.time()
        self._lock = asyncio.Lock()
        self._callbacks: List[Callable] = []
    
    def register_stage(self, stage_name: str) -> None:
        """Register a pipeline stage for monitoring."""
        if stage_name not in self._stages:
            self._stages[stage_name] = PipelineStageMetrics(name=stage_name)
            logger.info(f"Registered pipeline stage: {stage_name}")
    
    def record_processing(self, stage_name: str, latency_ms: float, success: bool = True) -> None:
        """Record a processing event.
        
        Args:
            stage_name: Name of the pipeline stage
            latency_ms: Processing latency in milliseconds
            success: Whether processing was successful
        """
        if stage_name not in self._stages:
            self.register_stage(stage_name)
        
        stage = self._stages[stage_name]
        stage.processed_count += 1
        
        if success:
            stage.total_latency_ms += latency_ms
            stage.min_latency_ms = min(stage.min_latency_ms, latency_ms)
            stage.max_latency_ms = max(stage.max_latency_ms, latency_ms)
            stage.throughput_samples.append(latency_ms)
        else:
            stage.error_count += 1
            stage.error_samples.append(time.time())
    
    def record_batch(self, stage_name: str, batch_size: int, latency_ms: float) -> None:
        """Record batch processing event.
        
        Args:
            stage_name: Stage name
            batch_size: Number of items in batch
            latency_ms: Batch processing latency
        """
        if stage_name not in self._stages:
            self.register_stage(stage_name)
        
        stage = self._stages[stage_name]
        stage.processed_count += batch_size
        stage.total_latency_ms += latency_ms
        
        per_item_ms = latency_ms / max(batch_size, 1)
        stage.min_latency_ms = min(stage.min_latency_ms, per_item_ms)
        stage.max_latency_ms = max(stage.max_latency_ms, per_item_ms)
        stage.throughput_samples.append(per_item_ms)
    
    def record_metric(self, metric_name: str, value: float, metric_type: MetricType = MetricType.GAUGE, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a custom metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            metric_type: Type of metric
            labels: Optional labels for the metric
        """
        if metric_name not in self._global_metrics:
            self._global_metrics[metric_name] = deque(maxlen=1000)
        
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {}
        )
        self._global_metrics[metric_name].append(point)
    
    def get_stage_stats(self, stage_name: str) -> Dict[str, Any]:
        """Get statistics for a specific stage.
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            Dictionary with stage statistics
        """
        if stage_name not in self._stages:
            return {"error": f"Stage '{stage_name}' not found"}
        
        stage = self._stages[stage_name]
        avg_latency = stage.total_latency_ms / max(stage.processed_count, 1)
        
        throughput_samples = list(stage.throughput_samples)
        p50_latency = self._percentile(throughput_samples, 50) if throughput_samples else 0
        p95_latency = self._percentile(throughput_samples, 95) if throughput_samples else 0
        p99_latency = self._percentile(throughput_samples, 99) if throughput_samples else 0
        
        return {
            "stage_name": stage_name,
            "processed_count": stage.processed_count,
            "error_count": stage.error_count,
            "error_rate": stage.error_count / max(stage.processed_count, 1),
            "latency_ms": {
                "avg": avg_latency,
                "min": stage.min_latency_ms if stage.min_latency_ms != float('inf') else 0,
                "max": stage.max_latency_ms,
                "p50": p50_latency,
                "p95": p95_latency,
                "p99": p99_latency,
            }
        }
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get overall pipeline statistics."""
        total_processed = sum(s.processed_count for s in self._stages.values())
        total_errors = sum(s.error_count for s in self._stages.values())
        uptime_seconds = time.time() - self._start_time
        
        return {
            "pipeline_name": self.name,
            "uptime_seconds": uptime_seconds,
            "total_processed": total_processed,
            "total_errors": total_errors,
            "overall_error_rate": total_errors / max(total_processed, 1),
            "throughput_per_second": total_processed / max(uptime_seconds, 1),
            "stages": {
                name: self.get_stage_stats(name)
                for name in self._stages
            }
        }
    
    def get_metric_history(self, metric_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get historical values for a metric.
        
        Args:
            metric_name: Name of the metric
            limit: Maximum number of points to return
            
        Returns:
            List of metric points
        """
        if metric_name not in self._global_metrics:
            return []
        
        points = list(self._global_metrics[metric_name])[-limit:]
        return [
            {
                "timestamp": p.timestamp,
                "datetime": datetime.fromtimestamp(p.timestamp).isoformat(),
                "value": p.value,
                "labels": p.labels
            }
            for p in points
        ]
    
    def register_alert_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for alert conditions.
        
        Args:
            callback: Function to call when alert triggers
        """
        self._callbacks.append(callback)
    
    async def check_health(self) -> Dict[str, Any]:
        """Perform health check across all stages.
        
        Returns:
            Health status dictionary
        """
        health = {
            "pipeline": self.name,
            "timestamp": datetime.now().isoformat(),
            "healthy": True,
            "stages": {},
            "alerts": []
        }
        
        for stage_name, stage in self._stages.items():
            stage_health = {
                "processed": stage.processed_count,
                "errors": stage.error_count,
                "healthy": True
            }
            
            error_rate = stage.error_count / max(stage.processed_count, 1)
            if error_rate > 0.1:
                stage_health["healthy"] = False
                health["alerts"].append(f"High error rate in {stage_name}: {error_rate:.2%}")
            
            if stage.throughput_samples:
                recent_samples = list(stage.throughput_samples)[-10:]
                avg_recent = sum(recent_samples) / len(recent_samples)
                if avg_recent > stage.max_latency_ms * 2:
                    health["alerts"].append(f"Latency spike in {stage_name}")
            
            health["stages"][stage_name] = stage_health
        
        health["healthy"] = len(health["alerts"]) == 0
        
        for callback in self._callbacks:
            try:
                await callback(health)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")
        
        return health
    
    def reset(self) -> None:
        """Reset all metrics."""
        self._stages.clear()
        self._global_metrics.clear()
        self._start_time = time.time()
    
    @staticmethod
    def _percentile(sorted_values: List[float], percentile: int) -> float:
        """Calculate percentile from sorted values."""
        if not sorted_values:
            return 0.0
        index = int(len(sorted_values) * percentile / 100)
        index = min(index, len(sorted_values) - 1)
        return sorted_values[index]


class AlertRule:
    """Rule for triggering alerts based on metrics."""
    
    def __init__(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        severity: str = "warning",
        message_template: str = ""
    ):
        self.name = name
        self.condition = condition
        self.severity = severity
        self.message_template = message_template
    
    def evaluate(self, health_status: Dict[str, Any]) -> Optional[str]:
        """Evaluate the rule and return message if triggered."""
        if self.condition(health_status):
            return self.message_template.format(**health_status)
        return None


class PipelineAlertManager:
    """Manages alerts for pipeline monitoring."""
    
    def __init__(self):
        self._rules: List[AlertRule] = []
        self._active_alerts: Dict[str, Dict[str, Any]] = {}
    
    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self._rules.append(rule)
    
    def add_error_rate_rule(self, threshold: float, stage_name: Optional[str] = None) -> None:
        """Add error rate threshold rule.
        
        Args:
            threshold: Error rate threshold (e.g., 0.05 for 5%)
            stage_name: Optional specific stage name
        """
        def condition(health: Dict[str, Any]) -> bool:
            if stage_name:
                return health.get("stages", {}).get(stage_name, {}).get("error_count", 0) > threshold
            return health.get("overall_error_rate", 0) > threshold
        
        self.add_rule(AlertRule(
            name=f"error_rate_{stage_name or 'overall'}",
            condition=condition,
            message_template=f"Error rate exceeded {threshold:.2%}"
        ))
    
    def add_latency_rule(self, threshold_ms: float, stage_name: str) -> None:
        """Add latency threshold rule."""
        def condition(health: Dict[str, Any]) -> bool:
            stage_stats = health.get("stages", {}).get(stage_name, {})
            avg_latency = stage_stats.get("latency_ms", {}).get("avg", 0)
            return avg_latency > threshold_ms
        
        self.add_rule(AlertRule(
            name=f"latency_{stage_name}",
            condition=condition,
            severity="critical" if threshold_ms > 1000 else "warning",
            message_template=f"Stage {stage_name} latency exceeded {threshold_ms}ms"
        ))
    
    async def evaluate(self, health_status: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate all rules against health status.
        
        Returns:
            List of triggered alerts
        """
        triggered = []
        for rule in self._rules:
            message = rule.evaluate(health_status)
            if message:
                alert = {
                    "rule": rule.name,
                    "severity": rule.severity,
                    "message": message,
                    "timestamp": datetime.now().isoformat()
                }
                triggered.append(alert)
                self._active_alerts[rule.name] = alert
        
        return triggered
