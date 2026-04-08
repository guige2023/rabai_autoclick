"""API monitor action module for RabAI AutoClick.

Provides API monitoring with health checks, latency tracking,
error rate monitoring, and alerting.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime, timedelta
from statistics import mean, median

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RequestMetric:
    """A single request metric."""
    timestamp: float
    endpoint: str
    method: str
    status_code: int
    latency_ms: float
    success: bool
    error_type: Optional[str] = None


@dataclass
class HealthStatus:
    """Health status of an API."""
    healthy: bool
    latency_ms: float
    uptime_percent: float
    error_rate_percent: float
    last_check: str


class APIMonitorAction(BaseAction):
    """Monitor API health, latency, and error rates.
    
    Tracks request metrics, calculates health indicators,
    and provides alerting on degradation.
    """
    action_type = "api_monitor"
    display_name = "API监控"
    description = "API健康检查和性能监控"
    
    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._health_checks: Dict[str, Callable] = {}
        self._alert_thresholds = {
            'latency_ms': 1000,
            'error_rate_percent': 5.0,
            'min_requests': 10
        }
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute monitoring operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'record', 'health', 'stats', 'alert', 'clear'
                - endpoint: API endpoint name
                - metric: Metric data dict
                - thresholds: Alert threshold overrides
        
        Returns:
            ActionResult with monitoring result.
        """
        operation = params.get('operation', 'record').lower()
        
        if operation == 'record':
            return self._record(params)
        elif operation == 'health':
            return self._health(params)
        elif operation == 'stats':
            return self._stats(params)
        elif operation == 'alert':
            return self._alert(params)
        elif operation == 'clear':
            return self._clear(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _record(self, params: Dict[str, Any]) -> ActionResult:
        """Record a request metric."""
        endpoint = params.get('endpoint', 'default')
        metric_data = params.get('metric', {})
        
        metric = RequestMetric(
            timestamp=time.time(),
            endpoint=endpoint,
            method=metric_data.get('method', 'GET'),
            status_code=metric_data.get('status_code', 0),
            latency_ms=metric_data.get('latency_ms', 0),
            success=200 <= metric_data.get('status_code', 0) < 300,
            error_type=metric_data.get('error_type')
        )
        
        self._metrics[endpoint].append(metric)
        
        return ActionResult(
            success=True,
            message=f"Recorded metric for {endpoint}",
            data={'endpoint': endpoint, 'latency_ms': metric.latency_ms}
        )
    
    def _health(self, params: Dict[str, Any]) -> ActionResult:
        """Get health status of an endpoint."""
        endpoint = params.get('endpoint', 'default')
        window_seconds = params.get('window_seconds', 300)
        
        cutoff = time.time() - window_seconds
        metrics = [m for m in self._metrics[endpoint] if m.timestamp >= cutoff]
        
        if len(metrics) < self._alert_thresholds['min_requests']:
            return ActionResult(
                success=True,
                message="Insufficient data for health check",
                data={
                    'healthy': None,
                    'sample_size': len(metrics)
                }
            )
        
        # Calculate health indicators
        total = len(metrics)
        successes = sum(1 for m in metrics if m.success)
        errors = total - successes
        
        latencies = [m.latency_ms for m in metrics]
        
        uptime = (successes / total) * 100
        error_rate = (errors / total) * 100
        avg_latency = mean(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
        
        healthy = (
            avg_latency < self._alert_thresholds['latency_ms'] and
            error_rate < self._alert_thresholds['error_rate_percent']
        )
        
        status = HealthStatus(
            healthy=healthy,
            latency_ms=avg_latency,
            uptime_percent=uptime,
            error_rate_percent=error_rate,
            last_check=datetime.utcnow().isoformat() + 'Z'
        )
        
        return ActionResult(
            success=True,
            message=f"{'Healthy' if healthy else 'Unhealthy'}",
            data={
                'healthy': status.healthy,
                'latency_ms': round(status.latency_ms, 2),
                'p95_latency_ms': round(p95_latency, 2),
                'uptime_percent': round(status.uptime_percent, 2),
                'error_rate_percent': round(status.error_rate_percent, 2),
                'sample_size': total
            }
        )
    
    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get detailed statistics."""
        endpoint = params.get('endpoint', 'default')
        window_seconds = params.get('window_seconds', 3600)
        
        cutoff = time.time() - window_seconds
        metrics = [m for m in self._metrics[endpoint] if m.timestamp >= cutoff]
        
        if not metrics:
            return ActionResult(
                success=True,
                message="No metrics available",
                data={'count': 0}
            )
        
        latencies = [m.latency_ms for m in metrics]
        status_codes = defaultdict(int)
        error_types = defaultdict(int)
        
        for m in metrics:
            status_codes[m.status_code] += 1
            if m.error_type:
                error_types[m.error_type] += 1
        
        return ActionResult(
            success=True,
            message=f"Statistics for {endpoint}",
            data={
                'count': len(metrics),
                'latency': {
                    'min': round(min(latencies), 2),
                    'max': round(max(latencies), 2),
                    'mean': round(mean(latencies), 2),
                    'median': round(median(latencies), 2),
                    'p95': round(sorted(latencies)[int(len(latencies) * 0.95)], 2),
                    'p99': round(sorted(latencies)[int(len(latencies) * 0.99)], 2)
                },
                'status_codes': dict(status_codes),
                'error_types': dict(error_types)
            }
        )
    
    def _alert(self, params: Dict[str, Any]) -> ActionResult:
        """Check if any endpoints need alerting."""
        alerts = []
        
        for endpoint in self._metrics:
            health_result = self._health({'endpoint': endpoint})
            if health_result.data and not health_result.data.get('healthy'):
                alerts.append({
                    'endpoint': endpoint,
                    'reason': 'Health check failed',
                    'details': health_result.data
                })
        
        return ActionResult(
            success=len(alerts) == 0,
            message=f"{len(alerts)} alerts" if alerts else "No alerts",
            data={'alerts': alerts, 'count': len(alerts)}
        )
    
    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear metrics for an endpoint."""
        endpoint = params.get('endpoint', 'default')
        if endpoint in self._metrics:
            self._metrics[endpoint].clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared metrics for {endpoint}"
        )


class LatencyTrackerAction(BaseAction):
    """Track and analyze API latency patterns."""
    action_type = "latency_tracker"
    display_name = "延迟跟踪"
    description = "API响应延迟跟踪分析"
    
    def __init__(self):
        super().__init__()
        self._requests: deque = deque(maxlen=10000)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute latency tracking operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'track', 'percentiles', 'trends'
                - endpoint: Endpoint name
                - latency_ms: Latency in milliseconds
                - window: Analysis window in seconds
        
        Returns:
            ActionResult with latency analysis.
        """
        operation = params.get('operation', 'track').lower()
        
        if operation == 'track':
            return self._track(params)
        elif operation == 'percentiles':
            return self._percentiles(params)
        elif operation == 'trends':
            return self._trends(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _track(self, params: Dict[str, Any]) -> ActionResult:
        """Track a single request latency."""
        endpoint = params.get('endpoint', 'default')
        latency_ms = params.get('latency_ms', 0)
        
        self._requests.append({
            'timestamp': time.time(),
            'endpoint': endpoint,
            'latency_ms': latency_ms
        })
        
        return ActionResult(
            success=True,
            message=f"Tracked latency {latency_ms}ms"
        )
    
    def _percentiles(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate latency percentiles."""
        endpoint = params.get('endpoint')
        window = params.get('window', 300)
        
        cutoff = time.time() - window
        requests = self._requests
        
        if endpoint:
            requests = [r for r in requests if r['endpoint'] == endpoint]
        
        latencies = sorted([r['latency_ms'] for r in requests if r['timestamp'] >= cutoff])
        
        if not latencies:
            return ActionResult(
                success=True,
                message="No latency data",
                data={}
            )
        
        def percentile(data, p):
            idx = int(len(data) * p)
            return data[min(idx, len(data) - 1)]
        
        return ActionResult(
            success=True,
            message="Latency percentiles calculated",
            data={
                'p50': round(percentile(latencies, 0.50), 2),
                'p75': round(percentile(latencies, 0.75), 2),
                'p90': round(percentile(latencies, 0.90), 2),
                'p95': round(percentile(latencies, 0.95), 2),
                'p99': round(percentile(latencies, 0.99), 2),
                'count': len(latencies)
            }
        )
    
    def _trends(self, params: Dict[str, Any]) -> ActionResult:
        """Analyze latency trends over time."""
        endpoint = params.get('endpoint', 'default')
        bucket_size = params.get('bucket_size', 60)  # 1 minute buckets
        window = params.get('window', 3600)
        
        cutoff = time.time() - window
        requests = [
            r for r in self._requests
            if r['endpoint'] == endpoint and r['timestamp'] >= cutoff
        ]
        
        if not requests:
            return ActionResult(
                success=True,
                message="No trend data",
                data={'buckets': []}
            )
        
        # Group into time buckets
        buckets = defaultdict(list)
        for r in requests:
            bucket_key = int(r['timestamp'] / bucket_size) * bucket_size
            buckets[bucket_key].append(r['latency_ms'])
        
        # Calculate stats per bucket
        result = []
        for ts in sorted(buckets.keys()):
            latencies = buckets[ts]
            result.append({
                'timestamp': datetime.fromtimestamp(ts).isoformat(),
                'latency_mean': round(mean(latencies), 2),
                'latency_max': round(max(latencies), 2),
                'count': len(latencies)
            })
        
        return ActionResult(
            success=True,
            message=f"Trend analysis with {len(result)} buckets",
            data={'buckets': result}
        )
