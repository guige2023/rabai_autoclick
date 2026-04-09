"""API Profiler Action Module.

Provides performance profiling and analysis for API operations.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIProfilerAction(BaseAction):
    """Profile API request performance.
    
    Tracks timing, throughput, and identifies performance bottlenecks.
    """
    action_type = "api_profiler"
    display_name = "API性能分析"
    description = "追踪API性能指标和瓶颈"
    
    def __init__(self):
        super().__init__()
        self._request_times = defaultdict(list)
        self._request_counts = defaultdict(int)
        self._error_counts = defaultdict(int)
        self._start_times = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute profiling operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, endpoint, tags.
        
        Returns:
            ActionResult with profiling data.
        """
        action = params.get('action', 'start')
        endpoint = params.get('endpoint', 'default')
        tags = params.get('tags', {})
        
        if action == 'start':
            return self._start_profiling(endpoint, tags)
        elif action == 'stop':
            return self._stop_profiling(endpoint, tags)
        elif action == 'record':
            return self._record_request(endpoint, params)
        elif action == 'report':
            return self._generate_report(endpoint)
        elif action == 'reset':
            return self._reset_profiling(endpoint)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _start_profiling(self, endpoint: str, tags: Dict) -> ActionResult:
        """Start profiling an endpoint."""
        self._start_times[endpoint] = {
            'start_time': time.time(),
            'tags': tags
        }
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'status': 'profiling_started',
                'tags': tags
            },
            error=None
        )
    
    def _stop_profiling(self, endpoint: str, tags: Dict) -> ActionResult:
        """Stop profiling an endpoint."""
        if endpoint not in self._start_times:
            return ActionResult(
                success=False,
                data=None,
                error=f"No profiling session found for {endpoint}"
            )
        
        start_info = self._start_times[endpoint]
        duration = time.time() - start_info['start_time']
        
        del self._start_times[endpoint]
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'duration': duration,
                'request_count': self._request_counts[endpoint]
            },
            error=None
        )
    
    def _record_request(self, endpoint: str, params: Dict) -> ActionResult:
        """Record a request's performance."""
        duration = params.get('duration', 0)
        status_code = params.get('status_code', 200)
        error = params.get('error', None)
        
        self._request_times[endpoint].append(duration)
        self._request_counts[endpoint] += 1
        
        if status_code >= 400 or error:
            self._error_counts[endpoint] += 1
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'duration': duration,
                'total_requests': self._request_counts[endpoint]
            },
            error=None
        )
    
    def _generate_report(self, endpoint: str) -> ActionResult:
        """Generate profiling report for endpoint."""
        if not self._request_times[endpoint]:
            return ActionResult(
                success=False,
                data=None,
                error=f"No profiling data for {endpoint}"
            )
        
        times = self._request_times[endpoint]
        total_requests = self._request_counts[endpoint]
        error_count = self._error_counts[endpoint]
        
        sorted_times = sorted(times)
        count = len(sorted_times)
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'total_requests': total_requests,
                'error_count': error_count,
                'error_rate': error_count / total_requests if total_requests > 0 else 0,
                'min_duration': min(times),
                'max_duration': max(times),
                'avg_duration': sum(times) / count,
                'p50': sorted_times[count // 2],
                'p90': sorted_times[int(count * 0.9)],
                'p95': sorted_times[int(count * 0.95)],
                'p99': sorted_times[int(count * 0.99)]
            },
            error=None
        )
    
    def _reset_profiling(self, endpoint: str) -> ActionResult:
        """Reset profiling data for endpoint."""
        if endpoint in self._request_times:
            del self._request_times[endpoint]
        if endpoint in self._request_counts:
            del self._request_counts[endpoint]
        if endpoint in self._error_counts:
            del self._error_counts[endpoint]
        if endpoint in self._start_times:
            del self._start_times[endpoint]
        
        return ActionResult(
            success=True,
            data={'endpoint': endpoint, 'status': 'reset'},
            error=None
        )


class APIAnalyzerAction(BaseAction):
    """Analyze API performance patterns.
    
    Identifies trends, anomalies, and optimization opportunities.
    """
    action_type = "api_analyzer"
    display_name = "API性能分析器"
    description = "分析API性能模式和优化建议"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API analysis.
        
        Args:
            context: Execution context.
            params: Dict with keys: data_points, time_window, threshold.
        
        Returns:
            ActionResult with analysis results.
        """
        data_points = params.get('data_points', [])
        time_window = params.get('time_window', 300)
        threshold = params.get('threshold', 1.5)
        
        if not data_points:
            return ActionResult(
                success=False,
                data=None,
                error="No data points provided"
            )
        
        # Calculate statistics
        values = [p.get('value', 0) for p in data_points]
        mean = sum(values) / len(values) if values else 0
        variance = sum((v - mean) ** 2 for v in values) / len(values) if values else 0
        stddev = variance ** 0.5
        
        # Find anomalies
        anomalies = []
        for i, point in enumerate(data_points):
            value = point.get('value', 0)
            if abs(value - mean) > threshold * stddev:
                anomalies.append({
                    'index': i,
                    'value': value,
                    'deviation': abs(value - mean) / stddev if stddev > 0 else 0,
                    'timestamp': point.get('timestamp', 0)
                })
        
        # Generate recommendations
        recommendations = self._generate_recommendations(values, mean, stddev, anomalies)
        
        return ActionResult(
            success=True,
            data={
                'mean': mean,
                'stddev': stddev,
                'anomaly_count': len(anomalies),
                'anomalies': anomalies,
                'recommendations': recommendations
            },
            error=None
        )
    
    def _generate_recommendations(
        self,
        values: List[float],
        mean: float,
        stddev: float,
        anomalies: List[Dict]
    ) -> List[str]:
        """Generate performance recommendations."""
        recommendations = []
        
        if stddev / mean > 0.5 if mean > 0 else False:
            recommendations.append("High variance detected - consider implementing caching")
        
        if len(anomalies) > len(values) * 0.1:
            recommendations.append("Too many anomalies - investigate underlying issues")
        
        if max(values) > mean * 3:
            recommendations.append("Extreme outliers detected - check for bottlenecks")
        
        if not recommendations:
            recommendations.append("Performance is within normal parameters")
        
        return recommendations


class APIThroughputTrackerAction(BaseAction):
    """Track API throughput over time.
    
    Monitors requests per second and identifies throughput trends.
    """
    action_type = "api_throughput_tracker"
    display_name = "API吞吐量追踪"
    description = "追踪API请求吞吐量趋势"
    
    def __init__(self):
        super().__init__()
        self._throughput_data = defaultdict(list)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute throughput tracking.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoint, interval, window_size.
        
        Returns:
            ActionResult with throughput metrics.
        """
        endpoint = params.get('endpoint', 'default')
        interval = params.get('interval', 60)
        window_size = params.get('window_size', 300)
        
        current_time = time.time()
        cutoff_time = current_time - window_size
        
        # Filter data within window
        relevant_data = [
            (t, count) for t, count in self._throughput_data[endpoint]
            if t >= cutoff_time
        ]
        
        if not relevant_data:
            return ActionResult(
                success=True,
                data={
                    'endpoint': endpoint,
                    'current_rps': 0,
                    'avg_rps': 0,
                    'peak_rps': 0,
                    'total_requests': 0
                },
                error=None
            )
        
        total_requests = sum(count for _, count in relevant_data)
        avg_rps = total_requests / window_size if window_size > 0 else 0
        peak_rps = max(count / interval for _, count in relevant_data)
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'current_rps': relevant_data[-1][1] / interval if relevant_data else 0,
                'avg_rps': avg_rps,
                'peak_rps': peak_rps,
                'total_requests': total_requests,
                'data_points': len(relevant_data)
            },
            error=None
        )
    
    def record_requests(self, endpoint: str, count: int):
        """Record request count for endpoint."""
        self._throughput_data[endpoint].append((time.time(), count))


def register_actions():
    """Register all API Profiler actions."""
    return [
        APIProfilerAction,
        APIAnalyzerAction,
        APIThroughputTrackerAction,
    ]
