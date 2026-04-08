"""API Analytics Action Module.

Provides API analytics and reporting capabilities including
usage tracking, performance metrics, and trend analysis.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIUsageTrackerAction(BaseAction):
    """Track API usage statistics.
    
    Supports request counting, error tracking, and per-endpoint analytics.
    """
    action_type = "api_usage_tracker"
    display_name = "API使用追踪"
    description = "追踪API使用统计"

    def __init__(self):
        super().__init__()
        self._request_counts: Dict[str, int] = defaultdict(int)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._response_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._timestamps: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Track API usage.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'track_request', 'track_error', 'track_response_time', 'get_stats'.
                - endpoint: API endpoint path.
                - method: HTTP method.
                - response_time: Response time in ms.
                - status_code: HTTP status code.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with tracking result or error.
        """
        operation = params.get('operation', 'track_request')
        endpoint = params.get('endpoint', '/')
        method = params.get('method', 'GET')
        response_time = params.get('response_time', 0)
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'usage_result')

        try:
            if operation == 'track_request':
                return self._track_request(endpoint, method, output_var)
            elif operation == 'track_error':
                return self._track_error(endpoint, method, output_var)
            elif operation == 'track_response_time':
                return self._track_response_time(endpoint, response_time, output_var)
            elif operation == 'get_stats':
                return self._get_stats(endpoint, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API usage tracker failed: {str(e)}"
            )

    def _track_request(self, endpoint: str, method: str, output_var: str) -> ActionResult:
        """Track an API request."""
        key = f"{method} {endpoint}"
        self._request_counts[key] += 1
        self._timestamps[key].append(time.time())

        result = {
            'key': key,
            'count': self._request_counts[key],
            'tracked': True
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Tracked request: {key} (total: {result['count']})"
        )

    def _track_error(self, endpoint: str, method: str, output_var: str) -> ActionResult:
        """Track an API error."""
        key = f"{method} {endpoint}"
        self._error_counts[key] += 1

        result = {
            'key': key,
            'error_count': self._error_counts[key],
            'tracked': True
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Tracked error: {key} (errors: {result['error_count']})"
        )

    def _track_response_time(
        self, endpoint: str, response_time: float, output_var: str
    ) -> ActionResult:
        """Track API response time."""
        key = f"GET {endpoint}"  # Simplified
        self._response_times[key].append(response_time)

        avg_time = sum(self._response_times[key]) / len(self._response_times[key])

        result = {
            'endpoint': endpoint,
            'response_time': response_time,
            'avg_response_time': avg_time,
            'sample_count': len(self._response_times[key])
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Tracked response time for {endpoint}: {response_time}ms (avg: {avg_time:.2f}ms)"
        )

    def _get_stats(self, endpoint: str, output_var: str) -> ActionResult:
        """Get usage statistics."""
        stats = {}
        for key, count in self._request_counts.items():
            if endpoint in key:
                error_count = self._error_counts.get(key, 0)
                response_times = list(self._response_times.get(key, []))

                stat = {
                    'requests': count,
                    'errors': error_count,
                    'error_rate': error_count / count if count > 0 else 0
                }

                if response_times:
                    stat['avg_response_time'] = sum(response_times) / len(response_times)
                    stat['min_response_time'] = min(response_times)
                    stat['max_response_time'] = max(response_times)

                stats[key] = stat

        total_requests = sum(self._request_counts.values())
        total_errors = sum(self._error_counts.values())

        result = {
            'endpoint': endpoint,
            'endpoints': stats,
            'total_requests': total_requests,
            'total_errors': total_errors,
            'overall_error_rate': total_errors / total_requests if total_requests > 0 else 0
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved stats for endpoint '{endpoint}': {total_requests} requests"
        )


class APIPerformanceAnalyzerAction(BaseAction):
    """Analyze API performance metrics.
    
    Supports latency analysis, throughput calculation, and bottleneck detection.
    """
    action_type = "api_performance_analyzer"
    display_name = "API性能分析"
    description = "分析API性能指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Analyze API performance.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'analyze', 'latency_percentiles', 'throughput'.
                - data: Response time data points.
                - time_window: Analysis time window in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with analysis result or error.
        """
        operation = params.get('operation', 'analyze')
        data = params.get('data', [])
        time_window = params.get('time_window', 3600)
        endpoint = params.get('endpoint', 'all')
        output_var = params.get('output_var', 'perf_result')

        try:
            if operation == 'analyze':
                return self._analyze_performance(data, time_window, output_var)
            elif operation == 'latency_percentiles':
                return self._latency_percentiles(data, output_var)
            elif operation == 'throughput':
                return self._calculate_throughput(data, time_window, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API performance analyzer failed: {str(e)}"
            )

    def _analyze_performance(
        self, data: List, time_window: int, output_var: str
    ) -> ActionResult:
        """Analyze performance data."""
        if not data:
            return ActionResult(
                success=False,
                message="No data to analyze"
            )

        # Calculate statistics
        response_times = [d.get('response_time', 0) for d in data if 'response_time' in d]
        if not response_times:
            return ActionResult(
                success=False,
                message="No response time data found"
            )

        response_times.sort()

        n = len(response_times)
        analysis = {
            'count': n,
            'min': response_times[0],
            'max': response_times[-1],
            'mean': sum(response_times) / n,
            'median': response_times[n // 2],
            'p95': response_times[int(n * 0.95)] if n > 0 else 0,
            'p99': response_times[int(n * 0.99)] if n > 0 else 0,
            'std_dev': self._calculate_std_dev(response_times)
        }

        # Detect anomalies (values > 2 std dev from mean)
        mean = analysis['mean']
        std = analysis['std_dev']
        anomalies = [d for d in data if d.get('response_time', 0) > mean + 2 * std]
        analysis['anomaly_count'] = len(anomalies)
        analysis['anomalies'] = anomalies[:10]  # Top 10

        context.variables[output_var] = analysis
        return ActionResult(
            success=True,
            data=analysis,
            message=f"Performance analysis: mean={analysis['mean']:.2f}ms, p99={analysis['p99']:.2f}ms"
        )

    def _latency_percentiles(self, data: List, output_var: str) -> ActionResult:
        """Calculate latency percentiles."""
        response_times = sorted([d.get('response_time', 0) for d in data if 'response_time' in d])
        n = len(response_times)

        if n == 0:
            return ActionResult(
                success=False,
                message="No response time data"
            )

        percentiles = [50, 75, 90, 95, 99, 99.9]
        result = {}

        for p in percentiles:
            idx = int(n * p / 100)
            if idx >= n:
                idx = n - 1
            result[f'p{p}'] = response_times[idx]

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Latency percentiles calculated: p99={result.get('p99', 0):.2f}ms"
        )

    def _calculate_throughput(
        self, data: List, time_window: int, output_var: str
    ) -> ActionResult:
        """Calculate throughput metrics."""
        if not data:
            return ActionResult(
                success=False,
                message="No data for throughput calculation"
            )

        timestamps = [d.get('timestamp', time.time()) for d in data]
        if len(timestamps) < 2:
            return ActionResult(
                success=False,
                message="Need at least 2 data points for throughput"
            )

        time_span = max(timestamps) - min(timestamps)
        request_count = len(data)

        throughput = {
            'requests_per_second': request_count / time_span if time_span > 0 else 0,
            'requests_per_minute': (request_count / time_span * 60) if time_span > 0 else 0,
            'requests_per_hour': (request_count / time_span * 3600) if time_span > 0 else 0,
            'total_requests': request_count,
            'time_span_seconds': time_span
        }

        context.variables[output_var] = throughput
        return ActionResult(
            success=True,
            data=throughput,
            message=f"Throughput: {throughput['requests_per_second']:.2f} req/s"
        )

    def _calculate_std_dev(self, values: List) -> float:
        """Calculate standard deviation."""
        if not values:
            return 0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return variance ** 0.5


class APITrendAnalyzerAction(BaseAction):
    """Analyze trends in API usage and performance.
    
    Supports time-series trend detection and forecasting.
    """
    action_type = "api_trend_analyzer"
    display_name = "API趋势分析"
    description = "分析API使用和性能趋势"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Analyze API trends.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'detect_trend', 'forecast', 'compare_periods'.
                - data: Historical data points.
                - metric: Metric to analyze.
                - period: Time period for analysis.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with trend analysis or error.
        """
        operation = params.get('operation', 'detect_trend')
        data = params.get('data', [])
        metric = params.get('metric', 'requests')
        period = params.get('period', 'hourly')
        output_var = params.get('output_var', 'trend_result')

        try:
            if operation == 'detect_trend':
                return self._detect_trend(data, metric, output_var)
            elif operation == 'forecast':
                return self._forecast(data, metric, params.get('horizon', 5), output_var)
            elif operation == 'compare_periods':
                return self._compare_periods(data, metric, period, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API trend analyzer failed: {str(e)}"
            )

    def _detect_trend(self, data: List, metric: str, output_var: str) -> ActionResult:
        """Detect trend in data."""
        values = [d.get(metric, 0) for d in data]

        if len(values) < 2:
            return ActionResult(
                success=False,
                message="Need at least 2 data points"
            )

        # Simple linear trend detection
        n = len(values)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(values) / n

        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        slope = numerator / denominator if denominator != 0 else 0

        # Determine trend direction
        if abs(slope) < 0.1:
            direction = 'stable'
        elif slope > 0:
            direction = 'increasing'
        else:
            direction = 'decreasing'

        # Calculate R-squared
        ss_res = sum((values[i] - (y_mean + slope * (x[i] - x_mean))) ** 2 for i in range(n))
        ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        result = {
            'metric': metric,
            'direction': direction,
            'slope': slope,
            'r_squared': r_squared,
            'trend_strength': 'strong' if r_squared > 0.7 else ('moderate' if r_squared > 0.4 else 'weak')
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Trend detected: {direction} (slope={slope:.4f}, R²={r_squared:.3f})"
        )

    def _forecast(
        self, data: List, metric: str, horizon: int, output_var: str
    ) -> ActionResult:
        """Forecast future values."""
        values = [d.get(metric, 0) for d in data]

        if len(values) < 2:
            return ActionResult(
                success=False,
                message="Need at least 2 data points for forecasting"
            )

        # Simple linear regression forecast
        n = len(values)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(values) / n

        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        slope = numerator / denominator if denominator != 0 else 0
        intercept = y_mean - slope * x_mean

        # Forecast future values
        forecast = []
        for i in range(n, n + horizon):
            forecasted_value = slope * i + intercept
            forecast.append({
                'period': i,
                'forecasted_value': max(0, forecasted_value)
            })

        result = {
            'metric': metric,
            'forecast': forecast,
            'slope': slope,
            'intercept': intercept
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Forecast generated: {horizon} periods ahead"
        )

    def _compare_periods(
        self, data: List, metric: str, period: str, output_var: str
    ) -> ActionResult:
        """Compare metrics between time periods."""
        if len(data) < 2:
            return ActionResult(
                success=False,
                message="Need at least 2 data points"
            )

        # Split data into two halves
        mid = len(data) // 2
        first_half = data[:mid]
        second_half = data[mid:]

        first_values = [d.get(metric, 0) for d in first_half]
        second_values = [d.get(metric, 0) for d in second_half]

        first_avg = sum(first_values) / len(first_values) if first_values else 0
        second_avg = sum(second_values) / len(second_values) if second_values else 0

        change = second_avg - first_avg
        change_pct = (change / first_avg * 100) if first_avg != 0 else 0

        result = {
            'metric': metric,
            'first_period_avg': first_avg,
            'second_period_avg': second_avg,
            'change': change,
            'change_percent': change_pct,
            'trend': 'up' if change > 0 else ('down' if change < 0 else 'unchanged')
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Period comparison: {first_avg:.2f} -> {second_avg:.2f} ({change_pct:+.1f}%)"
        )
