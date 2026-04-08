"""API Metrics Action.

Collects and aggregates API metrics including latency, throughput,
error rates, and status code distributions.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiMetricsAction(BaseAction):
    """Collect and aggregate API metrics.
    
    Tracks latency, throughput, error rates, status codes,
    and generates metric summaries.
    """
    action_type = "api_metrics"
    display_name = "API指标收集"
    description = "收集聚合API指标，包括延迟、吞吐、错误率"

    def __init__(self):
        super().__init__()
        self._requests: deque = deque(maxlen=10000)
        self._status_codes: Dict[int, int] = defaultdict(int)
        self._endpoints: Dict[str, Dict] = defaultdict(lambda: {
            'count': 0, 'total_latency': 0, 'errors': 0, 'latencies': deque(maxlen=1000)
        })

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Record or retrieve API metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'record', 'get', 'reset', 'summary'.
                - endpoint: API endpoint path.
                - latency_ms: Request latency in milliseconds.
                - status_code: HTTP status code.
                - method: HTTP method.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with metrics data.
        """
        try:
            action = params.get('action', 'record')
            save_to_var = params.get('save_to_var', 'metrics_result')

            if action == 'record':
                return self._record_request(params, save_to_var)
            elif action == 'get':
                return self._get_metrics(params, save_to_var)
            elif action == 'reset':
                return self._reset_metrics(save_to_var)
            elif action == 'summary':
                return self._get_summary(save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics error: {e}")

    def _record_request(self, params: Dict, save_to_var: str) -> ActionResult:
        """Record an API request."""
        endpoint = params.get('endpoint', 'unknown')
        latency_ms = params.get('latency_ms', 0)
        status_code = params.get('status_code', 200)
        method = params.get('method', 'GET')

        # Record in request history
        self._requests.append({
            'endpoint': endpoint,
            'method': method,
            'status_code': status_code,
            'latency_ms': latency_ms,
            'timestamp': time.time()
        })

        # Update status code counts
        self._status_codes[status_code] += 1

        # Update endpoint metrics
        ep = self._endpoints[endpoint]
        ep['count'] += 1
        ep['total_latency'] += latency_ms
        ep['latencies'].append(latency_ms)
        if status_code >= 400:
            ep['errors'] += 1

        context.set_variable(save_to_var, {'recorded': True, 'endpoint': endpoint})
        return ActionResult(success=True, message=f"Recorded {method} {endpoint}")

    def _get_metrics(self, params: Dict, save_to_var: str) -> ActionResult:
        """Get metrics for specific endpoint."""
        endpoint = params.get('endpoint')
        time_window = params.get('time_window', 60)

        cutoff = time.time() - time_window
        recent = [r for r in self._requests if r['timestamp'] >= cutoff]

        if endpoint:
            recent = [r for r in recent if r['endpoint'] == endpoint]

        result = {
            'requests': len(recent),
            'time_window': time_window,
            'requests_per_second': len(recent) / time_window if time_window > 0 else 0
        }

        if recent:
            latencies = [r['latency_ms'] for r in recent]
            result['avg_latency_ms'] = sum(latencies) / len(latencies)
            result['min_latency_ms'] = min(latencies)
            result['max_latency_ms'] = max(latencies)

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result)

    def _get_summary(self, save_to_var: str) -> ActionResult:
        """Get full metrics summary."""
        total_requests = len(self._requests)
        
        if total_requests == 0:
            return ActionResult(success=True, data={'message': 'No metrics recorded'}, message='No metrics')

        # Calculate overall stats
        latencies = [r['latency_ms'] for r in self._requests]
        error_count = sum(1 for r in self._requests if r['status_code'] >= 400)
        
        # Status code distribution
        status_dist = dict(self._status_codes)
        
        # Endpoint breakdown
        endpoints = {}
        for ep, metrics in self._endpoints.items():
            endpoints[ep] = {
                'count': metrics['count'],
                'avg_latency_ms': metrics['total_latency'] / metrics['count'] if metrics['count'] > 0 else 0,
                'error_rate': metrics['errors'] / metrics['count'] if metrics['count'] > 0 else 0
            }

        result = {
            'total_requests': total_requests,
            'error_count': error_count,
            'error_rate': error_count / total_requests,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'min_latency_ms': min(latencies),
            'max_latency_ms': max(latencies),
            'status_codes': status_dist,
            'endpoints': endpoints
        }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, message=f"Summary: {total_requests} requests")

    def _reset_metrics(self, save_to_var: str) -> ActionResult:
        """Reset all metrics."""
        self._requests.clear()
        self._status_codes.clear()
        self._endpoints.clear()

        context.set_variable(save_to_var, {'reset': True})
        return ActionResult(success=True, message="Metrics reset")
