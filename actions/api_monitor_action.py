"""API Monitor action module for RabAI AutoClick.

Provides API monitoring operations:
- APIHealthCheckAction: Check API health
- APIMetricsAction: Collect API metrics
- APIAlertAction: Trigger API alerts
- APIDashboardAction: Generate API dashboard
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIHealthCheckAction(BaseAction):
    """Check API health."""
    action_type = "api_health_check"
    display_name = "API健康检查"
    description = "API健康状态检查"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute health check."""
        url = params.get('url', '')
        timeout = params.get('timeout', 10)
        expected_codes = params.get('expected_codes', [200])
        output_var = params.get('output_var', 'health_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests

            resolved_url = context.resolve_value(url) if context else url

            start_time = time.time()
            response = requests.get(resolved_url, timeout=timeout)
            latency = (time.time() - start_time) * 1000

            is_healthy = response.status_code in expected_codes

            result = {
                'healthy': is_healthy,
                'url': resolved_url,
                'status_code': response.status_code,
                'latency_ms': round(latency, 2),
                'timestamp': time.time(),
            }

            return ActionResult(
                success=is_healthy,
                data={output_var: result},
                message=f"Health {'OK' if is_healthy else 'FAIL'}: {response.status_code} ({latency:.0f}ms)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                data={output_var: {'healthy': False, 'error': str(e)}},
                message=f"Health check failed: {e}"
            )


class APIMetricsAction(BaseAction):
    """Collect API metrics."""
    action_type = "api_metrics"
    display_name = "API指标"
    description = "收集API指标"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._metrics = defaultdict(lambda: {'count': 0, 'total_latency': 0, 'errors': 0, 'latencies': deque(maxlen=100)})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute metrics collection."""
        endpoint = params.get('endpoint', 'default')
        latency = params.get('latency', 0)
        status_code = params.get('status_code', 200)
        increment = params.get('increment', True)
        output_var = params.get('output_var', 'metrics_result')

        try:
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_latency = context.resolve_value(latency) if context else latency
            resolved_status = context.resolve_value(status_code) if context else status_code

            metrics = self._metrics[resolved_endpoint]

            if increment:
                metrics['count'] += 1
                metrics['total_latency'] += resolved_latency
                metrics['latencies'].append(resolved_latency)
                if resolved_status >= 400:
                    metrics['errors'] += 1

            avg_latency = metrics['total_latency'] / metrics['count'] if metrics['count'] > 0 else 0
            recent_latencies = list(metrics['latencies'])
            p50 = sorted(recent_latencies)[len(recent_latencies) // 2] if recent_latencies else 0
            p95 = sorted(recent_latencies)[int(len(recent_latencies) * 0.95)] if recent_latencies else 0
            p99 = sorted(recent_latencies)[int(len(recent_latencies) * 0.99)] if recent_latencies else 0

            result = {
                'endpoint': resolved_endpoint,
                'request_count': metrics['count'],
                'error_count': metrics['errors'],
                'error_rate': metrics['errors'] / metrics['count'] if metrics['count'] > 0 else 0,
                'avg_latency_ms': round(avg_latency, 2),
                'p50_latency_ms': round(p50, 2),
                'p95_latency_ms': round(p95, 2),
                'p99_latency_ms': round(p99, 2),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Metrics for {resolved_endpoint}: {metrics['count']} requests, {result['error_rate']:.1%} errors"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API metrics error: {e}")


class APIAlertAction(BaseAction):
    """Trigger API alerts."""
    action_type = "api_alert"
    display_name = "API告警"
    description = "触发API告警"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute alert trigger."""
        metric = params.get('metric', '')
        threshold = params.get('threshold', 0)
        current_value = params.get('current_value', 0)
        operator = params.get('operator', 'gt')
        message = params.get('message', '')
        severity = params.get('severity', 'warning')
        output_var = params.get('output_var', 'alert_result')

        try:
            resolved_metric = context.resolve_value(metric) if context else metric
            resolved_threshold = context.resolve_value(threshold) if context else threshold
            resolved_value = context.resolve_value(current_value) if context else current_value
            resolved_message = context.resolve_value(message) if context else message

            triggered = False
            if operator == 'gt':
                triggered = resolved_value > resolved_threshold
            elif operator == 'lt':
                triggered = resolved_value < resolved_threshold
            elif operator == 'eq':
                triggered = resolved_value == resolved_threshold
            elif operator == 'gte':
                triggered = resolved_value >= resolved_threshold
            elif operator == 'lte':
                triggered = resolved_value <= resolved_threshold

            result = {
                'triggered': triggered,
                'metric': resolved_metric,
                'threshold': resolved_threshold,
                'current_value': resolved_value,
                'operator': operator,
                'severity': severity,
                'message': resolved_message,
                'timestamp': time.time(),
            }

            return ActionResult(
                success=not triggered,
                data={output_var: result},
                message=f"Alert {'TRIGGERED' if triggered else 'OK'}: {resolved_metric} {resolved_value} {operator} {resolved_threshold}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API alert error: {e}")


class APIDashboardAction(BaseAction):
    """Generate API dashboard."""
    action_type = "api_dashboard"
    display_name = "API仪表盘"
    description = "生成API仪表盘"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dashboard generation."""
        metrics_data = params.get('metrics', {})
        time_range = params.get('time_range', '1h')
        output_var = params.get('output_var', 'dashboard')

        try:
            resolved_metrics = context.resolve_value(metrics_data) if context else metrics_data

            dashboard = {
                'title': 'API Dashboard',
                'time_range': time_range,
                'generated_at': time.time(),
                'summary': {
                    'total_requests': 0,
                    'total_errors': 0,
                    'avg_latency': 0,
                },
                'endpoints': [],
            }

            if isinstance(resolved_metrics, dict):
                for endpoint, metrics in resolved_metrics.items():
                    dashboard['endpoints'].append({
                        'endpoint': endpoint,
                        'requests': metrics.get('count', 0),
                        'errors': metrics.get('errors', 0),
                        'latency': metrics.get('avg_latency', 0),
                    })
                    dashboard['summary']['total_requests'] += metrics.get('count', 0)
                    dashboard['summary']['total_errors'] += metrics.get('errors', 0)

            if dashboard['summary']['total_requests'] > 0:
                dashboard['summary']['error_rate'] = dashboard['summary']['total_errors'] / dashboard['summary']['total_requests']
            else:
                dashboard['summary']['error_rate'] = 0

            return ActionResult(
                success=True,
                data={output_var: dashboard},
                message=f"Dashboard generated: {dashboard['summary']['total_requests']} total requests"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API dashboard error: {e}")
