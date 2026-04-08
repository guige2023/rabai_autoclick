"""API monitor action module for RabAI AutoClick.

Provides API monitoring with health checks, latency tracking,
error rate monitoring, and alerting capabilities.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from collections import deque
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricSample:
    """A single metric sample."""
    timestamp: float
    latency_ms: float
    status_code: int
    success: bool
    error: Optional[str] = None


@dataclass
class ApiEndpoint:
    """API endpoint definition."""
    name: str
    url: str
    method: str
    expected_status: int = 200
    timeout: float = 5.0
    headers: Dict[str, str] = field(default_factory=dict)


class ApiMonitorAction(BaseAction):
    """API monitor action with health checks and alerting.
    
    Monitors API endpoints with configurable health checks,
    latency thresholds, error rate tracking, and alerting.
    """
    action_type = "api_monitor"
    display_name = "API监控"
    description = "API健康检查与监控"
    
    def __init__(self):
        super().__init__()
        self._endpoints: Dict[str, ApiEndpoint] = {}
        self._metrics: Dict[str, deque] = {}
        self._max_history = 1000
        self._lock = threading.RLock()
        self._alerts: List[Dict[str, Any]] = []
    
    def add_endpoint(self, endpoint: ApiEndpoint) -> None:
        """Add an endpoint to monitor."""
        with self._lock:
            self._endpoints[endpoint.name] = endpoint
            if endpoint.name not in self._metrics:
                self._metrics[endpoint.name] = deque(maxlen=self._max_history)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API monitoring operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: check|add|remove|status|metrics|alert
                endpoint: Endpoint name (for check/status)
                latency_threshold: Alert threshold in ms
                error_rate_threshold: Alert threshold for error rate.
        
        Returns:
            ActionResult with monitoring result.
        """
        operation = params.get('operation', 'status')
        
        if operation == 'check':
            return self._check_endpoint(params)
        elif operation == 'add':
            return self._add_endpoint(params)
        elif operation == 'remove':
            return self._remove_endpoint(params)
        elif operation == 'status':
            return self._status(params)
        elif operation == 'metrics':
            return self._metrics(params)
        elif operation == 'alert':
            return self._check_alerts(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _add_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Add an endpoint to monitor."""
        endpoint = ApiEndpoint(
            name=params['name'],
            url=params['url'],
            method=params.get('method', 'GET'),
            expected_status=params.get('expected_status', 200),
            timeout=params.get('timeout', 5.0),
            headers=params.get('headers', {})
        )
        
        self.add_endpoint(endpoint)
        
        return ActionResult(
            success=True,
            message=f"Added endpoint {endpoint.name}",
            data={'name': endpoint.name, 'url': endpoint.url}
        )
    
    def _remove_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Remove an endpoint from monitoring."""
        name = params.get('endpoint')
        
        with self._lock:
            if name in self._endpoints:
                del self._endpoints[name]
                if name in self._metrics:
                    del self._metrics[name]
                return ActionResult(success=True, message=f"Removed endpoint {name}")
            
            return ActionResult(success=False, message=f"Endpoint {name} not found")
    
    def _check_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Check health of an endpoint."""
        name = params.get('endpoint')
        
        if not name:
            return ActionResult(success=False, message="Endpoint name required")
        
        with self._lock:
            endpoint = self._endpoints.get(name)
        
        if not endpoint:
            return ActionResult(success=False, message=f"Endpoint {name} not found")
        
        start = time.time()
        sample = self._make_request(endpoint)
        latency_ms = (time.time() - start) * 1000
        
        sample.latency_ms = latency_ms
        
        with self._lock:
            if name in self._metrics:
                self._metrics[name].append(sample)
        
        status = "healthy" if sample.success else "unhealthy"
        
        return ActionResult(
            success=sample.success,
            message=f"Endpoint {name}: {status} ({latency_ms:.1f}ms)",
            data={
                'endpoint': name,
                'healthy': sample.success,
                'latency_ms': round(latency_ms, 2),
                'status_code': sample.status_code,
                'error': sample.error
            }
        )
    
    def _make_request(self, endpoint: ApiEndpoint) -> MetricSample:
        """Make request to endpoint and return metric sample."""
        sample = MetricSample(
            timestamp=time.time(),
            latency_ms=0,
            status_code=0,
            success=False
        )
        
        try:
            req = Request(endpoint.url, method=endpoint.method, headers=endpoint.headers)
            with urlopen(req, timeout=endpoint.timeout) as response:
                sample.status_code = response.status
                sample.success = response.status == endpoint.expected_status
        except HTTPError as e:
            sample.status_code = e.code
            sample.success = False
            sample.error = f"HTTP {e.code}: {e.reason}"
        except URLError as e:
            sample.status_code = 0
            sample.success = False
            sample.error = str(e.reason)
        except Exception as e:
            sample.status_code = 0
            sample.success = False
            sample.error = str(e)
        
        return sample
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get status of all or specific endpoint."""
        name = params.get('endpoint')
        
        with self._lock:
            if name:
                if name not in self._endpoints:
                    return ActionResult(success=False, message=f"Endpoint {name} not found")
                
                endpoints = {name: self._endpoints[name]}
            else:
                endpoints = dict(self._endpoints)
        
        statuses = []
        
        for endpoint_name, endpoint in endpoints.items():
            with self._lock:
                metrics = self._metrics.get(endpoint_name, deque())
            
            if not metrics:
                statuses.append({
                    'name': endpoint_name,
                    'url': endpoint.url,
                    'status': 'unknown',
                    'last_check': None
                })
                continue
            
            latest = metrics[-1]
            
            if len(metrics) >= 5:
                recent_success = sum(1 for m in list(metrics)[-5:] if m.success)
                if recent_success == 0:
                    status = 'down'
                elif recent_success < 3:
                    status = 'degraded'
                else:
                    status = 'healthy'
            else:
                status = 'healthy' if latest.success else 'down'
            
            avg_latency = sum(m.latency_ms for m in list(metrics)[-10:]) / min(len(metrics), 10)
            
            statuses.append({
                'name': endpoint_name,
                'url': endpoint.url,
                'status': status,
                'last_check': latest.timestamp,
                'last_latency_ms': round(latest.latency_ms, 2),
                'avg_latency_ms': round(avg_latency, 2),
                'error_rate': round((len(metrics) - sum(1 for m in metrics if m.success)) / len(metrics) * 100, 1)
            })
        
        return ActionResult(
            success=True,
            message=f"Status for {len(statuses)} endpoints",
            data={'endpoints': statuses}
        )
    
    def _metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Get detailed metrics for endpoint."""
        name = params.get('endpoint')
        window = params.get('window', 100)
        
        if not name:
            return ActionResult(success=False, message="Endpoint name required")
        
        with self._lock:
            metrics = list(self._metrics.get(name, []))[-window:]
        
        if not metrics:
            return ActionResult(success=True, message=f"No metrics for {name}", data={'metrics': []})
        
        total = len(metrics)
        successful = sum(1 for m in metrics if m.success)
        failed = total - successful
        
        latencies = [m.latency_ms for m in metrics]
        
        return ActionResult(
            success=True,
            message=f"Metrics for {name}",
            data={
                'endpoint': name,
                'total_requests': total,
                'successful': successful,
                'failed': failed,
                'error_rate': round(failed / total * 100, 2) if total > 0 else 0,
                'min_latency_ms': round(min(latencies), 2) if latencies else 0,
                'max_latency_ms': round(max(latencies), 2) if latencies else 0,
                'avg_latency_ms': round(sum(latencies) / len(latencies), 2) if latencies else 0,
                'metrics': [
                    {
                        'timestamp': m.timestamp,
                        'latency_ms': round(m.latency_ms, 2),
                        'status_code': m.status_code,
                        'success': m.success,
                        'error': m.error
                    }
                    for m in metrics
                ]
            }
        )
    
    def _check_alerts(self, params: Dict[str, Any]) -> ActionResult:
        """Check for alert conditions."""
        latency_threshold = params.get('latency_threshold', 1000)
        error_rate_threshold = params.get('error_rate_threshold', 10)
        window = params.get('window', 50)
        
        alerts = []
        
        with self._lock:
            for name, metrics_deque in self._metrics.items():
                metrics = list(metrics_deque)[-window:]
                
                if len(metrics) < 5:
                    continue
                
                avg_latency = sum(m.latency_ms for m in metrics) / len(metrics)
                error_count = sum(1 for m in metrics if not m.success)
                error_rate = error_count / len(metrics) * 100
                
                if avg_latency > latency_threshold:
                    alerts.append({
                        'endpoint': name,
                        'type': 'latency',
                        'message': f"Avg latency {avg_latency:.1f}ms exceeds threshold {latency_threshold}ms",
                        'severity': 'warning' if avg_latency < latency_threshold * 2 else 'critical'
                    })
                
                if error_rate > error_rate_threshold:
                    alerts.append({
                        'endpoint': name,
                        'type': 'error_rate',
                        'message': f"Error rate {error_rate:.1f}% exceeds threshold {error_rate_threshold}%",
                        'severity': 'warning' if error_rate < error_rate_threshold * 2 else 'critical'
                    })
        
        self._alerts.extend(alerts)
        
        return ActionResult(
            success=True,
            message=f"{len(alerts)} alerts triggered",
            data={'alerts': alerts, 'total_alerts': len(self._alerts)}
        )
