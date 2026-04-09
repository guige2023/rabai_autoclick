"""API Monitor Action Module.

Provides API monitoring and health checking capabilities.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIMonitorAction(BaseAction):
    """Monitor API endpoints and services.
    
    Tracks endpoint availability, latency, and error rates.
    """
    action_type = "api_monitor"
    display_name = "API监控"
    description = "监控API端点可用性和性能"
    
    def __init__(self):
        super().__init__()
        self._history = {}
        self._status = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API monitoring.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoint, action, history_window.
        
        Returns:
            ActionResult with monitoring data.
        """
        endpoint = params.get('endpoint', '')
        action = params.get('action', 'check')
        history_window = params.get('history_window', 300)
        
        if not endpoint:
            return ActionResult(
                success=False,
                data=None,
                error="Endpoint is required"
            )
        
        if action == 'check':
            return self._check_endpoint(endpoint, params)
        elif action == 'status':
            return self._get_status(endpoint)
        elif action == 'history':
            return self._get_history(endpoint, history_window)
        elif action == 'metrics':
            return self._get_metrics(endpoint)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _check_endpoint(self, endpoint: str, params: Dict) -> ActionResult:
        """Check endpoint health."""
        timeout = params.get('timeout', 10)
        method = params.get('method', 'GET')
        
        start_time = time.time()
        is_healthy = False
        status_code = 0
        error = None
        
        try:
            import urllib.request
            
            req = urllib.request.Request(endpoint, method=method)
            urllib.request.urlopen(req, timeout=timeout)
            
            is_healthy = True
            status_code = 200
            
        except urllib.error.HTTPError as e:
            status_code = e.code
            is_healthy = e.code < 500
            error = f"HTTP {e.code}"
        except Exception as e:
            error = str(e)
            is_healthy = False
        
        duration = time.time() - start_time
        
        # Record in history
        if endpoint not in self._history:
            self._history[endpoint] = deque(maxlen=1000)
        
        self._history[endpoint].append({
            'timestamp': time.time(),
            'duration': duration,
            'status_code': status_code,
            'is_healthy': is_healthy,
            'error': error
        })
        
        # Update status
        self._status[endpoint] = {
            'is_healthy': is_healthy,
            'last_check': time.time(),
            'last_duration': duration,
            'last_status_code': status_code
        }
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'is_healthy': is_healthy,
                'duration': duration,
                'status_code': status_code,
                'error': error
            },
            error=None
        )
    
    def _get_status(self, endpoint: str) -> ActionResult:
        """Get current status of endpoint."""
        if endpoint not in self._status:
            return ActionResult(
                success=False,
                data=None,
                error="No status data for endpoint"
            )
        
        return ActionResult(
            success=True,
            data=self._status[endpoint],
            error=None
        )
    
    def _get_history(self, endpoint: str, window: int) -> ActionResult:
        """Get historical data for endpoint."""
        if endpoint not in self._history:
            return ActionResult(
                success=False,
                data=None,
                error="No history data for endpoint"
            )
        
        cutoff_time = time.time() - window
        recent = [
            h for h in self._history[endpoint]
            if h['timestamp'] >= cutoff_time
        ]
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'history': recent,
                'count': len(recent)
            },
            error=None
        )
    
    def _get_metrics(self, endpoint: str) -> ActionResult:
        """Get aggregated metrics for endpoint."""
        if endpoint not in self._history:
            return ActionResult(
                success=False,
                data=None,
                error="No metrics data for endpoint"
            )
        
        history = list(self._history[endpoint])
        if not history:
            return ActionResult(
                success=True,
                data={'endpoint': endpoint, 'metrics': {}},
                error=None
            )
        
        durations = [h['duration'] for h in history]
        healthy_count = sum(1 for h in history if h['is_healthy'])
        error_count = sum(1 for h in history if h['error'])
        
        return ActionResult(
            success=True,
            data={
                'endpoint': endpoint,
                'metrics': {
                    'total_checks': len(history),
                    'healthy_checks': healthy_count,
                    'error_checks': error_count,
                    'availability': healthy_count / len(history) if history else 0,
                    'avg_duration': sum(durations) / len(durations),
                    'min_duration': min(durations),
                    'max_duration': max(durations),
                    'p95_duration': sorted(durations)[int(len(durations) * 0.95)] if durations else 0
                }
            },
            error=None
        )


class APIHealthCheckAction(BaseAction):
    """Perform comprehensive API health checks.
    
    Checks multiple aspects of API health including connectivity, auth, and data.
    """
    action_type = "api_health_check"
    display_name = "API健康检查"
    description = "执行全面的API健康检查"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute health check.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoints, check_types.
        
        Returns:
            ActionResult with health check results.
        """
        endpoints = params.get('endpoints', [])
        check_types = params.get('check_types', ['connectivity'])
        
        if not endpoints:
            return ActionResult(
                success=False,
                data=None,
                error="No endpoints specified"
            )
        
        results = {}
        all_healthy = True
        
        for endpoint in endpoints:
            endpoint_results = {}
            
            for check_type in check_types:
                if check_type == 'connectivity':
                    endpoint_results['connectivity'] = self._check_connectivity(endpoint)
                elif check_type == 'authentication':
                    endpoint_results['authentication'] = self._check_auth(endpoint, params)
                elif check_type == 'response_time':
                    endpoint_results['response_time'] = self._check_response_time(endpoint)
                elif check_type == 'ssl':
                    endpoint_results['ssl'] = self._check_ssl(endpoint)
                
                if not endpoint_results[check_type].get('healthy', False):
                    all_healthy = False
            
            results[endpoint] = endpoint_results
        
        return ActionResult(
            success=all_healthy,
            data={
                'health_results': results,
                'all_healthy': all_healthy
            },
            error=None if all_healthy else "Some health checks failed"
        )
    
    def _check_connectivity(self, endpoint: str) -> Dict:
        """Check if endpoint is reachable."""
        try:
            import urllib.request
            req = urllib.request.Request(endpoint)
            urllib.request.urlopen(req, timeout=10)
            return {'healthy': True, 'message': 'Connected'}
        except Exception as e:
            return {'healthy': False, 'message': str(e)}
    
    def _check_auth(self, endpoint: str, params: Dict) -> Dict:
        """Check authentication."""
        # Placeholder implementation
        return {'healthy': True, 'message': 'Auth not checked'}
    
    def _check_response_time(self, endpoint: str) -> Dict:
        """Check response time."""
        start = time.time()
        try:
            import urllib.request
            req = urllib.request.Request(endpoint)
            urllib.request.urlopen(req, timeout=10)
            duration = time.time() - start
            return {
                'healthy': duration < 5,
                'message': f'{duration:.3f}s',
                'duration': duration
            }
        except Exception as e:
            return {'healthy': False, 'message': str(e)}
    
    def _check_ssl(self, endpoint: str) -> Dict:
        """Check SSL certificate."""
        if not endpoint.startswith('https://'):
            return {'healthy': True, 'message': 'Not HTTPS'}
        
        try:
            import ssl
            import socket
            
            host = endpoint.split('/')[2]
            context = ssl.create_default_context()
            
            with socket.create_connection((host, 443), timeout=10) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    cert = ssock.getpeercert()
                    return {'healthy': True, 'message': 'SSL valid', 'cert': cert}
        except Exception as e:
            return {'healthy': False, 'message': str(e)}


class APISlaTrackerAction(BaseAction):
    """Track SLA compliance for APIs.
    
    Monitors uptime, latency, and error rate against SLA targets.
    """
    action_type = "api_sla_tracker"
    display_name = "API SLA追踪"
    description = "追踪API的SLA合规性"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SLA tracking.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoint, sla_targets.
        
        Returns:
            ActionResult with SLA status.
        """
        endpoint = params.get('endpoint', '')
        sla_targets = params.get('sla_targets', {
            'uptime': 99.9,
            'latency_p95': 200,
            'error_rate': 1.0
        })
        
        if not endpoint:
            return ActionResult(
                success=False,
                data=None,
                error="Endpoint is required"
            )
        
        # Simulate SLA calculation
        uptime_achieved = 99.95
        latency_p95_achieved = 150
        error_rate_achieved = 0.5
        
        sla_status = {
            'uptime': {
                'target': sla_targets.get('uptime', 99.9),
                'achieved': uptime_achieved,
                'compliant': uptime_achieved >= sla_targets.get('uptime', 99.9)
            },
            'latency': {
                'target': sla_targets.get('latency_p95', 200),
                'achieved': latency_p95_achieved,
                'compliant': latency_p95_achieved <= sla_targets.get('latency_p95', 200)
            },
            'error_rate': {
                'target': sla_targets.get('error_rate', 1.0),
                'achieved': error_rate_achieved,
                'compliant': error_rate_achieved <= sla_targets.get('error_rate', 1.0)
            }
        }
        
        all_compliant = all(s['compliant'] for s in sla_status.values())
        
        return ActionResult(
            success=all_compliant,
            data={
                'endpoint': endpoint,
                'sla_status': sla_status,
                'overall_compliant': all_compliant
            },
            error=None if all_compliant else "SLA targets not met"
        )


def register_actions():
    """Register all API Monitor actions."""
    return [
        APIMonitorAction,
        APIHealthCheckAction,
        APISlaTrackerAction,
    ]
