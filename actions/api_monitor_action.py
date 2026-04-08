"""API monitor action module for RabAI AutoClick.

Provides API monitoring and health check actions for
tracking API availability and performance metrics.
"""

import time
import json
import sys
import os
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiHealthCheckAction(BaseAction):
    """Perform health check on API endpoints.
    
    Checks endpoint availability and response time.
    """
    action_type = "api_health_check"
    display_name = "API健康检查"
    description = "API端点健康检查"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform health check.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, expected_status, timeout,
                   check_body, expected_body.
        
        Returns:
            ActionResult with health check result.
        """
        url = params.get('url', '')
        expected_status = params.get('expected_status', 200)
        timeout = params.get('timeout', 10)
        check_body = params.get('check_body', False)
        expected_body = params.get('expected_body', '')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            start = time.time()
            
            req = urllib.request.Request(url)
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                elapsed = time.time() - start
                status = response.status
                body = response.read().decode('utf-8', errors='ignore')
                
                is_healthy = status == expected_status
                
                if check_body and expected_body:
                    is_healthy = is_healthy and expected_body in body
                
                return ActionResult(
                    success=is_healthy,
                    message=f"Health check: {'OK' if is_healthy else 'FAILED'} ({status}) in {elapsed:.3f}s",
                    data={
                        'healthy': is_healthy,
                        'status_code': status,
                        'response_time_ms': int(elapsed * 1000),
                        'url': url
                    }
                )

        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP error: {e.code} {e.reason}",
                data={'status_code': e.code, 'error': e.reason}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Health check failed: {str(e)}",
                data={'error': str(e)}
            )


class ApiMetricsCollectorAction(BaseAction):
    """Collect metrics from API responses.
    
    Tracks response times, status codes, and error rates.
    """
    action_type = "api_metrics_collector"
    display_name = "API指标收集"
    description = "收集API性能指标"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, List] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Collect metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, url, method, samples,
                   interval, timeout.
        
        Returns:
            ActionResult with collected metrics.
        """
        operation = params.get('operation', 'collect')
        url = params.get('url', '')
        method = params.get('method', 'GET')
        samples = params.get('samples', 10)
        interval = params.get('interval', 1.0)
        timeout = params.get('timeout', 30)
        metrics_key = params.get('metrics_key', 'default')

        try:
            if operation == 'collect':
                if not url:
                    return ActionResult(success=False, message="url is required")

                response_times = []
                status_codes = []
                errors = []

                for i in range(samples):
                    try:
                        start = time.time()
                        req = urllib.request.Request(url, method=method)
                        
                        with urllib.request.urlopen(req, timeout=timeout) as response:
                            elapsed = time.time() - start
                            response_times.append(elapsed * 1000)
                            status_codes.append(response.status)
                    
                    except Exception as e:
                        errors.append(str(e))
                    
                    if i < samples - 1:
                        time.sleep(interval)

                if not response_times:
                    return ActionResult(
                        success=False,
                        message="No successful requests",
                        data={'error_rate': 1.0, 'samples': samples}
                    )

                avg_time = sum(response_times) / len(response_times)
                min_time = min(response_times)
                max_time = max(response_times)
                error_rate = len(errors) / samples

                self._metrics[metrics_key] = {
                    'response_times': response_times,
                    'status_codes': status_codes,
                    'errors': errors,
                    'avg_time_ms': avg_time,
                    'min_time_ms': min_time,
                    'max_time_ms': max_time,
                    'error_rate': error_rate,
                    'samples': samples
                }

                return ActionResult(
                    success=True,
                    message=f"Collected {samples} samples: avg={avg_time:.1f}ms, error_rate={error_rate:.1%}",
                    data=self._metrics[metrics_key]
                )

            elif operation == 'get':
                if metrics_key not in self._metrics:
                    return ActionResult(
                        success=False,
                        message=f"No metrics for key: {metrics_key}"
                    )
                
                return ActionResult(
                    success=True,
                    message=f"Retrieved metrics for {metrics_key}",
                    data=self._metrics[metrics_key]
                )

            elif operation == 'clear':
                if metrics_key in self._metrics:
                    del self._metrics[metrics_key]
                return ActionResult(success=True, message=f"Cleared metrics: {metrics_key}")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics collection failed: {str(e)}")


class ApiAvailabilityTrackerAction(BaseAction):
    """Track API availability over time.
    
    Monitors uptime and generates availability reports.
    """
    action_type = "api_availability_tracker"
    display_name = "API可用性追踪"
    description = "追踪API可用性"

    def __init__(self):
        super().__init__()
        self._availability: Dict[str, Dict] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Track availability.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, url, timeout,
                   tracker_id, report_window.
        
        Returns:
            ActionResult with availability data.
        """
        operation = params.get('operation', 'record')
        url = params.get('url', '')
        timeout = params.get('timeout', 10)
        tracker_id = params.get('tracker_id', 'default')
        report_window = params.get('report_window', 3600)

        try:
            if operation == 'record':
                if not url:
                    return ActionResult(success=False, message="url is required")

                if tracker_id not in self._availability:
                    self._availability[tracker_id] = {
                        'total_checks': 0,
                        'successful_checks': 0,
                        'failed_checks': 0,
                        'total_downtime': 0,
                        'last_check': None,
                        'checks': []
                    }

                tracker = self._availability[tracker_id]
                tracker['total_checks'] += 1
                tracker['last_check'] = time.time()

                try:
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        if response.status < 400:
                            tracker['successful_checks'] += 1
                            success = True
                        else:
                            tracker['failed_checks'] += 1
                            success = False
                except:
                    tracker['failed_checks'] += 1
                    success = False

                tracker['checks'].append({
                    'timestamp': time.time(),
                    'success': success
                })

                cutoff = time.time() - report_window
                tracker['checks'] = [c for c in tracker['checks'] if c['timestamp'] > cutoff]

                recent = tracker['checks']
                recent_success = sum(1 for c in recent if c['success'])
                availability = (recent_success / len(recent) * 100) if recent else 0

                return ActionResult(
                    success=success,
                    message=f"Check recorded: {'OK' if success else 'FAILED'}",
                    data={
                        'tracker_id': tracker_id,
                        'success': success,
                        'availability_pct': round(availability, 2),
                        'total_checks': tracker['total_checks']
                    }
                )

            elif operation == 'report':
                if tracker_id not in self._availability:
                    return ActionResult(success=False, message=f"Unknown tracker: {tracker_id}")

                tracker = self._availability[tracker_id]
                cutoff = time.time() - report_window
                recent = [c for c in tracker['checks'] if c['timestamp'] > cutoff]
                
                recent_success = sum(1 for c in recent if c['success'])
                availability = (recent_success / len(recent) * 100) if recent else 0

                return ActionResult(
                    success=True,
                    message=f"Availability report for {tracker_id}: {availability:.2f}%",
                    data={
                        'tracker_id': tracker_id,
                        'availability_pct': round(availability, 2),
                        'total_checks': tracker['total_checks'],
                        'successful_checks': tracker['successful_checks'],
                        'failed_checks': tracker['failed_checks'],
                        'report_window_seconds': report_window
                    }
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Tracker failed: {str(e)}")


class ApiLatencyMonitorAction(BaseAction):
    """Monitor API latency patterns.
    
    Tracks latency distribution and detects anomalies.
    """
    action_type = "api_latency_monitor"
    display_name = "API延迟监控"
    description = "监控API延迟"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Monitor latency.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, samples, interval,
                   threshold_ms, timeout.
        
        Returns:
            ActionResult with latency analysis.
        """
        url = params.get('url', '')
        samples = params.get('samples', 50)
        interval = params.get('interval', 0.5)
        threshold_ms = params.get('threshold_ms', 1000)
        timeout = params.get('timeout', 30)

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            latencies = []
            errors = 0

            for _ in range(samples):
                try:
                    start = time.time()
                    req = urllib.request.Request(url)
                    
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        elapsed = (time.time() - start) * 1000
                        latencies.append(elapsed)
                
                except Exception as e:
                    errors += 1
                
                time.sleep(interval)

            if not latencies:
                return ActionResult(
                    success=False,
                    message="No successful requests",
                    data={'error_count': errors, 'samples': samples}
                )

            latencies.sort()
            n = len(latencies)
            
            avg = sum(latencies) / n
            p50 = latencies[n // 2]
            p90 = latencies[int(n * 0.9)]
            p95 = latencies[int(n * 0.95)]
            p99 = latencies[int(n * 0.99)]
            min_lat = min(latencies)
            max_lat = max(latencies)

            violations = sum(1 for l in latencies if l > threshold_ms)
            violation_rate = violations / n

            return ActionResult(
                success=violation_rate < 0.1,
                message=f"Latency: avg={avg:.1f}ms, p95={p95:.1f}ms, violations={violation_rate:.1%}",
                data={
                    'avg_ms': round(avg, 2),
                    'min_ms': round(min_lat, 2),
                    'max_ms': round(max_lat, 2),
                    'p50_ms': round(p50, 2),
                    'p90_ms': round(p90, 2),
                    'p95_ms': round(p95, 2),
                    'p99_ms': round(p99, 2),
                    'violations': violations,
                    'violation_rate': round(violation_rate, 4),
                    'threshold_ms': threshold_ms
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Latency monitor failed: {str(e)}")
