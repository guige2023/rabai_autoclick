"""API load tester action module for RabAI AutoClick.

Provides API load testing and stress testing capabilities
with configurable concurrency, duration, and reporting.
"""

import time
import concurrent.futures
import threading
import statistics
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LoadTesterAction(BaseAction):
    """Run load tests against API endpoints.
    
    Executes configurable number of requests with
    concurrency control and performance metrics collection.
    """
    action_type = "load_tester"
    display_name = "负载测试"
    description = "对API端点执行负载测试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Run load test.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   num_requests, concurrency, duration_seconds,
                   expected_status.
        
        Returns:
            ActionResult with load test results.
        """
        import urllib.request

        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        headers = params.get('headers', {})
        body = params.get('body', '')
        num_requests = params.get('num_requests', 100)
        concurrency = params.get('concurrency', 10)
        duration_seconds = params.get('duration_seconds', 0)
        expected_status = params.get('expected_status', 200)
        start_time = time.time()

        if not url:
            return ActionResult(success=False, message="url is required")

        results = []
        errors = []
        lock = threading.Lock()

        def make_request(req_id: int) -> Dict:
            req_start = time.time()
            try:
                req_body = json.dumps(body).encode() if isinstance(body, dict) else str(body).encode() if body else None
                req = urllib.request.Request(url, data=req_body, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp_body = resp.read().decode()
                    duration = time.time() - req_start
                    status = resp.status
                    success = status == expected_status
                    return {
                        'id': req_id,
                        'success': success,
                        'status': status,
                        'duration': duration,
                        'error': None
                    }
            except Exception as e:
                duration = time.time() - req_start
                return {
                    'id': req_id,
                    'success': False,
                    'status': 0,
                    'duration': duration,
                    'error': str(e)
                }

        if duration_seconds > 0:
            end_time = start_time + duration_seconds
            req_id = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = []
                while time.time() < end_time:
                    future = executor.submit(make_request, req_id)
                    futures.append(future)
                    req_id += 1
                for future in concurrent.futures.as_completed(futures):
                    with lock:
                        r = future.result()
                        results.append(r)
                        if not r['success']:
                            errors.append(r)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(make_request, i) for i in range(num_requests)]
                for future in concurrent.futures.as_completed(futures):
                    with lock:
                        r = future.result()
                        results.append(r)
                        if not r['success']:
                            errors.append(r)

        total_duration = time.time() - start_time
        durations = [r['duration'] for r in results]
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        metrics = {}
        if durations:
            sorted_durations = sorted(durations)
            metrics = {
                'total_requests': len(results),
                'successful': len(successful),
                'failed': len(failed),
                'pass_rate': round(len(successful) / len(results) * 100, 2),
                'min_duration_ms': round(min(durations) * 1000, 2),
                'max_duration_ms': round(max(durations) * 1000, 2),
                'mean_duration_ms': round(statistics.mean(durations) * 1000, 2),
                'median_duration_ms': round(statistics.median(durations) * 1000, 2),
                'p95_duration_ms': round(sorted_durations[int(len(sorted_durations) * 0.95)] * 1000, 2),
                'p99_duration_ms': round(sorted_durations[int(len(sorted_durations) * 0.99)] * 1000, 2),
                'requests_per_second': round(len(results) / total_duration, 2),
                'total_duration_seconds': round(total_duration, 2)
            }

        return ActionResult(
            success=len(failed) == 0,
            message=f"Load test: {len(successful)}/{len(results)} successful",
            data={
                'metrics': metrics,
                'errors': errors[:10]
            },
            duration=time.time() - start_time
        )


class StressTestAction(BaseAction):
    """Run stress tests with escalating load.
    
    Gradually increases load to identify breaking points
    and performance degradation patterns.
    """
    action_type = "stress_test"
    display_name = "压力测试"
    description = "逐步增加负载的压力测试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Run stress test.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, start_load, end_load,
                   step_size, duration_per_step, threshold_error_rate.
        
        Returns:
            ActionResult with stress test results.
        """
        import urllib.request

        url = params.get('url', '')
        start_load = params.get('start_load', 10)
        end_load = params.get('end_load', 100)
        step_size = params.get('step_size', 10)
        duration_per_step = params.get('duration_per_step', 5)
        threshold_error_rate = params.get('threshold_error_rate', 5.0)
        start_time = time.time()

        if not url:
            return ActionResult(success=False, message="url is required")

        step_results = []
        breaking_point = None

        current_load = start_load
        while current_load <= end_load:
            load_tester = LoadTesterAction()
            result = load_tester.execute(context, {
                'url': url,
                'concurrency': current_load,
                'duration_seconds': duration_per_step,
                'method': params.get('method', 'GET'),
                'headers': params.get('headers', {}),
                'expected_status': params.get('expected_status', 200)
            })

            metrics = result.data.get('metrics', {})
            error_rate = 100 - metrics.get('pass_rate', 100)
            rps = metrics.get('requests_per_second', 0)

            step_results.append({
                'load': current_load,
                'rps': rps,
                'error_rate': error_rate,
                'pass_rate': metrics.get('pass_rate', 0),
                'mean_duration_ms': metrics.get('mean_duration_ms', 0),
                'max_duration_ms': metrics.get('max_duration_ms', 0)
            })

            if error_rate > threshold_error_rate and breaking_point is None:
                breaking_point = current_load

            current_load += step_size

        return ActionResult(
            success=breaking_point is None,
            message=f"Stress test completed. Breaking point: {breaking_point}" if breaking_point else "No breaking point found",
            data={
                'step_results': step_results,
                'breaking_point': breaking_point,
                'threshold_error_rate': threshold_error_rate
            },
            duration=time.time() - start_time
        )


class EndpointMonitorAction(BaseAction):
    """Monitor API endpoint health over time.
    
    Periodically checks endpoint availability and
    collects latency metrics.
    """
    action_type = "endpoint_monitor"
    display_name = "端点监控"
    description = "监控API端点健康状态"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Monitor endpoint.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, checks, interval,
                   timeout, expected_status.
        
        Returns:
            ActionResult with monitoring results.
        """
        import urllib.request

        url = params.get('url', '')
        method = params.get('method', 'GET')
        checks = params.get('checks', 10)
        interval = params.get('interval', 1)
        timeout = params.get('timeout', 10)
        expected_status = params.get('expected_status', 200)
        start_time = time.time()

        if not url:
            return ActionResult(success=False, message="url is required")

        results = []
        for i in range(checks):
            check_start = time.time()
            try:
                req = urllib.request.Request(url, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    duration = time.time() - check_start
                    results.append({
                        'check': i + 1,
                        'success': resp.status == expected_status,
                        'status': resp.status,
                        'duration_ms': round(duration * 1000, 2),
                        'timestamp': check_start
                    })
            except Exception as e:
                results.append({
                    'check': i + 1,
                    'success': False,
                    'status': 0,
                    'duration_ms': round((time.time() - check_start) * 1000, 2),
                    'error': str(e),
                    'timestamp': check_start
                })

            if i < checks - 1:
                time.sleep(interval)

        successful = [r for r in results if r.get('success', False)]
        durations = [r['duration_ms'] for r in results]

        return ActionResult(
            success=len(successful) == len(results),
            message=f"Endpoint monitor: {len(successful)}/{len(results)} checks passed",
            data={
                'results': results,
                'uptime_percent': round(len(successful) / len(results) * 100, 2),
                'mean_latency_ms': round(sum(durations) / len(durations), 2) if durations else 0,
                'max_latency_ms': max(durations) if durations else 0
            },
            duration=time.time() - start_time
        )
