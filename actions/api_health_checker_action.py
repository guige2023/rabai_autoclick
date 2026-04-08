"""API Health Checker action module for RabAI AutoClick.

Provides health check, readiness, and liveness probe actions
for API endpoints and services.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiHealthCheckerAction(BaseAction):
    """Perform health checks on API endpoints.

    Supports /health, /ready, /live endpoints with
    configurable thresholds and timeout.
    """
    action_type = "api_health_checker"
    display_name = "API健康检查"
    description = "执行API端点健康检查"

    HEALTH_ENDPOINTS = ['/health', '/healthz', '/status', '/ready', '/live', '/ping']

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform health check.

        Args:
            context: Execution context.
            params: Dict with keys: url, health_path, timeout,
                   expected_status, check_response_time,
                   response_schema (optional).

        Returns:
            ActionResult with health status and latency.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            health_path = params.get('health_path', '/health')
            timeout = params.get('timeout', 5)
            expected_status = params.get('expected_status', 200)
            check_response_time = params.get('check_response_time', True)
            response_schema = params.get('response_schema', None)

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            full_url = url.rstrip('/') + '/' + health_path.lstrip('/')
            headers = params.get('headers', {})

            latency_ms = 0
            response_body = None

            req = Request(full_url, headers=headers)
            req_start = time.time()
            try:
                with urlopen(req, timeout=timeout) as resp:
                    latency_ms = int((time.time() - req_start) * 1000)
                    status = resp.status
                    try:
                        response_body = json.loads(resp.read().decode('utf-8'))
                    except Exception:
                        response_body = resp.read().decode('utf-8', errors='ignore')
            except HTTPError as e:
                latency_ms = int((time.time() - req_start) * 1000)
                status = e.code
                response_body = e.read().decode('utf-8', errors='ignore') if e.fp else None
            except Exception as e:
                duration = time.time() - start_time
                return ActionResult(
                    success=False,
                    message=f"Health check failed: {str(e)}",
                    duration=duration,
                )

            healthy = (status == expected_status)
            if check_response_time and latency_ms > (timeout * 1000 * 0.8):
                healthy = False

            # Validate response schema if provided
            schema_valid = True
            if response_schema and isinstance(response_body, dict):
                schema_valid = all(k in response_body for k in response_schema.get('required', []))

            duration = time.time() - start_time
            return ActionResult(
                success=healthy,
                message=f"Health check: {'OK' if healthy else 'FAIL'} (status={status}, latency={latency_ms}ms)",
                data={
                    'url': full_url,
                    'status': status,
                    'latency_ms': latency_ms,
                    'response': response_body,
                    'schema_valid': schema_valid,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Health check error: {str(e)}",
                duration=duration,
            )


class ApiReadinessProbeAction(BaseAction):
    """Perform Kubernetes-style readiness and liveness probes.

    Checks if service is ready to receive traffic and
    if it is alive/responding.
    """
    action_type = "api_readiness_probe"
    display_name = "API就绪探针"
    description = "Kubernetes风格的就绪和存活探针"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform readiness/liveness probe.

        Args:
            context: Execution context.
            params: Dict with keys: url, probe_type (readiness/liveness),
                   failure_threshold, success_threshold,
                   timeout, period_seconds.

        Returns:
            ActionResult with probe result.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            probe_type = params.get('probe_type', 'readiness')
            failure_threshold = params.get('failure_threshold', 3)
            success_threshold = params.get('success_threshold', 1)
            timeout = params.get('timeout', 5)
            period_seconds = params.get('period_seconds', 10)
            attempts = params.get('attempts', 3)

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            health_path = '/ready' if probe_type == 'readiness' else '/health'
            full_url = url.rstrip('/') + '/' + health_path.lstrip('/')

            successes = 0
            failures = 0
            results = []

            for i in range(attempts):
                req_start = time.time()
                try:
                    req = Request(full_url)
                    with urlopen(req, timeout=timeout) as resp:
                        latency = int((time.time() - req_start) * 1000)
                        status_ok = 200 <= resp.status < 300
                        if status_ok:
                            successes += 1
                            results.append({'attempt': i + 1, 'status': 'success', 'latency_ms': latency})
                        else:
                            failures += 1
                            results.append({'attempt': i + 1, 'status': 'fail', 'status_code': resp.status})
                except Exception as e:
                    failures += 1
                    results.append({'attempt': i + 1, 'status': 'error', 'error': str(e)})

                if i < attempts - 1:
                    time.sleep(0.5)

            ready = successes >= success_threshold and failures < failure_threshold

            duration = time.time() - start_time
            return ActionResult(
                success=ready,
                message=f"{probe_type} probe: {'PASS' if ready else 'FAIL'} (successes={successes}, failures={failures})",
                data={
                    'probe_type': probe_type,
                    'ready': ready,
                    'successes': successes,
                    'failures': failures,
                    'attempts': attempts,
                    'results': results,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Probe failed: {str(e)}",
                duration=duration,
            )


class ApiHealthAggregatorAction(BaseAction):
    """Aggregate health status from multiple API endpoints.

    Performs parallel health checks and returns
    consolidated status.
    """
    action_type = "api_health_aggregator"
    display_name = "API健康聚合"
    description = "聚合多个API端点的健康状态"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Aggregate health checks.

        Args:
            context: Execution context.
            params: Dict with keys: endpoints (list of dicts),
                   timeout, required_healthy_ratio.

        Returns:
            ActionResult with aggregated health status.
        """
        start_time = time.time()
        try:
            endpoints = params.get('endpoints', [])
            timeout = params.get('timeout', 10)
            required_healthy_ratio = params.get('required_healthy_ratio', 0.5)

            if not endpoints:
                return ActionResult(
                    success=False,
                    message="At least one endpoint is required",
                    duration=time.time() - start_time,
                )

            import concurrent.futures

            def check_endpoint(ep: Dict) -> Dict:
                url = ep.get('url', '')
                health_path = ep.get('health_path', '/health')
                full_url = url.rstrip('/') + '/' + health_path.lstrip('/')
                req_start = time.time()
                try:
                    req = Request(full_url)
                    with urlopen(req, timeout=timeout) as resp:
                        latency = int((time.time() - req_start) * 1000)
                        return {
                            'url': url,
                            'healthy': 200 <= resp.status < 300,
                            'status': resp.status,
                            'latency_ms': latency,
                        }
                except Exception as e:
                    return {
                        'url': url,
                        'healthy': False,
                        'error': str(e),
                        'latency_ms': int((time.time() - req_start) * 1000),
                    }

            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(endpoints), 10)) as executor:
                futures = [executor.submit(check_endpoint, ep) for ep in endpoints]
                for f in concurrent.futures.as_completed(futures):
                    results.append(f.result())

            healthy_count = sum(1 for r in results if r.get('healthy', False))
            total_count = len(results)
            healthy_ratio = healthy_count / total_count if total_count > 0 else 0
            all_healthy = healthy_ratio >= required_healthy_ratio

            duration = time.time() - start_time
            return ActionResult(
                success=all_healthy,
                message=f"Aggregated health: {healthy_count}/{total_count} healthy ({healthy_ratio:.0%})",
                data={
                    'total': total_count,
                    'healthy': healthy_count,
                    'unhealthy': total_count - healthy_count,
                    'healthy_ratio': healthy_ratio,
                    'all_healthy': all_healthy,
                    'endpoints': results,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Health aggregation failed: {str(e)}",
                duration=duration,
            )
