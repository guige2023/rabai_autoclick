"""API adaptive action module for RabAI AutoClick.

Provides adaptive API operations:
- ApiAdaptiveRetryAction: Adaptive retry based on error type
- ApiAdaptiveTimeoutAction: Dynamic timeout based on operation type
- ApiAdaptiveRoutingAction: Route requests based on response patterns
- ApiAdaptiveLoadBalanceAction: Load balance based on latency
"""

import time
import random
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ErrorSeverity(Enum):
    """Error severity levels."""
    TRANSIENT = "transient"
    CLIENT = "client"
    SERVER = "server"
    FATAL = "fatal"


class ApiAdaptiveRetryAction(BaseAction):
    """Adaptive retry based on error type and severity."""
    action_type = "api_adaptive_retry"
    display_name = "API自适应重试"
    description = "根据错误类型自适应重试"

    def _classify_error(self, status_code: int, message: str) -> ErrorSeverity:
        """Classify error severity."""
        if status_code in (408, 429, 500, 502, 503, 504):
            return ErrorSeverity.TRANSIENT
        elif status_code in (400, 401, 403, 404):
            return ErrorSeverity.CLIENT
        elif status_code >= 500:
            return ErrorSeverity.SERVER
        return ErrorSeverity.FATAL

    def _get_retry_config(self, severity: ErrorSeverity) -> Dict[str, Any]:
        """Get retry configuration based on severity."""
        configs = {
            ErrorSeverity.TRANSIENT: {"max_retries": 5, "base_delay": 1.0, "backoff": 2.0},
            ErrorSeverity.SERVER: {"max_retries": 3, "base_delay": 2.0, "backoff": 2.0},
            ErrorSeverity.CLIENT: {"max_retries": 1, "base_delay": 0.5, "backoff": 1.5},
            ErrorSeverity.FATAL: {"max_retries": 0, "base_delay": 0, "backoff": 1.0},
        }
        return configs.get(severity, configs[ErrorSeverity.TRANSIENT])

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body")
            max_total_retries = params.get("max_total_retries", 5)
            callback = params.get("callback")

            if not url:
                return ActionResult(success=False, message="url is required")

            import urllib.request
            import urllib.error

            last_error = None
            total_retries = 0

            while total_retries <= max_total_retries:
                try:
                    req = urllib.request.Request(url, method=method, headers=headers)
                    if body:
                        import json as json_module
                        req.data = json_module.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = response.read().decode()
                        return ActionResult(
                            success=True,
                            message=f"Request succeeded after {total_retries} retries",
                            data={"content": content, "status": response.status, "retries": total_retries}
                        )
                except urllib.error.HTTPError as e:
                    severity = self._classify_error(e.code, str(e))
                    config = self._get_retry_config(severity)
                    last_error = e

                    if config["max_retries"] == 0 or total_retries >= max_total_retries:
                        break

                    delay = config["base_delay"] * (config["backoff"] ** total_retries)
                    delay = min(delay, 30.0)
                    time.sleep(delay)
                    total_retries += 1
                except urllib.error.URLError as e:
                    last_error = e
                    if total_retries >= max_total_retries:
                        break
                    delay = 1.0 * (2.0 ** total_retries)
                    time.sleep(delay)
                    total_retries += 1

            return ActionResult(
                success=False,
                message=f"Request failed after {total_retries} retries: {last_error}",
                data={"retries": total_retries}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive retry error: {e}")


class ApiAdaptiveTimeoutAction(BaseAction):
    """Dynamic timeout based on operation type."""
    action_type = "api_adaptive_timeout"
    display_name = "API自适应超时"
    description = "根据操作类型动态调整超时"

    def _get_timeout_for_operation(self, url: str, method: str, body: Any = None) -> float:
        """Calculate adaptive timeout."""
        base_timeout = 30.0

        if method == "GET":
            base_timeout = 15.0
        elif method == "POST":
            base_timeout = 30.0
            if body and len(str(body)) > 10000:
                base_timeout = 60.0
        elif method in ("PUT", "PATCH"):
            base_timeout = 45.0
        elif method == "DELETE":
            base_timeout = 20.0

        if "upload" in url.lower() or "file" in url.lower():
            base_timeout = max(base_timeout, 120.0)
        if "download" in url.lower() or "export" in url.lower():
            base_timeout = max(base_timeout, 180.0)

        return base_timeout

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body")
            override_timeout = params.get("timeout")

            if not url:
                return ActionResult(success=False, message="url is required")

            timeout = override_timeout or self._get_timeout_for_operation(url, method, body)

            import urllib.request
            import json as json_module

            req = urllib.request.Request(url, method=method, headers=headers)
            if body:
                req.data = json_module.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

            with urllib.request.urlopen(req, timeout=timeout) as response:
                content = response.read().decode()
                return ActionResult(
                    success=True,
                    message=f"Request succeeded with timeout {timeout}s",
                    data={"content": content, "status": response.status, "timeout_used": timeout}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive timeout error: {e}")


class ApiAdaptiveRoutingAction(BaseAction):
    """Route requests based on response patterns."""
    action_type = "api_adaptive_routing"
    display_name = "API自适应路由"
    description = "根据响应模式自适应路由"

    def __init__(self):
        super().__init__()
        self._endpoint_performance: Dict[str, List[float]] = {}
        self._failure_count: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body")

            if not endpoints:
                return ActionResult(success=False, message="endpoints list is required")

            best_endpoint = self._select_best_endpoint(endpoints)
            if not best_endpoint:
                return ActionResult(success=False, message="No healthy endpoints available")

            import urllib.request
            import json as json_module
            import json as json_mod

            req = urllib.request.Request(best_endpoint, method=method, headers=headers)
            if body:
                req.data = json_mod.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

            start_time = time.time()
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    latency = time.time() - start_time
                    self._record_success(best_endpoint, latency)
                    content = response.read().decode()
                    return ActionResult(
                        success=True,
                        message=f"Routed to {best_endpoint}",
                        data={"content": content, "endpoint": best_endpoint, "latency": latency}
                    )
            except Exception as e:
                self._record_failure(best_endpoint)
                return ActionResult(success=False, message=f"Endpoint {best_endpoint} failed: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive routing error: {e}")

    def _select_best_endpoint(self, endpoints: List[str]) -> Optional[str]:
        """Select best endpoint based on performance."""
        healthy = [ep for ep in endpoints if self._failure_count.get(ep, 0) < 3]
        if not healthy:
            return endpoints[0] if endpoints else None

        scores = {}
        for ep in healthy:
            latencies = self._endpoint_performance.get(ep, [])
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                failure_rate = self._failure_count.get(ep, 0)
                scores[ep] = avg_latency * (1 + failure_rate * 0.5)
            else:
                scores[ep] = 0

        return min(scores, key=scores.get) if scores else healthy[0]

    def _record_success(self, endpoint: str, latency: float) -> None:
        """Record successful request."""
        if endpoint not in self._endpoint_performance:
            self._endpoint_performance[endpoint] = []
        self._endpoint_performance[endpoint].append(latency)
        if len(self._endpoint_performance[endpoint]) > 100:
            self._endpoint_performance[endpoint] = self._endpoint_performance[endpoint][-100:]
        if endpoint in self._failure_count:
            self._failure_count[endpoint] = max(0, self._failure_count[endpoint] - 1)

    def _record_failure(self, endpoint: str) -> None:
        """Record failed request."""
        self._failure_count[endpoint] = self._failure_count.get(endpoint, 0) + 1


class ApiAdaptiveLoadBalanceAction(BaseAction):
    """Load balance based on latency and health."""
    action_type = "api_adaptive_load_balance"
    display_name = "API自适应负载均衡"
    description = "基于延迟和健康状态的负载均衡"

    def __init__(self):
        super().__init__()
        self._request_counts: Dict[str, int] = {}
        self._latencies: Dict[str, List[float]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body")

            if not endpoints:
                return ActionResult(success=False, message="endpoints list is required")

            selected = self._weighted_select(endpoints)

            import urllib.request
            import json as json_module

            req = urllib.request.Request(selected, method=method, headers=headers)
            if body:
                req.data = json_module.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

            start = time.time()
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    latency = time.time() - start
                    self._record(selected, latency)
                    content = response.read().decode()
                    return ActionResult(
                        success=True,
                        message=f"Load-balanced to {selected}",
                        data={"content": content, "endpoint": selected, "latency": latency}
                    )
            except Exception as e:
                self._record(selected, time.time() - start, failure=True)
                return ActionResult(success=False, message=f"Request failed: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Load balance error: {e}")

    def _weighted_select(self, endpoints: List[str]) -> str:
        """Weighted selection based on latency and request count."""
        weights = {}
        total_weight = 0.0

        for ep in endpoints:
            latencies = self._latencies.get(ep, [])
            avg_latency = sum(latencies) / len(latencies) if latencies else 1.0
            requests = self._request_counts.get(ep, 0) + 1
            weight = 1.0 / (avg_latency * (1 + requests * 0.01))
            weights[ep] = weight
            total_weight += weight

        if total_weight == 0:
            return random.choice(endpoints)

        rand = random.uniform(0, total_weight)
        cumulative = 0.0
        for ep, w in weights.items():
            cumulative += w
            if rand <= cumulative:
                return ep
        return endpoints[0]

    def _record(self, endpoint: str, latency: float, failure: bool = False) -> None:
        """Record request metrics."""
        self._request_counts[endpoint] = self._request_counts.get(endpoint, 0) + 1
        if not failure:
            if endpoint not in self._latencies:
                self._latencies[endpoint] = []
            self._latencies[endpoint].append(latency)
            if len(self._latencies[endpoint]) > 50:
                self._latencies[endpoint] = self._latencies[endpoint][-50:]
