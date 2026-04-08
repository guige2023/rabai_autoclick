"""API gateway action module for RabAI AutoClick.

Provides API gateway operations including routing, rate limiting,
authentication, and request/response transformation.
"""

import json
import time
import hashlib
import hmac
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiGatewayRouterAction(BaseAction):
    """Route API requests based on path, method, and headers.
    
    Supports dynamic routing with path parameter extraction.
    """
    action_type = "api_gateway_router"
    display_name = "API路由"
    description = "API请求路由和路径参数提取"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route request to backend service.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, method, routes, headers.
                   routes is list of {pattern, method, backend_url}.
        
        Returns:
            ActionResult with routed endpoint info.
        """
        path = params.get('path', '/')
        method = params.get('method', 'GET').upper()
        routes = params.get('routes', [])
        headers = params.get('headers', {})
        path_params = params.get('path_params', {})

        if not routes:
            return ActionResult(success=False, message="routes configuration is required")

        for route in routes:
            pattern = route.get('pattern', '')
            route_method = route.get('method', '').upper()
            backend_url = route.get('backend_url', '')
            
            if route_method and route_method != method:
                continue

            params_match = self._match_path(path, pattern)
            if params_match:
                path_params.update(params_match)
                
                return ActionResult(
                    success=True,
                    message=f"Routed to {backend_url}",
                    data={
                        'backend_url': backend_url,
                        'path_params': path_params,
                        'matched_pattern': pattern
                    }
                )

        return ActionResult(
            success=False,
            message=f"No route matched for {method} {path}",
            data={'available_routes': len(routes)}
        )

    def _match_path(self, path: str, pattern: str) -> Optional[Dict[str, str]]:
        """Match path against pattern and extract parameters."""
        import re
        
        param_pattern = re.sub(r'\{([^}]+)\}', r'(?P<\1>[^/]+)', pattern)
        param_pattern = f'^{param_pattern}$'
        
        match = re.match(param_pattern, path)
        if match:
            return match.groupdict()
        return None


class ApiGatewayAuthAction(BaseAction):
    """Handle API gateway authentication.
    
    Supports API key, Bearer token, and basic auth validation.
    """
    action_type = "api_gateway_auth"
    display_name = "API认证"
    description = "API网关认证和授权"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate API authentication.
        
        Args:
            context: Execution context.
            params: Dict with keys: auth_type, api_key, bearer_token,
                   username, password, valid_keys, valid_tokens.
        
        Returns:
            ActionResult with authentication status.
        """
        auth_type = params.get('auth_type', 'bearer')
        api_key = params.get('api_key', '')
        bearer_token = params.get('bearer_token', '')
        username = params.get('username', '')
        password = params.get('password', '')
        valid_keys = params.get('valid_keys', [])
        valid_tokens = params.get('valid_tokens', [])
        valid_users = params.get('valid_users', {})

        if auth_type == 'api_key':
            if api_key in valid_keys:
                return ActionResult(
                    success=True,
                    message="API key valid",
                    data={'auth_type': 'api_key', 'validated': True}
                )
            return ActionResult(success=False, message="Invalid API key")

        elif auth_type == 'bearer':
            if bearer_token in valid_tokens:
                return ActionResult(
                    success=True,
                    message="Bearer token valid",
                    data={'auth_type': 'bearer', 'validated': True}
                )
            return ActionResult(success=False, message="Invalid bearer token")

        elif auth_type == 'basic':
            expected_password = valid_users.get(username, '')
            if expected_password and password == expected_password:
                return ActionResult(
                    success=True,
                    message="Basic auth valid",
                    data={'auth_type': 'basic', 'username': username}
                )
            return ActionResult(success=False, message="Invalid credentials")

        return ActionResult(success=False, message=f"Unknown auth type: {auth_type}")


class ApiGatewayRateLimitAction(BaseAction):
    """Apply rate limiting to API requests.
    
    Implements token bucket and sliding window algorithms.
    """
    action_type = "api_gateway_rate_limit"
    display_name = "API限流"
    description = "API网关速率限制"

    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, Dict] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check and apply rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys: client_id, rate, capacity,
                   algorithm, window_size.
        
        Returns:
            ActionResult with rate limit status.
        """
        client_id = params.get('client_id', 'default')
        rate = params.get('rate', 100)
        capacity = params.get('capacity', 100)
        algorithm = params.get('algorithm', 'token_bucket')
        window_size = params.get('window_size', 60)

        if algorithm == 'token_bucket':
            return self._token_bucket(client_id, rate, capacity)
        elif algorithm == 'sliding_window':
            return self._sliding_window(client_id, rate, window_size)
        
        return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

    def _token_bucket(self, client_id: str, rate: float, capacity: int) -> ActionResult:
        """Token bucket rate limiting."""
        current_time = time.time()
        
        if client_id not in self._buckets:
            self._buckets[client_id] = {
                'tokens': capacity,
                'last_update': current_time
            }
        
        bucket = self._buckets[client_id]
        elapsed = current_time - bucket['last_update']
        new_tokens = elapsed * rate
        
        bucket['tokens'] = min(capacity, bucket['tokens'] + new_tokens)
        bucket['last_update'] = current_time
        
        if bucket['tokens'] >= 1:
            bucket['tokens'] -= 1
            return ActionResult(
                success=True,
                message="Request allowed",
                data={'remaining_tokens': int(bucket['tokens']), 'client_id': client_id}
            )
        
        return ActionResult(
            success=False,
            message="Rate limit exceeded",
            data={'remaining_tokens': 0, 'retry_after': int(1 / rate) if rate > 0 else 1}
        )

    def _sliding_window(self, client_id: str, max_requests: int, window: int) -> ActionResult:
        """Sliding window rate limiting."""
        current_time = time.time()
        
        if client_id not in self._buckets:
            self._buckets[client_id] = {'requests': []}
        
        bucket = self._buckets[client_id]
        cutoff = current_time - window
        
        bucket['requests'] = [t for t in bucket['requests'] if t > cutoff]
        
        if len(bucket['requests']) < max_requests:
            bucket['requests'].append(current_time)
            return ActionResult(
                success=True,
                message="Request allowed",
                data={'request_count': len(bucket['requests']), 'client_id': client_id}
            )
        
        oldest = bucket['requests'][0]
        retry_after = int(oldest + window - current_time)
        
        return ActionResult(
            success=False,
            message="Rate limit exceeded",
            data={'request_count': len(bucket['requests']), 'retry_after': max(1, retry_after)}
        )


class ApiGatewayTransformAction(BaseAction):
    """Transform API request/response payloads.
    
    Supports JSON path mapping, field renaming, and value transformation.
    """
    action_type = "api_gateway_transform"
    display_name = "API数据转换"
    description = "API请求响应数据转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform API payload.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, transformations, direction.
                   transformations is list of {type, field, value, new_name}.
        
        Returns:
            ActionResult with transformed payload.
        """
        payload = params.get('payload', {})
        transformations = params.get('transformations', [])
        direction = params.get('direction', 'request')

        if not payload:
            return ActionResult(success=False, message="payload is required")

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return ActionResult(success=False, message="payload must be valid JSON")

        transformed = self._deep_copy(payload)
        applied = 0

        for transform in transformations:
            t_type = transform.get('type', 'rename')
            field = transform.get('field', '')
            value = transform.get('value')
            new_name = transform.get('new_name', '')
            
            if t_type == 'rename':
                if field in transformed:
                    transformed[new_name] = transformed.pop(field)
                    applied += 1
            
            elif t_type == 'add':
                transformed[field] = value
                applied += 1
            
            elif t_type == 'remove':
                if field in transformed:
                    del transformed[field]
                    applied += 1
            
            elif t_type == 'static':
                transformed[field] = value
                applied += 1
            
            elif t_type == 'timestamp':
                transformed[field] = int(time.time())
                applied += 1

        return ActionResult(
            success=True,
            message=f"Applied {applied} transformations",
            data={'payload': transformed, 'transformations_applied': applied}
        )

    def _deep_copy(self, obj: Any) -> Any:
        """Deep copy a JSON-compatible object."""
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_copy(item) for item in obj]
        else:
            return obj


class ApiGatewayHealthCheckAction(BaseAction):
    """Perform health checks on API endpoints.
    
    Checks endpoint availability and response time.
    """
    action_type = "api_gateway_health"
    display_name = "API健康检查"
    description = "API端点健康状态检查"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform health check.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoints, timeout, expected_status,
                   check_interval, failure_threshold.
        
        Returns:
            ActionResult with health status for all endpoints.
        """
        endpoints = params.get('endpoints', [])
        timeout = params.get('timeout', 5)
        expected_status = params.get('expected_status', 200)
        failure_threshold = params.get('failure_threshold', 3)

        if not endpoints:
            return ActionResult(success=False, message="endpoints list is required")

        import urllib.request
        import urllib.error

        results = []
        healthy_count = 0

        for endpoint in endpoints:
            url = endpoint.get('url', '')
            name = endpoint.get('name', url)
            
            try:
                start = time.time()
                req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    elapsed = time.time() - start
                    status = response.status
                    
                    is_healthy = status == expected_status
                    if is_healthy:
                        healthy_count += 1
                    
                    results.append({
                        'name': name,
                        'url': url,
                        'status': 'healthy' if is_healthy else 'unhealthy',
                        'status_code': status,
                        'response_time_ms': int(elapsed * 1000)
                    })
                    
            except urllib.error.HTTPError as e:
                results.append({
                    'name': name,
                    'url': url,
                    'status': 'unhealthy',
                    'error': f"HTTP {e.code}"
                })
            except Exception as e:
                results.append({
                    'name': name,
                    'url': url,
                    'status': 'unhealthy',
                    'error': str(e)
                })

        all_healthy = healthy_count == len(endpoints)
        
        return ActionResult(
            success=all_healthy,
            message=f"Health check: {healthy_count}/{len(endpoints)} healthy",
            data={
                'results': results,
                'healthy_count': healthy_count,
                'total_count': len(endpoints),
                'all_healthy': all_healthy
            }
        )
