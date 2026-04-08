"""API Gateway Proxy Action Module.

Provides API gateway proxy capabilities including routing,
load balancing, and request/response transformation.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urlparse, parse_qs
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ProxyRouterAction(BaseAction):
    """Route API requests to appropriate backends.
    
    Supports path-based, header-based, and query-based routing.
    """
    action_type = "proxy_router"
    display_name = "代理路由"
    description = "将API请求路由到后端服务"

    def __init__(self):
        super().__init__()
        self._routes: Dict[str, Dict] = {}
        self._route_groups: Dict[str, List[str]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Route an API request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add_route', 'route', 'list_routes'.
                - route_id: Route identifier.
                - path_pattern: URL path pattern.
                - backend_url: Backend service URL.
                - methods: Allowed HTTP methods.
                - headers: Headers for routing.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with routing result or error.
        """
        operation = params.get('operation', 'route')
        route_id = params.get('route_id', '')
        path_pattern = params.get('path_pattern', '')
        backend_url = params.get('backend_url', '')
        methods = params.get('methods', ['GET'])
        headers = params.get('headers', {})
        request = params.get('request', {})
        output_var = params.get('output_var', 'route_result')

        try:
            if operation == 'add_route':
                return self._add_route(route_id, path_pattern, backend_url, methods, headers, output_var)
            elif operation == 'route':
                return self._route_request(request, output_var)
            elif operation == 'list_routes':
                return self._list_routes(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Proxy router failed: {str(e)}"
            )

    def _add_route(
        self,
        route_id: str,
        path_pattern: str,
        backend_url: str,
        methods: List[str],
        headers: Dict,
        output_var: str
    ) -> ActionResult:
        """Add a route to the router."""
        if not route_id or not path_pattern:
            return ActionResult(
                success=False,
                message="route_id and path_pattern are required"
            )

        self._routes[route_id] = {
            'path_pattern': path_pattern,
            'backend_url': backend_url,
            'methods': methods,
            'headers': headers,
            'created_at': time.time()
        }

        result = {
            'route_id': route_id,
            'path_pattern': path_pattern,
            'backend_url': backend_url
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Route '{route_id}' added: {path_pattern} -> {backend_url}"
        )

    def _route_request(self, request: Dict, output_var: str) -> ActionResult:
        """Route an incoming request."""
        path = request.get('path', '/')
        method = request.get('method', 'GET')

        # Find matching route
        matched_route = None
        route_id = None

        for rid, route in self._routes.items():
            if self._match_path(path, route['path_pattern']):
                if method in route['methods']:
                    matched_route = route
                    route_id = rid
                    break

        if not matched_route:
            return ActionResult(
                success=False,
                message=f"No route found for {method} {path}"
            )

        # Transform path
        transformed_path = self._transform_path(path, matched_route['path_pattern'])

        result = {
            'routed': True,
            'route_id': route_id,
            'backend_url': matched_route['backend_url'],
            'path': transformed_path,
            'headers': matched_route['headers']
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Routed {method} {path} to {matched_route['backend_url']}"
        )

    def _match_path(self, path: str, pattern: str) -> bool:
        """Match a path against a pattern."""
        import re
        # Convert pattern to regex
        regex = pattern.replace('/', '\\/').replace('*', '.*').replace('{.*}', '[^/]+')
        return bool(re.match(f'^{regex}$', path))

    def _transform_path(self, path: str, pattern: str) -> str:
        """Transform path by removing pattern prefix."""
        if pattern.endswith('*'):
            return path[len(pattern[:-1]):]
        return path

    def _list_routes(self, output_var: str) -> ActionResult:
        """List all configured routes."""
        routes = []
        for rid, route in self._routes.items():
            routes.append({
                'route_id': rid,
                'path_pattern': route['path_pattern'],
                'backend_url': route['backend_url'],
                'methods': route['methods']
            })

        context.variables[output_var] = routes
        return ActionResult(
            success=True,
            data={'routes': routes, 'count': len(routes)},
            message=f"Listed {len(routes)} routes"
        )


class LoadBalancerAction(BaseAction):
    """Balance load across multiple backend services.
    
    Supports round-robin, least-connections, and weighted distribution.
    """
    action_type = "load_balancer"
    display_name: "负载均衡"
    description: "在多个后端服务间平衡负载"

    def __init__(self):
        super().__init__()
        self._backends: Dict[str, List[Dict]] = {}
        self._counters: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Balance load across backends.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add_backend', 'select_backend', 'health_check'.
                - pool_id: Backend pool identifier.
                - backend_url: Backend URL to add.
                - weight: Backend weight for weighted distribution.
                - strategy: 'round_robin', 'least_conn', 'weighted'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with load balancing result or error.
        """
        operation = params.get('operation', 'select_backend')
        pool_id = params.get('pool_id', 'default')
        backend_url = params.get('backend_url', '')
        weight = params.get('weight', 1)
        strategy = params.get('strategy', 'round_robin')
        output_var = params.get('output_var', 'lb_result')

        try:
            if operation == 'add_backend':
                return self._add_backend(pool_id, backend_url, weight, output_var)
            elif operation == 'select_backend':
                return self._select_backend(pool_id, strategy, output_var)
            elif operation == 'health_check':
                return self._health_check(pool_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Load balancer failed: {str(e)}"
            )

    def _add_backend(
        self,
        pool_id: str,
        backend_url: str,
        weight: int,
        output_var: str
    ) -> ActionResult:
        """Add a backend to a pool."""
        if pool_id not in self._backends:
            self._backends[pool_id] = []

        self._backends[pool_id].append({
            'url': backend_url,
            'weight': weight,
            'healthy': True,
            'connections': 0,
            'added_at': time.time()
        })

        result = {
            'pool_id': pool_id,
            'backend_url': backend_url,
            'weight': weight
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Backend '{backend_url}' added to pool '{pool_id}'"
        )

    def _select_backend(
        self,
        pool_id: str,
        strategy: str,
        output_var: str
    ) -> ActionResult:
        """Select a backend based on strategy."""
        if pool_id not in self._backends or not self._backends[pool_id]:
            return ActionResult(
                success=False,
                message=f"No backends in pool '{pool_id}'"
            )

        backends = [b for b in self._backends[pool_id] if b.get('healthy', True)]

        if not backends:
            return ActionResult(
                success=False,
                message=f"No healthy backends in pool '{pool_id}'"
            )

        if strategy == 'round_robin':
            selected = self._round_robin_select(pool_id, backends)
        elif strategy == 'least_conn':
            selected = self._least_conn_select(backends)
        elif strategy == 'weighted':
            selected = self._weighted_select(backends)
        else:
            selected = backends[0]

        # Increment connection count
        selected['connections'] = selected.get('connections', 0) + 1

        result = {
            'pool_id': pool_id,
            'strategy': strategy,
            'selected_backend': selected['url'],
            'active_connections': selected.get('connections', 0)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Selected backend: {selected['url']}"
        )

    def _round_robin_select(self, pool_id: str, backends: List[Dict]) -> Dict:
        """Round-robin selection."""
        if pool_id not in self._counters:
            self._counters[pool_id] = 0

        index = self._counters[pool_id] % len(backends)
        self._counters[pool_id] += 1
        return backends[index]

    def _least_conn_select(self, backends: List[Dict]) -> Dict:
        """Select backend with least connections."""
        return min(backends, key=lambda x: x.get('connections', 0))

    def _weighted_select(self, backends: List[Dict]) -> Dict:
        """Weighted random selection."""
        total_weight = sum(b.get('weight', 1) for b in backends)
        import random
        r = random.uniform(0, total_weight)
        cumsum = 0
        for backend in backends:
            cumsum += backend.get('weight', 1)
            if r <= cumsum:
                return backend
        return backends[-1]

    def _health_check(self, pool_id: str, output_var: str) -> ActionResult:
        """Perform health check on pool."""
        if pool_id not in self._backends:
            return ActionResult(
                success=False,
                message=f"Pool '{pool_id}' not found"
            )

        results = []
        for backend in self._backends[pool_id]:
            # Simple health check - mark as healthy
            backend['healthy'] = True
            results.append({
                'url': backend['url'],
                'healthy': backend['healthy'],
                'connections': backend.get('connections', 0)
            })

        context.variables[output_var] = results
        return ActionResult(
            success=True,
            data={'health_checks': results, 'count': len(results)},
            message=f"Health check completed for pool '{pool_id}'"
        )


class CircuitBreakerAction(BaseAction):
    """Implement circuit breaker pattern for API resilience.
    
    Supports half-open, closed, and open states with failure tracking.
    """
    action_type: "circuit_breaker"
    display_name = "断路器"
    description = "实现API弹性断路器模式"

    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage circuit breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'call', 'record_success', 'record_failure', 'status'.
                - circuit_id: Circuit identifier.
                - threshold: Failure threshold to open circuit.
                - timeout: Timeout in seconds before half-open.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with circuit breaker result or error.
        """
        operation = params.get('operation', 'call')
        circuit_id = params.get('circuit_id', 'default')
        threshold = params.get('threshold', 5)
        timeout = params.get('timeout', 60)
        output_var = params.get('output_var', 'circuit_result')

        try:
            # Initialize circuit if needed
            if circuit_id not in self._circuits:
                self._circuits[circuit_id] = {
                    'state': 'closed',
                    'failures': 0,
                    'successes': 0,
                    'last_failure_time': None,
                    'threshold': threshold,
                    'timeout': timeout
                }

            circuit = self._circuits[circuit_id]

            if operation == 'call':
                return self._check_circuit(circuit_id, circuit, output_var)
            elif operation == 'record_success':
                return self._record_success(circuit_id, circuit, output_var)
            elif operation == 'record_failure':
                return self._record_failure(circuit_id, circuit, output_var)
            elif operation == 'status':
                return self._get_status(circuit_id, circuit, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Circuit breaker failed: {str(e)}"
            )

    def _check_circuit(self, circuit_id: str, circuit: Dict, output_var: str) -> ActionResult:
        """Check if circuit allows calls."""
        state = circuit['state']

        # Check if should transition from open to half-open
        if state == 'open':
            if circuit['last_failure_time']:
                elapsed = time.time() - circuit['last_failure_time']
                if elapsed >= circuit['timeout']:
                    state = 'half_open'
                    circuit['state'] = 'half_open'

        # Check if call is allowed
        allowed = state in ('closed', 'half_open')

        result = {
            'circuit_id': circuit_id,
            'state': state,
            'allowed': allowed,
            'failures': circuit['failures']
        }

        context.variables[output_var] = result
        return ActionResult(
            success=allowed,
            data=result,
            message=f"Circuit '{circuit_id}' state: {state}, allowed: {allowed}"
        )

    def _record_success(self, circuit_id: str, circuit: Dict, output_var: str) -> ActionResult:
        """Record successful call."""
        circuit['successes'] += 1
        circuit['failures'] = 0

        if circuit['state'] == 'half_open':
            circuit['state'] = 'closed'

        result = {
            'circuit_id': circuit_id,
            'state': circuit['state'],
            'successes': circuit['successes']
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Success recorded for circuit '{circuit_id}'"
        )

    def _record_failure(self, circuit_id: str, circuit: Dict, output_var: str) -> ActionResult:
        """Record failed call."""
        circuit['failures'] += 1
        circuit['last_failure_time'] = time.time()
        circuit['successes'] = 0

        # Open circuit if threshold exceeded
        if circuit['failures'] >= circuit['threshold']:
            circuit['state'] = 'open'

        result = {
            'circuit_id': circuit_id,
            'state': circuit['state'],
            'failures': circuit['failures'],
            'tripped': circuit['state'] == 'open'
        }

        context.variables[output_var] = result
        return ActionResult(
            success=circuit['state'] != 'open',
            data=result,
            message=f"Failure recorded: circuit {circuit['state']}"
        )

    def _get_status(self, circuit_id: str, circuit: Dict, output_var: str) -> ActionResult:
        """Get circuit status."""
        result = {
            'circuit_id': circuit_id,
            'state': circuit['state'],
            'failures': circuit['failures'],
            'successes': circuit['successes'],
            'threshold': circuit['threshold'],
            'timeout': circuit['timeout']
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Circuit '{circuit_id}' status retrieved"
        )


class RequestCacheAction(BaseAction):
    """Cache API responses for improved performance.
    
    Supports TTL-based expiration and cache invalidation.
    """
    action_type = "request_cache"
    display_name = "请求缓存"
    description = "缓存API响应"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage request cache.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'get', 'set', 'invalidate', 'clear'.
                - cache_key: Cache key.
                - value: Value to cache.
                - ttl: Time to live in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with cache result or error.
        """
        operation = params.get('operation', 'get')
        cache_key = params.get('cache_key', '')
        value = params.get('value', None)
        ttl = params.get('ttl', 300)
        output_var = params.get('output_var', 'cache_result')

        try:
            if operation == 'get':
                return self._get(cache_key, output_var)
            elif operation == 'set':
                return self._set(cache_key, value, ttl, output_var)
            elif operation == 'invalidate':
                return self._invalidate(cache_key, output_var)
            elif operation == 'clear':
                return self._clear(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Request cache failed: {str(e)}"
            )

    def _get(self, cache_key: str, output_var: str) -> ActionResult:
        """Get cached value."""
        if cache_key not in self._cache:
            return ActionResult(
                success=False,
                message=f"Cache miss for key '{cache_key}'"
            )

        entry = self._cache[cache_key]

        # Check expiration
        if time.time() > entry['expires_at']:
            del self._cache[cache_key]
            return ActionResult(
                success=False,
                message=f"Cache expired for key '{cache_key}'"
            )

        context.variables[output_var] = entry['value']
        return ActionResult(
            success=True,
            data={'key': cache_key, 'value': entry['value'], 'hit': True},
            message=f"Cache hit for key '{cache_key}'"
        )

    def _set(self, cache_key: str, value: Any, ttl: int, output_var: str) -> ActionResult:
        """Set cached value."""
        self._cache[cache_key] = {
            'value': value,
            'created_at': time.time(),
            'expires_at': time.time() + ttl
        }

        context.variables[output_var] = {'cached': True, 'key': cache_key}
        return ActionResult(
            success=True,
            data={'cached': True, 'key': cache_key, 'ttl': ttl},
            message=f"Cached value for key '{cache_key}'"
        )

    def _invalidate(self, cache_key: str, output_var: str) -> ActionResult:
        """Invalidate cached value."""
        if cache_key in self._cache:
            del self._cache[cache_key]
            return ActionResult(
                success=True,
                data={'invalidated': True, 'key': cache_key},
                message=f"Invalidated cache for key '{cache_key}'"
            )

        return ActionResult(
            success=False,
            message=f"Key '{cache_key}' not found in cache"
        )

    def _clear(self, output_var: str) -> ActionResult:
        """Clear all cache."""
        count = len(self._cache)
        self._cache.clear()

        context.variables[output_var] = {'cleared': count}
        return ActionResult(
            success=True,
            data={'cleared': count},
            message=f"Cleared {count} cache entries"
        )
