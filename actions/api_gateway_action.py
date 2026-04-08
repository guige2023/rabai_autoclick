"""API gateway action module for RabAI AutoClick.

Provides API gateway functionality with routing, load balancing,
request/response transformation, and authentication.
"""

import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from random import choice

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LoadBalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"


@dataclass
class Route:
    """A route definition."""
    path: str
    method: str
    backend: str
    backend_path: Optional[str] = None
    timeout: float = 30.0
    retries: int = 0
    weight: int = 1


@dataclass
class BackendPool:
    """Backend server pool."""
    name: str
    servers: List[str]
    strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN
    health_check_path: str = "/health"
    weights: Dict[str, int] = field(default_factory=dict)


class APIGatewayAction(BaseAction):
    """Route requests to backends with load balancing and transformation.
    
    Supports path-based routing, multiple load balancing strategies,
    automatic retries, and request/response transformation.
    """
    action_type = "api_gateway"
    display_name = "API网关"
    description = "请求路由和负载均衡"
    
    def __init__(self):
        super().__init__()
        self._routes: Dict[str, Route] = {}
        self._pools: Dict[str, BackendPool] = {}
        self._round_robin_counters: Dict[str, int] = {}
        self._connection_counts: Dict[str, int] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gateway operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'route', 'register', 'add_pool'
                - path: Request path
                - method: HTTP method
                - request: Request data
                - pool: Backend pool name (for add_pool)
                - servers: List of server URLs (for add_pool)
        
        Returns:
            ActionResult with routing result.
        """
        operation = params.get('operation', 'route').lower()
        
        if operation == 'route':
            return self._route(params)
        elif operation == 'register':
            return self._register(params)
        elif operation == 'add_pool':
            return self._add_pool(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _route(self, params: Dict[str, Any]) -> ActionResult:
        """Route a request to appropriate backend."""
        path = params.get('path', '/')
        method = params.get('method', 'GET').upper()
        request = params.get('request', {})
        
        # Find matching route
        route = self._find_route(path, method)
        
        if not route:
            return ActionResult(
                success=False,
                message=f"No route found for {method} {path}",
                data={'path': path, 'method': method}
            )
        
        # Select backend from pool
        pool_name = route.backend
        pool = self._pools.get(pool_name)
        
        if not pool:
            return ActionResult(
                success=False,
                message=f"Backend pool '{pool_name}' not found"
            )
        
        server = self._select_server(pool)
        
        if not server:
            return ActionResult(
                success=False,
                message=f"No healthy servers in pool '{pool_name}'"
            )
        
        # Build backend URL
        backend_path = route.backend_path or path
        backend_url = f"{server}{backend_path}"
        
        # Apply transformations if any
        transformed_request = self._transform_request(request, route)
        
        return ActionResult(
            success=True,
            message=f"Routed to {server}",
            data={
                'backend_url': backend_url,
                'server': server,
                'pool': pool_name,
                'route': path,
                'method': method,
                'timeout': route.timeout,
                'retries': route.retries,
                'transformed_request': transformed_request
            }
        )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a new route."""
        path = params.get('path')
        method = params.get('method', 'GET').upper()
        backend = params.get('backend')
        backend_path = params.get('backend_path')
        timeout = params.get('timeout', 30.0)
        retries = params.get('retries', 0)
        weight = params.get('weight', 1)
        
        if not path or not backend:
            return ActionResult(
                success=False,
                message="path and backend are required"
            )
        
        route = Route(
            path=path,
            method=method,
            backend=backend,
            backend_path=backend_path,
            timeout=timeout,
            retries=retries,
            weight=weight
        )
        
        key = f"{method}:{path}"
        self._routes[key] = route
        
        return ActionResult(
            success=True,
            message=f"Registered route {method} {path} -> {backend}",
            data={'route': path, 'backend': backend}
        )
    
    def _add_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Add a backend server pool."""
        name = params.get('pool')
        servers = params.get('servers', [])
        strategy = params.get('strategy', 'round_robin')
        weights = params.get('weights', {})
        
        if not name or not servers:
            return ActionResult(
                success=False,
                message="pool and servers are required"
            )
        
        pool = BackendPool(
            name=name,
            servers=servers,
            strategy=LoadBalanceStrategy(strategy),
            weights=weights
        )
        
        self._pools[name] = pool
        self._round_robin_counters[name] = 0
        
        return ActionResult(
            success=True,
            message=f"Added pool '{name}' with {len(servers)} servers",
            data={'pool': name, 'servers': servers}
        )
    
    def _find_route(self, path: str, method: str) -> Optional[Route]:
        """Find best matching route for path and method."""
        key = f"{method}:{path}"
        
        # Exact match
        if key in self._routes:
            return self._routes[key]
        
        # Prefix match
        for route_key, route in self._routes.items():
            if route.method == method and path.startswith(route.path):
                return route
        
        return None
    
    def _select_server(self, pool: BackendPool) -> Optional[str]:
        """Select a server based on load balancing strategy."""
        servers = pool.servers
        if not servers:
            return None
        
        strategy = pool.strategy
        
        if strategy == LoadBalanceStrategy.ROUND_ROBIN:
            counter = self._round_robin_counters.get(pool.name, 0)
            server = servers[counter % len(servers)]
            self._round_robin_counters[pool.name] = counter + 1
            return server
        
        elif strategy == LoadBalanceStrategy.RANDOM:
            return choice(servers)
        
        elif strategy == LoadBalanceStrategy.WEIGHTED:
            weighted = []
            for server in servers:
                weight = pool.weights.get(server, 1)
                weighted.extend([server] * weight)
            return choice(weighted) if weighted else choice(servers)
        
        elif strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            min_connections = float('inf')
            selected = servers[0]
            for server in servers:
                connections = self._connection_counts.get(server, 0)
                if connections < min_connections:
                    min_connections = connections
                    selected = server
            self._connection_counts[selected] = min_connections + 1
            return selected
        
        elif strategy == LoadBalanceStrategy.IP_HASH:
            # Would need client IP from request
            return servers[0]
        
        return servers[0]
    
    def _transform_request(
        self,
        request: Dict,
        route: Route
    ) -> Dict:
        """Transform request before forwarding."""
        # Basic transformation - in real impl would do more
        return {
            'headers': request.get('headers', {}),
            'body': request.get('body'),
            'query': request.get('query', {}),
            'timeout': route.timeout
        }


class RequestTransformAction(BaseAction):
    """Transform requests and responses in the gateway."""
    action_type = "request_transform"
    display_name = "请求转换"
    description = "网关请求响应转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'transform_request', 'transform_response'
                - data: Data to transform
                - rules: Transformation rules
        
        Returns:
            ActionResult with transformed data.
        """
        operation = params.get('operation', 'transform_request').lower()
        data = params.get('data', {})
        rules = params.get('rules', {})
        
        if operation == 'transform_request':
            return self._transform_request(data, rules)
        elif operation == 'transform_response':
            return self._transform_response(data, rules)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _transform_request(
        self,
        data: Dict,
        rules: Dict
    ) -> ActionResult:
        """Transform outgoing request."""
        result = dict(data)
        
        # Header transformations
        headers = result.get('headers', {})
        header_map = rules.get('headers', {})
        for target, source in header_map.items():
            if source in headers:
                result.setdefault('headers', {})[target] = headers[source]
        
        # Path parameter substitution
        path = result.get('path', '')
        path_params = rules.get('path_params', {})
        for param, path_template in path_params.items():
            if f'{{{param}}}' in path_template:
                value = data.get(param)
                if value:
                    path = path_template.replace(f'{{{param}}}', str(value))
        result['path'] = path
        
        return ActionResult(
            success=True,
            message="Request transformed",
            data={'result': result}
        )
    
    def _transform_response(
        self,
        data: Dict,
        rules: Dict
    ) -> ActionResult:
        """Transform incoming response."""
        result = dict(data)
        
        # Field mapping
        field_map = rules.get('fields', {})
        for target, source in field_map.items():
            if source in data:
                result[target] = data[source]
        
        return ActionResult(
            success=True,
            message="Response transformed",
            data={'result': result}
        )
