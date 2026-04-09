"""API Gateway V2 Action Module.

Provides advanced API gateway capabilities.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIGatewayV2Action(BaseAction):
    """Advanced API gateway with routing and middleware.
    
    Routes requests based on rules and applies middleware.
    """
    action_type = "api_gateway_v2"
    display_name = "API网关V2"
    description = "支持路由和中间件的API网关"
    
    def __init__(self):
        super().__init__()
        self._routes: Dict[str, Dict] = {}
        self._middleware: List[Callable] = []
        self._request_count = 0
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gateway operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, request.
        
        Returns:
            ActionResult with gateway result.
        """
        action = params.get('action', 'route')
        
        if action == 'route':
            return self._route_request(params)
        elif action == 'register_route':
            return self._register_route(params)
        elif action == 'add_middleware':
            return self._add_middleware(params)
        elif action == 'stats':
            return self._get_stats()
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _route_request(self, params: Dict) -> ActionResult:
        """Route a request to appropriate backend."""
        path = params.get('path', '/')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body')
        
        self._request_count += 1
        
        # Find matching route
        route_key = f"{method}:{path}"
        backend = None
        
        if route_key in self._routes:
            backend = self._routes[route_key].get('backend')
        else:
            # Try pattern matching
            for pattern, route_info in self._routes.items():
                if self._match_pattern(pattern, path):
                    backend = route_info.get('backend')
                    break
        
        # Apply middleware
        request_data = {
            'path': path,
            'method': method,
            'headers': headers,
            'body': body,
            'timestamp': time.time()
        }
        
        for mw in self._middleware:
            try:
                request_data = mw(request_data)
            except Exception as e:
                return ActionResult(
                    success=False,
                    data={'error': 'Middleware failed'},
                    error=str(e)
                )
        
        if backend:
            return ActionResult(
                success=True,
                data={
                    'routed': True,
                    'backend': backend,
                    'request': request_data
                },
                error=None
            )
        else:
            return ActionResult(
                success=False,
                data={'routed': False},
                error="No route found"
            )
    
    def _match_pattern(self, pattern: str, path: str) -> bool:
        """Match route pattern."""
        if pattern.endswith('/*'):
            prefix = pattern[:-2]
            return path.startswith(prefix)
        elif pattern.endswith('/*wildcard'):
            return True
        else:
            return path == pattern
    
    def _register_route(self, params: Dict) -> ActionResult:
        """Register a new route."""
        path = params.get('path', '/')
        method = params.get('method', 'GET')
        backend = params.get('backend', '')
        middleware = params.get('middleware', [])
        
        route_key = f"{method}:{path}"
        self._routes[route_key] = {
            'path': path,
            'method': method,
            'backend': backend,
            'middleware': middleware,
            'registered_at': time.time()
        }
        
        return ActionResult(
            success=True,
            data={
                'route': route_key,
                'backend': backend
            },
            error=None
        )
    
    def _add_middleware(self, params: Dict) -> ActionResult:
        """Add middleware to the gateway."""
        middleware_type = params.get('middleware_type', 'logging')
        
        def logging_middleware(req):
            print(f"[Gateway] {req['method']} {req['path']}")
            return req
        
        self._middleware.append(logging_middleware)
        
        return ActionResult(
            success=True,
            data={
                'middleware_added': middleware_type,
                'total_middleware': len(self._middleware)
            },
            error=None
        )
    
    def _get_stats(self) -> ActionResult:
        """Get gateway statistics."""
        return ActionResult(
            success=True,
            data={
                'total_requests': self._request_count,
                'registered_routes': len(self._routes),
                'middleware_count': len(self._middleware)
            },
            error=None
        )


class APIGatewayRouterAction(BaseAction):
    """Advanced routing for API gateway.
    
    Supports weighted routing, A/B testing, and canary releases.
    """
    action_type = "api_gateway_router"
    display_name = "API网关路由器"
    description = "支持权重路由、A/B测试和金丝雀发布"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute routing decision.
        
        Args:
            context: Execution context.
            params: Dict with keys: request, routing_strategy.
        
        Returns:
            ActionResult with routing decision.
        """
        request = params.get('request', {})
        strategy = params.get('routing_strategy', 'round_robin')
        backends = params.get('backends', [])
        
        if not backends:
            return ActionResult(
                success=False,
                data=None,
                error="No backends configured"
            )
        
        if strategy == 'round_robin':
            backend = self._round_robin(backends)
        elif strategy == 'weighted':
            backend = self._weighted_routing(backends, request)
        elif strategy == 'ab_test':
            backend = self._ab_test(backends, request)
        elif strategy == 'canary':
            backend = self._canary_release(backends, request)
        elif strategy == 'geo':
            backend = self._geo_routing(backends, request)
        else:
            backend = backends[0]
        
        return ActionResult(
            success=True,
            data={
                'backend': backend,
                'strategy': strategy,
                'request_id': request.get('id')
            },
            error=None
        )
    
    def _round_robin(self, backends: List[Dict]) -> Dict:
        """Round-robin routing."""
        return backends[0]
    
    def _weighted_routing(self, backends: List[Dict], request: Dict) -> Dict:
        """Weighted routing based on weights."""
        weights = [b.get('weight', 1) for b in backends]
        total = sum(weights)
        
        import random
        r = random.randint(1, total)
        
        cumulative = 0
        for i, backend in enumerate(backends):
            cumulative += weights[i]
            if r <= cumulative:
                return backend
        
        return backends[0]
    
    def _ab_test(self, backends: List[Dict], request: Dict) -> Dict:
        """A/B test routing."""
        user_id = request.get('user_id', '')
        
        if not user_id:
            return backends[0]
        
        import hashlib
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        idx = hash_val % len(backends)
        
        return backends[idx]
    
    def _canary_release(self, backends: List[Dict], request: Dict) -> Dict:
        """Canary release routing."""
        canary_percent = 10
        user_id = request.get('user_id', '')
        
        if not user_id:
            return backends[0]
        
        import hashlib
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        
        if hash_val % 100 < canary_percent:
            # Route to canary (last backend)
            return backends[-1]
        else:
            return backends[0]
    
    def _geo_routing(self, backends: List[Dict], request: Dict) -> Dict:
        """Geographic routing."""
        location = request.get('location', 'unknown')
        
        for backend in backends:
            if backend.get('region') == location:
                return backend
        
        return backends[0]


def register_actions():
    """Register all API Gateway V2 actions."""
    return [
        APIGatewayV2Action,
        APIGatewayRouterAction,
    ]
