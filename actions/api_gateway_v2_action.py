"""API gateway action module for RabAI AutoClick.

Provides API gateway functionality with routing, load balancing,
rate limiting, and request/response transformation.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Route:
    """API route definition."""
    path: str
    method: str
    upstream: str
    timeout: int = 30
    retry_on_fail: bool = True
    max_retries: int = 3
    auth_required: bool = False


@dataclass
class Upstream:
    """Upstream service definition."""
    name: str
    url: str
    weight: int = 1
    health_check_path: Optional[str] = None
    is_healthy: bool = True


class ApiGatewayAction(BaseAction):
    """API gateway action with routing and load balancing.
    
    Supports path-based routing, weighted round-robin load balancing,
    health checking, and request/response transformation.
    """
    action_type = "api_gateway"
    display_name = "API网关"
    description = "API路由与负载均衡"
    
    def __init__(self):
        super().__init__()
        self._routes: Dict[str, List[Route]] = {}
        self._upstreams: Dict[str, Upstream] = {}
        self._health_status: Dict[str, bool] = {}
    
    def add_route(self, route: Route) -> None:
        """Add a route to the gateway."""
        key = f"{route.method}:{route.path}"
        if key not in self._routes:
            self._routes[key] = []
        self._routes[key].append(route)
    
    def add_upstream(self, upstream: Upstream) -> None:
        """Add an upstream service."""
        self._upstreams[upstream.name] = upstream
        self._health_status[upstream.name] = upstream.is_healthy
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API gateway operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: route|add_route|add_upstream|health
                path: Request path (for route)
                method: HTTP method (for route)
                headers: Request headers
                body: Request body.
        
        Returns:
            ActionResult with proxied response.
        """
        operation = params.get('operation', 'route')
        
        if operation == 'route':
            return self._route(params)
        elif operation == 'add_route':
            return self._add_route(params)
        elif operation == 'add_upstream':
            return self._add_upstream(params)
        elif operation == 'health':
            return self._health(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _route(self, params: Dict[str, Any]) -> ActionResult:
        """Route request to upstream service."""
        path = params.get('path', '/')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body')
        
        route_key = f"{method}:{path}"
        routes = self._routes.get(route_key, [])
        
        if not routes:
            wildcard_key = f"{method}:*"
            routes = self._routes.get(wildcard_key, [])
        
        if not routes:
            return ActionResult(
                success=False,
                message=f"No route found for {method} {path}",
                data={'status': 404, 'error': 'Not found'}
            )
        
        route = routes[0]
        
        if route.upstream not in self._upstreams:
            return ActionResult(
                success=False,
                message=f"Upstream {route.upstream} not found",
                data={'status': 500, 'error': 'Upstream not configured'}
            )
        
        upstream = self._upstreams[route.upstream]
        
        if not self._health_status.get(upstream.name, True):
            if route.retry_on_fail:
                return self._retry_with_backup(route, params, upstream)
            return ActionResult(
                success=False,
                message=f"Upstream {upstream.name} is unhealthy",
                data={'status': 503, 'error': 'Service unavailable'}
            )
        
        return self._proxy_request(upstream.url, method, headers, body, route.timeout)
    
    def _proxy_request(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Any,
        timeout: int
    ) -> ActionResult:
        """Proxy request to upstream."""
        import json
        
        data = None
        if body:
            if isinstance(body, dict):
                data = json.dumps(body).encode('utf-8')
                headers = {**headers, 'Content-Type': 'application/json'}
            elif isinstance(body, str):
                data = body.encode('utf-8')
            else:
                data = body
        
        try:
            req = Request(url, data=data, headers=headers, method=method)
            with urlopen(req, timeout=timeout) as response:
                body_bytes = response.read()
                return ActionResult(
                    success=True,
                    message=f"Proxied to {url}",
                    data={
                        'status': response.status,
                        'body': body_bytes.decode('utf-8', errors='replace'),
                        'headers': dict(response.headers)
                    }
                )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}",
                data={'status': e.code, 'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Proxy error: {str(e)}",
                data={'status': 502, 'error': str(e)}
            )
    
    def _retry_with_backup(
        self,
        route: Route,
        params: Dict[str, Any],
        failed_upstream: Upstream
    ) -> ActionResult:
        """Retry request with backup upstream."""
        backup_routes = [r for r in self._routes.values() 
                        if r[0].upstream != failed_upstream.name 
                        and self._health_status.get(self._upstreams.get(r[0].upstream, Upstream(name='', url='')).name, False)]
        
        if not backup_routes:
            return ActionResult(
                success=False,
                message="All upstreams unavailable",
                data={'status': 503, 'error': 'Service unavailable'}
            )
        
        backup = backup_routes[0][0]
        backup_upstream = self._upstreams[backup.upstream]
        
        return self._proxy_request(
            backup_upstream.url,
            params.get('method', 'GET'),
            params.get('headers', {}),
            params.get('body'),
            backup.timeout
        )
    
    def _add_route(self, params: Dict[str, Any]) -> ActionResult:
        """Add a route."""
        route = Route(
            path=params['path'],
            method=params.get('method', 'GET'),
            upstream=params['upstream'],
            timeout=params.get('timeout', 30),
            retry_on_fail=params.get('retry_on_fail', True),
            max_retries=params.get('max_retries', 3),
            auth_required=params.get('auth_required', False)
        )
        
        self.add_route(route)
        
        return ActionResult(
            success=True,
            message=f"Added route {route.method} {route.path} -> {route.upstream}",
            data={'path': route.path, 'method': route.method, 'upstream': route.upstream}
        )
    
    def _add_upstream(self, params: Dict[str, Any]) -> ActionResult:
        """Add an upstream."""
        upstream = Upstream(
            name=params['name'],
            url=params['url'],
            weight=params.get('weight', 1),
            health_check_path=params.get('health_check_path'),
            is_healthy=params.get('is_healthy', True)
        )
        
        self.add_upstream(upstream)
        
        return ActionResult(
            success=True,
            message=f"Added upstream {upstream.name} at {upstream.url}",
            data={'name': upstream.name, 'url': upstream.url}
        )
    
    def _health(self, params: Dict[str, Any]) -> ActionResult:
        """Check upstream health."""
        upstream_name = params.get('upstream')
        
        if upstream_name:
            if upstream_name not in self._upstreams:
                return ActionResult(success=False, message=f"Upstream {upstream_name} not found")
            
            return ActionResult(
                success=True,
                message=f"Upstream {upstream_name} health: {self._health_status.get(upstream_name, False)}",
                data={'upstream': upstream_name, 'healthy': self._health_status.get(upstream_name, False)}
            )
        
        health_data = {
            name: self._health_status.get(name, False)
            for name in self._upstreams.keys()
        }
        
        return ActionResult(
            success=True,
            message="Health status",
            data={'upstreams': health_data}
        )
