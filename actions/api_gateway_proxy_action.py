"""API Gateway Proxy Action Module.

Provides API gateway capabilities including routing, authentication,
rate limiting, and request/response transformation.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class AuthType(Enum):
    """Authentication type."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"


@dataclass
class Route:
    """API route definition."""
    path: str
    method: str
    backend: str
    auth: AuthType
    rate_limit: Optional[str] = None
    timeout_seconds: float = 30.0
    transforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class APIKey:
    """API key information."""
    key: str
    name: str
    routes: List[str]
    rate_limit: Optional[str] = None
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class ProxyRequest:
    """Proxy request details."""
    path: str
    method: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[Any] = None
    auth: Optional[Dict[str, Any]] = None


@dataclass
class ProxyResponse:
    """Proxy response details."""
    status_code: int
    headers: Dict[str, str]
    body: Any
    from_cache: bool = False
    latency_ms: float = 0.0


class APIGatewayStore:
    """In-memory API gateway store."""

    def __init__(self):
        self._routes: List[Route] = []
        self._api_keys: Dict[str, APIKey] = {}
        self._cache: Dict[str, ProxyResponse] = {}
        self._request_counts: Dict[str, List[float]] = {}

    def add_route(self, route: Route) -> None:
        """Add route."""
        self._routes.append(route)

    def match_route(self, path: str, method: str) -> Optional[Route]:
        """Match route to path/method."""
        for route in self._routes:
            if route.method == method and self._path_matches(route.path, path):
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Simple path matching with params."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return False

        for p, a in zip(pattern_parts, path_parts):
            if p.startswith(":") or p == "*":
                continue
            if p != a:
                return False

        return True

    def add_api_key(self, key: str, name: str, routes: List[str],
                    rate_limit: Optional[str] = None) -> APIKey:
        """Add API key."""
        api_key = APIKey(key=key, name=name, routes=routes, rate_limit=rate_limit)
        self._api_keys[key] = api_key
        return api_key

    def get_api_key(self, key: str) -> Optional[APIKey]:
        """Get API key."""
        return self._api_keys.get(key)

    def check_rate_limit(self, key: str, limit: str) -> bool:
        """Check rate limit."""
        now = time.time()
        if key not in self._request_counts:
            self._request_counts[key] = []

        self._request_counts[key] = [
            t for t in self._request_counts[key]
            if now - t < 60
        ]

        max_requests = 100
        if limit == "low":
            max_requests = 10
        elif limit == "medium":
            max_requests = 100
        elif limit == "high":
            max_requests = 1000

        if len(self._request_counts[key]) >= max_requests:
            return False

        self._request_counts[key].append(now)
        return True

    def get_cached(self, key: str) -> Optional[ProxyResponse]:
        """Get cached response."""
        return self._cache.get(key)

    def set_cached(self, key: str, response: ProxyResponse, ttl_seconds: float = 60) -> None:
        """Cache response."""
        self._cache[key] = response


_global_gateway = APIGatewayStore()


class APIGatewayProxyAction:
    """API Gateway proxy action.

    Example:
        action = APIGatewayProxyAction()

        action.add_route("/api/users/:id", "GET", "http://users-service")
        action.add_api_key("sk_xxx", "MyApp", ["/api/*"])

        result = action.proxy(request)
    """

    def __init__(self, store: Optional[APIGatewayStore] = None):
        self._store = store or _global_gateway

    def add_route(self, path: str, method: str, backend: str,
                  auth: str = "none",
                  rate_limit: Optional[str] = None,
                  timeout_seconds: float = 30.0) -> Dict[str, Any]:
        """Add route."""
        try:
            auth_type = AuthType(auth)
        except ValueError:
            auth_type = AuthType.NONE

        route = Route(
            path=path,
            method=method,
            backend=backend,
            auth=auth_type,
            rate_limit=rate_limit,
            timeout_seconds=timeout_seconds
        )
        self._store.add_route(route)

        return {
            "success": True,
            "path": path,
            "method": method,
            "backend": backend,
            "message": f"Added route {method} {path}"
        }

    def add_api_key(self, key: str, name: str,
                   routes: Optional[List[str]] = None,
                   rate_limit: Optional[str] = None) -> Dict[str, Any]:
        """Add API key."""
        api_key = self._store.add_api_key(key, name, routes or ["*"], rate_limit)

        return {
            "success": True,
            "key_id": key[:8] + "...",
            "name": name,
            "routes": routes,
            "message": f"Added API key for {name}"
        }

    def proxy(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Proxy request to backend."""
        start = time.time()
        path = request.get("path", "/")
        method = request.get("method", "GET")
        headers = request.get("headers", {})
        api_key = headers.get("X-API-Key") or headers.get("Authorization", "").replace("Bearer ", "")

        route = self._store.match_route(path, method)
        if not route:
            return {
                "success": False,
                "status_code": 404,
                "message": "Route not found"
            }

        if route.auth != AuthType.NONE:
            if not api_key:
                return {
                    "success": False,
                    "status_code": 401,
                    "message": "Authentication required"
                }

            key_obj = self._store.get_api_key(api_key)
            if not key_obj or not key_obj.enabled:
                return {
                    "success": False,
                    "status_code": 401,
                    "message": "Invalid API key"
                }

        if route.rate_limit:
            if not self._store.check_rate_limit(api_key or "anonymous", route.rate_limit):
                return {
                    "success": False,
                    "status_code": 429,
                    "message": "Rate limit exceeded"
                }

        response = ProxyResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body={"simulated": True, "path": path, "backend": route.backend},
            latency_ms=(time.time() - start) * 1000
        )

        return {
            "success": True,
            "status_code": response.status_code,
            "headers": response.headers,
            "body": response.body,
            "latency_ms": response.latency_ms,
            "message": f"Proxied to {route.backend}"
        }

    def list_routes(self) -> Dict[str, Any]:
        """List all routes."""
        return {
            "success": True,
            "routes": [
                {
                    "path": r.path,
                    "method": r.method,
                    "backend": r.backend,
                    "auth": r.auth.value,
                    "timeout_seconds": r.timeout_seconds
                }
                for r in self._store._routes
            ],
            "count": len(self._store._routes)
        }

    def list_api_keys(self) -> Dict[str, Any]:
        """List API keys."""
        return {
            "success": True,
            "api_keys": [
                {
                    "name": k.name,
                    "key_id": k.key[:8] + "...",
                    "routes": k.routes,
                    "enabled": k.enabled
                }
                for k in self._store._api_keys.values()
            ],
            "count": len(self._store._api_keys)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute API gateway proxy action."""
    operation = params.get("operation", "")
    action = APIGatewayProxyAction()

    try:
        if operation == "add_route":
            path = params.get("path", "")
            method = params.get("method", "GET")
            backend = params.get("backend", "")
            if not path or not backend:
                return {"success": False, "message": "path and backend required"}
            return action.add_route(
                path=path,
                method=method,
                backend=backend,
                auth=params.get("auth", "none"),
                rate_limit=params.get("rate_limit"),
                timeout_seconds=params.get("timeout_seconds", 30.0)
            )

        elif operation == "add_api_key":
            key = params.get("key", "")
            name = params.get("name", "")
            if not key or not name:
                return {"success": False, "message": "key and name required"}
            return action.add_api_key(
                key=key,
                name=name,
                routes=params.get("routes"),
                rate_limit=params.get("rate_limit")
            )

        elif operation == "proxy":
            request = params.get("request", params)
            return action.proxy(request)

        elif operation == "list_routes":
            return action.list_routes()

        elif operation == "list_api_keys":
            return action.list_api_keys()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"API gateway error: {str(e)}"}
