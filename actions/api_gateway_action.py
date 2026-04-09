"""
API gateway action for request routing and aggregation.

Provides routing, rate limiting, authentication, and response aggregation.
"""

from typing import Any, Dict, List, Optional
import time
import hashlib


class APIGatewayAction:
    """API Gateway for routing and request handling."""

    def __init__(
        self,
        default_rate_limit: int = 100,
        timeout: float = 30.0,
        enable_caching: bool = True,
    ) -> None:
        """
        Initialize API gateway.

        Args:
            default_rate_limit: Default requests per minute
            timeout: Request timeout in seconds
            enable_caching: Enable response caching
        """
        self.default_rate_limit = default_rate_limit
        self.timeout = timeout
        self.enable_caching = enable_caching

        self._routes: Dict[str, Dict[str, Any]] = {}
        self._policies: Dict[str, Dict[str, Any]] = {}
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._request_counts: Dict[str, List[float]] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute gateway operation.

        Args:
            params: Dictionary containing:
                - operation: 'route', 'add_route', 'add_policy', 'clear_cache'
                - path: Request path
                - method: HTTP method
                - request: Original request

        Returns:
            Dictionary with routing decision
        """
        operation = params.get("operation", "route")

        if operation == "route":
            return self._route_request(params)
        elif operation == "add_route":
            return self._add_route(params)
        elif operation == "add_policy":
            return self._add_policy(params)
        elif operation == "clear_cache":
            return self._clear_cache(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _add_route(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add route to gateway."""
        path = params.get("path", "")
        method = params.get("method", "GET")
        backend = params.get("backend", "")
        route_key = f"{method}:{path}"

        if not path or not backend:
            return {"success": False, "error": "path and backend are required"}

        self._routes[route_key] = {
            "path": path,
            "method": method,
            "backend": backend,
            "timeout": params.get("timeout", self.timeout),
            "rate_limit": params.get("rate_limit", self.default_rate_limit),
            "auth_required": params.get("auth_required", False),
            "cache_ttl": params.get("cache_ttl", 300),
            "added_at": time.time(),
        }

        return {"success": True, "route_key": route_key, "backend": backend}

    def _add_policy(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add gateway policy."""
        policy_name = params.get("policy_name", "")
        policy_type = params.get("policy_type", "rate_limit")
        policy_config = params.get("config", {})

        if not policy_name:
            return {"success": False, "error": "policy_name is required"}

        self._policies[policy_name] = {
            "type": policy_type,
            "config": policy_config,
            "added_at": time.time(),
        }

        return {"success": True, "policy_name": policy_name}

    def _route_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Route request to backend."""
        path = params.get("path", "")
        method = params.get("method", "GET")
        request = params.get("request", {})
        headers = params.get("headers", {})
        client_id = params.get("client_id", "default")

        route_key = f"{method}:{path}"
        route = self._routes.get(route_key)

        if not route:
            route = self._find_matching_route(method, path)
            if not route:
                return {"success": False, "error": "No route found", "path": path}

        if not self._check_rate_limit(route, client_id):
            return {"success": False, "error": "Rate limit exceeded", "status_code": 429}

        if route.get("auth_required"):
            if not self._verify_auth(headers):
                return {"success": False, "error": "Authentication required", "status_code": 401}

        cache_key = self._get_cache_key(method, path, request)
        if self.enable_caching and request.get("use_cache", True):
            cached = self._get_from_cache(cache_key, route.get("cache_ttl", 300))
            if cached:
                return {"success": True, "cached": True, **cached}

        result = self._forward_request(route, request)

        if result.get("success") and self.enable_caching:
            self._store_in_cache(cache_key, result, route.get("cache_ttl", 300))

        return result

    def _find_matching_route(self, method: str, path: str) -> Optional[Dict[str, Any]]:
        """Find route matching path pattern."""
        for route_key, route in self._routes.items():
            if route["method"] != method:
                continue
            if self._path_matches(route["path"], path):
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Check if path matches route pattern."""
        if pattern == path:
            return True
        if "/" not in pattern:
            return False

        pattern_parts = pattern.split("/")
        path_parts = path.split("/")

        for pp, pa in zip(pattern_parts, path_parts):
            if pp.startswith(":"):
                continue
            if pp != pa:
                return False
        return True

    def _check_rate_limit(self, route: Dict[str, Any], client_id: str) -> bool:
        """Check if request is within rate limit."""
        limit = route.get("rate_limit", self.default_rate_limit)
        now = time.time()
        window = 60.0

        if client_id not in self._request_counts:
            self._request_counts[client_id] = []

        self._request_counts[client_id] = [
            t for t in self._request_counts[client_id] if now - t < window
        ]

        if len(self._request_counts[client_id]) >= limit:
            return False

        self._request_counts[client_id].append(now)
        return True

    def _verify_auth(self, headers: Dict[str, str]) -> bool:
        """Verify authentication headers."""
        auth_header = headers.get("Authorization", "")
        api_key = headers.get("X-API-Key", "")
        return bool(auth_header or api_key)

    def _get_cache_key(self, method: str, path: str, request: dict[str, Any]) -> str:
        """Generate cache key."""
        data = f"{method}:{path}:{str(request)}"
        return hashlib.md5(data.encode()).hexdigest()

    def _get_from_cache(self, cache_key: str, ttl: int) -> Optional[Dict[str, Any]]:
        """Get response from cache."""
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if time.time() - entry["cached_at"] < ttl:
                return entry["response"]
            del self._cache[cache_key]
        return None

    def _store_in_cache(self, cache_key: str, response: Dict[str, Any], ttl: int) -> None:
        """Store response in cache."""
        self._cache[cache_key] = {
            "response": response,
            "cached_at": time.time(),
            "ttl": ttl,
        }

    def _forward_request(self, route: Dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
        """Forward request to backend."""
        return {
            "success": True,
            "backend": route["backend"],
            "path": route["path"],
            "status_code": 200,
            "body": {"message": "Response from backend"},
        }

    def _clear_cache(self, params: dict[str, Any]) -> dict[str, Any]:
        """Clear gateway cache."""
        count = len(self._cache)
        self._cache.clear()
        return {"success": True, "cleared_entries": count}

    def get_routes(self) -> List[Dict[str, Any]]:
        """Get all registered routes."""
        return [
            {
                "route_key": k,
                "path": v["path"],
                "method": v["method"],
                "backend": v["backend"],
            }
            for k, v in self._routes.items()
        ]

    def get_stats(self) -> dict[str, Any]:
        """Get gateway statistics."""
        return {
            "total_routes": len(self._routes),
            "total_policies": len(self._policies),
            "cached_responses": len(self._cache),
            "tracked_clients": len(self._request_counts),
        }
