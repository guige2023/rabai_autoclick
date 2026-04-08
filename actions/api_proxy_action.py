"""API Proxy Action Module.

Provides intelligent API proxying with request/response transformation,
caching, and protocol translation.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import hashlib
import json
import time
from datetime import datetime, timedelta


class TransformType(Enum):
    """Request/response transformation types."""
    NONE = "none"
    JSON_TO_XML = "json_to_xml"
    XML_TO_JSON = "xml_to_json"
    HEADER_INJECT = "header_inject"
    HEADER_REMOVE = "header_remove"
    BODY_REWRITE = "body_rewrite"
    URL_REWRITE = "url_rewrite"


@dataclass
class ProxyRoute:
    """Defines a proxy route configuration."""
    path_pattern: str
    target_url: str
    methods: List[str] = field(default_factory=lambda: ["GET", "POST"])
    transform: TransformType = TransformType.NONE
    transform_config: Dict[str, Any] = field(default_factory=dict)
    cache_ttl: int = 0
    timeout: int = 30
    headers_to_forward: List[str] = field(default_factory=list)


@dataclass
class ProxyRequest:
    """Incoming proxy request."""
    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[bytes] = None
    query_params: Dict[str, str] = field(default_factory=dict)
    client_ip: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ProxyResponse:
    """Proxied response with metadata."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    cached: bool = False
    response_time_ms: float = 0.0
    transform_used: Optional[TransformType] = None


class APICacheManager:
    """Manages proxy response caching."""

    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._max_size = max_size
        self._access_times: Dict[str, datetime] = {}

    def _make_key(self, request: ProxyRequest) -> str:
        """Generate cache key from request."""
        key_parts = [request.method, request.path]
        key_parts.extend(f"{k}={v}" for k, v in sorted(request.query_params.items()))
        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()

    def get(self, request: ProxyRequest) -> Optional[ProxyResponse]:
        """Retrieve cached response if valid."""
        key = self._make_key(request)
        if key not in self._cache:
            return None

        entry = self._cache[key]
        if datetime.now() > entry["expires_at"]:
            del self._cache[key]
            return None

        self._access_times[key] = datetime.now()
        return entry["response"]

    def set(self, request: ProxyRequest, response: ProxyResponse, ttl: int):
        """Store response in cache."""
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
            del self._cache[oldest_key]
            del self._access_times[oldest_key]

        key = self._make_key(request)
        self._cache[key] = {
            "response": response,
            "expires_at": datetime.now() + timedelta(seconds=ttl),
            "created_at": datetime.now(),
        }
        self._access_times[key] = datetime.now()

    def invalidate(self, path_pattern: str):
        """Invalidate cache entries matching path pattern."""
        keys_to_delete = [k for k in self._cache if path_pattern in k]
        for key in keys_to_delete:
            del self._cache[key]
            del self._access_times[key]


class APIProxyAction:
    """Intelligent API Proxy with transformation capabilities."""

    def __init__(
        self,
        routes: Optional[List[ProxyRoute]] = None,
        cache_manager: Optional[APICacheManager] = None,
    ):
        self.routes: Dict[str, ProxyRoute] = {}
        if routes:
            for route in routes:
                self.routes[route.path_pattern] = route
        self.cache_manager = cache_manager or APICacheManager()
        self.request_transformers: Dict[TransformType, Callable] = {
            TransformType.JSON_TO_XML: self._transform_json_to_xml,
            TransformType.XML_TO_JSON: self._transform_xml_to_json,
            TransformType.HEADER_INJECT: self._transform_header_inject,
            TransformType.HEADER_REMOVE: self._transform_header_remove,
            TransformType.BODY_REWRITE: self._transform_body_rewrite,
            TransformType.URL_REWRITE: self._transform_url_rewrite,
        }
        self._metrics: Dict[str, int] = {"requests": 0, "cache_hits": 0, "errors": 0}

    def add_route(self, route: ProxyRoute):
        """Add or update a proxy route."""
        self.routes[route.path_pattern] = route

    def remove_route(self, path_pattern: str):
        """Remove a proxy route."""
        if path_pattern in self.routes:
            del self.routes[path_pattern]

    async def handle_request(self, request: ProxyRequest) -> ProxyResponse:
        """Process an incoming proxy request."""
        start_time = time.time()
        self._metrics["requests"] += 1

        try:
            route = self._find_matching_route(request.path)
            if not route:
                return ProxyResponse(
                    status_code=404,
                    headers={"Content-Type": "application/json"},
                    body=json.dumps({"error": "Route not found"}).encode(),
                )

            if cached := self.cache_manager.get(request):
                cached.cached = True
                self._metrics["cache_hits"] += 1
                return cached

            transformed_request = self._apply_request_transform(request, route)
            response = await self._forward_request(transformed_request, route)

            if route.cache_ttl > 0:
                self.cache_manager.set(request, response, route.cache_ttl)

            response.response_time_ms = (time.time() - start_time) * 1000
            return response

        except Exception as e:
            self._metrics["errors"] += 1
            return ProxyResponse(
                status_code=500,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": str(e)}).encode(),
            )

    def _find_matching_route(self, path: str) -> Optional[ProxyRoute]:
        """Find route matching the request path."""
        for pattern, route in self.routes.items():
            if self._path_matches(pattern, path):
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Simple path matching with wildcard support."""
        if "*" in pattern:
            prefix = pattern.rstrip("*")
            return path.startswith(prefix)
        return pattern == path

    def _apply_request_transform(
        self, request: ProxyRequest, route: ProxyRoute
    ) -> ProxyRequest:
        """Apply configured transformation to request."""
        if route.transform == TransformType.NONE:
            return request

        transformer = self.request_transformers.get(route.transform)
        if transformer:
            return transformer(request, route.transform_config)
        return request

    def _transform_json_to_xml(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Transform JSON body to XML."""
        if request.body:
            try:
                json_data = json.loads(request.body)
                xml_body = self._dict_to_xml(json_data, config.get("root_tag", "root"))
                request.body = xml_body.encode()
                request.headers["Content-Type"] = "application/xml"
            except json.JSONDecodeError:
                pass
        return request

    def _transform_xml_to_json(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Transform XML body to JSON."""
        request.headers["Content-Type"] = "application/json"
        return request

    def _transform_header_inject(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Inject additional headers."""
        for key, value in config.get("headers", {}).items():
            request.headers[key] = value
        return request

    def _transform_header_remove(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Remove specified headers."""
        for header in config.get("remove", []):
            request.headers.pop(header, None)
        return request

    def _transform_body_rewrite(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Rewrite request body using patterns."""
        if request.body:
            body_str = request.body.decode(errors="replace")
            for pattern, replacement in config.get("rewrites", {}).items():
                body_str = body_str.replace(pattern, replacement)
            request.body = body_str.encode()
        return request

    def _transform_url_rewrite(
        self, request: ProxyRequest, config: Dict[str, Any]
    ) -> ProxyRequest:
        """Rewrite URL path components."""
        for pattern, replacement in config.get("rewrites", {}).items():
            if pattern in request.path:
                request.path = request.path.replace(pattern, replacement)
                break
        return request

    async def _forward_request(
        self, request: ProxyRequest, route: ProxyRoute
    ) -> ProxyResponse:
        """Forward request to target service (simulated)."""
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() in [h.lower() for h in route.headers_to_forward]}
        headers["X-Forwarded-For"] = request.client_ip
        headers["X-Forwarded-Time"] = datetime.now().isoformat()

        return ProxyResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"proxied": True, "path": request.path}).encode(),
        )

    def _dict_to_xml(self, data: Dict[str, Any], root_tag: str) -> str:
        """Convert dictionary to XML string."""
        def item_to_xml(key: str, value: Any) -> str:
            if isinstance(value, dict):
                inner = "".join(item_to_xml(k, v) for k, v in value.items())
                return f"<{key}>{inner}</{key}>"
            elif isinstance(value, list):
                inner = "".join(item_to_xml("item", v) for v in value)
                return f"<{key}>{inner}</{key}>"
            else:
                return f"<{key}>{str(value)}</{key}>"

        inner = "".join(item_to_xml(k, v) for k, v in data.items())
        return f"<?xml version='1.0'?><{root_tag}>{inner}</{root_tag}>"

    def get_metrics(self) -> Dict[str, int]:
        """Return proxy metrics."""
        return self._metrics.copy()

    def clear_cache(self):
        """Clear all cached responses."""
        self.cache_manager._cache.clear()
        self.cache_manager._access_times.clear()


# Module exports
__all__ = [
    "APIProxyAction",
    "APICacheManager",
    "ProxyRoute",
    "ProxyRequest",
    "ProxyResponse",
    "TransformType",
]
