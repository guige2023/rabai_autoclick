"""API gateway proxy action module for RabAI AutoClick.

Provides API gateway proxy operations:
- ProxyRouterAction: Route requests through gateway
- ProxyLoadBalancerAction: Load balance across backends
- ProxyCacheAction: Cache gateway responses
- ProxyMonitorAction: Monitor gateway traffic
"""

import sys
import os
import time
import logging
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class Backend:
    """Backend server definition."""
    backend_id: str
    host: str
    port: int
    weight: int = 1
    max_requests: int = 1000
    current_requests: int = 0
    is_healthy: bool = True
    last_check: Optional[datetime] = None
    avg_latency: float = 0.0


@dataclass
class Route:
    """Gateway route definition."""
    route_id: str
    path_pattern: str
    backend_id: str
    methods: List[str] = field(default_factory=lambda: ["GET"])
    requires_auth: bool = False
    rate_limit: int = 100


@dataclass
class CachedResponse:
    """Cached HTTP response."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    cached_at: datetime
    expires_at: datetime


class LoadBalancer:
    """Load balancing strategies."""

    @staticmethod
    def round_robin(backends: List[Backend], index: Dict[str, int]) -> Backend:
        available = [b for b in backends if b.is_healthy]
        if not available:
            raise Exception("No healthy backends available")
        idx = index.get("round_robin", 0) % len(available)
        index["round_robin"] = idx + 1
        return available[idx]

    @staticmethod
    def weighted_round_robin(backends: List[Backend], index: Dict[str, int]) -> Backend:
        available = [b for b in backends if b.is_healthy and b.current_requests < b.max_requests]
        if not available:
            raise Exception("No healthy backends available")
        total_weight = sum(b.weight for b in available)
        idx = index.get("weighted_rr", 0) % total_weight
        current = 0
        for backend in available:
            current += backend.weight
            if idx < current:
                index["weighted_rr"] = index.get("weighted_rr", 0) + 1
                return backend
        return available[0]

    @staticmethod
    def least_connections(backends: List[Backend]) -> Backend:
        available = [b for b in backends if b.is_healthy and b.current_requests < b.max_requests]
        if not available:
            raise Exception("No healthy backends available")
        return min(available, key=lambda b: b.current_requests)


class ResponseCache:
    """In-memory response cache."""

    def __init__(self, max_size: int = 1000, default_ttl: int = 300) -> None:
        self._cache: Dict[str, CachedResponse] = {}
        self._lock = threading.Lock()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[CachedResponse]:
        with self._lock:
            if key in self._cache:
                if datetime.now() < self._cache[key].expires_at:
                    self._hits += 1
                    return self._cache[key]
                del self._cache[key]
            self._misses += 1
            return None

    def set(self, key: str, response: CachedResponse, ttl: Optional[int] = None) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].cached_at)
                del self._cache[oldest[0]]
            ttl = ttl or self._default_ttl
            response.expires_at = datetime.now() + timedelta(seconds=ttl)
            self._cache[key] = response

    def invalidate(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            return {
                "size": len(self._cache),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": round(hit_rate, 2)
            }


_backends: Dict[str, Backend] = {}
_routes: Dict[str, Route] = {}
_cache = ResponseCache()
_lb_index: Dict[str, int] = defaultdict(int)
_stats: Dict[str, Any] = {"requests": 0, "errors": 0, "latency_sum": 0.0}


class ProxyRouterAction(BaseAction):
    """Route requests through gateway."""
    action_type = "api_gateway_proxy_router"
    display_name = "网关路由"
    description = "通过API网关路由请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "route")
        route_id = params.get("route_id", "")
        path = params.get("path", "/")
        method = params.get("method", "GET")

        if operation == "add_route":
            if not route_id:
                return ActionResult(success=False, message="route_id是必需的")

            route = Route(
                route_id=route_id,
                path_pattern=path,
                backend_id=params.get("backend_id", ""),
                methods=params.get("methods", ["GET"]),
                requires_auth=params.get("requires_auth", False),
                rate_limit=params.get("rate_limit", 100)
            )
            _routes[route_id] = route
            return ActionResult(success=True, message=f"路由 {route_id} 已添加", data={"route_id": route_id})

        if operation == "route":
            for route in _routes.values():
                if path.startswith(route.path_pattern):
                    backend = _backends.get(route.backend_id)
                    if backend and backend.is_healthy:
                        _stats["requests"] += 1
                        return ActionResult(
                            success=True,
                            message=f"路由到 {backend.host}:{backend.port}",
                            data={"backend": backend.backend_id, "route": route.route_id}
                        )
            return ActionResult(success=False, message="无可用路由")

        if operation == "list":
            return ActionResult(
                success=True,
                message=f"共 {len(_routes)} 个路由",
                data={"routes": [{"id": r.route_id, "path": r.path_pattern, "backend": r.backend_id} for r in _routes.values()]}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ProxyLoadBalancerAction(BaseAction):
    """Load balance across backends."""
    action_type = "api_gateway_proxy_loadbalancer"
    display_name = "网关负载均衡"
    description = "在多个后端之间进行负载均衡"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "balance")
        backend_id = params.get("backend_id", "")
        strategy = params.get("strategy", "round_robin")

        if operation == "add":
            if not backend_id:
                return ActionResult(success=False, message="backend_id是必需的")

            backend = Backend(
                backend_id=backend_id,
                host=params.get("host", "localhost"),
                port=params.get("port", 8000),
                weight=params.get("weight", 1)
            )
            _backends[backend_id] = backend
            return ActionResult(success=True, message=f"后端 {backend_id} 已添加", data={"backend_id": backend_id})

        if operation == "balance":
            available = [b for b in _backends.values() if b.is_healthy]
            if not available:
                return ActionResult(success=False, message="没有健康的后端")

            if strategy == "round_robin":
                backend = LoadBalancer.round_robin(available, _lb_index)
            elif strategy == "weighted":
                backend = LoadBalancer.weighted_round_robin(available, _lb_index)
            elif strategy == "least_conn":
                backend = LoadBalancer.least_connections(available)
            else:
                backend = LoadBalancer.round_robin(available, _lb_index)

            return ActionResult(
                success=True,
                message=f"选择后端 {backend.backend_id}",
                data={"backend": backend.backend_id, "host": backend.host, "port": backend.port}
            )

        if operation == "status":
            if backend_id:
                b = _backends.get(backend_id)
                if not b:
                    return ActionResult(success=False, message=f"后端 {backend_id} 不存在")
                return ActionResult(
                    success=True,
                    message=f"后端 {backend_id}: healthy={b.is_healthy}, requests={b.current_requests}",
                    data={"backend_id": b.backend_id, "healthy": b.is_healthy, "requests": b.current_requests}
                )

            return ActionResult(
                success=True,
                message=f"共 {len(_backends)} 个后端",
                data={"backends": [{"id": b.backend_id, "healthy": b.is_healthy, "requests": b.current_requests} for b in _backends.values()]}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ProxyCacheAction(BaseAction):
    """Cache gateway responses."""
    action_type = "api_gateway_proxy_cache"
    display_name = "网关缓存"
    description = "缓存API网关响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "get")
        cache_key = params.get("cache_key", "")
        ttl = params.get("ttl", 300)

        if operation == "get":
            cached = _cache.get(cache_key)
            if cached:
                return ActionResult(
                    success=True,
                    message="缓存命中",
                    data={"status_code": cached.status_code, "body_size": len(cached.body)}
                )
            return ActionResult(success=False, message="缓存未命中", data={"cached": False})

        if operation == "set":
            response = CachedResponse(
                status_code=params.get("status_code", 200),
                headers=params.get("headers", {}),
                body=params.get("body", b"").encode() if isinstance(params.get("body"), str) else params.get("body", b""),
                cached_at=datetime.now(),
                expires_at=datetime.now() + timedelta(seconds=ttl)
            )
            _cache.set(cache_key, response, ttl)
            return ActionResult(success=True, message=f"响应已缓存，TTL={ttl}s")

        if operation == "invalidate":
            if _cache.invalidate(cache_key):
                return ActionResult(success=True, message=f"缓存 {cache_key} 已失效")
            return ActionResult(success=False, message=f"缓存 {cache_key} 不存在")

        if operation == "stats":
            stats = _cache.get_stats()
            return ActionResult(success=True, message="缓存统计", data=stats)

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ProxyMonitorAction(BaseAction):
    """Monitor gateway traffic."""
    action_type = "api_gateway_proxy_monitor"
    display_name = "网关监控"
    description = "监控API网关流量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "stats")

        if operation == "stats":
            total = _stats["requests"]
            errors = _stats["errors"]
            avg_latency = _stats["latency_sum"] / total if total > 0 else 0

            return ActionResult(
                success=True,
                message=f"网关统计: {total} 请求, {errors} 错误",
                data={
                    "total_requests": total,
                    "errors": errors,
                    "error_rate_percent": round(errors / total * 100, 2) if total > 0 else 0,
                    "avg_latency_ms": round(avg_latency, 4)
                }
            )

        if operation == "reset":
            _stats["requests"] = 0
            _stats["errors"] = 0
            _stats["latency_sum"] = 0.0
            return ActionResult(success=True, message="统计已重置")

        if operation == "backends":
            summary = [
                {
                    "id": b.backend_id,
                    "healthy": b.is_healthy,
                    "current_requests": b.current_requests,
                    "avg_latency": b.avg_latency
                }
                for b in _backends.values()
            ]
            return ActionResult(success=True, message=f"后端健康状况: {len(summary)}", data={"backends": summary})

        return ActionResult(success=False, message=f"未知操作: {operation}")
