"""API Gateway Action Module.

Provides unified API gateway with rate limiting, request routing,
and response caching for external service integration.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

import humps

T = TypeVar("T")


class RateLimitStrategy(Enum):
    """Rate limiting strategy."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET


@dataclass
class CacheEntry:
    """Cache entry with TTL."""
    value: Any
    expires_at: float


@dataclass
class RouteConfig:
    """API route configuration."""
    path: str
    method: str
    handler: Callable
    rate_limit: Optional[RateLimitConfig] = None
    cache_ttl: Optional[float] = None
    requires_auth: bool = False


class APIGatewayAction:
    """API Gateway with routing, rate limiting, and caching.

    Example:
        gateway = APIGatewayAction()
        gateway.register_route(RouteConfig(
            path="/api/v1/data",
            method="GET",
            handler=handle_data_request
        ))
        result = await gateway.handle_request("/api/v1/data", "GET")
    """

    def __init__(
        self,
        default_rate_limit: Optional[RateLimitConfig] = None,
        enable_caching: bool = True,
    ) -> None:
        self.routes: Dict[str, RouteConfig] = {}
        self.default_rate_limit = default_rate_limit or RateLimitConfig()
        self.enable_caching = enable_caching
        self._cache: Dict[str, CacheEntry] = {}
        self._token_buckets: Dict[str, float] = {}
        self._request_counts: Dict[str, List[float]] = {}
        self._middleware: List[Callable] = []

    def register_route(self, config: RouteConfig) -> None:
        """Register an API route."""
        key = f"{config.method}:{config.path}"
        self.routes[key] = config

    def add_middleware(self, middleware: Callable) -> None:
        """Add request middleware."""
        self._middleware.append(middleware)

    async def handle_request(
        self,
        path: str,
        method: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Handle incoming API request."""
        key = f"{method}:{path}"

        for mw in self._middleware:
            result = mw(path, method, data, headers)
            if result is not None:
                return result

        if key not in self.routes:
            return {
                "status": 404,
                "error": "Route not found",
                "path": path,
                "method": method,
            }

        route = self.routes[key]

        if route.requires_auth and not user_id:
            return {"status": 401, "error": "Authentication required"}

        if route.rate_limit:
            allowed = await self._check_rate_limit(
                key, route.rate_limit, user_id
            )
            if not allowed:
                return {
                    "status": 429,
                    "error": "Rate limit exceeded",
                    "retry_after": 1.0,
                }

        cache_key = self._make_cache_key(method, path, data)
        if self.enable_caching and route.cache_ttl:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return {"status": 200, "data": cached, "cached": True}

        try:
            result = await self._execute_handler(route.handler, data, headers)
            if self.enable_caching and route.cache_ttl:
                self._set_cache(cache_key, result, route.cache_ttl)
            return {"status": 200, "data": result, "cached": False}
        except Exception as e:
            return {"status": 500, "error": str(e)}

    async def _check_rate_limit(
        self,
        key: str,
        config: RateLimitConfig,
        user_id: Optional[str],
    ) -> bool:
        """Check rate limit for request."""
        bucket_key = user_id or key

        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            return await self._check_token_bucket(bucket_key, config)
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            return await self._check_sliding_window(bucket_key, config)
        return await self._check_fixed_window(bucket_key, config)

    async def _check_token_bucket(
        self, key: str, config: RateLimitConfig
    ) -> bool:
        """Token bucket rate limiting."""
        now = time.time()
        if key not in self._token_buckets:
            self._token_buckets[key] = now

        last_refill = self._token_buckets[key]
        elapsed = now - last_refill
        tokens_to_add = elapsed * config.requests_per_second
        available = min(
            config.burst_size,
            tokens_to_add
        )

        if available >= 1:
            self._token_buckets[key] = now
            return True
        return False

    async def _check_sliding_window(
        self, key: str, config: RateLimitConfig
    ) -> bool:
        """Sliding window rate limiting."""
        now = time.time()
        window = 1.0 / config.requests_per_second

        if key not in self._request_counts:
            self._request_counts[key] = []

        self._request_counts[key] = [
            t for t in self._request_counts[key]
            if now - t < window
        ]

        if len(self._request_counts[key]) < config.burst_size:
            self._request_counts[key].append(now)
            return True
        return False

    async def _check_fixed_window(
        self, key: str, config: RateLimitConfig
    ) -> bool:
        """Fixed window rate limiting."""
        now = time.time()
        window_start = int(now / window) * window

        if key not in self._request_counts:
            self._request_counts[key] = {}

        self._request_counts[key] = {
            k: v for k, v in self._request_counts[key].items()
            if k >= window_start
        }

        current_count = len(self._request_counts[key])
        if current_count < config.burst_size:
            self._request_counts[key][now] = True
            return True
        return False

    def _make_cache_key(
        self, method: str, path: str, data: Optional[Dict]
    ) -> str:
        """Generate cache key."""
        content = f"{method}:{path}:{humps.dumps(data) if data else ''}"
        return hashlib.md5(content.encode()).hexdigest()

    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached value."""
        if key not in self._cache:
            return None
        entry = self._cache[key]
        if time.time() > entry.expires_at:
            del self._cache[key]
            return None
        return entry.value

    def _set_cache(self, key: str, value: Any, ttl: float) -> None:
        """Set cache entry."""
        self._cache[key] = CacheEntry(
            value=value, expires_at=time.time() + ttl
        )

    async def _execute_handler(
        self,
        handler: Callable,
        data: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]],
    ) -> Any:
        """Execute route handler."""
        if asyncio.iscoroutinefunction(handler):
            return await handler(data, headers)
        return handler(data, headers)

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        return {
            "total_routes": len(self.routes),
            "cache_entries": len(self._cache),
            "active_rate_limits": len(self._token_buckets),
        }


# Backward compatibility alias
API Gateway = APIGatewayAction
