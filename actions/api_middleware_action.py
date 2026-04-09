"""
API Middleware Action Module.

Provides middleware components for API request/response processing
including authentication, caching, compression, and logging.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class MiddlewareType(Enum):
    """Types of middleware."""
    AUTH = "auth"
    CACHE = "cache"
    LOGGING = "logging"
    COMPRESSION = "compression"
    RATE_LIMIT = "rate_limit"
    VALIDATION = "validation"
    TRANSFORM = "transform"


@dataclass
class MiddlewareContext:
    """Context passed through middleware chain."""
    request: Dict[str, Any]
    response: Optional[Dict[str, Any]] = None
    state: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MiddlewareResult:
    """Result of middleware processing."""
    success: bool
    context: MiddlewareContext
    error: Optional[str] = None
    short_circuit: bool = False


class Middleware(ABC):
    """Abstract base class for middleware."""

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    async def process(self, ctx: MiddlewareContext) -> MiddlewareResult:
        """Process request/response through middleware."""
        pass

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        """Handle errors in middleware."""
        pass


class AuthMiddleware(Middleware):
    """Authentication middleware."""

    def __init__(
        self,
        auth_header: str = "Authorization",
        schemes: List[str] = None,
    ) -> None:
        super().__init__("auth")
        self.auth_header = auth_header
        self.schemes = schemes or ["Bearer", "Basic"]

    async def process(self, ctx: MiddlewareContext) -> MiddlewareResult:
        """Process authentication."""
        auth_header = ctx.request.get("headers", {}).get(self.auth_header)

        if not auth_header:
            return MiddlewareResult(
                success=False,
                context=ctx,
                error="Missing authorization header",
                short_circuit=True,
            )

        # Parse auth scheme
        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            return MiddlewareResult(
                success=False,
                context=ctx,
                error="Invalid authorization header format",
                short_circuit=True,
            )

        scheme, token = parts
        if scheme not in self.schemes:
            return MiddlewareResult(
                success=False,
                context=ctx,
                error=f"Unsupported auth scheme: {scheme}",
                short_circuit=True,
            )

        ctx.metadata["auth_scheme"] = scheme
        ctx.metadata["auth_token"] = token

        return MiddlewareResult(success=True, context=ctx)


class CacheMiddleware(Middleware):
    """Caching middleware for API responses."""

    def __init__(
        self,
        ttl: int = 300,
        max_size: int = 1000,
        key_func: Optional[Callable] = None,
    ) -> None:
        super().__init__("cache")
        self.ttl = ttl
        self.max_size = max_size
        self.key_func = key_func or self._default_key_func
        self._cache: Dict[str, tuple] = {}
        self._access_order: List[str] = []

    def _default_key_func(self, ctx: MiddlewareContext) -> str:
        """Generate cache key from request."""
        req = ctx.request
        key_data = f"{req.get('method', 'GET')}:{req.get('path', '/')}"
        return hashlib.md5(key_data.encode()).hexdigest()

    async def process(self, ctx: MiddlewareContext) -> MiddlewareResult:
        """Process caching."""
        key = self.key_func(ctx)

        if key in self._cache:
            cached_data, timestamp = self._cache[key]
            if time.time() - timestamp < self.ttl:
                ctx.response = cached_data
                ctx.metadata["cache_hit"] = True
                return MiddlewareResult(success=True, context=ctx)

        ctx.metadata["cache_key"] = key
        return MiddlewareResult(success=True, context=ctx)

    def store_response(self, ctx: MiddlewareContext) -> None:
        """Store response in cache."""
        key = ctx.metadata.get("cache_key")
        if key and ctx.response:
            if len(self._cache) >= self.max_size:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[key] = (ctx.response, time.time())
            self._access_order.append(key)


class LoggingMiddleware(Middleware):
    """Request/response logging middleware."""

    def __init__(self, log_body: bool = False) -> None:
        super().__init__("logging")
        self.log_body = log_body

    async def process(self, ctx: MiddlewareContext) -> MiddlewareResult:
        """Process logging."""
        req = ctx.request
        ctx.metadata["start_time"] = time.time()

        log_data = {
            "method": req.get("method"),
            "path": req.get("path"),
            "query_params": req.get("query_params"),
        }

        if self.log_body:
            log_data["body"] = req.get("body")

        ctx.metadata["log_data"] = log_data

        return MiddlewareResult(success=True, context=ctx)

    async def on_complete(self, ctx: MiddlewareContext) -> None:
        """Log completion."""
        start = ctx.metadata.get("start_time", time.time())
        duration = time.time() - start
        log_data = ctx.metadata.get("log_data", {})
        log_data["duration"] = duration
        log_data["response_status"] = ctx.response.get("status_code") if ctx.response else None
        # In production, send to logging service


class CompressionMiddleware(Middleware):
    """Request/response compression middleware."""

    def __init__(self, algorithms: List[str] = None) -> None:
        super().__init__("compression")
        self.algorithms = algorithms or ["gzip", "deflate"]

    async def process(self, ctx: MiddlewareContext) -> MiddlewareResult:
        """Process compression."""
        accept_encoding = ctx.request.get("headers", {}).get("Accept-Encoding", "")

        # Check if client accepts compression
        if not any(alg in accept_encoding for alg in self.algorithms):
            return MiddlewareResult(success=True, context=ctx)

        ctx.metadata["compression_enabled"] = True
        return MiddlewareResult(success=True, context=ctx)


class MiddlewareChain:
    """Chain of middleware processors."""

    def __init__(self) -> None:
        self.middlewares: List[Middleware] = []

    def add(self, middleware: Middleware) -> "MiddlewareChain":
        """Add middleware to chain."""
        self.middlewares.append(middleware)
        return self

    def insert(self, index: int, middleware: Middleware) -> "MiddlewareChain":
        """Insert middleware at specific position."""
        self.middlewares.insert(index, middleware)
        return self

    async def process(
        self,
        request: Dict[str, Any],
        handler: Callable,
    ) -> Dict[str, Any]:
        """Process request through middleware chain."""
        ctx = MiddlewareContext(request=request)

        # Process request through middleware
        for middleware in self.middlewares:
            result = await middleware.process(ctx)
            if not result.success or result.short_circuit:
                return self._error_response(result.error, 401 if "auth" in middleware.name else 400)

            if ctx.response:
                # Short-circuited with cached response
                return ctx.response

        # Call main handler
        try:
            if asyncio.iscoroutinefunction(handler):
                response = await handler(ctx)
            else:
                response = handler(ctx)

            ctx.response = response

            # Process response through middleware in reverse
            for middleware in reversed(self.middlewares):
                if hasattr(middleware, "on_complete"):
                    await middleware.on_complete(ctx)
                if isinstance(middleware, CacheMiddleware):
                    middleware.store_response(ctx)

            return ctx.response

        except Exception as e:
            # Error handling
            for middleware in reversed(self.middlewares):
                await middleware.on_error(ctx, e)
            raise e

    def _error_response(self, error: str, status_code: int) -> Dict[str, Any]:
        """Create error response."""
        return {
            "status_code": status_code,
            "body": {"error": error},
        }


class APIMiddleware:
    """Main API middleware manager."""

    def __init__(self) -> None:
        self.chain = MiddlewareChain()
        self.middleware_types: Dict[MiddlewareType, Middleware] = {}

    def use(self, middleware: Middleware) -> "APIMiddleware":
        """Add middleware to chain."""
        self.chain.add(middleware)
        return self

    def with_auth(
        self,
        auth_header: str = "Authorization",
    ) -> "APIMiddleware":
        """Add authentication middleware."""
        return self.use(AuthMiddleware(auth_header))

    def with_cache(
        self,
        ttl: int = 300,
    ) -> "APIMiddleware":
        """Add caching middleware."""
        cache = CacheMiddleware(ttl=ttl)
        self.middleware_types[MiddlewareType.CACHE] = cache
        return self.use(cache)

    def with_logging(
        self,
        log_body: bool = False,
    ) -> "APIMiddleware":
        """Add logging middleware."""
        return self.use(LoggingMiddleware(log_body))

    def with_compression(
        self,
        algorithms: List[str] = None,
    ) -> "APIMiddleware":
        """Add compression middleware."""
        return self.use(CompressionMiddleware(algorithms))

    async def handle(
        self,
        request: Dict[str, Any],
        handler: Callable,
    ) -> Dict[str, Any]:
        """Handle request through middleware chain."""
        return await self.chain.process(request, handler)
