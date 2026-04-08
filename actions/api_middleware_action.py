"""
API Middleware Action - Middleware for API request/response processing.

This module provides middleware capabilities including
authentication, transformation, and request/response interception.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class MiddlewareConfig:
    """Configuration for middleware."""
    enabled: bool = True
    order: int = 0


@dataclass
class RequestContext:
    """Context for request processing."""
    method: str
    url: str
    headers: dict[str, str]
    params: dict[str, Any]
    body: Any | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResponseContext:
    """Context for response processing."""
    status_code: int
    headers: dict[str, str]
    body: Any
    duration_ms: float
    metadata: dict[str, Any] = field(default_factory=dict)


class Middleware:
    """Base middleware class."""
    
    def __init__(self, config: MiddlewareConfig | None = None) -> None:
        self.config = config or MiddlewareConfig()
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Process request."""
        return ctx
    
    async def process_response(self, ctx: ResponseContext) -> ResponseContext:
        """Process response."""
        return ctx


class AuthMiddleware(Middleware):
    """Authentication middleware."""
    
    def __init__(self, token: str, header: str = "Authorization") -> None:
        super().__init__()
        self.token = token
        self.header = header
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Add auth header to request."""
        ctx.headers[self.header] = f"Bearer {self.token}"
        return ctx


class HeaderMiddleware(Middleware):
    """Header transformation middleware."""
    
    def __init__(self, add_headers: dict[str, str] | None = None) -> None:
        super().__init__()
        self.add_headers = add_headers or {}
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Add headers to request."""
        ctx.headers.update(self.add_headers)
        return ctx
    
    async def process_response(self, ctx: ResponseContext) -> ResponseContext:
        """Remove headers from response."""
        return ctx


class TimingMiddleware(Middleware):
    """Request timing middleware."""
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Record request start time."""
        ctx.metadata["_start_time"] = time.time()
        return ctx
    
    async def process_response(self, ctx: ResponseContext) -> ResponseContext:
        """Calculate request duration."""
        start_time = ctx.metadata.get("_start_time", ctx.metadata.get("_start_time", time.time()))
        ctx.duration_ms = (time.time() - start_time) * 1000
        return ctx


class APIMiddlewareChain:
    """Chains multiple middleware together."""
    
    def __init__(self) -> None:
        self._middleware: list[Middleware] = []
    
    def add(self, middleware: Middleware) -> None:
        """Add middleware to chain."""
        self._middleware.append(middleware)
        self._middleware.sort(key=lambda m: m.config.order)
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Process request through middleware chain."""
        for m in self._middleware:
            if m.config.enabled:
                ctx = await m.process_request(ctx)
        return ctx
    
    async def process_response(self, ctx: ResponseContext) -> ResponseContext:
        """Process response through middleware chain."""
        for m in reversed(self._middleware):
            if m.config.enabled:
                ctx = await m.process_response(ctx)
        return ctx


class APIMiddlewareAction:
    """API middleware action for automation workflows."""
    
    def __init__(self) -> None:
        self.chain = APIMiddlewareChain()
    
    def add_auth(self, token: str, header: str = "Authorization") -> None:
        """Add authentication middleware."""
        self.chain.add(AuthMiddleware(token, header))
    
    def add_headers(self, headers: dict[str, str]) -> None:
        """Add header middleware."""
        self.chain.add(HeaderMiddleware(headers))
    
    def add_timing(self) -> None:
        """Add timing middleware."""
        self.chain.add(TimingMiddleware())
    
    async def process_request(self, ctx: RequestContext) -> RequestContext:
        """Process request through middleware."""
        return await self.chain.process_request(ctx)
    
    async def process_response(self, ctx: ResponseContext) -> ResponseContext:
        """Process response through middleware."""
        return await self.chain.process_response(ctx)


__all__ = ["MiddlewareConfig", "RequestContext", "ResponseContext", "Middleware", "AuthMiddleware", "HeaderMiddleware", "TimingMiddleware", "APIMiddlewareChain", "APIMiddlewareAction"]
