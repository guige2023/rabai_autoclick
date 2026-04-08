"""API middleware action module for RabAI AutoClick.

Provides API middleware:
- MiddlewareChain: Chain middleware
- AuthMiddleware: Authentication middleware
- LoggingMiddleware: Logging middleware
- CompressionMiddleware: Compression middleware
- CORSMiddleware: CORS middleware
"""

import time
import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Request:
    """API request."""
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Any
    client_ip: str = ""


@dataclass
class Response:
    """API response."""
    status_code: int
    headers: Dict[str, str]
    body: Any
    duration: float = 0.0


MiddlewareFunc = Callable[[Request, Callable], Response]


class MiddlewareChain:
    """Chain of middleware."""

    def __init__(self):
        self._middleware: List[MiddlewareFunc] = []

    def add(self, middleware: MiddlewareFunc) -> "MiddlewareChain":
        """Add middleware to chain."""
        self._middleware.append(middleware)
        return self

    def execute(self, request: Request, handler: Callable) -> Response:
        """Execute middleware chain."""
        def chain(index: int) -> Response:
            if index >= len(self._middleware):
                return handler(request)

            middleware = self._middleware[index]

            def next_handler(req: Request) -> Response:
                return chain(index + 1)

            return middleware(req, next_handler)

        return chain(0)


class AuthMiddleware:
    """Authentication middleware."""

    def __init__(self, required_paths: Optional[List[str]] = None):
        self.required_paths = required_paths or []

    def __call__(self, request: Request, next_handler: Callable) -> Response:
        """Authenticate request."""
        for path in self.required_paths:
            if request.path.startswith(path):
                auth_header = request.headers.get("Authorization", "")
                if not auth_header:
                    return Response(
                        status_code=401,
                        headers={"Content-Type": "application/json"},
                        body={"error": "Authentication required"},
                    )
        return next_handler(request)


class LoggingMiddleware:
    """Logging middleware."""

    def __call__(self, request: Request, next_handler: Callable) -> Response:
        """Log request."""
        start_time = time.time()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {request.method} {request.path}")

        response = next_handler(request)

        duration = time.time() - start_time
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {request.method} {request.path} - {response.status_code} ({duration:.3f}s)")

        response.duration = duration
        return response


class CompressionMiddleware:
    """Compression middleware."""

    def __call__(self, request: Request, next_handler: Callable) -> Response:
        """Handle compression."""
        response = next_handler(request)

        accept_encoding = request.headers.get("Accept-Encoding", "")
        if "gzip" in accept_encoding and isinstance(response.body, str):
            import gzip
            body_bytes = response.body.encode()
            compressed = gzip.compress(body_bytes)
            response.headers["Content-Encoding"] = "gzip"
            response.body = compressed

        return response


class CORSMiddleware:
    """CORS middleware."""

    def __init__(
        self,
        allowed_origins: Optional[List[str]] = None,
        allowed_methods: Optional[List[str]] = None,
    ):
        self.allowed_origins = allowed_origins or ["*"]
        self.allowed_methods = allowed_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]

    def __call__(self, request: Request, next_handler: Callable) -> Response:
        """Handle CORS."""
        if request.method == "OPTIONS":
            return Response(
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": ", ".join(self.allowed_origins),
                    "Access-Control-Allow-Methods": ", ".join(self.allowed_methods),
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                },
                body="",
            )

        response = next_handler(request)

        origin = request.headers.get("Origin", "")
        if origin in self.allowed_origins or "*" in self.allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin or "*"

        return response


class RateLimitMiddleware:
    """Rate limiting middleware."""

    def __init__(self, max_requests: int = 100, window: float = 60.0):
        self.max_requests = max_requests
        self.window = window
        self._requests: Dict[str, List[float]] = {}

    def __call__(self, request: Request, next_handler: Callable) -> Response:
        """Rate limit request."""
        client_ip = request.client_ip or request.headers.get("X-Forwarded-For", "unknown")

        now = time.time()
        if client_ip not in self._requests:
            self._requests[client_ip] = []

        self._requests[client_ip] = [
            t for t in self._requests[client_ip]
            if now - t < self.window
        ]

        if len(self._requests[client_ip]) >= self.max_requests:
            return Response(
                status_code=429,
                headers={"Content-Type": "application/json"},
                body={"error": "Rate limit exceeded"},
            )

        self._requests[client_ip].append(now)
        return next_handler(request)


class APIMiddlewareAction(BaseAction):
    """API middleware action."""
    action_type = "api_middleware"
    display_name = "API中间件"
    description = "API中间件链管理"

    def __init__(self):
        super().__init__()
        self._chain = MiddlewareChain()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")

            if operation == "add":
                return self._add_middleware(params)
            elif operation == "list":
                return self._list_middleware()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Middleware error: {str(e)}")

    def _add_middleware(self, params: Dict) -> ActionResult:
        """Add middleware."""
        middleware_type = params.get("type", "logging").lower()

        if middleware_type == "auth":
            mw = AuthMiddleware(params.get("required_paths", []))
        elif middleware_type == "logging":
            mw = LoggingMiddleware()
        elif middleware_type == "cors":
            mw = CORSMiddleware(
                allowed_origins=params.get("allowed_origins"),
                allowed_methods=params.get("allowed_methods"),
            )
        elif middleware_type == "compression":
            mw = CompressionMiddleware()
        elif middleware_type == "ratelimit":
            mw = RateLimitMiddleware(
                max_requests=params.get("max_requests", 100),
                window=params.get("window", 60.0),
            )
        else:
            return ActionResult(success=False, message=f"Unknown middleware type: {middleware_type}")

        self._chain.add(mw)
        return ActionResult(success=True, message=f"Middleware '{middleware_type}' added")

    def _list_middleware(self) -> ActionResult:
        """List middleware."""
        count = len(self._chain._middleware)
        return ActionResult(success=True, message=f"{count} middleware in chain")
