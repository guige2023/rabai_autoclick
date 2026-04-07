"""API Gateway utilities: routing, rate limiting, authentication, and request transformation."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Route",
    "APIGateway",
    "RateLimiter",
    "AuthMiddleware",
    "RequestTransformer",
]


@dataclass
class Route:
    """An API route definition."""

    path: str
    method: str
    handler: Callable[..., Any]
    middleware: list[Callable] = field(default_factory=list)
    requires_auth: bool = False
    rate_limit: int = 0
    description: str = ""

    def matches(self, method: str, path: str) -> bool:
        """Check if this route matches a method/path pair."""
        if self.method != method and self.method != "ANY":
            return False
        return self._match_path(self.path, path)

    def _match_path(self, pattern: str, path: str) -> bool:
        """Match a path pattern against an actual path."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")
        if len(pattern_parts) != len(path_parts):
            return False
        for p, a in zip(pattern_parts, path_parts):
            if p.startswith(":") or p.startswith("{"):
                continue
            if p != a:
                return False
        return True

    def extract_params(self, path: str) -> dict[str, str]:
        """Extract path parameters from the actual path."""
        pattern_parts = self.path.strip("/").split("/")
        path_parts = path.strip("/").split("/")
        params: dict[str, str] = {}
        for p, a in zip(pattern_parts, path_parts):
            if p.startswith(":") or p.startswith("{"):
                params[p.lstrip(":").lstrip("{").rstrip("}").rstrip("}")] = a
        return params


class APIGateway:
    """Lightweight API gateway with routing and middleware."""

    def __init__(self) -> None:
        self._routes: list[Route] = []
        self._middleware: list[Callable] = []

    def add_route(
        self,
        path: str,
        method: str,
        handler: Callable[..., Any],
        **kwargs,
    ) -> None:
        """Register a route."""
        route = Route(path=path, method=method, handler=handler, **kwargs)
        self._routes.append(route)

    def add_middleware(self, middleware: Callable) -> None:
        """Add global middleware."""
        self._middleware.append(middleware)

    def handle(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        query_params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Handle an incoming request."""
        headers = headers or {}
        body = body or b""

        for mw in self._middleware:
            result = mw(method=method, path=path, headers=headers, body=body)
            if result is not None:
                return result

        for route in self._routes:
            if route.matches(method, path):
                for mw in route.middleware:
                    result = mw(method=method, path=path, headers=headers, body=body)
                    if result is not None:
                        return result

                if route.requires_auth:
                    auth_result = self._check_auth(headers)
                    if auth_result is not None:
                        return auth_result

                params = route.extract_params(path)
                try:
                    parsed_body = json.loads(body.decode()) if body else {}
                except (json.JSONDecodeError, UnicodeDecodeError):
                    parsed_body = {}

                return route.handler(
                    method=method,
                    path=path,
                    params=params,
                    headers=headers,
                    body=parsed_body,
                    query=query_params or {},
                )

        return {"status": 404, "body": {"error": "Not Found"}}

    def _check_auth(self, headers: dict[str, str]) -> dict[str, Any] | None:
        """Check authentication from headers."""
        auth = headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return {"status": 401, "body": {"error": "Unauthorized"}}
        return None


class RateLimiter:
    """Token bucket rate limiter for API gateway."""

    def __init__(self, capacity: int = 100, refill_rate: float = 10.0) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._buckets: dict[str, dict[str, float]] = {}

    def _get_bucket(self, key: str) -> dict[str, float]:
        """Get or create a token bucket for a key."""
        if key not in self._buckets:
            self._buckets[key] = {
                "tokens": float(self.capacity),
                "last_refill": time.time(),
            }
        return self._buckets[key]

    def _refill(self, bucket: dict[str, float]) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket["last_refill"]
        bucket["tokens"] = min(
            self.capacity,
            bucket["tokens"] + elapsed * self.refill_rate,
        )
        bucket["last_refill"] = now

    def allow_request(self, key: str, cost: int = 1) -> bool:
        """Check if a request is allowed under the rate limit."""
        bucket = self._get_bucket(key)
        self._refill(bucket)

        if bucket["tokens"] >= cost:
            bucket["tokens"] -= cost
            return True
        return False

    def get_status(self, key: str) -> dict[str, float]:
        """Get rate limit status for a key."""
        bucket = self._get_bucket(key)
        self._refill(bucket)
        return {
            "tokens": bucket["tokens"],
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
        }


class AuthMiddleware:
    """Authentication middleware for API gateway."""

    def __init__(self, secret: str = "") -> None:
        self.secret = secret

    def __call__(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> dict[str, Any] | None:
        """Check for valid API key."""
        api_key = headers.get("X-API-Key", "")
        if api_key:
            expected = self._generate_expected_key()
            if not self._verify_key(api_key, expected):
                return {"status": 403, "body": {"error": "Invalid API key"}}
        return None

    def _generate_expected_key(self) -> str:
        return hashlib.sha256(self.secret.encode()).hexdigest()

    def _verify_key(self, provided: str, expected: str) -> bool:
        return provided == expected


class RequestTransformer:
    """Transform request/response data."""

    @staticmethod
    def snake_to_camel(data: dict[str, Any]) -> dict[str, Any]:
        """Convert snake_case keys to camelCase."""
        result: dict[str, Any] = {}
        for key, value in data.items():
            camel_key = "".join(
                word.capitalize() if i > 0 else word
                for i, word in enumerate(key.split("_"))
            )
            if isinstance(value, dict):
                result[camel_key] = RequestTransformer.snake_to_camel(value)
            elif isinstance(value, list):
                result[camel_key] = [
                    RequestTransformer.snake_to_camel(v) if isinstance(v, dict) else v
                    for v in value
                ]
            else:
                result[camel_key] = value
        return result

    @staticmethod
    def camel_to_snake(data: dict[str, Any]) -> dict[str, Any]:
        """Convert camelCase keys to snake_case."""
        result: dict[str, Any] = {}
        for key, value in data.items():
            snake_key = "".join(
                f"_{c.lower()}" if c.isupper() else c
                for c in key
            ).lstrip("_")
            if isinstance(value, dict):
                result[snake_key] = RequestTransformer.camel_to_snake(value)
            elif isinstance(value, list):
                result[snake_key] = [
                    RequestTransformer.camel_to_snake(v) if isinstance(v, dict) else v
                    for v in value
                ]
            else:
                result[snake_key] = value
        return result

    @staticmethod
    def filter_fields(data: dict[str, Any], allowed: list[str]) -> dict[str, Any]:
        """Filter data to only include allowed fields."""
        return {k: v for k, v in data.items() if k in allowed}
