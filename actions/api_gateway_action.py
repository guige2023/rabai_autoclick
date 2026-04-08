"""
API Gateway Action Module.

Provides unified API gateway functionality with routing, rate limiting,
authentication, and request/response transformation.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import hmac
import json
import time
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class RateLimitType(Enum):
    """Rate limiting algorithm types."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 100.0
    burst_size: int = 200
    limit_type: RateLimitType = RateLimitType.TOKEN_BUCKET


@dataclass
class RouteConfig:
    """Configuration for a single route."""
    path: str
    method: str
    handler: Callable
    auth_required: bool = False
    rate_limit: Optional[RateLimitConfig] = None
    timeout: float = 30.0
    retry_count: int = 0
    circuit_breaker: bool = False


@dataclass
class CircuitBreakerState:
    """State for circuit breaker pattern."""
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "closed"  # closed, open, half_open
    next_retry_time: Optional[datetime] = None


class TokenBucket:
    """Token bucket algorithm implementation."""

    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()

    def consume(self, tokens: int = 1) -> bool:
        """Attempt to consume tokens from the bucket."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class CircuitBreaker:
    """Circuit breaker for fault tolerance."""

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self.state = CircuitBreakerState()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state.state == "open":
            if self.state.next_retry_time and datetime.now() >= self.state.next_retry_time:
                self.state.state = "half_open"
                self.state.success_count = 0
            else:
                raise CircuitBreakerOpenError("Circuit breaker is open")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful call."""
        self.state.success_count += 1
        if self.state.state == "half_open":
            if self.state.success_count >= self.success_threshold:
                self.state.state = "closed"
                self.state.failure_count = 0
        elif self.state.state == "closed":
            self.state.failure_count = max(0, self.state.failure_count - 1)

    def _on_failure(self):
        """Handle failed call."""
        self.state.failure_count += 1
        self.state.last_failure_time = datetime.now()
        if self.state.failure_count >= self.failure_threshold:
            self.state.state = "open"
            self.state.next_retry_time = datetime.now() + timedelta(seconds=self.timeout)


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


@dataclass
class AuthToken:
    """Authentication token."""
    token: str
    user_id: str
    scopes: List[str]
    expires_at: datetime


class JWTAuthenticator:
    """JWT-based authentication."""

    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm

    def create_token(
        self,
        user_id: str,
        scopes: List[str],
        expires_in: int = 3600
    ) -> AuthToken:
        """Create a new authentication token."""
        expiry = datetime.now() + timedelta(seconds=expires_in)
        token_data = f"{user_id}:{','.join(scopes)}:{expiry.isoformat()}"
        signature = hmac.new(
            self.secret_key.encode(),
            token_data.encode(),
            hashlib.sha256
        ).hexdigest()
        token = f"{token_data}:{signature}"
        return AuthToken(token=token, user_id=user_id, scopes=scopes, expires_at=expiry)

    def verify_token(self, token: str) -> Optional[AuthToken]:
        """Verify and decode a token."""
        try:
            parts = token.split(":")
            if len(parts) != 4:
                return None
            user_id, scopes_str, expiry_str, signature = parts
            expected_sig = hmac.new(
                self.secret_key.encode(),
                f"{user_id}:{scopes_str}:{expiry_str}".encode(),
                hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(signature, expected_sig):
                return None
            expiry = datetime.fromisoformat(expiry_str)
            if expiry < datetime.now():
                return None
            return AuthToken(
                token=token,
                user_id=user_id,
                scopes=scopes_str.split(","),
                expires_at=expiry
            )
        except Exception:
            return None


class RequestTransformer:
    """Transform request/response data."""

    def transform_request(
        self,
        data: Dict[str, Any],
        transformers: List[Callable]
    ) -> Dict[str, Any]:
        """Apply a series of transformations to request data."""
        result = data.copy()
        for transformer in transformers:
            result = transformer(result)
        return result

    def transform_response(
        self,
        data: Any,
        formatters: List[Callable]
    ) -> Any:
        """Apply a series of formatters to response data."""
        result = data
        for formatter in formatters:
            result = formatter(result)
        return result


class APIRouter:
    """Main API Gateway Router."""

    def __init__(self):
        self.routes: Dict[str, RouteConfig] = {}
        self.middlewares: List[Callable] = []
        self.rate_limiters: Dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(100.0, 200)
        )
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.authenticator: Optional[JWTAuthenticator] = None
        self.transformer = RequestTransformer()

    def add_route(self, config: RouteConfig):
        """Add a new route configuration."""
        key = f"{config.method}:{config.path}"
        self.routes[key] = config
        if config.rate_limit:
            self.rate_limiters[key] = TokenBucket(
                config.rate_limit.requests_per_second,
                config.rate_limit.burst_size
            )
        if config.circuit_breaker:
            self.circuit_breakers[key] = CircuitBreaker()

    def set_authenticator(self, secret_key: str):
        """Set JWT authenticator."""
        self.authenticator = JWTAuthenticator(secret_key)

    def add_middleware(self, middleware: Callable):
        """Add middleware function."""
        self.middlewares.append(middleware)

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes] = None
    ) -> Dict[str, Any]:
        """Handle incoming API request."""
        route_key = f"{method}:{path}"
        route = self.routes.get(route_key)

        if not route:
            return {"status": 404, "error": "Route not found"}

        for middleware in self.middlewares:
            result = middleware(method, path, headers, body)
            if result is not None:
                return result

        if route.auth_required:
            token = headers.get("Authorization", "").replace("Bearer ", "")
            if self.authenticator:
                auth_token = self.authenticator.verify_token(token)
                if not auth_token:
                    return {"status": 401, "error": "Unauthorized"}
            else:
                return {"status": 401, "error": "Authentication not configured"}

        rate_limiter = self.rate_limiters.get(route_key)
        if rate_limiter and not rate_limiter.consume():
            return {"status": 429, "error": "Rate limit exceeded"}

        try:
            request_data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return {"status": 400, "error": "Invalid JSON"}

        if route.circuit_breaker and route_key in self.circuit_breakers:
            cb = self.circuit_breakers[route_key]
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(cb.call, route.handler, request_data),
                    timeout=route.timeout
                )
            except CircuitBreakerOpenError:
                return {"status": 503, "error": "Service unavailable"}
        else:
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(route.handler, request_data),
                    timeout=route.timeout
                )
            except asyncio.TimeoutError:
                return {"status": 504, "error": "Gateway timeout"}

        return {"status": 200, "data": result}

    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        return {
            "total_routes": len(self.routes),
            "middleware_count": len(self.middlewares),
            "rate_limiter_count": len(self.rate_limiters),
            "circuit_breaker_count": len(self.circuit_breakers),
            "authenticator_configured": self.authenticator is not None
        }


async def demo_handler(request: Dict[str, Any]) -> Dict[str, Any]:
    """Demo request handler."""
    await asyncio.sleep(0.01)
    return {"message": "Hello from API Gateway", "received": request}


async def main():
    """Demonstrate API Gateway functionality."""
    router = APIRouter()

    router.add_route(RouteConfig(
        path="/api/v1/hello",
        method="GET",
        handler=lambda r: {"message": "Hello"},
        rate_limit=RateLimitConfig(requests_per_second=10.0, burst_size=20)
    ))

    router.add_route(RouteConfig(
        path="/api/v1/echo",
        method="POST",
        handler=demo_handler,
        auth_required=True,
        circuit_breaker=True
    ))

    router.set_authenticator("super_secret_key_123")
    router.add_middleware(
        lambda m, p, h, b: None  # Allow all requests
    )

    response = await router.handle_request(
        "GET",
        "/api/v1/hello",
        {}
    )
    print(f"Response: {response}")

    response = await router.handle_request(
        "POST",
        "/api/v1/echo",
        {"Authorization": "Bearer user1:read,write:2026-12-31T23:59:59:signature"}
    )
    print(f"Response: {response}")

    print(f"Stats: {router.get_stats()}")


if __name__ == "__main__":
    asyncio.run(main())
