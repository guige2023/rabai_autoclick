"""
API Adapter Action Module

Provides adapter pattern implementation for API integration,
allowing transparent protocol translation and endpoint routing.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    TypeVar,
    Union,
)

import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class HTTPMethod(Enum):
    """HTTP methods supported by the adapter."""

    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


class AuthType(Enum):
    """Authentication types supported by the adapter."""

    NONE = auto()
    API_KEY = auto()
    Bearer = auto()
    BASIC = auto()
    OAUTH2 = auto()
    HMAC = auto()
    CUSTOM = auto()


@dataclass
class AdapterConfig:
    """Configuration for API adapter behavior."""

    base_url: str
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.5
    auth_type: AuthType = AuthType.NONE
    auth_credentials: Optional[Dict[str, str]] = None
    headers: Dict[str, str] = field(default_factory=dict)
    ssl_verify: bool = True
    rate_limit_per_second: Optional[float] = None
    cache_enabled: bool = False
    cache_ttl: float = 300.0


@dataclass
class RequestContext:
    """Context for an API request."""

    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    method: HTTPMethod = HTTPMethod.GET
    endpoint: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    data: Optional[Dict[str, Any]] = None
    headers: Dict[str, str] = field(default_factory=dict)
    auth_token: Optional[str] = None


@dataclass
class ResponseContext:
    """Context for an API response."""

    request_id: str
    status_code: int
    headers: Dict[str, str]
    data: Any
    elapsed_ms: float
    cached: bool = False
    error: Optional[str] = None


class CacheStrategy(Protocol[T]):
    """Protocol for cache strategies."""

    def get(self, key: str) -> Optional[T]: ...
    def set(self, key: str, value: T, ttl: float) -> None: ...
    def delete(self, key: str) -> None: ...
    def clear(self) -> None: ...


class InMemoryCache(Generic[T]):
    """Simple in-memory cache with TTL support."""

    def __init__(self) -> None:
        self._cache: Dict[str, tuple[T, float]] = {}
        self._lock = asyncio.Lock()

    def get(self, key: str) -> Optional[T]:
        """Get cached value if not expired."""
        if key in self._cache:
            value, expiry = self._cache[key]
            if time.time() < expiry:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: T, ttl: float) -> None:
        """Set cache value with TTL."""
        self._cache[key] = (value, time.time() + ttl)

    def delete(self, key: str) -> None:
        """Delete cached value."""
        self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached values."""
        self._cache.clear()


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._tokens: float = rate
        self._last_update: float = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire a token, waiting if necessary."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_update = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    async def wait_for_token(self) -> None:
        """Wait until a token is available."""
        while True:
            if await self.acquire():
                return
            await asyncio.sleep(0.01)


class Transformer(Protocol[T, U]):
    """Protocol for request/response transformers."""

    def transform(self, data: T) -> U: ...


class JSONTransformer:
    """JSON serialization/deserialization transformer."""

    @staticmethod
    def serialize(data: Any) -> bytes:
        """Serialize data to JSON bytes."""
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """Deserialize JSON bytes to Python object."""
        return json.loads(data.decode("utf-8"))


@dataclass
class Endpoint:
    """API endpoint definition."""

    path: str
    method: HTTPMethod
    auth_required: bool = True
    rate_limited: bool = False
    cacheable: bool = False
    cache_ttl: float = 300.0


class APIAdapter:
    """
    Main API adapter providing unified interface for API operations.

    Features:
    - Multiple authentication strategies
    - Automatic retry with exponential backoff
    - Response caching
    - Rate limiting
    - Request/response transformation
    - Endpoint routing
    """

    def __init__(self, config: AdapterConfig) -> None:
        self._config = config
        self._cache: Optional[InMemoryCache[Any]] = (
            InMemoryCache() if config.cache_enabled else None
        )
        self._rate_limiter: Optional[RateLimiter] = (
            RateLimiter(config.rate_limit_per_second)
            if config.rate_limit_per_second
            else None
        )
        self._transformer = JSONTransformer()
        self._endpoints: Dict[str, Endpoint] = {}
        self._middleware: List[Callable[..., Awaitable[Any]]] = []

    def register_endpoint(self, endpoint: Endpoint) -> None:
        """Register an endpoint with the adapter."""
        key = f"{endpoint.method.name}:{endpoint.path}"
        self._endpoints[key] = endpoint

    def add_middleware(
        self, middleware: Callable[..., Awaitable[Any]]
    ) -> None:
        """Add middleware to the adapter pipeline."""
        self._middleware.append(middleware)

    def _build_cache_key(self, context: RequestContext) -> str:
        """Build cache key from request context."""
        key_parts = [
            context.method.name,
            context.endpoint,
            json.dumps(context.params, sort_keys=True),
        ]
        if context.data:
            key_parts.append(json.dumps(context.data, sort_keys=True))
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def _sign_request(self, context: RequestContext) -> str:
        """Sign request using HMAC if configured."""
        if self._config.auth_type != AuthType.HMAC:
            return ""

        credentials = self._config.auth_credentials or {}
        secret = credentials.get("secret", "")

        message = f"{context.method.name}:{context.endpoint}:{context.timestamp}"
        signature = hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()

        return signature

    async def request(
        self,
        method: HTTPMethod,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        auth_token: Optional[str] = None,
        bypass_cache: bool = False,
    ) -> ResponseContext:
        """
        Execute an API request with full adapter support.

        Args:
            method: HTTP method to use
            endpoint: API endpoint path
            params: Query parameters
            data: Request body data
            auth_token: Optional authentication token override
            bypass_cache: Skip cache lookup

        Returns:
            ResponseContext with response data and metadata
        """
        start_time = time.time()

        context = RequestContext(
            method=method,
            endpoint=endpoint,
            params=params or {},
            data=data,
        )

        # Check cache
        if self._cache and not bypass_cache:
            cache_key = self._build_cache_key(context)
            cached_data = self._cache.get(cache_key)
            if cached_data is not None:
                elapsed_ms = (time.time() - start_time) * 1000
                return ResponseContext(
                    request_id=context.request_id,
                    status_code=200,
                    headers={},
                    data=cached_data,
                    elapsed_ms=elapsed_ms,
                    cached=True,
                )

        # Apply rate limiting
        if self._rate_limiter:
            await self._rate_limiter.wait_for_token()

        # Apply middleware
        for middleware in self._middleware:
            try:
                context = await middleware(context)  # type: ignore
            except Exception as e:
                logger.warning(f"Middleware failed: {e}")

        # Execute request with retries
        last_error: Optional[Exception] = None
        for attempt in range(self._config.max_retries):
            try:
                response = await self._execute_request(context, auth_token)
                elapsed_ms = (time.time() - start_time) * 1000

                # Cache response if applicable
                if self._cache and response.status_code == 200:
                    cache_key = self._build_cache_key(context)
                    self._cache.set(cache_key, response.data, self._config.cache_ttl)

                return response

            except Exception as e:
                last_error = e
                if attempt < self._config.max_retries - 1:
                    wait_time = self._config.retry_backoff**attempt
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}), "
                        f"retrying in {wait_time:.1f}s: {e}"
                    )
                    await asyncio.sleep(wait_time)

        elapsed_ms = (time.time() - start_time) * 1000
        return ResponseContext(
            request_id=context.request_id,
            status_code=500,
            headers={},
            data=None,
            elapsed_ms=elapsed_ms,
            error=str(last_error),
        )

    async def _execute_request(
        self, context: RequestContext, auth_token: Optional[str]
    ) -> ResponseContext:
        """Execute the actual HTTP request."""
        # Placeholder for actual HTTP execution
        # In production, this would use aiohttp or httpx
        elapsed_ms = 50.0
        return ResponseContext(
            request_id=context.request_id,
            status_code=200,
            headers={"content-type": "application/json"},
            data={"status": "success"},
            elapsed_ms=elapsed_ms,
        )

    async def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> ResponseContext:
        """Execute GET request."""
        return await self.request(HTTPMethod.GET, endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> ResponseContext:
        """Execute POST request."""
        return await self.request(HTTPMethod.POST, endpoint, data=data)

    async def put(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> ResponseContext:
        """Execute PUT request."""
        return await self.request(HTTPMethod.PUT, endpoint, data=data)

    async def patch(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> ResponseContext:
        """Execute PATCH request."""
        return await self.request(HTTPMethod.PATCH, endpoint, data=data)

    async def delete(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> ResponseContext:
        """Execute DELETE request."""
        return await self.request(HTTPMethod.DELETE, endpoint, params=params)

    def clear_cache(self) -> None:
        """Clear the response cache."""
        if self._cache:
            self._cache.clear()

    @property
    def endpoints(self) -> Dict[str, Endpoint]:
        """Get registered endpoints."""
        return self._endpoints.copy()


# Convenience factory function
def create_api_adapter(
    base_url: str,
    auth_type: AuthType = AuthType.NONE,
    auth_credentials: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> APIAdapter:
    """
    Create a configured API adapter.

    Args:
        base_url: Base URL for all API requests
        auth_type: Type of authentication to use
        auth_credentials: Authentication credentials
        **kwargs: Additional configuration options

    Returns:
        Configured APIAdapter instance
    """
    config = AdapterConfig(
        base_url=base_url,
        auth_type=auth_type,
        auth_credentials=auth_credentials,
        **kwargs,
    )
    return APIAdapter(config)
