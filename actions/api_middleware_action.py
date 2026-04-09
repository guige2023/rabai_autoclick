"""
API Middleware and Interceptor Module.

Provides middleware chain for API clients supporting
request/response transformation, logging, caching,
retry logic, and custom interceptors.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Union,
    Tuple, TypeVar, Generic
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MiddlewarePhase(Enum):
    """Middleware execution phases."""
    PRE_REQUEST = auto()
    POST_REQUEST = auto()
    ON_ERROR = auto()
    PRE_RESPONSE = auto()


@dataclass
class RequestContext:
    """Context passed through middleware chain."""
    url: str
    method: str
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    body: Any = None
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Runtime state
    retries: int = 0
    start_time: Optional[datetime] = None
    response: Optional["ResponseContext"] = None
    error: Optional[Exception] = None


@dataclass 
class ResponseContext:
    """Response context from API call."""
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    duration_ms: float = 0
    cached: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


MiddlewareFunc = Callable[[RequestContext], RequestContext]
ResponseHandler = Callable[[ResponseContext, RequestContext], ResponseContext]
ErrorHandler = Callable[[Exception, RequestContext], Optional[ResponseContext]]


class Middleware:
    """Base middleware class."""
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        """Called before request is sent."""
        return ctx
    
    def post_request(
        self,
        response: ResponseContext,
        ctx: RequestContext
    ) -> ResponseContext:
        """Called after successful request."""
        return response
    
    def on_error(
        self,
        error: Exception,
        ctx: RequestContext
    ) -> Optional[ResponseContext]:
        """Called when request fails."""
        return None
    
    def pre_response(
        self,
        response: ResponseContext,
        ctx: RequestContext
    ) -> ResponseContext:
        """Called before returning response to client."""
        return response


class LoggingMiddleware(Middleware):
    """Logs request/response information."""
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        ctx.start_time = datetime.now()
        logger.info(f"→ {ctx.method} {ctx.url}")
        return ctx
    
    def post_request(
        self,
        response: ResponseContext,
        ctx: RequestContext
    ) -> ResponseContext:
        duration = (datetime.now() - ctx.start_time).total_seconds() * 1000
        logger.info(f"← {ctx.method} {ctx.url} [{response.status_code}] {duration:.0f}ms")
        return response
    
    def on_error(
        self,
        error: Exception,
        ctx: RequestContext
    ) -> Optional[ResponseContext]:
        logger.error(f"✗ {ctx.method} {ctx.url}: {error}")
        return None


class RetryMiddleware(Middleware):
    """Implements retry logic with exponential backoff."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        retry_on: Optional[Callable[[int], bool]] = None
    ) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_on = retry_on or self._default_retry_on
    
    def _default_retry_on(self, status_code: int) -> bool:
        """Default: retry on 5xx and 429."""
        return status_code >= 500 or status_code == 429
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        if ctx.retries > 0:
            delay = min(self.base_delay * (2 ** (ctx.retries - 1)), self.max_delay)
            logger.info(f"Retry {ctx.retries} after {delay:.1f}s delay")
            time.sleep(delay)
        return ctx
    
    def on_error(
        self,
        error: Exception,
        ctx: RequestContext
    ) -> Optional[ResponseContext]:
        if ctx.retries < self.max_retries:
            ctx.retries += 1
            return None  # Will retry
        return None  # Max retries exceeded


class CacheMiddleware(Middleware):
    """In-memory cache for GET requests."""
    
    def __init__(self, ttl_seconds: float = 300) -> None:
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        if ctx.method == "GET":
            cache_key = self._make_key(ctx.url, ctx.params)
            
            if cache_key in self._cache:
                cached_data, cached_at = self._cache[cache_key]
                if datetime.now() - cached_at < timedelta(seconds=self.ttl_seconds):
                    logger.debug(f"Cache hit: {ctx.url}")
                    ctx.metadata["cached"] = True
                    ctx.metadata["cached_data"] = cached_data
        
        return ctx
    
    def post_request(
        self,
        response: ResponseContext,
        ctx: RequestContext
    ) -> ResponseContext:
        if ctx.method == "GET" and response.status_code == 200:
            cache_key = self._make_key(ctx.url, ctx.params)
            self._cache[cache_key] = (response.body, datetime.now())
            logger.debug(f"Cached: {ctx.url}")
        
        return response
    
    def _make_key(self, url: str, params: Dict[str, Any]) -> str:
        """Generate cache key from URL and params."""
        sorted_params = sorted(params.items())
        return f"{url}?{sorted_params}"


class HeaderMiddleware(Middleware):
    """Adds common headers to requests."""
    
    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        user_agent: Optional[str] = None
    ) -> None:
        self.headers = headers or {}
        if user_agent:
            self.headers["User-Agent"] = user_agent
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        for key, value in self.headers.items():
            if key not in ctx.headers:
                ctx.headers[key] = value
        return ctx


class TransformMiddleware(Middleware):
    """Transforms request/response bodies."""
    
    def __init__(
        self,
        request_transform: Optional[Callable[[Any], Any]] = None,
        response_transform: Optional[Callable[[Any], Any]] = None
    ) -> None:
        self.request_transform = request_transform
        self.response_transform = response_transform
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        if self.request_transform and ctx.body is not None:
            ctx.body = self.request_transform(ctx.body)
        return ctx
    
    def pre_response(
        self,
        response: ResponseContext,
        ctx: RequestContext
    ) -> ResponseContext:
        if self.response_transform and response.body is not None:
            response.body = self.response_transform(response.body)
        return response


class RateLimitMiddleware(Middleware):
    """Rate limiting middleware."""
    
    def __init__(
        self,
        max_calls: int = 100,
        window_seconds: float = 60.0
    ) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._calls: List[datetime] = []
    
    def pre_request(self, ctx: RequestContext) -> RequestContext:
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window_seconds)
        
        self._calls = [t for t in self._calls if t > cutoff]
        
        if len(self._calls) >= self.max_calls:
            wait_time = (self._calls[0] - cutoff).total_seconds()
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.1f}s")
                time.sleep(wait_time)
                self._calls = self._calls[1:]
        
        self._calls.append(now)
        return ctx


class ApiClient:
    """
    API client with middleware chain support.
    
    Provides a flexible middleware system for intercepting
    and modifying requests/responses.
    """
    
    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = base_url
        self.middlewares: List[Middleware] = []
        self._error_handlers: List[ErrorHandler] = []
    
    def add_middleware(self, middleware: Middleware) -> "ApiClient":
        """Add middleware to the chain."""
        self.middlewares.append(middleware)
        return self
    
    def add_error_handler(self, handler: ErrorHandler) -> "ApiClient":
        """Add error handler."""
        self._error_handlers.append(handler)
        return self
    
    def request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> ResponseContext:
        """
        Execute request with middleware chain.
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters
            
        Returns:
            ResponseContext
        """
        # Build request context
        ctx = RequestContext(
            url=self._build_url(url),
            method=method.upper(),
            headers=kwargs.pop("headers", {}),
            params=kwargs.pop("params", {}),
            body=kwargs.pop("data", kwargs.pop("json", None)),
            timeout=kwargs.pop("timeout", 30.0)
        )
        
        # Pre-request phase
        for mw in self.middlewares:
            ctx = mw.pre_request(ctx)
        
        # Check if we have cached response
        if ctx.metadata.get("cached"):
            cached_data = ctx.metadata["cached_data"]
            return ResponseContext(
                status_code=200,
                body=cached_data,
                cached=True
            )
        
        # Execute actual request (placeholder)
        try:
            response = self._execute_request(ctx)
            
            # Post-request phase
            for mw in self.middlewares:
                response = mw.post_request(response, ctx)
            
            # Pre-response phase
            for mw in self.middlewares:
                response = mw.pre_response(response, ctx)
            
            return response
        
        except Exception as e:
            return self._handle_error(e, ctx)
    
    def _build_url(self, url: str) -> str:
        """Build full URL."""
        if url.startswith(("http://", "https://")):
            return url
        base = self.base_url or ""
        return f"{base.rstrip('/')}/{url.lstrip('/')}"
    
    def _execute_request(self, ctx: RequestContext) -> ResponseContext:
        """Execute the actual HTTP request (placeholder)."""
        # This would integrate with requests/httpx/etc.
        return ResponseContext(
            status_code=200,
            body={"message": "ok"}
        )
    
    def _handle_error(
        self,
        error: Exception,
        ctx: RequestContext
    ) -> ResponseContext:
        """Handle request error."""
        ctx.error = error
        
        for handler in self._error_handlers:
            result = handler(error, ctx)
            if result is not None:
                return result
        
        for mw in self.middlewares:
            result = mw.on_error(error, ctx)
            if result is not None:
                return result
        
        raise error
    
    def get(self, url: str, **kwargs) -> ResponseContext:
        """Execute GET request."""
        return self.request("GET", url, **kwargs)
    
    def post(self, url: str, **kwargs) -> ResponseContext:
        """Execute POST request."""
        return self.request("POST", url, **kwargs)
    
    def put(self, url: str, **kwargs) -> ResponseContext:
        """Execute PUT request."""
        return self.request("PUT", url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> ResponseContext:
        """Execute DELETE request."""
        return self.request("DELETE", url, **kwargs)


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    client = ApiClient(base_url="https://api.example.com")
    
    # Add middleware chain
    client.add_middleware(LoggingMiddleware())
    client.add_middleware(HeaderMiddleware(user_agent="MyClient/1.0"))
    client.add_middleware(CacheMiddleware(ttl_seconds=60))
    client.add_middleware(RateLimitMiddleware(max_calls=10, window_seconds=60))
    client.add_middleware(RetryMiddleware(max_retries=3))
    
    # Note: This is a demo - actual HTTP execution would need requests library
    print("API Client configured with middleware chain:")
    for mw in client.middlewares:
        print(f"  - {mw.__class__.__name__}")
