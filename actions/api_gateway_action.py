"""
API Gateway Action.

Provides API gateway functionality.
Supports:
- Request routing
- Protocol translation
- Rate limiting
- Request/response transformation
- Circuit breaker
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import json
import time
import hashlib
import threading

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class Route:
    """Route configuration."""
    path_pattern: str
    upstream_url: str
    methods: List[str] = field(default_factory=lambda: ["GET"])
    timeout: float = 30.0
    retry_count: int = 0
    strip_path: bool = False
    headers: Dict[str, str] = field(default_factory=dict)
    rate_limit: Optional[int] = None


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_requests: int = 3


@dataclass
class GatewayRequest:
    """Gateway request context."""
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes] = None
    client_ip: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    request_id: str = ""
    
    def get_header(self, name: str, default: str = "") -> str:
        """Get header value."""
        return self.headers.get(name.lower(), default)


@dataclass
class GatewayResponse:
    """Gateway response context."""
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[bytes] = None
    error: Optional[str] = None
    from_cache: bool = False
    duration_ms: float = 0.0


@dataclass
class RateLimitEntry:
    """Rate limit tracking entry."""
    count: int = 0
    window_start: datetime = field(default_factory=datetime.utcnow)


class CircuitBreaker:
    """Circuit breaker for upstream services."""
    
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.next_attempt_time: Optional[datetime] = None
        self._lock = threading.RLock()
    
    def record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            else:
                self.failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed request."""
        with self._lock:
            self.failure_count += 1
            
            if self.state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self.failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
    
    def can_attempt(self) -> bool:
        """Check if a request can be attempted."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            if self.state == CircuitState.OPEN:
                if self.next_attempt_time and datetime.utcnow() >= self.next_attempt_time:
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
                return False
            
            return True  # HALF_OPEN
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        logger.info(f"Circuit breaker '{self.name}': {self.state.value} -> {new_state.value}")
        self.state = new_state
        
        if new_state == CircuitState.OPEN:
            self.next_attempt_time = datetime.utcnow() + timedelta(seconds=self.config.timeout)
            self.failure_count = 0
        elif new_state == CircuitState.HALF_OPEN:
            self.success_count = 0
        elif new_state == CircuitState.CLOSED:
            self.failure_count = 0
            self.success_count = 0
            self.next_attempt_time = None


class ApiGatewayAction:
    """
    API Gateway Action.
    
    Provides API gateway functionality with support for:
    - Request routing
    - Circuit breaker pattern
    - Rate limiting
    - Request/response transformation
    - Caching
    """
    
    def __init__(
        self,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    ):
        """
        Initialize the API Gateway Action.
        
        Args:
            circuit_breaker_config: Circuit breaker configuration
        """
        self.circuit_breaker_config = circuit_breaker_config or CircuitBreakerConfig()
        self.routes: List[Route] = []
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.rate_limits: Dict[str, RateLimitEntry] = {}
        self.cache: Dict[str, tuple] = {}  # (response, expiry)
        self._cache_lock = threading.RLock()
        self._rate_limit_lock = threading.RLock()
        self.request_transformers: List[Callable[[GatewayRequest], GatewayRequest]] = []
        self.response_transformers: List[Callable[[GatewayResponse], GatewayResponse]] = []
    
    def add_route(
        self,
        path_pattern: str,
        upstream_url: str,
        methods: Optional[List[str]] = None,
        timeout: float = 30.0,
        retry_count: int = 0,
        strip_path: bool = False,
        headers: Optional[Dict[str, str]] = None,
        rate_limit: Optional[int] = None
    ) -> "ApiGatewayAction":
        """
        Add a route.
        
        Args:
            path_pattern: URL path pattern to match
            upstream_url: Upstream service URL
            methods: Allowed HTTP methods
            timeout: Request timeout in seconds
            retry_count: Number of retries
            strip_path: Whether to strip the matched path prefix
            headers: Headers to add/modify
            rate_limit: Rate limit per window
        
        Returns:
            Self for chaining
        """
        route = Route(
            path_pattern=path_pattern,
            upstream_url=upstream_url,
            methods=methods or ["GET"],
            timeout=timeout,
            retry_count=retry_count,
            strip_path=strip_path,
            headers=headers or {},
            rate_limit=rate_limit
        )
        self.routes.append(route)
        
        # Create circuit breaker for this upstream
        cb_name = self._get_upstream_name(upstream_url)
        self.circuit_breakers[cb_name] = CircuitBreaker(cb_name, self.circuit_breaker_config)
        
        logger.info(f"Added route: {path_pattern} -> {upstream_url}")
        return self
    
    def add_request_transformer(
        self,
        transformer: Callable[[GatewayRequest], GatewayRequest]
    ) -> "ApiGatewayAction":
        """Add a request transformer."""
        self.request_transformers.append(transformer)
        return self
    
    def add_response_transformer(
        self,
        transformer: Callable[[GatewayResponse], GatewayResponse]
    ) -> "ApiGatewayAction":
        """Add a response transformer."""
        self.response_transformers.append(transformer)
        return self
    
    async def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """
        Handle an incoming request.
        
        Args:
            request: Gateway request
        
        Returns:
            Gateway response
        """
        start_time = time.time()
        request.request_id = request.request_id or self._generate_request_id()
        
        # Find matching route
        route = self._match_route(request)
        if not route:
            return GatewayResponse(
                status_code=404,
                body=b'{"error": "Route not found"}',
                duration_ms=(time.time() - start_time) * 1000
            )
        
        # Check rate limit
        if route.rate_limit:
            rate_result = self._check_rate_limit(request, route.rate_limit)
            if not rate_result:
                return GatewayResponse(
                    status_code=429,
                    body=b'{"error": "Rate limit exceeded"}',
                    duration_ms=(time.time() - start_time) * 1000
                )
        
        # Apply request transformers
        for transformer in self.request_transformers:
            request = transformer(request)
        
        # Get circuit breaker
        cb_name = self._get_upstream_name(route.upstream_url)
        cb = self.circuit_breakers.get(cb_name)
        
        # Check circuit breaker
        if cb and not cb.can_attempt():
            return GatewayResponse(
                status_code=503,
                body=b'{"error": "Service unavailable"}',
                duration_ms=(time.time() - start_time) * 1000
            )
        
        # Check cache
        cache_key = self._get_cache_key(request, route)
        cached_response = self._get_from_cache(cache_key)
        if cached_response:
            cached_response.from_cache = True
            return cached_response
        
        # Forward request
        try:
            response = await self._forward_request(request, route)
            
            if cb:
                cb.record_success()
            
            # Cache response
            if response.status_code == 200:
                self._put_in_cache(cache_key, response)
            
            # Apply response transformers
            for transformer in self.response_transformers:
                response = transformer(response)
            
            return response
        
        except Exception as e:
            logger.error(f"Request failed: {e}")
            
            if cb:
                cb.record_failure()
            
            return GatewayResponse(
                status_code=502,
                body=json.dumps({"error": str(e)}).encode(),
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )
    
    async def _forward_request(
        self,
        request: GatewayRequest,
        route: Route
    ) -> GatewayResponse:
        """Forward request to upstream."""
        # This is a placeholder - would use httpx/aiohttp in production
        await asyncio.sleep(0.01)  # Simulate network delay
        
        return GatewayResponse(
            status_code=200,
            body=b'{"message": "OK"}',
            duration_ms=10.0
        )
    
    def _match_route(self, request: GatewayRequest) -> Optional[Route]:
        """Match request to a route."""
        for route in self.routes:
            if request.method not in route.methods:
                continue
            
            # Simple path matching (would use proper regex in production)
            if self._path_matches(request.path, route.path_pattern):
                return route
        
        return None
    
    def _path_matches(self, path: str, pattern: str) -> bool:
        """Check if path matches pattern."""
        # Simplified - would support wildcards and regex in production
        if pattern.endswith("*"):
            return path.startswith(pattern[:-1])
        return path == pattern or path.startswith(pattern.rstrip("/") + "/")
    
    def _check_rate_limit(self, request: GatewayRequest, limit: int) -> bool:
        """Check rate limit for request."""
        key = f"{request.client_ip}:{request.get_header('authorization', 'anonymous')}"
        
        with self._rate_limit_lock:
            now = datetime.utcnow()
            window = timedelta(seconds=60)
            
            if key not in self.rate_limits:
                self.rate_limits[key] = RateLimitEntry()
            
            entry = self.rate_limits[key]
            
            # Reset window if expired
            if now - entry.window_start > window:
                entry.count = 0
                entry.window_start = now
            
            # Check limit
            if entry.count >= limit:
                return False
            
            entry.count += 1
            return True
    
    def _get_cache_key(self, request: GatewayRequest, route: Route) -> str:
        """Generate cache key for request."""
        data = f"{route.path_pattern}:{request.path}:{request.query_params}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _get_from_cache(self, key: str) -> Optional[GatewayResponse]:
        """Get response from cache."""
        with self._cache_lock:
            if key in self.cache:
                response, expiry = self.cache[key]
                if datetime.utcnow() < expiry:
                    return response
                del self.cache[key]
        return None
    
    def _put_in_cache(self, key: str, response: GatewayResponse, ttl: int = 60) -> None:
        """Put response in cache."""
        with self._cache_lock:
            self.cache[key] = (
                response,
                datetime.utcnow() + timedelta(seconds=ttl)
            )
    
    def _get_upstream_name(self, url: str) -> str:
        """Get upstream name from URL."""
        return url.split("://")[1].split("/")[0] if "://" in url else url
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID."""
        return f"req-{int(time.time() * 1000)}-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        with self._cache_lock:
            cache_size = len(self.cache)
        
        return {
            "routes_count": len(self.routes),
            "circuit_breakers": {
                name: {
                    "state": cb.state.value,
                    "failure_count": cb.failure_count
                }
                for name, cb in self.circuit_breakers.items()
            },
            "cache_size": cache_size,
            "rate_limits_active": len(self.rate_limits)
        }


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create gateway
    gateway = ApiGatewayAction()
    
    # Add routes
    gateway.add_route(
        "/api/users/*",
        "http://user-service:8001",
        methods=["GET", "POST"],
        timeout=10.0,
        rate_limit=100
    )
    gateway.add_route(
        "/api/orders/*",
        "http://order-service:8002",
        methods=["GET", "POST", "PUT", "DELETE"],
        timeout=30.0,
        rate_limit=50
    )
    
    # Add transformers
    gateway.add_request_transformer(
        lambda req: req  # Add auth headers, etc.
    )
    
    async def main():
        # Simulate requests
        for i in range(5):
            request = GatewayRequest(
                method="GET",
                path="/api/users/123",
                headers={"host": "api.example.com"},
                query_params={},
                client_ip="192.168.1.1"
            )
            
            response = await gateway.handle_request(request)
            print(f"Request {i}: {response.status_code} ({response.duration_ms:.1f}ms)")
        
        print(f"\nStats: {json.dumps(gateway.get_stats(), indent=2)}")
    
    asyncio.run(main())
