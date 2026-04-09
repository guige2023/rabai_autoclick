"""
API Gateway Action Module.

Provides API gateway capabilities including routing, authentication,
rate limiting, request/response transformation, and monitoring.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Pattern
from dataclasses import dataclass, field
from enum import Enum
import re
import time
import threading
from datetime import datetime
from collections import defaultdict


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    Bearer = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"


@dataclass
class Route:
    """Represents a gateway route."""
    path_pattern: str
    method: str
    backend_url: str
    auth_type: AuthType = AuthType.NONE
    auth_config: Dict[str, Any] = field(default_factory=dict)
    rate_limit: Optional[float] = None
    transforms: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayRequest:
    """Represents a gateway request."""
    path: str
    method: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[Any] = None
    client_ip: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class GatewayResponse:
    """Represents a gateway response."""
    status_code: int
    headers: Dict[str, str]
    body: Any
    latency_ms: float


@dataclass
class AuthResult:
    """Result of authentication."""
    authenticated: bool
    user_id: Optional[str] = None
    error: Optional[str] = None


class RouteMatcher:
    """
    Matches incoming requests to routes using patterns.
    
    Example:
        matcher = RouteMatcher()
        matcher.add_route("/api/users/{id}", "GET", backend)
        
        route, params = matcher.match("/api/users/123", "GET")
    """
    
    def __init__(self):
        self.routes: List[Route] = []
        self._compiled: Dict[str, Pattern] = {}
        self._lock = threading.RLock()
    
    def add_route(self, route: Route) -> "RouteMatcher":
        """Add a route to the matcher."""
        with self._lock:
            # Pre-compile route pattern
            pattern = self._compile_pattern(route.path_pattern)
            self._compiled[route.path_pattern] = pattern
            self.routes.append(route)
        return self
    
    def match(self, path: str, method: str) -> tuple[Optional[Route], Dict[str, str]]:
        """Match a request path and method to a route."""
        with self._lock:
            for route in self.routes:
                if route.method != method and route.method != "*":
                    continue
                
                pattern = self._compiled.get(route.path_pattern)
                if pattern:
                    match = pattern.match(path)
                    if match:
                        params = match.groupdict()
                        return route, params
        
        return None, {}
    
    def _compile_pattern(self, path_pattern: str) -> Pattern:
        """Convert route pattern to regex."""
        # Replace {param} with named groups
        regex = re.sub(r'\{(\w+)\}', r'(?P<\1>[^/]+)', path_pattern)
        regex = f"^{regex}$"
        return re.compile(regex)


class Authenticator:
    """
    Handles request authentication.
    
    Example:
        auth = Authenticator()
        auth.add_provider(AuthType.API_KEY, api_key_validator)
        
        result = auth.authenticate(request, AuthType.API_KEY)
    """
    
    def __init__(self):
        self.providers: Dict[AuthType, Callable] = {}
        self._lock = threading.RLock()
    
    def add_provider(self, auth_type: AuthType, provider: Callable) -> "Authenticator":
        """Add an authentication provider."""
        with self._lock:
            self.providers[auth_type] = provider
        return self
    
    def authenticate(
        self,
        request: GatewayRequest,
        auth_type: AuthType
    ) -> AuthResult:
        """Authenticate a request."""
        if auth_type == AuthType.NONE:
            return AuthResult(authenticated=True)
        
        provider = self.providers.get(auth_type)
        if not provider:
            return AuthResult(
                authenticated=False,
                error=f"No provider for auth type: {auth_type}"
            )
        
        try:
            return provider(request)
        except Exception as e:
            return AuthResult(authenticated=False, error=str(e))


class RateLimiter:
    """Simple rate limiter for gateway."""
    
    def __init__(self, requests_per_second: float = 100):
        self.rate = requests_per_second
        self.tokens: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()
    
    def allow(self, client_id: str) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.monotonic()
            if now >= self.tokens[client_id]:
                self.tokens[client_id] = now + (1.0 / self.rate)
                return True
            return False
    
    def get_retry_after(self, client_id: str) -> float:
        """Get seconds until next request allowed."""
        with self._lock:
            now = time.monotonic()
            return max(0, self.tokens[client_id] - now)


class RequestTransformer:
    """
    Transforms requests and responses.
    
    Example:
        transformer = RequestTransformer()
        transformer.add_header_mapping("X-User-Id", "user_id")
        
        transformed = transformer.transform_request(request)
    """
    
    def __init__(self):
        self.header_mappings: Dict[str, str] = {}
        self.body_mappings: Dict[str, str] = {}
        self._lock = threading.RLock()
    
    def add_header_mapping(self, source: str, target: str) -> "RequestTransformer":
        """Add header mapping."""
        with self._lock:
            self.header_mappings[source] = target
        return self
    
    def transform_request(
        self,
        request: GatewayRequest,
        transforms: Dict[str, Any]
    ) -> GatewayRequest:
        """Transform incoming request."""
        headers = dict(request.headers)
        
        # Apply header mappings
        for source, target in self.header_mappings.items():
            if source in headers:
                headers[target] = headers[source]
        
        # Apply custom transforms
        add_headers = transforms.get("add_headers", {})
        headers.update(add_headers)
        
        remove_headers = transforms.get("remove_headers", [])
        for h in remove_headers:
            headers.pop(h, None)
        
        request.headers = headers
        return request
    
    def transform_response(
        self,
        response: GatewayResponse,
        transforms: Dict[str, Any]
    ) -> GatewayResponse:
        """Transform outgoing response."""
        headers = dict(response.headers)
        
        # Apply custom transforms
        add_headers = transforms.get("response_add_headers", {})
        headers.update(add_headers)
        
        remove_headers = transforms.get("response_remove_headers", [])
        for h in remove_headers:
            headers.pop(h, None)
        
        response.headers = headers
        return response


class APIGateway:
    """
    Complete API Gateway implementation.
    
    Example:
        gateway = APIGateway()
        gateway.add_route("/api/users/{id}", "GET", "http://users-service:8000")
        
        response = gateway.handle_request(request)
    """
    
    def __init__(self):
        self.route_matcher = RouteMatcher()
        self.authenticator = Authenticator()
        self.rate_limiter = RateLimiter()
        self.transformer = RequestTransformer()
        self.request_handlers: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        
        # Metrics
        self.metrics = {
            "total_requests": 0,
            "authenticated_requests": 0,
            "failed_requests": 0,
            "total_latency_ms": 0.0
        }
    
    def add_route(
        self,
        path: str,
        method: str,
        backend_url: str,
        auth_type: AuthType = AuthType.NONE,
        **kwargs
    ) -> "APIGateway":
        """Add a route to the gateway."""
        route = Route(
            path_pattern=path,
            method=method,
            backend_url=backend_url,
            auth_type=auth_type,
            **kwargs
        )
        self.route_matcher.add_route(route)
        return self
    
    def set_request_handler(self, backend_url: str, handler: Callable) -> "APIGateway":
        """Set handler for backend URL."""
        with self._lock:
            self.request_handlers[backend_url] = handler
        return self
    
    def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """Handle an incoming request."""
        start_time = time.monotonic()
        
        with self._lock:
            self.metrics["total_requests"] += 1
        
        # Match route
        route, params = self.route_matcher.match(request.path, request.method)
        
        if not route:
            return self._error_response(404, "Route not found", start_time)
        
        # Check rate limit
        client_id = request.client_ip or "default"
        if route.rate_limit:
            if not self.rate_limiter.allow(client_id):
                retry_after = self.rate_limiter.get_retry_after(client_id)
                return self._rate_limit_response(retry_after, start_time)
        
        # Authenticate
        auth_result = self.authenticator.authenticate(request, route.auth_type)
        if not auth_result.authenticated:
            with self._lock:
                self.metrics["failed_requests"] += 1
            return self._error_response(401, auth_result.error or "Unauthorized", start_time)
        
        with self._lock:
            self.metrics["authenticated_requests"] += 1
        
        # Transform request
        request = self.transformer.transform_request(request, route.transforms)
        
        # Get handler
        handler = self.request_handlers.get(route.backend_url)
        
        # Execute request
        try:
            if handler:
                response = handler(request, params, route)
            else:
                response = self._proxy_response(route.backend_url, request, params)
            
            # Transform response
            response = self.transformer.transform_response(response, route.transforms)
            
        except Exception as e:
            with self._lock:
                self.metrics["failed_requests"] += 1
            return self._error_response(500, str(e), start_time)
        
        # Update metrics
        latency = (time.monotonic() - start_time) * 1000
        with self._lock:
            self.metrics["total_latency_ms"] += latency
        
        response.latency_ms = latency
        return response
    
    def _proxy_response(
        self,
        backend_url: str,
        request: GatewayRequest,
        params: Dict[str, str]
    ) -> GatewayResponse:
        """Proxy request to backend (placeholder)."""
        return GatewayResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body={"message": "Proxied to " + backend_url, "params": params},
            latency_ms=0
        )
    
    def _error_response(
        self,
        status_code: int,
        message: str,
        start_time: float
    ) -> GatewayResponse:
        """Create error response."""
        latency = (time.monotonic() - start_time) * 1000
        return GatewayResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body={"error": message},
            latency_ms=latency
        )
    
    def _rate_limit_response(
        self,
        retry_after: float,
        start_time: float
    ) -> GatewayResponse:
        """Create rate limit response."""
        latency = (time.monotonic() - start_time) * 1000
        return GatewayResponse(
            status_code=429,
            headers={
                "Content-Type": "application/json",
                "Retry-After": str(int(retry_after))
            },
            body={"error": "Rate limit exceeded", "retry_after": retry_after},
            latency_ms=latency
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get gateway metrics."""
        with self._lock:
            avg_latency = 0
            if self.metrics["total_requests"] > 0:
                avg_latency = self.metrics["total_latency_ms"] / self.metrics["total_requests"]
            
            return {
                **self.metrics,
                "avg_latency_ms": avg_latency,
                "success_rate": (
                    (self.metrics["total_requests"] - self.metrics["failed_requests"])
                    / max(1, self.metrics["total_requests"])
                )
            }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APIGatewayAction(BaseAction):
    """
    API Gateway action for routing and proxying.
    
    Parameters:
        operation: Operation type (add_route/handle_request/get_metrics)
        path: Route path pattern
        method: HTTP method
        backend_url: Backend service URL
    
    Example:
        action = APIGatewayAction()
        result = action.execute({}, {
            "operation": "add_route",
            "path": "/api/users/{id}",
            "method": "GET",
            "backend_url": "http://users-service:8000"
        })
    """
    
    _gateway: Optional[APIGateway] = None
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute gateway operation."""
        operation = params.get("operation", "add_route")
        
        with self._lock:
            if self._gateway is None:
                self._gateway = APIGateway()
        
        if operation == "add_route":
            path = params.get("path")
            method = params.get("method", "GET")
            backend_url = params.get("backend_url")
            auth_type_str = params.get("auth_type", "none")
            
            auth_type = AuthType(auth_type_str)
            
            self._gateway.add_route(path, method, backend_url, auth_type)
            
            return {
                "success": True,
                "operation": "add_route",
                "path": path,
                "method": method,
                "backend_url": backend_url,
                "added_at": datetime.now().isoformat()
            }
        
        elif operation == "get_metrics":
            metrics = self._gateway.get_metrics()
            return {
                "success": True,
                "operation": "get_metrics",
                "metrics": metrics
            }
        
        elif operation == "list_routes":
            routes = [
                {"path": r.path_pattern, "method": r.method, "backend": r.backend_url}
                for r in self._gateway.route_matcher.routes
            ]
            return {
                "success": True,
                "operation": "list_routes",
                "routes": routes
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
