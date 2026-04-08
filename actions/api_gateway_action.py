"""
API Gateway Action Module.

Provides API gateway capabilities: routing, authentication, rate limiting,
request/response transforms, and backend service orchestration.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class RateLimitUnit(Enum):
    """Rate limit time units."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


@dataclass
class RouteConfig:
    """API route configuration."""
    path: str
    method: str
    handler: Callable
    auth_required: bool = True
    rate_limit: Optional[int] = None
    rate_unit: RateLimitUnit = RateLimitUnit.MINUTE
    timeout: float = 30.0
    retries: int = 0
    backend_url: Optional[str] = None
    transforms: List[Callable] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitRecord:
    """Tracks rate limit usage per client."""
    client_id: str
    requests: List[float] = field(default_factory=list)
    blocked: bool = False


@dataclass
class GatewayConfig:
    """API Gateway configuration."""
    routes: Dict[str, RouteConfig] = field(default_factory=dict)
    global_rate_limit: Optional[int] = None
    global_rate_unit: RateLimitUnit = RateLimitUnit.MINUTE
    default_timeout: float = 30.0
    enable_metrics: bool = True
    enable_logging: bool = True
    cors_origins: List[str] = field(default_factory=list)
    api_keys: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class APIGatewayAction:
    """
    API Gateway action handler.
    
    Provides routing, authentication, rate limiting, and orchestration
    for backend API services.
    
    Example:
        gateway = APIGatewayAction()
        gateway.register_route("/users", "GET", handler)
        result = gateway.handle_request("/users", "GET", headers={})
    """
    
    def __init__(self, config: Optional[GatewayConfig] = None):
        """Initialize the API gateway."""
        self.config = config or GatewayConfig()
        self._routes: Dict[str, RouteConfig] = self.config.routes.copy()
        self._rate_limits: Dict[str, RateLimitRecord] = {}
        self._metrics: Dict[str, List[float]] = {}
        self._request_counts: Dict[str, int] = {}
    
    def register_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        **kwargs
    ) -> None:
        """
        Register a new route with the gateway.
        
        Args:
            path: URL path pattern
            method: HTTP method
            handler: Function to handle requests
            **kwargs: Additional route configuration
        """
        route_key = f"{method.upper()}:{path}"
        self._routes[route_key] = RouteConfig(
            path=path,
            method=method.upper(),
            handler=handler,
            **kwargs
        )
        logger.info(f"Registered route: {route_key}")
    
    def unregister_route(self, path: str, method: str) -> bool:
        """
        Unregister a route from the gateway.
        
        Args:
            path: URL path
            method: HTTP method
            
        Returns:
            True if route was removed, False if not found
        """
        route_key = f"{method.upper()}:{path}"
        if route_key in self._routes:
            del self._routes[route_key]
            logger.info(f"Unregistered route: {route_key}")
            return True
        return False
    
    def authenticate_request(
        self,
        headers: Dict[str, str],
        route: RouteConfig
    ) -> Union[str, None]:
        """
        Authenticate an incoming request.
        
        Args:
            headers: Request headers
            route: Route configuration
            
        Returns:
            Client ID if authenticated, None otherwise
        """
        if not route.auth_required:
            return "anonymous"
        
        auth_header = headers.get("Authorization", "")
        api_key = headers.get("X-API-Key", "")
        
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return self._validate_token(token)
        
        if api_key and api_key in self.config.api_keys:
            key_info = self.config.api_keys[api_key]
            if self._check_key_scopes(api_key, route):
                return key_info.get("client_id", api_key)
        
        return None
    
    def _validate_token(self, token: str) -> Optional[str]:
        """Validate a bearer token."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        if hasattr(self, "_token_cache"):
            if token_hash in self._token_cache:
                return self._token_cache[token_hash]
        return None
    
    def _check_key_scopes(self, api_key: str, route: RouteConfig) -> bool:
        """Check if API key has required scopes."""
        key_info = self.config.api_keys.get(api_key, {})
        scopes = key_info.get("scopes", [])
        required = route.metadata.get("required_scopes", [])
        
        if not required:
            return True
        
        return all(s in scopes for s in required)
    
    def check_rate_limit(
        self,
        client_id: str,
        route: Optional[RouteConfig] = None
    ) -> tuple[bool, Optional[str]]:
        """
        Check if request is within rate limits.
        
        Args:
            client_id: Client identifier
            route: Optional route-specific rate limit
            
        Returns:
            Tuple of (allowed, reason)
        """
        limit = route.rate_limit if route else self.config.global_rate_limit
        unit = (route.rate_unit if route 
                else self.config.global_rate_unit)
        
        if limit is None:
            return True, None
        
        now = time.time()
        window = self._get_window_seconds(unit)
        
        if client_id not in self._rate_limits:
            self._rate_limits[client_id] = RateLimitRecord(
                client_id=client_id
            )
        
        record = self._rate_limits[client_id]
        record.requests = [t for t in record.requests if now - t < window]
        
        if len(record.requests) >= limit:
            return False, f"Rate limit exceeded: {limit} per {unit.value}"
        
        record.requests.append(now)
        return True, None
    
    def _get_window_seconds(self, unit: RateLimitUnit) -> float:
        """Convert rate limit unit to seconds."""
        return {
            RateLimitUnit.SECOND: 1,
            RateLimitUnit.MINUTE: 60,
            RateLimitUnit.HOUR: 3600,
            RateLimitUnit.DAY: 86400,
        }[unit]
    
    def apply_transforms(
        self,
        data: Any,
        transforms: List[Callable],
        direction: str = "request"
    ) -> Any:
        """
        Apply transforms to request/response data.
        
        Args:
            data: Data to transform
            transforms: List of transform functions
            direction: "request" or "response"
            
        Returns:
            Transformed data
        """
        result = data
        for transform in transforms:
            try:
                if direction == "request":
                    result = transform(result)
                else:
                    result = transform(result)
            except Exception as e:
                logger.warning(
                    f"Transform failed ({direction}): {e}"
                )
        return result
    
    def route_request(
        self,
        path: str,
        method: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Route and handle an API request.
        
        Args:
            path: Request path
            method: HTTP method
            headers: Request headers
            body: Request body
            params: Query/path parameters
            
        Returns:
            Response dictionary with status, body, headers
        """
        headers = headers or {}
        route_key = f"{method.upper()}:{path}"
        route = self._routes.get(route_key)
        
        if not route:
            return {
                "status": 404,
                "body": {"error": "Route not found"},
                "headers": {}
            }
        
        client_id = self.authenticate_request(headers, route)
        if client_id is None:
            return {
                "status": 401,
                "body": {"error": "Authentication required"},
                "headers": {}
            }
        
        allowed, reason = self.check_rate_limit(client_id, route)
        if not allowed:
            return {
                "status": 429,
                "body": {"error": reason},
                "headers": {"Retry-After": "60"}
            }
        
        if self.config.enable_metrics:
            self._record_metric(route_key)
        
        try:
            if route.transforms and body:
                body = self.apply_transforms(
                    body, route.transforms, "request"
                )
            
            result = route.handler(
                headers=headers,
                body=body,
                params=params
            )
            
            if route.transforms and result:
                result = self.apply_transforms(
                    result, route.transforms, "response"
                )
            
            return {
                "status": 200,
                "body": result,
                "headers": self._build_response_headers(route)
            }
            
        except Exception as e:
            logger.error(f"Request handling failed: {e}")
            return {
                "status": 500,
                "body": {"error": str(e)},
                "headers": {}
            }
    
    def _record_metric(self, route_key: str) -> None:
        """Record request metric."""
        if route_key not in self._metrics:
            self._metrics[route_key] = []
        self._metrics[route_key].append(time.time())
    
    def _build_response_headers(self, route: RouteConfig) -> Dict[str, str]:
        """Build response headers."""
        headers = {
            "Content-Type": "application/json",
            "X-Content-Type-Options": "nosniff",
        }
        
        if self.config.cors_origins:
            headers["Access-Control-Allow-Origin"] = (
                self.config.cors_origins[0]
            )
        
        return headers
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get gateway metrics."""
        now = time.time()
        metrics = {}
        
        for route_key, times in self._metrics.items():
            recent = [t for t in times if now - t < 3600]
            self._metrics[route_key] = recent
            metrics[route_key] = {
                "requests_last_hour": len(recent),
                "requests_per_minute": len(recent) / 60
            }
        
        return metrics
    
    def get_rate_limit_status(self, client_id: str) -> Dict[str, Any]:
        """Get rate limit status for a client."""
        if client_id not in self._rate_limits:
            return {"status": "unknown"}
        
        record = self._rate_limits[client_id]
        now = time.time()
        
        return {
            "client_id": client_id,
            "blocked": record.blocked,
            "request_count": len(record.requests)
        }
