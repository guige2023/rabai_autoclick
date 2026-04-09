"""API Gateway Action Module.

Provides API gateway capabilities including request routing, rate limiting,
authentication, logging, and transformation middleware with support for
microservices backend integration.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    JWT = "jwt"


class LoadBalancingMode(Enum):
    """Backend load balancing modes."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"


@dataclass
class BackendService:
    """A backend service target."""
    service_id: str
    url: str
    weight: int = 1
    max_connections: int = 100
    timeout_seconds: float = 30.0
    health_check_path: str = "/health"
    healthy: bool = True
    active_connections: int = 0


@dataclass
class RouteConfig:
    """Configuration for a gateway route."""
    route_id: str
    path_prefix: str
    backend_service_id: str
    methods: Set[str] = field(default_factory=set)
    auth_type: AuthType = AuthType.NONE
    rate_limit: Optional[int] = None
    timeout_seconds: float = 30.0
    strip_path: bool = True
    add_prefix: Optional[str] = None
    transforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayConfig:
    """Configuration for API gateway."""
    port: int = 8080
    host: str = "0.0.0.0"
    log_requests: bool = True
    log_responses: bool = False
    load_balancing: LoadBalancingMode = LoadBalancingMode.ROUND_ROBIN
    global_rate_limit: int = 1000
    auth_enabled: bool = True
    middleware: List[str] = field(default_factory=list)


@dataclass
class RequestLog:
    """Log entry for a gateway request."""
    timestamp: datetime
    method: str
    path: str
    status_code: Optional[int] = None
    response_time_ms: float = 0.0
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    authenticated: bool = False
    route_id: Optional[str] = None
    backend_service_id: Optional[str] = None
    error: Optional[str] = None


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float):
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def allow_request(self) -> bool:
        """Check if request is allowed under rate limit."""
        with self._lock:
            self._refill()

            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        refill_amount = elapsed * self._refill_rate
        self._tokens = min(self._capacity, self._tokens + refill_amount)
        self._last_refill = now


class Authenticator:
    """Handle authentication for gateway requests."""

    def __init__(self):
        self._valid_api_keys: Set[str] = set()
        self._valid_tokens: Set[str] = set()

    def authenticate(
        self,
        auth_type: AuthType,
        headers: Dict[str, str],
        api_key: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """Authenticate a request."""
        if auth_type == AuthType.NONE:
            return True, None

        if auth_type == AuthType.API_KEY:
            key = headers.get("X-API-Key") or api_key
            if key and key in self._valid_api_keys:
                return True, key
            return False, "Invalid API key"

        if auth_type == AuthType.BEARER:
            token = headers.get("Authorization", "").replace("Bearer ", "")
            if token in self._valid_tokens:
                return True, token
            return False, "Invalid bearer token"

        return True, None

    def add_api_key(self, key: str):
        """Register a valid API key."""
        self._valid_api_keys.add(key)

    def add_token(self, token: str):
        """Register a valid token."""
        self._valid_tokens.add(token)


class ApiGatewayAction(BaseAction):
    """Action for API gateway operations."""

    def __init__(self):
        super().__init__(name="api_gateway")
        self._config = GatewayConfig()
        self._routes: Dict[str, RouteConfig] = {}
        self._backends: Dict[str, BackendService] = {}
        self._rate_limiters: Dict[str, RateLimiter] = {}
        self._authenticator = Authenticator()
        self._request_logs: List[RequestLog] = []
        self._lock = threading.Lock()
        self._route_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._balancer_index: Dict[str, int] = defaultdict(int)

    def configure(self, config: GatewayConfig):
        """Configure gateway settings."""
        self._config = config

    def register_backend(
        self,
        service_id: str,
        url: str,
        weight: int = 1,
        max_connections: int = 100
    ) -> ActionResult:
        """Register a backend service."""
        try:
            with self._lock:
                if service_id in self._backends:
                    return ActionResult(success=False, error=f"Backend {service_id} already exists")

                backend = BackendService(
                    service_id=service_id,
                    url=url,
                    weight=weight,
                    max_connections=max_connections
                )
                self._backends[service_id] = backend
                self._rate_limiters[service_id] = RateLimiter(
                    capacity=weight * 10,
                    refill_rate=weight
                )

                return ActionResult(success=True, data={"service_id": service_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def register_route(
        self,
        route_id: str,
        path_prefix: str,
        backend_service_id: str,
        methods: List[str],
        auth_type: AuthType = AuthType.NONE,
        rate_limit: Optional[int] = None,
        timeout_seconds: float = 30.0
    ) -> ActionResult:
        """Register a gateway route."""
        try:
            with self._lock:
                if route_id in self._routes:
                    return ActionResult(success=False, error=f"Route {route_id} already exists")

                if backend_service_id not in self._backends:
                    return ActionResult(success=False, error=f"Backend {backend_service_id} not found")

                route = RouteConfig(
                    route_id=route_id,
                    path_prefix=path_prefix,
                    backend_service_id=backend_service_id,
                    methods=set(m.upper() for m in methods),
                    auth_type=auth_type,
                    rate_limit=rate_limit,
                    timeout_seconds=timeout_seconds
                )
                self._routes[route_id] = route

                if rate_limit:
                    self._rate_limiters[route_id] = RateLimiter(
                        capacity=rate_limit,
                        refill_rate=rate_limit / 60
                    )

                return ActionResult(success=True, data={"route_id": route_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_api_key(self, api_key: str) -> ActionResult:
        """Add a valid API key."""
        try:
            self._authenticator.add_api_key(api_key)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def log_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        route_id: Optional[str] = None
    ) -> ActionResult:
        """Log a gateway request."""
        try:
            log = RequestLog(
                timestamp=datetime.now(),
                method=method,
                path=path,
                client_ip=headers.get("X-Forwarded-For", headers.get("Remote-Addr")) if headers else None,
                user_agent=headers.get("User-Agent") if headers else None,
                route_id=route_id
            )
            with self._lock:
                self._request_logs.append(log)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def route_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        client_ip: Optional[str] = None
    ) -> ActionResult:
        """Route a request through the gateway."""
        start_time = time.time()

        try:
            matched_route = None
            for route in self._routes.values():
                if path.startswith(route.path_prefix):
                    if not route.methods or method.upper() in route.methods:
                        matched_route = route
                        break

            if not matched_route:
                return ActionResult(
                    success=False,
                    error="No matching route found",
                    data={"path": path, "method": method}
                )

            if matched_route.rate_limit:
                limiter = self._rate_limiters.get(matched_route.route_id)
                if limiter and not limiter.allow_request():
                    return ActionResult(
                        success=False,
                        error="Rate limit exceeded",
                        data={"route_id": matched_route.route_id}
                    )

            if self._config.auth_enabled and matched_route.auth_type != AuthType.NONE:
                authenticated, error = self._authenticator.authenticate(
                    matched_route.auth_type,
                    headers or {}
                )
                if not authenticated:
                    return ActionResult(
                        success=False,
                        error=error or "Authentication failed"
                    )

            backend = self._select_backend(matched_route.backend_service_id)
            if not backend:
                return ActionResult(success=False, error="No healthy backend available")

            backend.active_connections += 1

            try:
                duration_ms = (time.time() - start_time) * 1000
                return ActionResult(
                    success=True,
                    data={
                        "route_id": matched_route.route_id,
                        "backend_url": backend.url,
                        "response_time_ms": duration_ms,
                        "status": "forwarded"
                    }
                )
            finally:
                backend.active_connections = max(0, backend.active_connections - 1)

        except Exception as e:
            logger.exception("Request routing failed")
            return ActionResult(success=False, error=str(e))

    def _select_backend(self, service_id: str) -> Optional[BackendService]:
        """Select a backend using configured load balancing."""
        backends = [b for b in self._backends.values() if b.service_id == service_id]
        if not backends:
            return None

        healthy = [b for b in backends if b.healthy]
        if not healthy:
            healthy = backends

        if self._config.load_balancing == LoadBalancingMode.ROUND_ROBIN:
            idx = self._balancer_index[service_id]
            self._balancer_index[service_id] = (idx + 1) % len(healthy)
            return healthy[idx]
        elif self._config.load_balancing == LoadBalancingMode.LEAST_CONNECTIONS:
            return min(healthy, key=lambda b: b.active_connections)
        elif self._config.load_balancing == LoadBalancingMode.RANDOM:
            import random
            return random.choice(healthy)

        return healthy[0]

    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        with self._lock:
            total_requests = len(self._request_logs)
            recent_requests = [
                r for r in self._request_logs
                if r.timestamp > datetime.now() - timedelta(minutes=5)
            ]

            return {
                "total_requests": total_requests,
                "recent_requests": len(recent_requests),
                "total_routes": len(self._routes),
                "total_backends": len(self._backends),
                "healthy_backends": sum(1 for b in self._backends.values() if b.healthy),
                "routes": {
                    route_id: {
                        "path_prefix": r.path_prefix,
                        "methods": list(r.methods),
                        "auth_type": r.auth_type.value,
                        "stats": dict(self._route_stats[route_id])
                    }
                    for route_id, r in self._routes.items()
                }
            }

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute gateway action."""
        try:
            action = params.get("action", "route")

            if action == "route":
                return self.route_request(
                    params["method"],
                    params["path"],
                    params.get("headers"),
                    params.get("body"),
                    params.get("client_ip")
                )
            elif action == "register_backend":
                return self.register_backend(
                    params["service_id"],
                    params["url"],
                    params.get("weight", 1)
                )
            elif action == "register_route":
                return self.register_route(
                    params["route_id"],
                    params["path_prefix"],
                    params["backend_service_id"],
                    params.get("methods", ["GET"]),
                    AuthType(params.get("auth_type", "none"))
                )
            elif action == "stats":
                return ActionResult(success=True, data=self.get_stats())
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
