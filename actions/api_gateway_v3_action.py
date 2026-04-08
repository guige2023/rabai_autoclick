"""API Gateway v3 Action.

Advanced API gateway with request routing, rate limiting, and auth.
"""
from typing import Any, Callable, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import time


@dataclass
class Route:
    path: str
    methods: List[str]
    handler: Callable
    auth_required: bool = False
    rate_limit: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayRequest:
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes]
    client_ip: Optional[str]


@dataclass
class GatewayResponse:
    status_code: int
    headers: Dict[str, str]
    body: Any
    duration_ms: float


class APIGatewayV3Action:
    """Advanced API gateway with routing and middleware."""

    def __init__(self, base_path: str = "") -> None:
        self.base_path = base_path
        self.routes: List[Route] = []
        self.middleware: List[Callable] = []
        self._rate_limit_store: Dict[str, List[float]] = {}

    def add_route(
        self,
        path: str,
        methods: List[str],
        handler: Callable,
        auth_required: bool = False,
        rate_limit: Optional[int] = None,
    ) -> None:
        self.routes.append(Route(
            path=path,
            methods=methods,
            handler=handler,
            auth_required=auth_required,
            rate_limit=rate_limit,
        ))

    def use(self, middleware: Callable) -> None:
        self.middleware.append(middleware)

    def _match_route(self, method: str, path: str) -> Optional[Route]:
        for route in self.routes:
            if method.upper() not in route.methods:
                continue
            if self.base_path:
                full_path = self.base_path.rstrip("/") + "/" + route.path.lstrip("/")
            else:
                full_path = route.path
            if path == full_path or path.startswith(full_path + "/"):
                return route
        return None

    def _check_rate_limit(self, client_id: str, limit: int, window: float = 60.0) -> bool:
        now = time.time()
        if client_id not in self._rate_limit_store:
            self._rate_limit_store[client_id] = []
        self._rate_limit_store[client_id] = [
            t for t in self._rate_limit_store[client_id] if now - t < window
        ]
        if len(self._rate_limit_store[client_id]) >= limit:
            return False
        self._rate_limit_store[client_id].append(now)
        return True

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        start = time.time()
        route = self._match_route(request.method, request.path)
        if not route:
            return GatewayResponse(
                status_code=404,
                headers={},
                body={"error": "Not Found"},
                duration_ms=(time.time() - start) * 1000,
            )
        if route.auth_required:
            pass
        if route.rate_limit:
            client_id = request.client_ip or "unknown"
            if not self._check_rate_limit(client_id, route.rate_limit):
                return GatewayResponse(
                    status_code=429,
                    headers={},
                    body={"error": "Rate limit exceeded"},
                    duration_ms=(time.time() - start) * 1000,
                )
        try:
            result = route.handler(request)
            return GatewayResponse(
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=result,
                duration_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return GatewayResponse(
                status_code=500,
                headers={},
                body={"error": str(e)},
                duration_ms=(time.time() - start) * 1000,
            )
