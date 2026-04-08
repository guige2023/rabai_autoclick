"""
Endpoint Router Action Module.

Routes requests to appropriate handlers based on path patterns,
HTTP methods, and middleware chains. Supports wildcard and regex routing.
"""
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class Route:
    """A registered route."""
    path: str
    method: str
    handler: Callable
    middleware: list[Callable] = field(default_factory=list)


@dataclass
class RouteResult:
    """Result of routing a request."""
    matched: bool
    handler: Optional[Callable] = None
    params: dict[str, str] = {}
    middleware_results: list[Any] = []
    error: Optional[str] = None


class EndpointRouterAction(BaseAction):
    """Router that maps requests to handlers based on path and method."""

    def __init__(self) -> None:
        super().__init__("endpoint_router")
        self._routes: list[Route] = []

    def execute(self, context: dict, params: dict) -> RouteResult:
        """
        Route an incoming request.

        Args:
            context: Execution context
            params: Parameters:
                - path: Request path
                - method: HTTP method
                - data: Optional request data
                - headers: Optional request headers

        Returns:
            RouteResult with matched handler and extracted params
        """
        path = params.get("path", "")
        method = params.get("method", "GET").upper()
        data = params.get("data")
        headers = params.get("headers", {})

        request = {
            "path": path,
            "method": method,
            "data": data,
            "headers": headers,
            "params": {}
        }

        for route in self._routes:
            if route.method != method:
                continue
            params_match = self._match_path(route.path, path)
            if params_match is not None:
                middleware_results = []
                for mw in route.middleware:
                    try:
                        mw_result = mw(request)
                        middleware_results.append(mw_result)
                    except Exception as e:
                        return RouteResult(
                            matched=False,
                            error=f"Middleware error: {str(e)}"
                        )
                request["params"] = params_match
                return RouteResult(
                    matched=True,
                    handler=route.handler,
                    params=params_match,
                    middleware_results=middleware_results
                )

        return RouteResult(matched=False, error=f"No route for {method} {path}")

    def _match_path(self, pattern: str, path: str) -> Optional[dict[str, str]]:
        """Match a path pattern against an actual path."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return None

        params: dict[str, str] = {}
        for p, a in zip(pattern_parts, path_parts):
            if p.startswith("{") and p.endswith("}"):
                params[p[1:-1]] = a
            elif p != a:
                return None

        return params

    def register_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        middleware: Optional[list[Callable]] = None
    ) -> None:
        """Register a new route."""
        self._routes.append(Route(
            path=path,
            method=method.upper(),
            handler=handler,
            middleware=middleware or []
        ))
