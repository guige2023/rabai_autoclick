"""API Router Action Module.

Provides dynamic API routing with path matching,
method filtering, and middleware chain support.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Pattern, Union
import asyncio
import logging

logger = logging.getLogger(__name__)


@dataclass
class Route:
    """API route definition."""
    path: str
    method: str
    handler: Callable
    name: Optional[str] = None
    middleware: List[Callable] = field(default_factory=list)


@dataclass
class RouteMatch:
    """Route match result."""
    route: Route
    path_params: Dict[str, str]
    query_params: Dict[str, str]


class APIRouterAction:
    """Dynamic API router.

    Example:
        router = APIRouterAction()

        router.add_route(
            path="/api/users/{id}",
            method="GET",
            handler=get_user
        )

        router.add_route(
            path="/api/users",
            method="POST",
            handler=create_user
        )

        match = router.match("/api/users/123", "GET")
        if match:
            result = await match.route.handler(match.path_params)
    """

    def __init__(self) -> None:
        self._routes: List[Route] = []
        self._compiled_patterns: Dict[str, Pattern] = {}
        self._middleware: List[Callable] = []

    def add_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        name: Optional[str] = None,
    ) -> "APIRouterAction":
        """Add route to router.

        Args:
            path: Route path with {param} placeholders
            method: HTTP method
            handler: Route handler function
            name: Optional route name

        Returns:
            Self for chaining
        """
        self._routes.append(Route(
            path=path,
            method=method.upper(),
            handler=handler,
            name=name,
        ))
        return self

    def add_routes(self, routes: List[Dict[str, Any]]) -> "APIRouterAction":
        """Add multiple routes at once."""
        for route_def in routes:
            self.add_route(
                path=route_def["path"],
                method=route_def["method"],
                handler=route_def["handler"],
                name=route_def.get("name"),
            )
        return self

    def use(self, middleware: Callable) -> "APIRouterAction":
        """Add global middleware."""
        self._middleware.append(middleware)
        return self

    def match(
        self,
        path: str,
        method: str,
        query_string: Optional[str] = None,
    ) -> Optional[RouteMatch]:
        """Match path and method to route.

        Args:
            path: Request path
            method: HTTP method
            query_string: Optional query string

        Returns:
            RouteMatch if found, None otherwise
        """
        method = method.upper()

        for route in self._routes:
            if route.method != method:
                continue

            path_params = self._match_path(route.path, path)
            if path_params is not None:
                query_params = self._parse_query(query_string) if query_string else {}
                return RouteMatch(
                    route=route,
                    path_params=path_params,
                    query_params=query_params,
                )

        return None

    def _match_path(
        self,
        pattern: str,
        path: str,
    ) -> Optional[Dict[str, str]]:
        """Match path against pattern.

        Args:
            pattern: Route pattern with {param} placeholders
            path: Actual path

        Returns:
            Dict of path parameters or None
        """
        if pattern == path:
            return {}

        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return None

        params: Dict[str, str] = {}

        for p_part, path_part in zip(pattern_parts, path_parts):
            if p_part.startswith("{") and p_part.endswith("}"):
                param_name = p_part[1:-1]
                params[param_name] = path_part
            elif p_part != path_part:
                return None

        return params

    def _parse_query(self, query_string: str) -> Dict[str, str]:
        """Parse query string."""
        params: Dict[str, str] = {}
        for pair in query_string.split("&"):
            if "=" in pair:
                key, value = pair.split("=", 1)
                params[key] = value
        return params

    async def handle_request(
        self,
        path: str,
        method: str,
        data: Optional[Dict] = None,
        query_string: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Handle request through router.

        Args:
            path: Request path
            method: HTTP method
            data: Request data
            query_string: Query string

        Returns:
            Response data
        """
        match = self.match(path, method, query_string)

        if not match:
            return {
                "status": 404,
                "error": "Not found",
                "path": path,
                "method": method,
            }

        for mw in self._middleware:
            try:
                result = mw(path, method, data)
                if result is not None:
                    return result
            except Exception as e:
                logger.error(f"Middleware error: {e}")

        for mw in match.route.middleware:
            try:
                result = mw(path, method, data)
                if result is not None:
                    return result
            except Exception as e:
                logger.error(f"Route middleware error: {e}")

        try:
            handler = match.route.handler
            if asyncio.iscoroutinefunction(handler):
                result = await handler(
                    path_params=match.path_params,
                    query_params=match.query_params,
                    data=data,
                )
            else:
                result = handler(
                    path_params=match.path_params,
                    query_params=match.query_params,
                    data=data,
                )
            return result
        except Exception as e:
            logger.error(f"Handler error: {e}")
            return {"status": 500, "error": str(e)}

    def reverse(
        self,
        name: str,
        path_params: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Generate URL from route name.

        Args:
            name: Route name
            path_params: Path parameter values

        Returns:
            Generated URL or None
        """
        for route in self._routes:
            if route.name == name:
                path = route.path
                if path_params:
                    for key, value in path_params.items():
                        path = path.replace(f"{{{key}}}", str(value))
                return path
        return None

    def get_routes(self) -> List[Dict[str, Any]]:
        """Get all registered routes."""
        return [
            {
                "path": r.path,
                "method": r.method,
                "name": r.name,
            }
            for r in self._routes
        ]
