"""
API Namespace Action Module.

Provides namespaced API routing and organization for multi-tenant
or modular API environments, enabling clean separation and routing
of API concerns.
"""

from typing import Any, Callable, Dict, List, Optional, Pattern, Union
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import re
import logging

logger = logging.getLogger(__name__)


class NamespaceType(Enum):
    """Types of API namespaces."""
    TENANT = auto()
    VERSION = auto()
    SERVICE = auto()
    FEATURE = auto()
    CUSTOM = auto()


@dataclass
class NamespaceRoute:
    """A route within a namespace."""
    path: str
    handler: Callable
    methods: List[str] = field(default_factory=lambda: ["GET"])
    middleware: List[Callable] = field(default_factory=list)
    description: str = ""
    deprecated: bool = False

    def matches(self, method: str, path: str) -> bool:
        """Check if this route matches the request."""
        if method.upper() not in [m.upper() for m in self.methods]:
            return False

        pattern = self._path_to_pattern(self.path)
        return bool(pattern.match(path))

    @staticmethod
    def _path_to_pattern(path: str) -> Pattern:
        """Convert path with params to regex pattern."""
        pattern_str = re.sub(r":(\w+)", r"(?P<\1>[^/]+)", path)
        return re.compile(f"^{pattern_str}$")


@dataclass
class Namespace:
    """Represents an API namespace."""
    name: str
    namespace_type: NamespaceType
    prefix: str
    routes: Dict[str, NamespaceRoute] = field(default_factory=dict)
    middleware: List[Callable] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent: Optional["Namespace"] = None
    children: List["Namespace"] = field(default_factory=list)

    def add_route(self, route: NamespaceRoute) -> None:
        """Add a route to this namespace."""
        self.routes[route.path] = route

    def get_route(self, path: str, method: str) -> Optional[NamespaceRoute]:
        """Get a matching route for the path and method."""
        for route in self.routes.values():
            if route.matches(method, path):
                return route
        return None

    def full_prefix(self) -> str:
        """Get the full prefixed path including parent namespaces."""
        if self.parent:
            return f"{self.parent.full_prefix()}{self.prefix}"
        return self.prefix


@dataclass
class NamespaceResolution:
    """Result of resolving a request to a namespace."""
    namespace: Optional[Namespace]
    route: Optional[NamespaceRoute]
    params: Dict[str, Any] = field(default_factory=dict)
    matched_path: str = ""
    error: Optional[str] = None


class NamespaceRegistry:
    """Registry for managing API namespaces."""

    def __init__(self):
        """Initialize the namespace registry."""
        self._namespaces: Dict[str, Namespace] = {}
        self._root = Namespace(
            name="root",
            namespace_type=NamespaceType.CUSTOM,
            prefix="",
        )

    def create_namespace(
        self,
        name: str,
        namespace_type: NamespaceType,
        prefix: str,
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Namespace:
        """
        Create a new namespace.

        Args:
            name: Unique name for the namespace.
            namespace_type: Type of namespace.
            prefix: URL prefix for this namespace.
            parent: Optional parent namespace name.
            metadata: Optional metadata.

        Returns:
            Created namespace.
        """
        if name in self._namespaces:
            raise ValueError(f"Namespace '{name}' already exists")

        parent_namespace = None
        if parent:
            parent_namespace = self._namespaces.get(parent)
            if not parent_namespace:
                raise ValueError(f"Parent namespace '{parent}' not found")

        namespace = Namespace(
            name=name,
            namespace_type=namespace_type,
            prefix=prefix,
            parent=parent_namespace,
            metadata=metadata or {},
        )

        self._namespaces[name] = namespace

        if parent_namespace:
            parent_namespace.children.append(namespace)

        logger.info(f"Created namespace: {name} ({namespace_type.name})")
        return namespace

    def get_namespace(self, name: str) -> Optional[Namespace]:
        """Get a namespace by name."""
        return self._namespaces.get(name)

    def list_namespaces(
        self,
        namespace_type: Optional[NamespaceType] = None,
    ) -> List[Namespace]:
        """List all namespaces, optionally filtered by type."""
        namespaces = list(self._namespaces.values())

        if namespace_type:
            namespaces = [n for n in namespaces if n.namespace_type == namespace_type]

        return namespaces

    def delete_namespace(self, name: str) -> bool:
        """Delete a namespace and its children."""
        if name not in self._namespaces:
            return False

        namespace = self._namespaces[name]

        for child in namespace.children:
            self.delete_namespace(child.name)

        if namespace.parent:
            namespace.parent.children.remove(namespace)

        del self._namespaces[name]
        return True


class ApiNamespaceAction:
    """
    Handles API namespace routing and organization.

    This action provides namespaced API routing for modular API design,
    supporting multi-tenant scenarios, API versioning, and service
    decomposition.

    Example:
        >>> action = ApiNamespaceAction()
        >>> ns = action.create_namespace("v1", NamespaceType.VERSION, "/api/v1")
        >>> action.add_route("v1", "/users", handler, ["GET"])
        >>> resolution = action.resolve("/api/v1/users", "GET")
        >>> print(resolution.namespace.name)
        v1
    """

    def __init__(self):
        """Initialize the API Namespace Action."""
        self.registry = NamespaceRegistry()
        self._global_middleware: List[Callable] = []

    def create_namespace(
        self,
        name: str,
        namespace_type: NamespaceType,
        prefix: str,
        parent: Optional[str] = None,
        **metadata,
    ) -> Namespace:
        """
        Create a new namespace.

        Args:
            name: Unique name for the namespace.
            namespace_type: Type of namespace.
            prefix: URL prefix.
            parent: Optional parent namespace name.
            **metadata: Additional metadata.

        Returns:
            Created namespace.
        """
        return self.registry.create_namespace(
            name=name,
            namespace_type=namespace_type,
            prefix=prefix,
            parent=parent,
            metadata=metadata,
        )

    def add_route(
        self,
        namespace_name: str,
        path: str,
        handler: Callable,
        methods: Optional[List[str]] = None,
        middleware: Optional[List[Callable]] = None,
        description: str = "",
        deprecated: bool = False,
    ) -> None:
        """
        Add a route to a namespace.

        Args:
            namespace_name: Name of the namespace.
            path: Route path.
            handler: Handler function.
            methods: HTTP methods.
            middleware: Route-specific middleware.
            description: Route description.
            deprecated: Whether route is deprecated.
        """
        namespace = self.registry.get_namespace(namespace_name)
        if not namespace:
            raise ValueError(f"Namespace '{namespace_name}' not found")

        route = NamespaceRoute(
            path=path,
            handler=handler,
            methods=methods or ["GET"],
            middleware=middleware or [],
            description=description,
            deprecated=deprecated,
        )

        namespace.add_route(route)
        logger.debug(f"Added route {methods} {path} to namespace '{namespace_name}'")

    def resolve(
        self,
        full_path: str,
        method: str,
    ) -> NamespaceResolution:
        """
        Resolve a request to a namespace and route.

        Args:
            full_path: Full request path.
            method: HTTP method.

        Returns:
            NamespaceResolution with matched namespace and route.
        """
        for namespace in self.registry.list_namespaces():
            prefix = namespace.full_prefix()

            if full_path.startswith(prefix):
                relative_path = full_path[len(prefix):]
                route = namespace.get_route(relative_path, method)

                if route:
                    params = self._extract_params(route.path, relative_path)
                    return NamespaceResolution(
                        namespace=namespace,
                        route=route,
                        params=params,
                        matched_path=relative_path,
                    )

        return NamespaceResolution(
            namespace=None,
            route=None,
            error="No matching namespace or route found",
        )

    def _extract_params(self, route_path: str, request_path: str) -> Dict[str, Any]:
        """Extract parameters from path."""
        params = {}
        route_parts = route_path.strip("/").split("/")
        path_parts = request_path.strip("/").split("/")

        for route_part, path_part in zip(route_parts, path_parts):
            if route_part.startswith(":"):
                params[route_part[1:]] = path_part

        return params

    def add_middleware(
        self,
        middleware: Callable,
        namespace_name: Optional[str] = None,
    ) -> None:
        """
        Add middleware globally or to a specific namespace.

        Args:
            middleware: Middleware function.
            namespace_name: Optional namespace to add to.
        """
        if namespace_name:
            namespace = self.registry.get_namespace(namespace_name)
            if namespace:
                namespace.middleware.append(middleware)
        else:
            self._global_middleware.append(middleware)

    async def execute(
        self,
        full_path: str,
        method: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute the handler for a resolved request.

        Args:
            full_path: Full request path.
            method: HTTP method.
            context: Request context.

        Returns:
            Handler response.
        """
        resolution = self.resolve(full_path, method)

        if resolution.error:
            raise ValueError(resolution.error)

        if not resolution.route:
            raise ValueError("No route matched")

        context = context or {}
        context["params"] = resolution.params
        context["namespace"] = resolution.namespace.name

        middleware = (
            self._global_middleware +
            resolution.namespace.middleware +
            resolution.route.middleware
        )

        for mw in middleware:
            context = await mw(context) or context

        return await resolution.route.handler(context)

    def get_routes_for_namespace(
        self,
        namespace_name: str,
    ) -> List[Dict[str, Any]]:
        """Get all routes for a namespace as dictionaries."""
        namespace = self.registry.get_namespace(namespace_name)
        if not namespace:
            return []

        return [
            {
                "path": f"{namespace.full_prefix()}{route.path}",
                "methods": route.methods,
                "description": route.description,
                "deprecated": route.deprecated,
            }
            for route in namespace.routes.values()
        ]

    def generate_openapi_paths(
        self,
    ) -> Dict[str, Any]:
        """Generate OpenAPI-compatible paths definition."""
        paths = {}

        for namespace in self.registry.list_namespaces():
            for route in namespace.routes.values():
                full_path = f"{namespace.full_prefix()}{route.path}"

                path_item = {}
                for method in route.methods:
                    method_lower = method.lower()
                    path_item[method_lower] = {
                        "description": route.description,
                        "deprecated": route.deprecated,
                    }

                paths[full_path] = path_item

        return paths

    def find_conflicts(self) -> List[Dict[str, Any]]:
        """Find conflicting routes across namespaces."""
        all_routes: List[Dict[str, Any]] = []

        for namespace in self.registry.list_namespaces():
            for route in namespace.routes.values():
                full_path = f"{namespace.full_prefix()}{route.path}"
                for method in route.methods:
                    all_routes.append({
                        "namespace": namespace.name,
                        "path": full_path,
                        "method": method,
                    })

        conflicts = []
        seen: Dict[str, int] = {}

        for route in all_routes:
            key = f"{route['method']}:{route['path']}"
            if key in seen:
                conflicts.append({
                    "path": route["path"],
                    "method": route["method"],
                    "namespaces": [seen[key], route["namespace"]],
                })
            else:
                seen[key] = route["namespace"]

        return conflicts


def create_namespace_action() -> ApiNamespaceAction:
    """Factory function to create an ApiNamespaceAction."""
    return ApiNamespaceAction()
