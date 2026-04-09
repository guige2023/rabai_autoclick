"""
API endpoint management module.

Provides endpoint registration, routing, versioning, and
metadata management for API automation workflows.

Author: Aito Auto Agent
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional, Any


class HttpMethod(Enum):
    """HTTP method types."""
    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


class EndpointStatus(Enum):
    """Endpoint lifecycle status."""
    ACTIVE = auto()
    DEPRECATED = auto()
    SUNSET = auto()
    retired = auto()


@dataclass
class EndpointParameter:
    """API endpoint parameter definition."""
    name: str
    location: str  # path, query, header, body, cookie
    param_type: str  # string, integer, boolean, array, object
    required: bool = False
    default: Any = None
    description: str = ""
    pattern: Optional[str] = None
    enum_values: Optional[list[Any]] = None


@dataclass
class EndpointSecurity:
    """Security configuration for an endpoint."""
    auth_required: bool = True
    scopes: list[str] = field(default_factory=list)
    api_key_location: Optional[str] = None  # header, query, cookie


@dataclass
class EndpointRateLimit:
    """Rate limiting configuration."""
    requests_per_second: Optional[float] = None
    requests_per_minute: Optional[int] = None
    requests_per_hour: Optional[int] = None
    requests_per_day: Optional[int] = None
    burst_size: Optional[int] = None


@dataclass
class EndpointMetadata:
    """Additional endpoint metadata."""
    tags: list[str] = field(default_factory=list)
    summary: str = ""
    description: str = ""
    deprecated_message: str = ""
    version: str = "1.0.0"
    sunset_date: Optional[str] = None
    external_docs: Optional[str] = None


@dataclass
class ApiEndpoint:
    """
    Complete API endpoint definition.

    Represents a single API endpoint with all its configuration,
    parameters, and metadata.
    """
    path: str
    method: HttpMethod
    handler: Optional[Callable[..., Any]] = None
    parameters: list[EndpointParameter] = field(default_factory=list)
    security: EndpointSecurity = field(default_factory=EndpointSecurity)
    rate_limit: Optional[EndpointRateLimit] = None
    metadata: EndpointMetadata = field(default_factory=EndpointMetadata)
    status: EndpointStatus = EndpointStatus.ACTIVE
    response_schema: Optional[dict[str, Any]] = None
    error_schema: Optional[dict[str, Any]] = None

    @property
    def full_path(self) -> str:
        """Get full path with method."""
        return f"{self.method.name} {self.path}"

    def get_required_params(self) -> list[EndpointParameter]:
        """Get list of required parameters."""
        return [p for p in self.parameters if p.required]

    def get_optional_params(self) -> list[EndpointParameter]:
        """Get list of optional parameters."""
        return [p for p in self.parameters if not p.required]


class PathMatcher:
    """Utility for matching paths against endpoint patterns."""

    PARAM_PATTERN = re.compile(r'\{([^}]+)\}')

    @classmethod
    def match(cls, pattern: str, path: str) -> Optional[dict[str, str]]:
        """
        Match a path against a pattern with {param} placeholders.

        Args:
            pattern: Path pattern with {param} placeholders
            path: Actual path to match

        Returns:
            Dict of captured parameters if matched, None otherwise
        """
        param_names = cls.PARAM_PATTERN.findall(pattern)
        regex_pattern = cls.PARAM_PATTERN.sub(r'([^/]+)', pattern)
        regex_pattern = f'^{regex_pattern}$'

        match = re.match(regex_pattern, path)
        if not match:
            return None

        return {name: value for name, value in zip(param_names, match.groups())}

    @classmethod
    def extract_params(cls, pattern: str) -> list[str]:
        """Extract parameter names from a pattern."""
        return cls.PARAM_PATTERN.findall(pattern)


class EndpointRegistry:
    """
    Central registry for API endpoints.

    Provides registration, lookup, and routing capabilities.

    Example:
        registry = EndpointRegistry()

        @registry.register(method=HttpMethod.GET, path="/users/{id}")
        def get_user(id: str):
            return {"user_id": id}

        endpoint = registry.find(HttpMethod.GET, "/users/123")
        if endpoint:
            print(f"Found: {endpoint.full_path}")
    """

    def __init__(self):
        self._endpoints: dict[str, ApiEndpoint] = {}
        self._path_index: dict[str, list[str]] = {}
        self._method_index: dict[HttpMethod, list[str]] = {m: [] for m in HttpMethod}
        self._tag_index: dict[str, list[str]] = {}

    def register(
        self,
        path: str,
        method: HttpMethod,
        handler: Optional[Callable[..., Any]] = None,
        **kwargs
    ) -> Callable:
        """
        Decorator to register an endpoint.

        Args:
            path: Endpoint path
            method: HTTP method
            handler: Optional handler function
            **kwargs: Additional ApiEndpoint fields

        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            endpoint = ApiEndpoint(
                path=path,
                method=method,
                handler=func,
                **{k: v for k, v in kwargs.items() if v is not None}
            )
            self.add(endpoint)
            return func

        return decorator

    def add(self, endpoint: ApiEndpoint) -> None:
        """
        Add an endpoint to the registry.

        Args:
            endpoint: The endpoint to add
        """
        key = endpoint.full_path
        self._endpoints[key] = endpoint

        self._method_index[endpoint.method].append(key)

        for tag in endpoint.metadata.tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = []
            self._tag_index[tag].append(key)

        for param_name in PathMatcher.extract_params(endpoint.path):
            if param_name not in self._path_index:
                self._path_index[param_name] = []
            self._path_index[param_name].append(key)

    def find(self, method: HttpMethod, path: str) -> Optional[ApiEndpoint]:
        """
        Find an endpoint by method and path.

        Args:
            method: HTTP method
            path: Request path

        Returns:
            Matching endpoint or None
        """
        for key in self._method_index.get(method, []):
            endpoint = self._endpoints[key]
            params = PathMatcher.match(endpoint.path, path)
            if params is not None:
                return endpoint

        return None

    def find_by_tag(self, tag: str) -> list[ApiEndpoint]:
        """Find all endpoints with a specific tag."""
        keys = self._tag_index.get(tag, [])
        return [self._endpoints[k] for k in keys]

    def find_by_path_pattern(self, pattern: str) -> list[ApiEndpoint]:
        """Find endpoints matching a path pattern."""
        results = []
        for endpoint in self._endpoints.values():
            if PathMatcher.match(endpoint.path, pattern):
                results.append(endpoint)
        return results

    def get_all(self) -> list[ApiEndpoint]:
        """Get all registered endpoints."""
        return list(self._endpoints.values())

    def get_by_status(self, status: EndpointStatus) -> list[ApiEndpoint]:
        """Get all endpoints with a specific status."""
        return [e for e in self._endpoints.values() if e.status == status]

    def get_Active(self) -> list[ApiEndpoint]:
        """Get all active endpoints."""
        return self.get_by_status(EndpointStatus.ACTIVE)

    def get_deprecated(self) -> list[ApiEndpoint]:
        """Get all deprecated endpoints."""
        return self.get_by_status(EndpointStatus.DEPRECATED)

    def remove(self, path: str, method: HttpMethod) -> bool:
        """Remove an endpoint from the registry."""
        key = f"{method.name} {path}"
        if key in self._endpoints:
            del self._endpoints[key]
            self._method_index[method].remove(key)
            return True
        return False

    def count(self) -> int:
        """Get total number of registered endpoints."""
        return len(self._endpoints)


class EndpointRouter:
    """
    Router for dispatching requests to registered endpoints.

    Supports middleware, filtering, and custom dispatch logic.
    """

    def __init__(self, registry: Optional[EndpointRegistry] = None):
        self._registry = registry or EndpointRegistry()
        self._middleware: list[Callable[..., Any]] = []
        self._filters: list[Callable[[ApiEndpoint], bool]] = []

    @property
    def registry(self) -> EndpointRegistry:
        """Get the endpoint registry."""
        return self._registry

    def add_middleware(self, middleware: Callable[..., Any]) -> None:
        """Add middleware to the router."""
        self._middleware.append(middleware)

    def add_filter(self, filter_func: Callable[[ApiEndpoint], bool]) -> None:
        """Add a filter function to the router."""
        self._filters.append(filter_func)

    def dispatch(
        self,
        method: HttpMethod,
        path: str,
        **context
    ) -> Optional[Any]:
        """
        Dispatch a request to the matching endpoint.

        Args:
            method: HTTP method
            path: Request path
            **context: Additional context to pass to handlers

        Returns:
            Handler response or None
        """
        endpoint = self._registry.find(method, path)
        if not endpoint:
            return None

        for filter_func in self._filters:
            if not filter_func(endpoint):
                return None

        for middleware in self._middleware:
            result = middleware(endpoint, **context)
            if result is not None:
                return result

        if endpoint.handler:
            params = PathMatcher.match(endpoint.path, path) or {}
            context.update(params)
            return endpoint.handler(**context)

        return None

    def list_routes(self) -> list[tuple[str, str, str]]:
        """
        List all registered routes.

        Returns:
            List of (method, path, summary) tuples
        """
        routes = []
        for endpoint in self._registry.get_Active():
            routes.append((
                endpoint.method.name,
                endpoint.path,
                endpoint.metadata.summary or endpoint.full_path
            ))
        return sorted(routes)


class EndpointBuilder:
    """Fluent builder for ApiEndpoint objects."""

    def __init__(self, path: str, method: HttpMethod):
        self._endpoint = ApiEndpoint(path=path, method=method)

    def with_handler(self, handler: Callable) -> EndpointBuilder:
        self._endpoint.handler = handler
        return self

    def with_parameter(self, param: EndpointParameter) -> EndpointBuilder:
        self._endpoint.parameters.append(param)
        return self

    def with_parameters(self, params: list[EndpointParameter]) -> EndpointBuilder:
        self._endpoint.parameters.extend(params)
        return self

    def with_security(
        self,
        auth_required: bool = True,
        scopes: Optional[list[str]] = None
    ) -> EndpointBuilder:
        self._endpoint.security = EndpointSecurity(
            auth_required=auth_required,
            scopes=scopes or []
        )
        return self

    def with_rate_limit(self, rate_limit: EndpointRateLimit) -> EndpointBuilder:
        self._endpoint.rate_limit = rate_limit
        return self

    def with_metadata(
        self,
        tags: Optional[list[str]] = None,
        summary: str = "",
        description: str = "",
        version: str = "1.0.0"
    ) -> EndpointBuilder:
        self._endpoint.metadata = EndpointMetadata(
            tags=tags or [],
            summary=summary,
            description=description,
            version=version
        )
        return self

    def with_status(self, status: EndpointStatus) -> EndpointBuilder:
        self._endpoint.status = status
        return self

    def deprecated(self, message: str = "") -> EndpointBuilder:
        self._endpoint.status = EndpointStatus.DEPRECATED
        self._endpoint.metadata.deprecated_message = message
        return self

    def build(self) -> ApiEndpoint:
        return self._endpoint


def create_registry() -> EndpointRegistry:
    """Factory function to create an EndpointRegistry."""
    return EndpointRegistry()


def create_router(registry: Optional[EndpointRegistry] = None) -> EndpointRouter:
    """Factory function to create an EndpointRouter."""
    return EndpointRouter(registry=registry)


def builder(path: str, method: HttpMethod) -> EndpointBuilder:
    """Create an EndpointBuilder for fluent endpoint definition."""
    return EndpointBuilder(path, method)
