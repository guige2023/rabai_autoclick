"""API versioning and route management action."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class VersionFormat(str, Enum):
    """API version format."""

    HEADER = "header"  # X-API-Version
    PATH = "path"  # /v1/resource
    QUERY = "query"  # ?version=1
    ACCEPT_HEADER = "accept"  # Accept: application/vnd.api+json; version=1


@dataclass
class VersionConfig:
    """Configuration for an API version."""

    version: str
    deprecated: bool = False
    sunset_date: Optional[datetime] = None
    sunset_url: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RouteConfig:
    """Configuration for an API route."""

    path_pattern: str
    methods: list[str]
    version: str
    handler: Optional[Callable[..., Any]] = None
    deprecated: bool = False
    deprecation_message: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RouteMatch:
    """Result of matching a route."""

    route: RouteConfig
    matched: bool
    path_params: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, str] = field(default_factory=dict)
    version_warning: Optional[str] = None
    deprecation_warning: Optional[str] = None


class APIVersionRouterAction:
    """Routes API requests based on version and path."""

    def __init__(
        self,
        default_version: str = "v1",
        version_format: VersionFormat = VersionFormat.PATH,
    ):
        """Initialize version router.

        Args:
            default_version: Default API version.
            version_format: How version is specified in requests.
        """
        self._default_version = default_version
        self._version_format = version_format
        self._versions: dict[str, VersionConfig] = {}
        self._routes: list[RouteConfig] = []
        self._path_patterns: dict[str, re.Pattern] = {}
        self._on_deprecation_warning: Optional[Callable[[str, RouteConfig], None]] = None

    def register_version(self, config: VersionConfig) -> None:
        """Register an API version."""
        self._versions[config.version] = config

    def register_route(self, config: RouteConfig) -> None:
        """Register a route."""
        pattern = self._compile_pattern(config.path_pattern)
        self._path_patterns[config.path_pattern] = pattern
        self._routes.append(config)

    def _compile_pattern(self, path_pattern: str) -> re.Pattern:
        """Compile a path pattern to regex."""
        pattern_str = path_pattern
        pattern_str = re.sub(r"\{([^}]+)\}", r"(?P<\1>[^/]+)", pattern_str)
        pattern_str = f"^{pattern_str}$"
        return re.compile(pattern_str)

    def _extract_version(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        query_params: dict[str, str],
    ) -> tuple[str, Optional[str]]:
        """Extract version from request."""
        version = self._default_version
        warning = None

        if self._version_format == VersionFormat.PATH:
            match = re.match(r"^/(v\d+)/", path)
            if match:
                version = match.group(1)

        elif self._version_format == VersionFormat.HEADER:
            version = headers.get("X-API-Version", version)

        elif self._version_format == VersionFormat.QUERY:
            version = query_params.get("version", version)

        elif self._version_format == VersionFormat.ACCEPT_HEADER:
            accept = headers.get("Accept", "")
            match = re.search(r'version=(\d+)', accept)
            if match:
                version = f"v{match.group(1)}"

        if version not in self._versions:
            warning = f"Unknown version {version}, using default"
            version = self._default_version

        return version, warning

    def match_route(
        self,
        method: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
        query_params: Optional[dict[str, str]] = None,
    ) -> RouteMatch:
        """Match a request to a route.

        Args:
            method: HTTP method.
            path: Request path.
            headers: Request headers.
            query_params: Query parameters.

        Returns:
            RouteMatch with match result.
        """
        headers = headers or {}
        query_params = query_params or {}

        version, version_warning = self._extract_version(
            method, path, headers, query_params
        )

        for route in self._routes:
            if version != route.version:
                continue

            if method.upper() not in [m.upper() for m in route.methods]:
                continue

            pattern = self._path_patterns.get(route.path_pattern)
            if not pattern:
                continue

            match = pattern.match(path)
            if match:
                path_params = match.groupdict()

                deprecation_warning = None
                if route.deprecated or self._versions.get(version, VersionConfig(version="")).deprecated:
                    deprecation_warning = (
                        route.deprecation_message
                        or f"Route {method} {path} is deprecated"
                    )
                    if self._on_deprecation_warning:
                        self._on_deprecation_warning(version, route)

                return RouteMatch(
                    route=route,
                    matched=True,
                    path_params=path_params,
                    query_params=query_params,
                    version_warning=version_warning,
                    deprecation_warning=deprecation_warning,
                )

        return RouteMatch(
            route=RouteConfig(path_pattern="", methods=[], version=""),
            matched=False,
        )

    def get_routes_for_version(self, version: str) -> list[RouteConfig]:
        """Get all routes for a specific version."""
        return [r for r in self._routes if r.version == version]

    def get_deprecated_routes(self) -> list[RouteConfig]:
        """Get all deprecated routes."""
        return [r for r in self._routes if r.deprecated]

    def check_sunset_versions(self) -> list[tuple[str, VersionConfig]]:
        """Check for versions past their sunset date."""
        now = datetime.now()
        sunset = []
        for version, config in self._versions.items():
            if config.sunset_date and config.sunset_date < now:
                sunset.append((version, config))
        return sunset

    def set_deprecation_callback(
        self,
        callback: Callable[[str, RouteConfig], None],
    ) -> None:
        """Set callback for deprecation warnings."""
        self._on_deprecation_warning = callback
