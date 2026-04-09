"""
API Version Router Action Module.

Handles API version negotiation and routing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple


@dataclass
class VersionRange:
    """Represents a version range constraint."""
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    exact_version: Optional[str] = None


@dataclass
class VersionedEndpoint:
    """An API endpoint with version constraints."""
    path: str
    method: str
    version_constraint: VersionRange
    handler: Callable[..., Any]
    deprecated: bool = False


@dataclass
class RouteResult:
    """Result of route operation."""
    handler: Optional[Callable[..., Any]]
    path_params: Dict[str, str]
    version: str
    deprecated: bool
    matched: bool


class ApiVersionRouterAction:
    """
    Routes API requests based on version.

    Supports semantic versioning and version ranges.
    """

    VERSION_PATTERN: Pattern[str] = re.compile(
        r"v?(\d+)(?:\.(\d+))?(?:\.(\d+))?"
    )

    def __init__(self, default_version: str = "1.0.0") -> None:
        self.default_version = default_version
        self._endpoints: List[VersionedEndpoint] = []
        self._version_cache: Dict[str, List[VersionedEndpoint]] = {}

    def register(
        self,
        path: str,
        method: str,
        handler: Callable[..., Any],
        version_constraint: VersionRange,
        deprecated: bool = False,
    ) -> None:
        """
        Register a versioned endpoint.

        Args:
            path: URL path
            method: HTTP method
            handler: Handler function
            version_constraint: Version constraints
            deprecated: Mark as deprecated
        """
        endpoint = VersionedEndpoint(
            path=path,
            method=method.upper(),
            version_constraint=version_constraint,
            handler=handler,
            deprecated=deprecated,
        )
        self._endpoints.append(endpoint)
        self._version_cache.clear()

    def route(
        self,
        path: str,
        method: str,
        version: Optional[str] = None,
    ) -> RouteResult:
        """
        Route a request to appropriate handler.

        Args:
            path: Request path
            method: HTTP method
            version: API version

        Returns:
            RouteResult with handler and params
        """
        version = version or self.default_version
        method = method.upper()

        path_params = self._extract_path_params(path)

        for endpoint in self._endpoints:
            if not self._matches_endpoint(
                endpoint, method, path, path_params, version
            ):
                continue

            if self._version_matches(version, endpoint.version_constraint):
                return RouteResult(
                    handler=endpoint.handler,
                    path_params=path_params,
                    version=version,
                    deprecated=endpoint.deprecated,
                    matched=True,
                )

        return RouteResult(
            handler=None,
            path_params={},
            version=version,
            deprecated=False,
            matched=False,
        )

    def _extract_path_params(self, path: str) -> Dict[str, str]:
        """Extract path parameters from URL."""
        params = {}
        parts = path.strip("/").split("/")

        for endpoint in self._endpoints:
            e_parts = endpoint.path.strip("/").split("/")

            if len(parts) != len(e_parts):
                continue

            for i, (p, e) in enumerate(zip(parts, e_parts)):
                if e.startswith("{") and e.endswith("}"):
                    param_name = e[1:-1]
                    params[param_name] = p
                elif p != e:
                    break
            else:
                return params

        return params

    def _matches_endpoint(
        self,
        endpoint: VersionedEndpoint,
        method: str,
        path: str,
        params: Dict[str, str],
        version: str,
    ) -> bool:
        """Check if request matches endpoint."""
        if endpoint.method != method:
            return False

        if not params and endpoint.path != path:
            return False

        return True

    def _version_matches(
        self,
        version: str,
        constraint: VersionRange,
    ) -> bool:
        """Check if version matches constraint."""
        if constraint.exact_version:
            return version == constraint.exact_version

        v_parts = self._parse_version(version)
        v_int = self._version_to_int(v_parts)

        if constraint.min_version:
            min_parts = self._parse_version(constraint.min_version)
            if v_int < self._version_to_int(min_parts):
                return False

        if constraint.max_version:
            max_parts = self._parse_version(constraint.max_version)
            if v_int > self._version_to_int(max_parts):
                return False

        return True

    def _parse_version(self, version: str) -> Tuple[int, int, int]:
        """Parse version string into parts."""
        match = self.VERSION_PATTERN.search(version)
        if not match:
            return (0, 0, 0)

        major = int(match.group(1)) if match.group(1) else 0
        minor = int(match.group(2)) if match.group(2) else 0
        patch = int(match.group(3)) if match.group(3) else 0

        return (major, minor, patch)

    def _version_to_int(self, parts: Tuple[int, int, int]) -> int:
        """Convert version parts to comparable int."""
        return parts[0] * 10000 + parts[1] * 100 + parts[2]

    def get_deprecated_endpoints(
        self,
        current_version: str,
    ) -> List[VersionedEndpoint]:
        """Get endpoints deprecated in current version."""
        result = []
        current_int = self._version_to_int(self._parse_version(current_version))

        for endpoint in self._endpoints:
            if not endpoint.deprecated:
                continue

            if endpoint.version_constraint.exact_version:
                constraint_int = self._version_to_int(
                    self._parse_version(endpoint.version_constraint.exact_version)
                )
                if current_int >= constraint_int:
                    result.append(endpoint)

        return result

    def negotiate_version(
        self,
        supported_versions: List[str],
        client_version: Optional[str] = None,
        accept_header: Optional[str] = None,
    ) -> str:
        """
        Negotiate best matching version.

        Args:
            supported_versions: List of supported versions
            client_version: Explicit client version
            accept_header: Accept header value

        Returns:
            Best matching version
        """
        if client_version:
            for version in supported_versions:
                if self._version_matches(version, VersionRange(exact_version=client_version)):
                    return version

        if accept_header and "version=" in accept_header:
            match = re.search(r'version=[\""]?([^\"\";]+)', accept_header)
            if match:
                header_version = match.group(1).strip()
                for version in supported_versions:
                    if self._version_matches(version, VersionRange(exact_version=header_version)):
                        return version

        return supported_versions[0] if supported_versions else self.default_version
