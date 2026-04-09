"""API versioning utilities.

This module provides API versioning:
- Version parsing and comparison
- Version negotiation
- Deprecation handling
- Version routing

Example:
    >>> from actions.api_versioning_action import VersionRouter
    >>> router = VersionRouter()
    >>> handler = router.route("v2", api_handlers)
"""

from __future__ import annotations

import re
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass
from functools import total_ordering

logger = logging.getLogger(__name__)


@total_ordering
class APIVersion:
    """Semantic API version.

    Example:
        >>> v1 = APIVersion("1.2.3")
        >>> v2 = APIVersion("2.0.0")
        >>> v1 < v2  # True
    """

    def __init__(self, version: str) -> None:
        self.original = version
        self.major, self.minor, self.patch = self._parse(version)

    def _parse(self, version: str) -> tuple[int, int, int]:
        """Parse version string."""
        match = re.match(r"v?(\d+)(?:\.(\d+))?(?:\.(\d+))?", version)
        if not match:
            return 0, 0, 0
        major = int(match.group(1))
        minor = int(match.group(2)) if match.group(2) else 0
        patch = int(match.group(3)) if match.group(3) else 0
        return major, minor, patch

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, APIVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, APIVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __repr__(self) -> str:
        return f"APIVersion({self.major}.{self.minor}.{self.patch})"

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def is_compatible_with(self, other: APIVersion) -> bool:
        """Check if versions are compatible (same major)."""
        return self.major == other.major


@dataclass
class VersionInfo:
    """Version information."""
    version: APIVersion
    deprecated: bool = False
    sunset_date: Optional[str] = None
    sunset_url: Optional[str] = None
    alternatives: list[str] = field(default_factory=list)


class VersionRouter:
    """Route requests based on API version.

    Example:
        >>> router = VersionRouter()
        >>> router.register("v1", handler_v1)
        >>> router.register("v2", handler_v2)
        >>> handler = router.route("v2")
    """

    def __init__(self, default_version: Optional[str] = None) -> None:
        self.default_version = APIVersion(default_version) if default_version else None
        self._handlers: dict[str, Callable[..., Any]] = {}
        self._version_info: dict[str, VersionInfo] = {}

    def register(
        self,
        version: str,
        handler: Callable[..., Any],
        deprecated: bool = False,
        alternatives: Optional[list[str]] = None,
    ) -> None:
        """Register a versioned handler.

        Args:
            version: Version string (e.g., "v1.2.0").
            handler: Handler function.
            deprecated: Whether this version is deprecated.
            alternatives: Alternative version suggestions.
        """
        self._handlers[version] = handler
        self._version_info[version] = VersionInfo(
            version=APIVersion(version),
            deprecated=deprecated,
            alternatives=alternatives or [],
        )
        logger.info(f"Registered handler for version: {version}")

    def route(
        self,
        version: str,
        default: Optional[Callable[..., Any]] = None,
    ) -> Optional[Callable[..., Any]]:
        """Route to the appropriate handler.

        Args:
            version: Requested version.
            default: Default handler if version not found.

        Returns:
            Handler function or None.
        """
        if version in self._handlers:
            return self._handlers[version]
        if default:
            return default
        if self.default_version:
            return self._handlers.get(str(self.default_version))
        return None

    def get_version_info(self, version: str) -> Optional[VersionInfo]:
        """Get information about a version."""
        return self._version_info.get(version)

    def get_supported_versions(self) -> list[str]:
        """Get list of supported versions."""
        return list(self._handlers.keys())

    def is_supported(self, version: str) -> bool:
        """Check if version is supported."""
        return version in self._handlers

    def is_deprecated(self, version: str) -> bool:
        """Check if version is deprecated."""
        info = self._version_info.get(version)
        return info.deprecated if info else False

    def negotiate_version(
        self,
        accept: Optional[str] = None,
        supported: Optional[list[str]] = None,
    ) -> Optional[str]:
        """Negotiate best version from Accept header.

        Args:
            accept: Accept header value.
            supported: List of supported versions.

        Returns:
            Best matching version or None.
        """
        if not accept:
            return None
        supported = supported or list(self._handlers.keys())
        requested = self._parse_accept_header(accept)
        for req_version in requested:
            if req_version in supported:
                return req_version
        return None

    def _parse_accept_header(self, accept: str) -> list[str]:
        """Parse Accept header to extract versions."""
        versions = []
        for part in accept.split(","):
            part = part.strip()
            match = re.search(r"v?(\d+(?:\.\d+)?(?:\.\d+)?)", part)
            if match:
                versions.append(match.group(1))
        return versions


class VersioningMiddleware:
    """Middleware for handling API versioning in requests."""

    def __init__(self, router: VersionRouter) -> None:
        self.router = router

    def process_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Process request and add version info.

        Args:
            request: Request dictionary.

        Returns:
            Modified request with version info.
        """
        version = request.get("version") or request.get("headers", {}).get("X-API-Version")
        if version:
            request["api_version"] = APIVersion(version)
            request["version_info"] = self.router.get_version_info(version)
            request["handler"] = self.router.route(version)
        return request


def parse_version_string(version: str) -> APIVersion:
    """Parse a version string to APIVersion.

    Args:
        version: Version string.

    Returns:
        APIVersion object.
    """
    return APIVersion(version)


def compare_versions(v1: str, v2: str) -> int:
    """Compare two version strings.

    Args:
        v1: First version.
        v2: Second version.

    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
    """
    a, b = APIVersion(v1), APIVersion(v2)
    if a < b:
        return -1
    elif a > b:
        return 1
    return 0
