"""
API versioning manager for handling multiple API versions.

This module provides API version negotiation, routing, and deprecation
management across multiple API versions.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import threading

logger = logging.getLogger(__name__)


class VersionScheme(Enum):
    """API version schemes."""
    PATH = auto()       # /v1/resource
    HEADER = auto()     # Accept: application/vnd.api.v1+json
    QUERY = auto()      # /resource?version=1
    MEDIA_TYPE = auto() # Accept: application/vnd.example.v1+json


@dataclass
class APIVersion:
    """Represents an API version."""
    major: int
    minor: int = 0
    patch: int = 0

    def __str__(self) -> str:
        if self.patch > 0:
            return f"{self.major}.{self.minor}.{self.patch}"
        if self.minor > 0:
            return f"{self.major}.{self.minor}"
        return f"v{self.major}"

    @property
    def version_string(self) -> str:
        """Get full version string."""
        return str(self)

    def is_compatible(self, other: "APIVersion") -> bool:
        """Check if version is compatible with another."""
        return self.major == other.major

    def __lt__(self, other: "APIVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other: "APIVersion") -> bool:
        return self == other or self < other

    def __gt__(self, other: "APIVersion") -> bool:
        return other < self

    def __ge__(self, other: "APIVersion") -> bool:
        return self == other or self > other

    @classmethod
    def parse(cls, version_str: str) -> "APIVersion":
        """Parse version string to APIVersion."""
        version_str = version_str.strip().lstrip("v")

        if "-" in version_str:
            version_str = version_str.split("-")[0]

        parts = version_str.split(".")
        major = int(parts[0]) if parts else 1
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0

        return cls(major=major, minor=minor, patch=patch)


@dataclass
class VersionInfo:
    """Information about an API version."""
    version: APIVersion
    is_deprecated: bool = False
    deprecation_date: Optional[float] = None
    sunset_date: Optional[float] = None
    migration_guide: Optional[str] = None
    handlers: Dict[str, Callable] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=lambda: defaultdict(int))


class VersioningManager:
    """
    Manage multiple API versions with routing and deprecation.

    Features:
    - Multiple version schemes (path, header, query, media type)
    - Version negotiation
    - Deprecation and sunset management
    - Request routing to versioned handlers
    - Migration assistance

    Example:
        >>> manager = VersioningManager(scheme=VersionScheme.PATH)
        >>> manager.register_version(APIVersion(1, 0))
        >>> manager.register_version(APIVersion(2, 0))
        >>> manager.register_handler(APIVersion(1, 0), "users", get_users_v1)
        >>> manager.register_handler(APIVersion(2, 0), "users", get_users_v2)
        >>>
        >>> response = manager.route_request(request, endpoint="users")
    """

    def __init__(
        self,
        scheme: VersionScheme = VersionScheme.PATH,
        default_version: Optional[APIVersion] = None,
        supported_versions: Optional[List[APIVersion]] = None,
    ):
        """
        Initialize versioning manager.

        Args:
            scheme: Version scheme to use
            default_version: Default version for requests
            supported_versions: List of supported versions
        """
        self.scheme = scheme
        self.default_version = default_version or APIVersion(1, 0)
        self._versions: Dict[str, VersionInfo] = {}
        self._endpoints: Dict[str, Dict[str, Callable]] = defaultdict(dict)
        self._lock = threading.RLock()
        self._stats = {
            "total_requests": 0,
            "requests_by_version": defaultdict(int),
            "deprecated_requests": 0,
        }

        if supported_versions:
            for v in supported_versions:
                self.register_version(v)

        logger.info(
            f"VersioningManager initialized (scheme={scheme.name}, "
            f"default={self.default_version})"
        )

    def register_version(
        self,
        version: APIVersion,
        deprecation_date: Optional[float] = None,
        sunset_date: Optional[float] = None,
        migration_guide: Optional[str] = None,
    ) -> VersionInfo:
        """
        Register an API version.

        Args:
            version: API version
            deprecation_date: Unix timestamp when deprecated
            sunset_date: Unix timestamp when sunset
            migration_guide: URL or guide for migration

        Returns:
            Created VersionInfo
        """
        with self._lock:
            key = str(version)
            info = VersionInfo(
                version=version,
                deprecation_date=deprecation_date,
                sunset_date=sunset_date,
                migration_guide=migration_guide,
            )
            self._versions[key] = info
            logger.info(f"Registered API version: {version}")
            return info

    def register_handler(
        self,
        version: APIVersion,
        endpoint: str,
        handler: Callable[..., Any],
    ) -> None:
        """
        Register a handler for a versioned endpoint.

        Args:
            version: API version
            endpoint: Endpoint name
            handler: Handler function
        """
        with self._lock:
            key = str(version)
            if key not in self._versions:
                self.register_version(version)

            self._endpoints[endpoint][key] = handler
            logger.debug(f"Registered handler: {endpoint}@{version}")

    def deprecate_version(
        self,
        version: APIVersion,
        sunset_date: Optional[float] = None,
        migration_guide: Optional[str] = None,
    ) -> None:
        """
        Mark a version as deprecated.

        Args:
            version: Version to deprecate
            sunset_date: When to stop serving
            migration_guide: Migration instructions
        """
        with self._lock:
            key = str(version)
            if key in self._versions:
                self._versions[key].is_deprecated = True
                self._versions[key].deprecation_date = time.time()
                if sunset_date:
                    self._versions[key].sunset_date = sunset_date
                if migration_guide:
                    self._versions[key].migration_guide = migration_guide
                logger.warning(f"Version {version} marked as deprecated")

    def get_version(self, version_str: str) -> Optional[APIVersion]:
        """Parse and validate a version string."""
        try:
            return APIVersion.parse(version_str)
        except Exception:
            return None

    def parse_version_from_request(
        self,
        path: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> Tuple[Optional[APIVersion], bool]:
        """
        Extract version from request.

        Args:
            path: Request path
            headers: Request headers
            query_params: Query parameters

        Returns:
            Tuple of (version, is_deprecated)
        """
        version_str = None

        if self.scheme == VersionScheme.PATH and path:
            import re
            match = re.search(r"/v(\d+)(?:\.(\d+))?(?:\.(\d+))?/", path)
            if match:
                major = int(match.group(1))
                minor = int(match.group(2)) if match.group(2) else 0
                patch = int(match.group(3)) if match.group(3) else 0
                version_str = f"v{major}"
                if minor > 0:
                    version_str += f".{minor}"
                if patch > 0:
                    version_str += f".{patch}"

        elif self.scheme == VersionScheme.HEADER and headers:
            accept = headers.get("Accept", "")
            match_header = accept.find("v")
            if match_header >= 0:
                version_str = accept[match_header:].split("+")[0]

        elif self.scheme == VersionScheme.QUERY and query_params:
            version_str = query_params.get("version")

        if not version_str:
            return self.default_version, False

        version = self.get_version(version_str)
        if not version:
            return None, False

        version_key = str(version)
        is_deprecated = (
            version_key in self._versions
            and self._versions[version_key].is_deprecated
        )

        return version, is_deprecated

    def route_request(
        self,
        version: APIVersion,
        endpoint: str,
        *args,
        **kwargs,
    ) -> Any:
        """
        Route a request to the appropriate versioned handler.

        Args:
            version: API version
            endpoint: Endpoint name
            *args: Handler positional arguments
            **kwargs: Handler keyword arguments

        Returns:
            Handler result
        """
        with self._lock:
            self._stats["total_requests"] += 1
            version_key = str(version)
            self._stats["requests_by_version"][version_key] += 1

            if version_key not in self._versions:
                logger.warning(f"Unknown version {version}, falling back to default")
                version = self.default_version
                version_key = str(version)

            if version_key in self._versions:
                if self._versions[version_key].is_deprecated:
                    self._stats["deprecated_requests"] += 1

            if endpoint not in self._endpoints:
                raise ValueError(f"Unknown endpoint: {endpoint}")

            handler = self._endpoints[endpoint].get(version_key)
            if not handler:
                raise ValueError(
                    f"No handler for endpoint {endpoint}@version {version}"
                )

            self._versions[version_key].stats["requests"] += 1
            return handler(*args, **kwargs)

    def negotiate_version(
        self,
        client_versions: List[str],
    ) -> Tuple[Optional[APIVersion], bool]:
        """
        Negotiate best version with client.

        Args:
            client_versions: Versions client accepts

        Returns:
            Tuple of (best_version, is_deprecated)
        """
        client_parsed = []
        for v in client_versions:
            parsed = self.get_version(v)
            if parsed:
                client_parsed.append(parsed)

        if not client_parsed:
            return None, False

        client_parsed.sort(reverse=True)

        for client_v in client_parsed:
            version_key = str(client_v)
            if version_key in self._versions:
                is_deprecated = self._versions[version_key].is_deprecated
                return client_v, is_deprecated

        return client_parsed[0], False

    def get_deprecation_warning(
        self,
        version: APIVersion,
    ) -> Optional[Dict[str, Any]]:
        """Get deprecation info for a version."""
        version_key = str(version)
        if version_key not in self._versions:
            return None

        info = self._versions[version_key]
        if not info.is_deprecated:
            return None

        warning = {
            "deprecated": True,
            "deprecation_date": info.deprecation_date,
        }

        if info.sunset_date:
            warning["sunset_date"] = info.sunset_date
            warning["days_remaining"] = max(
                0, int((info.sunset_date - time.time()) / 86400)
            )

        if info.migration_guide:
            warning["migration_guide"] = info.migration_guide

        return warning

    def get_stats(self) -> Dict[str, Any]:
        """Get versioning statistics."""
        return {
            "total_requests": self._stats["total_requests"],
            "requests_by_version": dict(self._stats["requests_by_version"]),
            "deprecated_requests": self._stats["deprecated_requests"],
            "registered_versions": [
                {
                    "version": key,
                    "is_deprecated": info.is_deprecated,
                    "requests": sum(info.stats.values()),
                }
                for key, info in self._versions.items()
            ],
        }
