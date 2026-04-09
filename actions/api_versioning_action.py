"""
API Versioning Action Module.

Provides API versioning strategies including header-based,
URL-based, and content negotiation versioning with deprecation support.
"""

import re
import time
from typing import Optional, Dict, Any, Callable, List, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class VersioningStrategy(Enum):
    """API versioning strategy types."""
    HEADER = "header"  # X-API-Version header
    URL_PATH = "url_path"  # /v1/resource
    QUERY_PARAM = "query_param"  # ?version=1
    CONTENT_NEGOTIATION = "content_negotiation"  # Accept header


@dataclass
class APIVersion:
    """Represents an API version."""
    version: str  # e.g., "v1", "v2"
    major: int
    minor: int
    deprecated: bool = False
    sunset_date: Optional[str] = None  # ISO date for deprecation
    sunset_timestamp: Optional[float] = None
    docs_url: Optional[str] = None

    def __post_init__(self):
        if self.sunset_date:
            try:
                from datetime import datetime
                self.sunset_timestamp = datetime.fromisoformat(
                    self.sunset_date.replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                self.sunset_timestamp = None

    def is_sunset(self) -> bool:
        """Check if version has passed sunset date."""
        if self.sunset_timestamp:
            return time.time() >= self.sunset_timestamp
        return False

    def compare_to(self, other: "APIVersion") -> int:
        """Compare versions. Returns -1, 0, or 1."""
        if self.major < other.major:
            return -1
        if self.major > other.major:
            return 1
        if self.minor < other.minor:
            return -1
        if self.minor > other.minor:
            return 1
        return 0


@dataclass
class VersioningConfig:
    """Configuration for API versioning."""
    strategy: VersioningStrategy = VersioningStrategy.URL_PATH
    default_version: str = "v1"
    supported_versions: List[str] = field(default_factory=lambda: ["v1", "v2"])
    header_name: str = "X-API-Version"
    query_param_name: str = "version"
    strict_mode: bool = True  # Reject unknown versions


@dataclass
class DeprecationWarning:
    """Deprecation warning details."""
    version: str
    message: str
    migration_guide: Optional[str] = None
    alternatives: List[str] = field(default_factory=list)


class APIVersioningAction:
    """
    API versioning management action.

    Handles version detection, routing, and deprecation warnings
    for multi-version API support.
    """

    def __init__(self, config: Optional[VersioningConfig] = None):
        self.config = config or VersioningConfig()
        self._versions: Dict[str, APIVersion] = {}
        self._handlers: Dict[str, Dict[str, Callable]] = defaultdict(dict)
        self._deprecations: Dict[str, DeprecationWarning] = {}
        self._stats: Dict[str, int] = defaultdict(int)

        # Initialize default versions
        for v in self.config.supported_versions:
            self.register_version(v)

    def register_version(
        self,
        version: str,
        deprecated: bool = False,
        sunset_date: Optional[str] = None,
        docs_url: Optional[str] = None,
    ) -> "APIVersioningAction":
        """Register a new API version."""
        major, minor = self._parse_version(version)
        api_version = APIVersion(
            version=version,
            major=major,
            minor=minor,
            deprecated=deprecated,
            sunset_date=sunset_date,
            docs_url=docs_url,
        )
        self._versions[version] = api_version
        return self

    def register_handler(
        self,
        version: str,
        endpoint: str,
        handler: Callable,
    ) -> "APIVersioningAction":
        """Register a handler for a versioned endpoint."""
        self._handlers[version][endpoint] = handler
        return self

    def set_deprecation(
        self,
        version: str,
        message: str,
        migration_guide: Optional[str] = None,
        alternatives: Optional[List[str]] = None,
    ) -> "APIVersioningAction":
        """Set deprecation warning for a version."""
        self._deprecations[version] = DeprecationWarning(
            version=version,
            message=message,
            migration_guide=migration_guide,
            alternatives=alternatives or [],
        )
        if version in self._versions:
            self._versions[version].deprecated = True
        return self

    def _parse_version(self, version: str) -> tuple[int, int]:
        """Parse version string into major and minor numbers."""
        match = re.match(r"v?(\d+)(?:\.(\d+))?", version)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2) or 0)
            return major, minor
        return 1, 0

    def _extract_version_from_header(
        self,
        headers: Dict[str, str],
    ) -> Optional[str]:
        """Extract version from request headers."""
        header_name = self.config.header_name.replace("-", "_").lower()
        for key, value in headers.items():
            if key.lower().replace("-", "_") == header_name:
                return value
        return None

    def _extract_version_from_url(self, path: str) -> Optional[str]:
        """Extract version from URL path."""
        match = re.match(r"^/(v\d+(?:/\d+(?:\.\d+)?)?)", path)
        if match:
            return match.group(1)
        match = re.match(r"^/(api/v\d+)", path)
        if match:
            return match.group(1)
        return None

    def _extract_version_from_query(
        self,
        query_params: Dict[str, str],
    ) -> Optional[str]:
        """Extract version from query parameters."""
        return query_params.get(self.config.query_param_name)

    def detect_version(
        self,
        path: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Detect API version from request.

        Returns:
            Tuple of (version, warning_message)
        """
        version = None
        warning = None

        if self.config.strategy == VersioningStrategy.HEADER:
            if headers:
                version = self._extract_version_from_header(headers)

        elif self.config.strategy == VersioningStrategy.URL_PATH:
            if path:
                version = self._extract_version_from_url(path)

        elif self.config.strategy == VersioningStrategy.QUERY_PARAM:
            if query_params:
                version = self._extract_version_from_query(query_params)

        elif self.config.strategy == VersioningStrategy.CONTENT_NEGOTIATION:
            if headers:
                accept = headers.get("Accept", "")
                match = re.search(r"version=v?(\d+(?:\.\d+)?)", accept)
                if match:
                    version = f"v{match.group(1)}"

        # Validate version
        if version:
            if version in self._versions:
                self._stats[version] += 1
                api_ver = self._versions[version]
                if api_ver.is_sunset():
                    warning = f"Version {version} has reached sunset date"
                elif api_ver.deprecated:
                    warning = f"Version {version} is deprecated"
                    if version in self._deprecations:
                        warning = self._deprecations[version].message
                return version, warning
            elif self.config.strict_mode:
                return None, f"Unsupported version: {version}"

        return self.config.default_version, None

    def route(
        self,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> tuple[Optional[Callable], Optional[str], Optional[str]]:
        """
        Route request to appropriate versioned handler.

        Returns:
            Tuple of (handler, version, warning)
        """
        version, warning = self.detect_version(path, headers, query_params)

        if version is None:
            return None, None, warning

        # Extract endpoint from path
        endpoint = re.sub(r"^/(?:v\d+|api/v\d+)/?", "", path)

        if version in self._handlers and endpoint in self._handlers[version]:
            return self._handlers[version][endpoint], version, warning

        return None, version, f"No handler found for {endpoint} in {version}"

    def get_version_info(self, version: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific version."""
        if version not in self._versions:
            return None

        api_ver = self._versions[version]
        return {
            "version": api_ver.version,
            "major": api_ver.major,
            "minor": api_ver.minor,
            "deprecated": api_ver.deprecated,
            "sunset": api_ver.sunset_date,
            "is_sunset": api_ver.is_sunset(),
            "docs_url": api_ver.docs_url,
            "deprecation_warning": self._deprecations.get(version),
        }

    def get_supported_versions(self) -> List[Dict[str, Any]]:
        """Get list of all supported versions."""
        return [
            self.get_version_info(v)
            for v in sorted(
                self._versions.keys(),
                key=lambda x: self._versions[x].major * 1000 + self._versions[x].minor
            )
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get versioning statistics."""
        return {
            "requests_by_version": dict(self._stats),
            "total_requests": sum(self._stats.values()),
            "supported_versions": list(self._versions.keys()),
            "deprecated_versions": [
                v for v, info in self._versions.items() if info.deprecated
            ],
        }

    def create_version_router(
        self,
    ) -> Callable[[str, Dict, Dict], tuple[Optional[Callable], str, Optional[str]]]:
        """Create a router function for the current configuration."""
        def router(
            path: str,
            headers: Optional[Dict[str, str]] = None,
            query_params: Optional[Dict[str, str]] = None,
        ) -> tuple[Optional[Callable], str, Optional[str]]:
            return self.route(path, headers, query_params)
        return router
