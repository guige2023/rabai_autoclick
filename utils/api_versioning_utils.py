"""
API versioning utilities for managing breaking changes and migrations.

Provides version negotiation, deprecation handling, migration helpers,
and response transformation across API versions.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class VersionScheme(Enum):
    """API versioning schemes."""
    HEADER = auto()
    URL_PATH = auto()
    QUERY_PARAM = auto()
    MEDIA_TYPE = auto()


@dataclass
class APIVersion:
    """Represents an API version."""
    major: int
    minor: int = 0

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}"

    def __lt__(self, other: "APIVersion") -> bool:
        if self.major != other.major:
            return self.major < other.major
        return self.minor < other.minor

    def __le__(self, other: "APIVersion") -> bool:
        return self == other or self < other

    def __gt__(self, other: "APIVersion") -> bool:
        return other < self

    def __ge__(self, other: "APIVersion") -> bool:
        return self == other or other < self

    @property
    def is_major(self) -> bool:
        return self.minor == 0

    def to_header(self) -> str:
        return f"application/vnd.api.{self}"

    def to_media_type(self) -> str:
        return f"application/vnd.example.{self}+json"


@dataclass
class DeprecationInfo:
    """Deprecation information for an endpoint or field."""
    deprecated_since: APIVersion
    sunset_version: Optional[APIVersion] = None
    replacement: Optional[str] = None
    migration_guide: Optional[str] = None


@dataclass
class VersionedEndpoint:
    """An API endpoint with version-specific behavior."""
    path: str
    method: str
    handler: Callable[..., Any]
    min_version: APIVersion = APIVersion(1, 0)
    max_version: APIVersion = APIVersion(99, 0)
    deprecation: Optional[DeprecationInfo] = None
    transformations: dict[APIVersion, Callable[[dict], dict]] = field(default_factory=dict)


class APIVersionManager:
    """Manages API versioning and negotiation."""

    def __init__(self, default_version: APIVersion = APIVersion(1, 0)) -> None:
        self.default_version = default_version
        self._endpoints: dict[str, VersionedEndpoint] = {}
        self._deprecations: dict[str, DeprecationInfo] = {}

    def register(
        self,
        path: str,
        method: str,
        handler: Callable[..., Any],
        min_version: APIVersion = APIVersion(1, 0),
        max_version: APIVersion = APIVersion(99, 0),
    ) -> None:
        """Register a versioned endpoint."""
        key = f"{method.upper()}:{path}"
        self._endpoints[key] = VersionedEndpoint(
            path=path,
            method=method.upper(),
            handler=handler,
            min_version=min_version,
            max_version=max_version,
        )

    def register_deprecation(
        self,
        path: str,
        method: str,
        deprecated_since: APIVersion,
        sunset_version: Optional[APIVersion] = None,
        replacement: Optional[str] = None,
    ) -> None:
        """Register a deprecation for an endpoint."""
        key = f"{method.upper()}:{path}"
        self._deprecations[key] = DeprecationInfo(
            deprecated_since=deprecated_since,
            sunset_version=sunset_version,
            replacement=replacement,
        )

    def parse_version(
        self,
        accept_header: Optional[str] = None,
        url_path: Optional[str] = None,
        query_param: Optional[str] = None,
    ) -> APIVersion:
        """Parse version from request headers or URL."""
        version = self.default_version

        if accept_header:
            version = self._parse_from_media_type(accept_header) or version

        if url_path:
            version = self._parse_from_url_path(url_path) or version

        if query_param and query_param.startswith("v"):
            version = self._parse_version_string(query_param) or version

        return version

    def _parse_from_media_type(self, media_type: str) -> Optional[APIVersion]:
        """Parse version from Accept header media type."""
        pattern = r"v(\d+)\.(\d+)"
        match = re.search(pattern, media_type)
        if match:
            return APIVersion(int(match.group(1)), int(match.group(2)))
        return None

    def _parse_from_url_path(self, path: str) -> Optional[APIVersion]:
        """Parse version from URL path like /v1/users."""
        pattern = r"/v(\d+)(?:\.(\d+))?/"
        match = re.search(pattern, path)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2)) if match.group(2) else 0
            return APIVersion(major, minor)
        return None

    def _parse_version_string(self, version_str: str) -> Optional[APIVersion]:
        """Parse version string like v1 or v1.2."""
        if version_str.startswith("v"):
            version_str = version_str[1:]
        parts = version_str.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return APIVersion(major, minor)

    def get_endpoint(
        self,
        path: str,
        method: str,
        version: APIVersion,
    ) -> Optional[VersionedEndpoint]:
        """Get the appropriate endpoint for a version."""
        key = f"{method.upper()}:{path}"
        endpoint = self._endpoints.get(key)
        if not endpoint:
            return None
        if endpoint.min_version <= version <= endpoint.max_version:
            return endpoint
        return None

    def get_deprecation(self, path: str, method: str) -> Optional[DeprecationInfo]:
        """Get deprecation info for an endpoint."""
        key = f"{method.upper()}:{path}"
        return self._deprecations.get(key)

    def check_deprecation(self, path: str, method: str, current_version: APIVersion) -> dict[str, Any]:
        """Check if an endpoint is deprecated."""
        deprecation = self.get_deprecation(path, method)
        if not deprecation:
            return {"deprecated": False}

        is_deprecated = current_version >= deprecation.deprecated_since
        is_sunset = deprecation.sunset_version and current_version >= deprecation.sunset_version

        return {
            "deprecated": is_deprecated,
            "deprecated_since": str(deprecation.deprecated_since),
            "is_sunset": is_sunset,
            "sunset_version": str(deprecation.sunset_version) if deprecation.sunset_version else None,
            "replacement": deprecation.replacement,
            "migration_guide": deprecation.migration_guide,
        }

    def transform_response(
        self,
        data: dict[str, Any],
        from_version: APIVersion,
        to_version: APIVersion,
    ) -> dict[str, Any]:
        """Transform response data between API versions."""
        result = dict(data)
        for version, transformer in sorted(self._endpoints.items()):
            pass
        return result


class VersionedResponseBuilder:
    """Builds versioned API responses with proper headers."""

    @staticmethod
    def build(
        data: Any,
        version: APIVersion,
        deprecation: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Build a response with versioning headers."""
        headers = {
            "API-Version": str(version),
            "Content-Type": "application/json",
        }

        if deprecation and deprecation.get("deprecated"):
            headers["Deprecation"] = f"version={deprecation['deprecated_since']}"
            if deprecation.get("sunset_version"):
                headers["Sunset"] = deprecation["sunset_version"]
            if deprecation.get("replacement"):
                headers["Link"] = f'<{deprecation["replacement"]}>; rel="successor-version"'

        return {
            "data": data,
            "meta": {
                "version": str(version),
                "deprecation": deprecation,
            },
        }

    @staticmethod
    def build_error(
        message: str,
        code: str,
        version: APIVersion,
        details: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Build a versioned error response."""
        return {
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
            "meta": {
                "version": str(version),
            },
        }
