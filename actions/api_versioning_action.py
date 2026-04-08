"""API Versioning Action Module.

Handles API version negotiation, routing, and deprecation management
across multiple API versions.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import json
import re


class VersionStatus(Enum):
    """API version lifecycle status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    retired = "retired"


@dataclass
class APIVersion:
    """Represents an API version configuration."""
    version: str
    status: VersionStatus
    release_date: datetime
    deprecation_date: Optional[datetime] = None
    sunset_date: Optional[datetime] = None
    min_version: Optional[str] = None
    features: List[str] = field(default_factory=list)
    breaking_changes: List[str] = field(default_factory=list)
    migration_guide: str = ""


@dataclass
class VersionRoute:
    """Maps version-specific routes to handlers."""
    pattern: str
    version: str
    handler: Callable
    priority: int = 0


@dataclass
class VersionedRequest:
    """Request with version information extracted."""
    original_path: str
    version: Optional[str]
    version_agnostic_path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes] = None


@dataclass
class VersionNegotiation:
    """Result of version negotiation."""
    selected_version: str
    negotiation_method: str
    warnings: List[str] = field(default_factory=list)
    deprecation_notice: Optional[str] = None


class APIVersionRegistry:
    """Central registry for API versions."""

    def __init__(self):
        self._versions: Dict[str, APIVersion] = {}
        self._default_version: Optional[str] = None

    def register(self, version: APIVersion):
        """Register a new API version."""
        self._versions[version.version] = version
        if self._default_version is None:
            self._default_version = version.version

    def get(self, version: str) -> Optional[APIVersion]:
        """Get version configuration."""
        return self._versions.get(version)

    def list_versions(self, status: Optional[VersionStatus] = None) -> List[APIVersion]:
        """List all versions, optionally filtered by status."""
        versions = list(self._versions.values())
        if status:
            versions = [v for v in versions if v.status == status]
        return sorted(versions, key=lambda v: v.version, reverse=True)

    def get_active_versions(self) -> List[str]:
        """Get list of active version strings."""
        return [v.version for v in self.list_versions(VersionStatus.ACTIVE)]

    def is_version_valid(self, version: str) -> bool:
        """Check if version exists and is not retired."""
        v = self._versions.get(version)
        return v is not None and v.status != VersionStatus.retired

    def get_deprecated_versions(self) -> List[str]:
        """Get versions that are deprecated or sunset."""
        return [
            v.version for v in self._versions.values()
            if v.status in (VersionStatus.DEPRECATED, VersionStatus.SUNSET)
        ]


class APIVersioningAction:
    """Handles API version negotiation and routing."""

    def __init__(
        self,
        registry: Optional[APIVersionRegistry] = None,
        default_version: str = "v1",
        allow_version_fallback: bool = True,
    ):
        self.registry = registry or APIVersionRegistry()
        self.default_version = default_version
        self.allow_version_fallback = allow_version_fallback
        self._routes: List[VersionRoute] = []
        self._deprecation_warnings: Dict[str, str] = {}

    def add_route(self, pattern: str, version: str, handler: Callable, priority: int = 0):
        """Add a versioned route."""
        self._routes.append(VersionRoute(
            pattern=pattern,
            version=version,
            handler=handler,
            priority=priority,
        ))
        self._routes.sort(key=lambda r: r.priority, reverse=True)

    def negotiate_version(self, request: VersionedRequest) -> VersionNegotiation:
        """Negotiate API version based on request."""
        warnings: List[str] = []
        deprecation_notice: Optional[str] = None

        version = request.version

        if not version:
            version = self._extract_version_from_accept_header(request.headers)
        if not version:
            version = self._extract_version_from_path(request.original_path)

        if not version:
            version = self.default_version
            warnings.append(f"No version specified, defaulting to {version}")

        if not self.registry.is_version_valid(version):
            if self.allow_version_fallback:
                fallback = self._find_compatible_fallback(version)
                if fallback:
                    version = fallback
                    warnings.append(
                        f"Version {version} not found, using compatible version {fallback}"
                    )
                else:
                    version = self.default_version
                    warnings.append(f"Invalid version, falling back to {version}")
            else:
                return VersionNegotiation(
                    selected_version="",
                    negotiation_method="error",
                    warnings=["Invalid version specified"],
                )

        version_config = self.registry.get(version)
        if version_config:
            if version_config.status == VersionStatus.DEPRECATED:
                deprecation_notice = f"Version {version} is deprecated"
                if version_config.sunset_date:
                    days_left = (version_config.sunset_date - datetime.now()).days
                    deprecation_notice += f". Sunset in {days_left} days"
                warnings.append(deprecation_notice)
            elif version_config.status == VersionStatus.SUNSET:
                warnings.append(f"Version {version} has reached sunset date")

        return VersionNegotiation(
            selected_version=version,
            negotiation_method=self._get_negotiation_method(request),
            warnings=warnings,
            deprecation_notice=deprecation_notice,
        )

    def route_request(self, request: VersionedRequest) -> Optional[Callable]:
        """Route request to appropriate handler based on version."""
        negotiation = self.negotiate_version(request)
        if not negotiation.selected_version:
            return None

        version_path = f"{negotiation.selected_version}/"

        for route in self._routes:
            if request.version_agnostic_path.startswith(route.pattern.lstrip("*")):
                if route.version == negotiation.selected_version or route.version == "*":
                    return route.handler

        return None

    def _extract_version_from_accept_header(self, headers: Dict[str, str]) -> Optional[str]:
        """Extract version from Accept header."""
        accept = headers.get("Accept", "")
        version_match = re.search(r"version[=/](\w+)", accept, re.IGNORECASE)
        if version_match:
            return version_match.group(1)
        return None

    def _extract_version_from_path(self, path: str) -> Optional[str]:
        """Extract version from URL path."""
        version_patterns = [
            r"/(v\d+(?:/\d+)?)/",
            r"/api/(\w+\d+)/",
            r"/(\w+\d+)\.",
        ]
        for pattern in version_patterns:
            match = re.search(pattern, path)
            if match:
                return match.group(1)
        return None

    def _find_compatible_fallback(self, requested_version: str) -> Optional[str]:
        """Find a compatible fallback version."""
        major_version = re.match(r"v?(\d+)", requested_version)
        if not major_version:
            return self.default_version

        major = int(major_version.group(1))
        active = self.registry.get_active_versions()

        for v in active:
            v_major = int(re.match(r"v?(\d+)", v).group(1))
            if v_major == major:
                return v

        return active[0] if active else None

    def _get_negotiation_method(self, request: VersionedRequest) -> str:
        """Determine which header was used for negotiation."""
        if "version" in request.headers.get("Accept", "").lower():
            return "accept_header"
        if self._extract_version_from_path(request.original_path):
            return "url_path"
        if request.query_params.get("version"):
            return "query_param"
        return "default"

    def get_version_info(self, version: str) -> Dict[str, Any]:
        """Get detailed version information."""
        v = self.registry.get(version)
        if not v:
            return {"error": f"Version {version} not found"}

        return {
            "version": v.version,
            "status": v.status.value,
            "release_date": v.release_date.isoformat(),
            "deprecation_date": v.deprecation_date.isoformat() if v.deprecation_date else None,
            "sunset_date": v.sunset_date.isoformat() if v.sunset_date else None,
            "features": v.features,
            "breaking_changes": v.breaking_changes,
            "migration_guide": v.migration_guide,
        }

    def set_deprecation_warning(self, version: str, message: str):
        """Set custom deprecation warning message."""
        self._deprecation_warnings[version] = message

    def get_openapi_versions(self) -> Dict[str, Any]:
        """Generate OpenAPI version components."""
        return {
            "versions": [
                {
                    "version": v.version,
                    "status": v.status.value,
                    "path": f"/{v.version.lstrip('v')}",
                }
                for v in self.registry.list_versions()
            ],
            "default": self.default_version,
        }


# Module exports
__all__ = [
    "APIVersioningAction",
    "APIVersionRegistry",
    "APIVersion",
    "VersionedRequest",
    "VersionNegotiation",
    "VersionRoute",
    "VersionStatus",
]
