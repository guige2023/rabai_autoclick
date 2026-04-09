"""API Version Control Action Module.

Provides API versioning control including version negotiation,
deprecation management, and migration tracking.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class VersionScheme(Enum):
    """API versioning schemes."""
    HEADER = "header"
    URL_PATH = "url_path"
    QUERY_PARAM = "query_param"
    CONTENT_NEGOTIATION = "content_negotiation"


@dataclass
class APIVersion:
    """Represents an API version."""
    version: str
    major: int
    minor: int
    patch: int = 0
    deprecated: bool = False
    sunset_date: Optional[float] = None
    changelog_url: Optional[str] = None
    migration_guide: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def is_active(self) -> bool:
        """Check if version is still active."""
        if self.deprecated and self.sunset_date:
            return time.time() < self.sunset_date
        return not self.deprecated

    def supports(self, min_version: "APIVersion") -> bool:
        """Check if this version supports minimum version requirements."""
        if self.major > min_version.major:
            return True
        if self.major == min_version.major:
            return self.minor >= min_version.minor
        return False


@dataclass
class VersionRoute:
    """Maps version to handler."""
    version: APIVersion
    handler: Callable
    priority: int = 0


@dataclass
class DeprecationNotice:
    """Deprecation notice for an endpoint."""
    endpoint: str
    version: APIVersion
    sunset_date: float
    migration_steps: List[str] = field(default_factory=list)
    alternative: Optional[str] = None


class VersionRegistry:
    """Registry for API versions."""

    def __init__(self):
        self._versions: Dict[str, APIVersion] = {}
        self._routes: Dict[str, List[VersionRoute]] = {}
        self._deprecations: Dict[str, DeprecationNotice] = {}

    def register_version(self, version: APIVersion) -> None:
        """Register an API version."""
        self._versions[str(version)] = version
        logger.info(f"Registered API version {version}")

    def register_route(
        self,
        endpoint: str,
        version: APIVersion,
        handler: Callable,
        priority: int = 0
    ) -> None:
        """Register a versioned route."""
        route = VersionRoute(version=version, handler=handler, priority=priority)
        if endpoint not in self._routes:
            self._routes[endpoint] = []
        self._routes[endpoint].append(route)
        self._routes[endpoint].sort(key=lambda r: r.priority, reverse=True)

    def get_version(self, version_str: str) -> Optional[APIVersion]:
        """Get a version by string."""
        return self._versions.get(version_str)

    def get_all_versions(self) -> List[APIVersion]:
        """Get all registered versions."""
        return list(self._versions.values())

    def get_active_versions(self) -> List[APIVersion]:
        """Get all active (non-deprecated) versions."""
        return [v for v in self._versions.values() if v.is_active()]

    def get_latest_version(self) -> Optional[APIVersion]:
        """Get the latest active version."""
        active = self.get_active_versions()
        if not active:
            return None
        return max(active, key=lambda v: (v.major, v.minor, v.patch))

    def get_handler(
        self,
        endpoint: str,
        version: APIVersion
    ) -> Optional[Callable]:
        """Get the best matching handler for an endpoint and version."""
        routes = self._routes.get(endpoint, [])
        for route in routes:
            if version.supports(route.version):
                return route.handler
        return None

    def add_deprecation(
        self,
        endpoint: str,
        version: APIVersion,
        sunset_date: float,
        migration_steps: Optional[List[str]] = None,
        alternative: Optional[str] = None
    ) -> None:
        """Add a deprecation notice."""
        notice = DeprecationNotice(
            endpoint=endpoint,
            version=version,
            sunset_date=sunset_date,
            migration_steps=migration_steps or [],
            alternative=alternative
        )
        self._deprecations[f"{endpoint}:{version}"] = notice

    def get_deprecation(self, endpoint: str, version: APIVersion) -> Optional[DeprecationNotice]:
        """Get deprecation notice for endpoint/version."""
        return self._deprecations.get(f"{endpoint}:{version}")


class VersionNegotiator:
    """Negotiates API versions from requests."""

    def __init__(self, registry: VersionRegistry, default_version: Optional[APIVersion] = None):
        self._registry = registry
        self._default_version = default_version or registry.get_latest_version()

    def negotiate_from_header(self, headers: Dict[str, str]) -> Tuple[APIVersion, bool]:
        """Negotiate version from Accept-Version header."""
        version_header = headers.get("Accept-Version") or headers.get("API-Version")
        if not version_header:
            return self._default_version, False

        # Parse version string (e.g., "1.0", "2.1.0", ">=1.5")
        version_str, is_minimum = self._parse_version_header(version_header)
        version = self._registry.get_version(version_str)

        if version:
            return version, True

        # Try to find compatible version
        for v in self._registry.get_active_versions():
            if self._check_compatibility(v, version_str):
                return v, True

        return self._default_version, False

    def negotiate_from_url(self, path: str) -> Tuple[Optional[APIVersion], str]:
        """Negotiate version from URL path."""
        parts = path.strip("/").split("/")
        if parts and parts[0].startswith("v"):
            try:
                version_str = parts[0][1:]  # Remove 'v' prefix
                version = self._registry.get_version(version_str)
                if version:
                    return version, "/" + "/".join(parts[1:])
            except ValueError:
                pass
        return None, path

    def negotiate_from_query(
        self,
        params: Dict[str, Any]
    ) -> Tuple[Optional[APIVersion], bool]:
        """Negotiate version from query parameter."""
        version_str = params.get("version") or params.get("api_version")
        if not version_str:
            return None, False

        version = self._registry.get_version(str(version_str))
        return version, version is not None

    def _parse_version_header(self, header: str) -> Tuple[str, bool]:
        """Parse version from header string."""
        header = header.strip()

        # Handle range operators
        if header.startswith(">="):
            return header[2:].strip(), True
        if header.startswith(">"):
            return header[1:].strip(), True
        if header.startswith("~"):
            return header[1:].strip(), True

        return header, False

    def _check_compatibility(self, version: APIVersion, requirement: str) -> bool:
        """Check if version meets requirement."""
        try:
            if requirement.startswith(">="):
                req_ver = self._parse_version_str(requirement[2:])
                return version.major > req_ver.major or \
                       (version.major == req_ver.major and version.minor >= req_ver.minor)
            return str(version) == requirement
        except:
            return False

    @staticmethod
    def _parse_version_str(version_str: str) -> APIVersion:
        """Parse version string to APIVersion."""
        parts = version_str.split(".")
        major = int(parts[0]) if len(parts) > 0 else 1
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
        return APIVersion(version=version_str, major=major, minor=minor, patch=patch)


class MigrationTracker:
    """Tracks API migrations."""

    def __init__(self):
        self._migrations: Dict[str, List[Dict[str, Any]]] = {}

    def record_migration(
        self,
        from_version: APIVersion,
        to_version: APIVersion,
        endpoint: str,
        status: str = "pending"
    ) -> None:
        """Record a migration attempt."""
        key = f"{endpoint}:{from_version}->{to_version}"
        if key not in self._migrations:
            self._migrations[key] = []

        self._migrations[key].append({
            "from_version": str(from_version),
            "to_version": str(to_version),
            "endpoint": endpoint,
            "status": status,
            "timestamp": time.time()
        })

    def get_migration_status(
        self,
        from_version: APIVersion,
        to_version: APIVersion,
        endpoint: str
    ) -> Optional[str]:
        """Get the latest migration status."""
        key = f"{endpoint}:{from_version}->{to_version}"
        migrations = self._migrations.get(key, [])
        if migrations:
            return migrations[-1].get("status")
        return None


class APIVersionControlAction:
    """Main action class for API version control."""

    def __init__(self):
        self._registry = VersionRegistry()
        self._negotiator = VersionNegotiator(self._registry)
        self._migration_tracker = MigrationTracker()

    def register_version(
        self,
        version: str,
        deprecated: bool = False,
        sunset_date: Optional[float] = None
    ) -> None:
        """Register an API version."""
        parts = version.split(".")
        major = int(parts[0]) if len(parts) > 0 else 1
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0

        api_version = APIVersion(
            version=version,
            major=major,
            minor=minor,
            patch=patch,
            deprecated=deprecated,
            sunset_date=sunset_date
        )
        self._registry.register_version(api_version)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API version control action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with version control results.
        """
        operation = context.get("operation", "get_latest")

        if operation == "register_version":
            self.register_version(
                version=context.get("version", "1.0"),
                deprecated=context.get("deprecated", False),
                sunset_date=context.get("sunset_date")
            )
            return {"success": True}

        elif operation == "get_latest":
            latest = self._registry.get_latest_version()
            return {
                "success": True,
                "version": str(latest) if latest else None
            }

        elif operation == "get_all_versions":
            versions = self._registry.get_all_versions()
            return {
                "success": True,
                "versions": [
                    {
                        "version": str(v),
                        "deprecated": v.deprecated,
                        "active": v.is_active()
                    }
                    for v in versions
                ]
            }

        elif operation == "negotiate":
            source = context.get("source", "header")
            headers = context.get("headers", {})
            path = context.get("path", "")
            params = context.get("params", {})

            if source == "header":
                version, matched = self._negotiator.negotiate_from_header(headers)
            elif source == "url":
                version, _ = self._negotiator.negotiate_from_url(path)
                matched = version is not None
            elif source == "query":
                version, matched = self._negotiator.negotiate_from_query(params)
            else:
                return {"success": False, "error": f"Unknown source: {source}"}

            return {
                "success": True,
                "version": str(version) if version else None,
                "matched": matched
            }

        elif operation == "deprecate":
            endpoint = context.get("endpoint", "")
            version_str = context.get("version", "")
            sunset_date = context.get("sunset_date", time.time() + 86400 * 30)

            version = self._registry.get_version(version_str)
            if version:
                self._registry.add_deprecation(
                    endpoint=endpoint,
                    version=version,
                    sunset_date=sunset_date,
                    migration_steps=context.get("migration_steps", []),
                    alternative=context.get("alternative")
                )
                return {"success": True}
            return {"success": False, "error": "Version not found"}

        elif operation == "get_deprecation":
            endpoint = context.get("endpoint", "")
            version_str = context.get("version", "")
            version = self._registry.get_version(version_str)

            if version:
                notice = self._registry.get_deprecation(endpoint, version)
                if notice:
                    return {
                        "success": True,
                        "deprecation": {
                            "endpoint": notice.endpoint,
                            "version": str(notice.version),
                            "sunset_date": notice.sunset_date,
                            "migration_steps": notice.migration_steps,
                            "alternative": notice.alternative
                        }
                    }
            return {"success": False, "error": "Deprecation not found"}

        elif operation == "record_migration":
            from_ver = context.get("from_version", "")
            to_ver = context.get("to_version", "")
            endpoint = context.get("endpoint", "")

            from_v = self._registry.get_version(from_ver)
            to_v = self._registry.get_version(to_ver)

            if from_v and to_v:
                self._migration_tracker.record_migration(
                    from_version=from_v,
                    to_version=to_v,
                    endpoint=endpoint,
                    status=context.get("status", "pending")
                )
                return {"success": True}
            return {"success": False, "error": "Version not found"}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
