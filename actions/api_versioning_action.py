"""
API Versioning Action Module.

Provides API versioning strategies including URL-based,
header-based, and content negotiation versioning.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import re

logger = logging.getLogger(__name__)


class VersionStrategy(Enum):
    """Versioning strategies."""
    URL_PATH = "url_path"
    HEADER = "header"
    QUERY_PARAM = "query_param"
    CONTENT_NEGOTIATION = "content_negotiation"


@dataclass
class APIVersion:
    """API version definition."""
    version: str
    major: int
    minor: int
    deprecated: bool = False
    sunset_date: Optional[datetime] = None
    docs_url: Optional[str] = None

    def is_compatible_with(self, other: "APIVersion") -> bool:
        """Check if versions are compatible."""
        return self.major == other.major

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}"

    def __lt__(self, other: "APIVersion") -> bool:
        if self.major != other.major:
            return self.major < other.major
        return self.minor < other.minor


@dataclass
class VersionedEndpoint:
    """Versioned API endpoint."""
    path: str
    method: str
    versions: Dict[str, Callable]
    default_version: Optional[str] = None
    deprecation_warnings: Dict[str, str] = field(default_factory=dict)


@dataclass
class VersionMigration:
    """Migration path between versions."""
    from_version: str
    to_version: str
    migration_func: Callable
    breaking_changes: List[str] = field(default_factory=list)


class VersionManager:
    """Manages API versions."""

    def __init__(self, strategy: VersionStrategy = VersionStrategy.URL_PATH):
        self.strategy = strategy
        self.endpoints: Dict[str, VersionedEndpoint] = {}
        self.migrations: Dict[Tuple[str, str], VersionMigration] = {}
        self.supported_versions: Set[str] = set()

    def register_endpoint(
        self,
        endpoint: VersionedEndpoint
    ):
        """Register a versioned endpoint."""
        key = f"{endpoint.method}:{endpoint.path}"
        self.endpoints[key] = endpoint

        for version in endpoint.versions.keys():
            self.supported_versions.add(version)

    def add_migration(self, migration: VersionMigration):
        """Add migration path."""
        key = (migration.from_version, migration.to_version)
        self.migrations[key] = migration

    def parse_version(self, version_str: str) -> Optional[APIVersion]:
        """Parse version string."""
        match = re.match(r"v?(\d+)\.?(\d*)", version_str)
        if not match:
            return None

        major = int(match.group(1))
        minor = int(match.group(2)) if match.group(2) else 0

        return APIVersion(version=version_str, major=major, minor=minor)

    def get_version_from_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        query_params: Dict[str, str]
    ) -> Optional[str]:
        """Extract version from request."""
        if self.strategy == VersionStrategy.URL_PATH:
            return self._get_version_from_path(path)

        elif self.strategy == VersionStrategy.HEADER:
            return headers.get("API-Version") or headers.get("Accept-Version")

        elif self.strategy == VersionStrategy.QUERY_PARAM:
            return query_params.get("version")

        elif self.strategy == VersionStrategy.CONTENT_NEGOTIATION:
            accept = headers.get("Accept", "")
            match = re.search(r"version=[\"']?v?(\d+\.?\d*)", accept)
            if match:
                return match.group(1)

        return None

    def _get_version_from_path(self, path: str) -> Optional[str]:
        """Extract version from URL path."""
        match = re.search(r"/v(\d+)/", path)
        if match:
            return f"v{match.group(1)}"

        match = re.search(r"/version/(\d+)", path)
        if match:
            return f"v{match.group(1)}"

        return None

    def resolve_endpoint(
        self,
        method: str,
        path: str,
        version: Optional[str] = None
    ) -> Optional[Callable]:
        """Resolve endpoint handler for version."""
        key = f"{method}:{path}"
        endpoint = self.endpoints.get(key)

        if not endpoint:
            return None

        if version and version in endpoint.versions:
            return endpoint.versions[version]

        if endpoint.default_version:
            return endpoint.versions.get(endpoint.default_version)

        versions = sorted(
            [self.parse_version(v) for v in endpoint.versions.keys()],
            reverse=True
        )

        if versions:
            latest = versions[0]
            return endpoint.versions.get(str(latest))

        return None

    def get_deprecation_warning(self, method: str, path: str, version: str) -> Optional[str]:
        """Get deprecation warning for endpoint."""
        key = f"{method}:{path}"
        endpoint = self.endpoints.get(key)

        if endpoint and version in endpoint.deprecation_warnings:
            return endpoint.deprecation_warnings[version]

        return None

    def check_migration_path(
        self,
        from_version: str,
        to_version: str
    ) -> Optional[VersionMigration]:
        """Check if migration path exists."""
        key = (from_version, to_version)
        return self.migrations.get(key)


class VersionCompatChecker:
    """Checks version compatibility."""

    def __init__(self, version_manager: VersionManager):
        self.version_manager = version_manager

    def is_breaking_change(
        self,
        from_version: str,
        to_version: str
    ) -> bool:
        """Check if version change is breaking."""
        from_ver = self.version_manager.parse_version(from_version)
        to_ver = self.version_manager.parse_version(to_version)

        if not from_ver or not to_ver:
            return True

        if from_ver.major != to_ver.major:
            return True

        return False

    def get_compatible_version(
        self,
        requested_version: str,
        supported_versions: List[str]
    ) -> Optional[str]:
        """Get best compatible version."""
        requested = self.version_manager.parse_version(requested_version)
        if not requested:
            return None

        for version in supported_versions:
            supported = self.version_manager.parse_version(version)
            if supported and supported.is_compatible_with(requested):
                return version

        return None


async def main():
    """Demonstrate API versioning."""
    manager = VersionManager(strategy=VersionStrategy.URL_PATH)

    async def handler_v1():
        return {"version": "1.0", "data": "v1 handler"}

    async def handler_v2():
        return {"version": "2.0", "data": "v2 handler"}

    endpoint = VersionedEndpoint(
        path="/api/users",
        method="GET",
        versions={"v1": handler_v1, "v2": handler_v2},
        default_version="v2"
    )

    manager.register_endpoint(endpoint)

    version = manager.get_version_from_request("GET", "/api/users", {}, {})
    print(f"Detected version: {version}")

    handler = manager.resolve_endpoint("GET", "/api/users", "v1")
    if handler:
        result = await handler()
        print(f"Handler result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
