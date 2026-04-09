"""API Versioning Action Module.

Provides API version management, migration support, deprecation
handling, and backward compatibility management for API evolution.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class VersionScheme(Enum):
    """API versioning schemes."""
    SEMVER = "semver"
    DATE = "date"
    SIMPLE = "simple"


class VersionStatus(Enum):
    """Version lifecycle status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    RETIRED = "retired"


@dataclass
class APIVersion:
    """Represents an API version."""
    version: str
    status: VersionStatus = VersionStatus.ACTIVE
    release_date: datetime = field(default_factory=datetime.now)
    sunset_date: Optional[datetime] = None
    deprecation_date: Optional[datetime] = None
    migration_guide: Optional[str] = None
    breaking_changes: List[str] = field(default_factory=list)
    features: List[str] = field(default_factory=list)


@dataclass
class VersionRoute:
    """Maps requests to API versions."""
    path_pattern: str
    version: str
    handler: Optional[Callable] = None
    middleware: List[Callable] = field(default_factory=list)


@dataclass
class DeprecationPolicy:
    """Policy for handling deprecated versions."""
    deprecation_period_days: int = 180
    sunset_period_days: int = 90
    require_version_header: bool = False
    default_to_latest: bool = True
    warn_on_deprecated: bool = True


@dataclass
class VersioningConfig:
    """Configuration for API versioning."""
    scheme: VersionScheme = VersionScheme.SEMVER
    current_version: str = "1.0.0"
    supported_versions: List[str] = field(default_factory=lambda: ["1.0.0"])
    deprecation_policy: DeprecationPolicy = field(default_factory=DeprecationPolicy)
    header_name: str = "API-Version"
    path_prefix: str = "/api/v"


class VersionComparator:
    """Compare semantic versions."""

    @staticmethod
    def parse_version(version: str) -> Tuple[int, int, int]:
        """Parse semantic version string."""
        try:
            parts = version.replace("v", "").split(".")
            return (int(parts[0]), int(parts[1]), int(parts[2])) if len(parts) >= 3 else (1, 0, 0)
        except (ValueError, IndexError):
            return (1, 0, 0)

    @staticmethod
    def compare(v1: str, v2: str) -> int:
        """Compare two versions. Returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2."""
        parsed_v1 = VersionComparator.parse_version(v1)
        parsed_v2 = VersionComparator.parse_version(v2)
        if parsed_v1 < parsed_v2:
            return -1
        elif parsed_v1 > parsed_v2:
            return 1
        return 0

    @staticmethod
    def is_compatible(v1: str, v2: str) -> bool:
        """Check if two versions are API-compatible (same major version)."""
        return VersionComparator.parse_version(v1)[0] == VersionComparator.parse_version(v2)[0]

    @staticmethod
    def is_newer(v1: str, v2: str) -> bool:
        """Check if v1 is newer than v2."""
        return VersionComparator.compare(v1, v2) > 0


class MigrationManager:
    """Manage API version migrations."""

    def __init__(self):
        self._migrations: Dict[str, Dict[str, Callable]] = defaultdict(dict)

    def register_migration(
        self,
        from_version: str,
        to_version: str,
        migration_func: Callable[[Dict[str, Any]], Dict[str, Any]]
    ):
        """Register a migration function between versions."""
        self._migrations[from_version][to_version] = migration_func

    def migrate(
        self,
        data: Dict[str, Any],
        from_version: str,
        to_version: str
    ) -> Dict[str, Any]:
        """Migrate data from one version to another."""
        if from_version == to_version:
            return data

        migration_path = self._find_migration_path(from_version, to_version)
        if not migration_path:
            raise ValueError(f"No migration path from {from_version} to {to_version}")

        result = data
        for step_from, step_to in migration_path:
            if step_from in self._migrations and step_to in self._migrations[step_from]:
                result = self._migrations[step_from][step_to](result)
            else:
                result = self._apply_default_migration(result, step_from, step_to)

        return result

    def _find_migration_path(
        self,
        from_ver: str,
        to_ver: str
    ) -> List[Tuple[str, str]]:
        """Find migration path using BFS."""
        from collections import deque

        queue = deque([(from_ver, [(from_ver, to_ver)])])
        visited = {from_ver}

        while queue:
            current, path = queue.popleft()

            if current == to_ver:
                return [(path[i][0], path[i][1]) for i in range(len(path) - 1)]

            if current in self._migrations:
                for next_ver in self._migrations[current]:
                    if next_ver not in visited:
                        visited.add(next_ver)
                        new_path = path + [(current, next_ver)]
                        queue.append((next_ver, new_path))

        return []

    def _apply_default_migration(
        self,
        data: Dict[str, Any],
        from_ver: str,
        to_ver: str
    ) -> Dict[str, Any]:
        """Apply default migration transformations."""
        return data


class ApiVersioningAction(BaseAction):
    """Action for API version management."""

    def __init__(self):
        super().__init__(name="api_versioning")
        self._config = VersioningConfig()
        self._versions: Dict[str, APIVersion] = {}
        self._routes: Dict[str, VersionRoute] = {}
        self._migration_manager = MigrationManager()
        self._lock = threading.Lock()
        self._version_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def configure(self, config: VersioningConfig):
        """Configure API versioning settings."""
        self._config = config

    def register_version(
        self,
        version: str,
        status: VersionStatus = VersionStatus.ACTIVE,
        features: Optional[List[str]] = None,
        breaking_changes: Optional[List[str]] = None,
        migration_guide: Optional[str] = None,
        sunset_date: Optional[datetime] = None
    ) -> ActionResult:
        """Register a new API version."""
        try:
            with self._lock:
                if version in self._versions:
                    return ActionResult(success=False, error=f"Version {version} already registered")

                api_version = APIVersion(
                    version=version,
                    status=status,
                    features=features or [],
                    breaking_changes=breaking_changes or [],
                    migration_guide=migration_guide,
                    sunset_date=sunset_date
                )
                self._versions[version] = api_version
                return ActionResult(success=True, data={"version": version, "status": status.value})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def deprecate_version(
        self,
        version: str,
        sunset_date: Optional[datetime] = None,
        migration_guide: Optional[str] = None
    ) -> ActionResult:
        """Mark a version as deprecated."""
        try:
            with self._lock:
                if version not in self._versions:
                    return ActionResult(success=False, error=f"Version {version} not found")

                api_version = self._versions[version]
                api_version.status = VersionStatus.DEPRECATED
                api_version.deprecation_date = datetime.now()
                if sunset_date:
                    api_version.sunset_date = sunset_date
                if migration_guide:
                    api_version.migration_guide = migration_guide

                return ActionResult(success=True, data={"version": version, "status": "deprecated"})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def resolve_version(
        self,
        requested_version: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Tuple[Optional[APIVersion], Dict[str, Any]]:
        """Resolve the appropriate version for a request."""
        resolved = None
        warnings = []

        if requested_version and requested_version in self._versions:
            resolved = self._versions[requested_version]
            with self._lock:
                self._version_stats[requested_version]["requested"] += 1
        elif headers and self._config.header_name in headers:
            header_version = headers[self._config.header_name]
            if header_version in self._versions:
                resolved = self._versions[header_version]
                with self._lock:
                    self._version_stats[header_version]["requested"] += 1
        elif self._config.deprecation_policy.default_to_latest:
            resolved = self._versions.get(self._config.current_version)
            warnings.append("No version specified, defaulting to current")

        if resolved and resolved.status == VersionStatus.DEPRECATED:
            warnings.append(f"Version {resolved.version} is deprecated")

        if resolved and resolved.status == VersionStatus.SUNSET:
            warnings.append(f"Version {resolved.version} has reached sunset date")

        metadata = {
            "warnings": warnings,
            "resolved_version": resolved.version if resolved else None
        }

        return resolved, metadata

    def register_route(
        self,
        path_pattern: str,
        version: str,
        handler: Optional[Callable] = None
    ) -> ActionResult:
        """Register a versioned route."""
        try:
            if version not in self._versions:
                return ActionResult(success=False, error=f"Version {version} not registered")

            route = VersionRoute(
                path_pattern=path_pattern,
                version=version,
                handler=handler
            )
            self._routes[f"{version}:{path_pattern}"] = route
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def register_migration(
        self,
        from_version: str,
        to_version: str,
        migration_func: Callable[[Dict[str, Any]], Dict[str, Any]]
    ) -> ActionResult:
        """Register a migration function."""
        try:
            self._migration_manager.register_migration(from_version, to_version, migration_func)
            return ActionResult(success=True, data={
                "from": from_version,
                "to": to_version
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def migrate_request(
        self,
        data: Dict[str, Any],
        from_version: str,
        to_version: Optional[str] = None
    ) -> ActionResult:
        """Migrate request data between versions."""
        try:
            target = to_version or self._config.current_version
            migrated_data = self._migration_manager.migrate(data, from_version, target)
            return ActionResult(success=True, data={
                "original_version": from_version,
                "target_version": target,
                "data": migrated_data
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_version_info(self, version: str) -> ActionResult:
        """Get information about a specific version."""
        try:
            if version not in self._versions:
                return ActionResult(success=False, error=f"Version {version} not found")

            api_version = self._versions[version]
            stats = self._version_stats.get(version, {})

            return ActionResult(success=True, data={
                "version": api_version.version,
                "status": api_version.status.value,
                "release_date": api_version.release_date.isoformat(),
                "deprecation_date": api_version.deprecation_date.isoformat() if api_version.deprecation_date else None,
                "sunset_date": api_version.sunset_date.isoformat() if api_version.sunset_date else None,
                "features": api_version.features,
                "breaking_changes": api_version.breaking_changes,
                "migration_guide": api_version.migration_guide,
                "stats": dict(stats)
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def list_versions(self, status_filter: Optional[VersionStatus] = None) -> List[APIVersion]:
        """List all registered versions."""
        with self._lock:
            if status_filter:
                return [v for v in self._versions.values() if v.status == status_filter]
            return list(self._versions.values())

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute version management action."""
        try:
            action = params.get("action", "list")

            if action == "list":
                versions = self.list_versions()
                return ActionResult(success=True, data={
                    "versions": [
                        {"version": v.version, "status": v.status.value}
                        for v in versions
                    ]
                })
            elif action == "deprecate":
                return self.deprecate_version(
                    params["version"],
                    params.get("sunset_date"),
                    params.get("migration_guide")
                )
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
