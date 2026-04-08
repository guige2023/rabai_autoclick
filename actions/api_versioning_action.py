"""
API versioning and lifecycle management module.

Handles version negotiation, deprecation warnings, migration guides,
and breaking change detection for APIs.
"""
from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class VersionPhase(Enum):
    """API version lifecycle phase."""
    ACTIVE = "active"
    BETA = "beta"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    RETIRED = "retired"


@dataclass
class APIVersion:
    """An API version definition."""
    version: str
    phase: VersionPhase = VersionPhase.ACTIVE
    release_date: Optional[str] = None
    sunset_date: Optional[str] = None
    deprecation_date: Optional[str] = None
    description: str = ""
    breaking_changes: list[str] = field(default_factory=list)
    migration_guide: str = ""
    features: list[str] = field(default_factory=list)
    docs_url: str = ""
    swagger_url: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class DeprecationNotice:
    """Deprecation notice for an endpoint or parameter."""
    id: str
    sunset_date: str
    deprecation_date: str
    reason: str
    migration_steps: list[str]
    alternative: str
    severity: str = "medium"


@dataclass
class APIChange:
    """Record of an API change."""
    id: str
    change_type: str
    resource: str
    description: str
    breaking: bool
    date: str
    migration: str = ""


class APIVersionManager:
    """
    API versioning and lifecycle management service.

    Handles version negotiation, deprecation, migration,
    and breaking change tracking.
    """

    def __init__(self, api_name: str = "API"):
        self.api_name = api_name
        self._versions: dict[str, APIVersion] = {}
        self._current_version: Optional[str] = None
        self._deprecations: dict[str, DeprecationNotice] = {}
        self._changes: list[APIChange] = []

    def register_version(
        self,
        version: str,
        phase: VersionPhase = VersionPhase.ACTIVE,
        release_date: Optional[str] = None,
        description: str = "",
        features: Optional[list[str]] = None,
        docs_url: str = "",
    ) -> APIVersion:
        """Register a new API version."""
        api_version = APIVersion(
            version=version,
            phase=phase,
            release_date=release_date or datetime.now().strftime("%Y-%m-%d"),
            description=description,
            features=features or [],
            docs_url=docs_url,
        )
        self._versions[version] = api_version

        if not self._current_version:
            self._current_version = version

        return api_version

    def get_version(self, version: str) -> Optional[APIVersion]:
        """Get a version by version string."""
        return self._versions.get(version)

    def get_current_version(self) -> Optional[APIVersion]:
        """Get the current active version."""
        if self._current_version:
            return self._versions.get(self._current_version)
        return None

    def set_current_version(self, version: str) -> None:
        """Set the current active version."""
        if version in self._versions:
            self._current_version = version

    def deprecate_version(
        self,
        version: str,
        sunset_date: str,
        reason: str = "",
        migration_guide: str = "",
    ) -> Optional[APIVersion]:
        """Mark a version as deprecated."""
        api_version = self._versions.get(version)
        if not api_version:
            return None

        api_version.phase = VersionPhase.DEPRECATED
        api_version.deprecation_date = datetime.now().strftime("%Y-%m-%d")
        api_version.sunset_date = sunset_date
        api_version.migration_guide = migration_guide

        return api_version

    def sunset_version(self, version: str) -> Optional[APIVersion]:
        """Sunset a version (end of life)."""
        api_version = self._versions.get(version)
        if not api_version:
            return None

        api_version.phase = VersionPhase.SUNSET
        return api_version

    def retire_version(self, version: str) -> Optional[APIVersion]:
        """Retire a version completely."""
        api_version = self._versions.get(version)
        if not api_version:
            return None

        api_version.phase = VersionPhase.RETIRED
        return api_version

    def negotiate_version(
        self,
        requested_version: Optional[str],
        supported_versions: Optional[list[str]] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Negotiate an API version between client request and server support.

        Returns (selected_version, warning_message).
        """
        supported = supported_versions or list(self._versions.keys())
        supported.sort(key=lambda v: self._parse_version(v), reverse=True)

        if not requested_version:
            default = supported[0] if supported else None
            return default, None

        if requested_version in supported:
            if self._versions[requested_version].phase in (
                VersionPhase.DEPRECATED,
                VersionPhase.SUNSET,
            ):
                warning = f"Version {requested_version} is deprecated"
                if self._versions[requested_version].sunset_date:
                    warning += f" and will sunset on {self._versions[requested_version].sunset_date}"
                return requested_version, warning
            return requested_version, None

        compatible = self._find_compatible_version(requested_version, supported)
        if compatible:
            return compatible, f"Version {requested_version} not found, using {compatible}"

        latest = supported[0] if supported else None
        return latest, f"Version {requested_version} not supported, using {latest}"

    def _parse_version(self, version: str) -> tuple[int, int]:
        """Parse version string into components."""
        match = re.match(r"v?(\d+)\.?(\d+)?", version)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2) or 0)
            return (major, minor)
        return (0, 0)

    def _find_compatible_version(
        self,
        requested: str,
        supported: list[str],
    ) -> Optional[str]:
        """Find the latest compatible version for a requested major version."""
        req_major, _ = self._parse_version(requested)

        for version in supported:
            major, _ = self._parse_version(version)
            if major == req_major:
                return version

        return None

    def register_deprecation(
        self,
        resource: str,
        sunset_date: str,
        reason: str,
        alternative: str = "",
        migration_steps: Optional[list[str]] = None,
        severity: str = "medium",
    ) -> DeprecationNotice:
        """Register a deprecation notice."""
        notice = DeprecationNotice(
            id=str(uuid.uuid4())[:8],
            sunset_date=sunset_date,
            deprecation_date=datetime.now().strftime("%Y-%m-%d"),
            reason=reason,
            migration_steps=migration_steps or [],
            alternative=alternative,
            severity=severity,
        )
        self._deprecations[resource] = notice
        return notice

    def get_deprecation(self, resource: str) -> Optional[DeprecationNotice]:
        """Get deprecation notice for a resource."""
        return self._deprecations.get(resource)

    def check_deprecation_headers(
        self,
        version: str,
        resource: str,
    ) -> dict:
        """Check what deprecation headers should be returned."""
        version_info = self._versions.get(version)
        deprecation = self._deprecations.get(resource)

        headers = {}

        if version_info and version_info.phase in (
            VersionPhase.DEPRECATED,
            VersionPhase.SUNSET,
        ):
            headers["Deprecation"] = "true"
            if version_info.sunset_date:
                headers["Sunset"] = version_info.sunset_date
            headers["Preferred"] = self._current_version or ""

        if deprecation:
            headers["X-Deprecation-Reason"] = deprecation.reason
            headers["X-Deprecation-Alternative"] = deprecation.alternative

        return headers

    def record_change(
        self,
        change_type: str,
        resource: str,
        description: str,
        breaking: bool = False,
        migration: str = "",
    ) -> APIChange:
        """Record an API change."""
        change = APIChange(
            id=str(uuid.uuid4())[:8],
            change_type=change_type,
            resource=resource,
            description=description,
            breaking=breaking,
            date=datetime.now().strftime("%Y-%m-%d"),
            migration=migration,
        )
        self._changes.append(change)
        return change

    def get_changelog(
        self,
        from_version: Optional[str] = None,
        to_version: Optional[str] = None,
        breaking_only: bool = False,
    ) -> list[APIChange]:
        """Get API changelog between versions."""
        changes = self._changes

        if breaking_only:
            changes = [c for c in changes if c.breaking]

        return changes

    def detect_breaking_changes(
        self,
        old_version: str,
        new_version: str,
    ) -> list[str]:
        """Detect breaking changes between two versions."""
        breaking = []

        old_ver = self._versions.get(old_version)
        new_ver = self._versions.get(new_version)

        if not old_ver or not new_ver:
            return breaking

        for change in self._changes:
            if change.breaking:
                breaking.append(f"{change.resource}: {change.description}")

        return breaking

    def generate_migration_guide(
        self,
        from_version: str,
        to_version: str,
    ) -> str:
        """Generate a migration guide between versions."""
        old_ver = self._versions.get(from_version)
        new_ver = self._versions.get(to_version)

        if not old_ver or not new_ver:
            return "Migration guide not available."

        guide = f"# Migration Guide: {from_version} → {to_version}\n\n"

        if old_ver.migration_guide:
            guide += f"## From {from_version}\n{old_ver.migration_guide}\n\n"

        breaking = self.detect_breaking_changes(from_version, to_version)
        if breaking:
            guide += "## Breaking Changes\n"
            for change in breaking:
                guide += f"- {change}\n"
            guide += "\n"

        if new_ver.features:
            guide += "## New Features\n"
            for feature in new_ver.features:
                guide += f"- {feature}\n"
            guide += "\n"

        return guide

    def list_versions(
        self,
        phase: Optional[VersionPhase] = None,
    ) -> list[APIVersion]:
        """List all API versions with optional phase filter."""
        versions = list(self._versions.values())

        if phase:
            versions = [v for v in versions if v.phase == phase]

        return sorted(
            versions,
            key=lambda v: self._parse_version(v.version),
            reverse=True,
        )

    def get_version_stats(self) -> dict:
        """Get version statistics."""
        by_phase = {}
        for version in self._versions.values():
            phase_key = version.phase.value
            by_phase[phase_key] = by_phase.get(phase_key, 0) + 1

        return {
            "total_versions": len(self._versions),
            "by_phase": by_phase,
            "current_version": self._current_version,
            "active_deprecations": len(self._deprecations),
            "total_changes": len(self._changes),
        }
