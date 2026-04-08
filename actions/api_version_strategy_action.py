"""API Version Strategy Action Module.

Manages API versioning strategies including semantic versioning,
deprecation policies, and version migration paths.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VersionStrategy(Enum):
    """API versioning strategy types."""
    SEMANTIC = "semantic"
    DATE_BASED = "date_based"
    HEADER_BASED = "header_based"
    URI_BASED = "uri_based"
    MEDIA_TYPE = "media_type"


class ChangeType(Enum):
    """Type of API change."""
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"
    BREAKING = "breaking"


class DeprecationPhase(Enum):
    """Deprecation phases."""
    ANNOUNCED = "announced"
    ACTIVE = "active"
    GRACE_PERIOD = "grace_period"
    END_OF_LIFE = "end_of_life"


@dataclass
class APIVersion:
    """Represents an API version."""
    version: str
    major: int
    minor: int
    patch: int
    released_at: float
    status: DeprecationPhase
    sunset_date: Optional[float] = None
    deprecated_features: List[str] = field(default_factory=list)
    migration_guide: str = ""


@dataclass
class VersionMigration:
    """Defines a migration path between versions."""
    from_version: str
    to_version: str
    steps: List[Dict[str, Any]] = field(default_factory=list)
    backward_compatible: bool = True
    estimated_effort: str = ""


class APIVersionStrategyAction(BaseAction):
    """
    Manages API versioning strategies and migrations.

    Handles semantic versioning, deprecation policies, and
    provides migration paths between API versions.

    Example:
        versioner = APIVersionStrategyAction()
        result = versioner.execute(ctx, {
            "action": "compare_versions",
            "v1": "2.0.0",
            "v2": "2.1.0"
        })
    """
    action_type = "api_version_strategy"
    display_name = "API版本策略"
    description = "管理API版本策略、弃用策略和版本迁移路径"

    def __init__(self) -> None:
        super().__init__()
        self._versions: Dict[str, APIVersion] = {}
        self._migrations: Dict[Tuple[str, str], VersionMigration] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a version strategy action.

        Args:
            context: Execution context.
            params: Dict with keys: action, versions, etc.

        Returns:
            ActionResult with operation result.
        """
        action = params.get("action", "")

        try:
            if action == "register_version":
                return self._register_version(params)
            elif action == "compare_versions":
                return self._compare_versions(params)
            elif action == "get_change_type":
                return self._get_change_type(params)
            elif action == "deprecate":
                return self._deprecate_version(params)
            elif action == "get_migration":
                return self._get_migration_path(params)
            elif action == "list_versions":
                return self._list_versions(params)
            elif action == "parse_version":
                return self._parse_version(params)
            elif action == "bump_version":
                return self._bump_version(params)
            elif action == "validate_version_string":
                return self._validate_version_string(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Version strategy error: {str(e)}")

    def _register_version(self, params: Dict[str, Any]) -> ActionResult:
        """Register a new API version."""
        version_str = params.get("version", "")
        status_str = params.get("status", "active")
        sunset_date = params.get("sunset_date")
        deprecated_features = params.get("deprecated_features", [])
        migration_guide = params.get("migration_guide", "")

        parsed = self._parse_version_string(version_str)
        if not parsed:
            return ActionResult(success=False, message=f"Invalid version string: {version_str}")

        major, minor, patch = parsed

        try:
            status = DeprecationPhase(status_str)
        except ValueError:
            status = DeprecationPhase.ACTIVE

        version = APIVersion(
            version=version_str,
            major=major,
            minor=minor,
            patch=patch,
            released_at=time.time(),
            status=status,
            sunset_date=sunset_date,
            deprecated_features=deprecated_features,
            migration_guide=migration_guide,
        )

        self._versions[version_str] = version

        return ActionResult(
            success=True,
            message=f"Version registered: {version_str}",
            data={
                "version": version_str,
                "major": major,
                "minor": minor,
                "patch": patch,
                "status": status.value,
            }
        )

    def _compare_versions(self, params: Dict[str, Any]) -> ActionResult:
        """Compare two semantic versions."""
        v1_str = params.get("v1", "")
        v2_str = params.get("v2", "")

        v1 = self._parse_version_string(v1_str)
        v2 = self._parse_version_string(v2_str)

        if not v1 or not v2:
            return ActionResult(success=False, message="Invalid version strings")

        v1_tuple = (v1[0], v1[1], v1[2])
        v2_tuple = (v2[0], v2[1], v2[2])

        if v1_tuple == v2_tuple:
            relation = "equal"
        elif v1_tuple > v2_tuple:
            relation = "greater"
        else:
            relation = "less"

        distance = self._calculate_version_distance(v1, v2)

        return ActionResult(
            success=True,
            data={
                "v1": v1_str,
                "v2": v2_str,
                "relation": relation,
                "distance": distance,
                "change_type": self._determine_change_type(v1, v2).value,
            }
        )

    def _get_change_type(self, params: Dict[str, Any]) -> ActionResult:
        """Determine the type of change between versions."""
        from_version = params.get("from_version", "")
        to_version = params.get("to_version", "")

        v1 = self._parse_version_string(from_version)
        v2 = self._parse_version_string(to_version)

        if not v1 or not v2:
            return ActionResult(success=False, message="Invalid version strings")

        change_type = self._determine_change_type(v1, v2)

        return ActionResult(
            success=True,
            data={
                "from": from_version,
                "to": to_version,
                "change_type": change_type.value,
                "breaking": change_type in (ChangeType.MAJOR, ChangeType.BREAKING),
            }
        )

    def _deprecate_version(self, params: Dict[str, Any]) -> ActionResult:
        """Mark a version as deprecated."""
        version_str = params.get("version", "")
        phase_str = params.get("phase", "active")
        sunset_date = params.get("sunset_date")

        if version_str not in self._versions:
            return ActionResult(success=False, message=f"Version not found: {version_str}")

        try:
            phase = DeprecationPhase(phase_str)
        except ValueError:
            phase = DeprecationPhase.ACTIVE

        version = self._versions[version_str]
        old_phase = version.status
        version.status = phase

        if sunset_date:
            version.sunset_date = sunset_date

        return ActionResult(
            success=True,
            message=f"Version {version_str} deprecated to {phase.value}",
            data={
                "version": version_str,
                "old_phase": old_phase.value,
                "new_phase": phase.value,
                "sunset_date": version.sunset_date,
            }
        )

    def _get_migration_path(self, params: Dict[str, Any]) -> ActionResult:
        """Get migration path between versions."""
        from_version = params.get("from_version", "")
        to_version = params.get("to_version", "")

        migration_key = (from_version, to_version)

        if migration_key in self._migrations:
            migration = self._migrations[migration_key]
            return ActionResult(
                success=True,
                data={
                    "from": from_version,
                    "to": to_version,
                    "backward_compatible": migration.backward_compatible,
                    "steps": migration.steps,
                    "estimated_effort": migration.estimated_effort,
                }
            )

        v1 = self._parse_version_string(from_version)
        v2 = self._parse_version_string(to_version)

        if not v1 or not v2:
            return ActionResult(success=False, message="Invalid version strings")

        steps = self._generate_migration_steps(from_version, to_version, v1, v2)

        return ActionResult(
            success=True,
            data={
                "from": from_version,
                "to": to_version,
                "steps": steps,
                "backward_compatible": v1[0] == v2[0],
            }
        )

    def _list_versions(self, params: Dict[str, Any]) -> ActionResult:
        """List all registered versions."""
        status_filter = params.get("status")

        versions = list(self._versions.values())

        if status_filter:
            try:
                phase = DeprecationPhase(status_filter)
                versions = [v for v in versions if v.status == phase]
            except ValueError:
                pass

        versions.sort(key=lambda v: (v.major, v.minor, v.patch), reverse=True)

        return ActionResult(
            success=True,
            data={
                "versions": [
                    {
                        "version": v.version,
                        "status": v.status.value,
                        "released_at": v.released_at,
                        "sunset_date": v.sunset_date,
                        "deprecated_features": v.deprecated_features,
                    }
                    for v in versions
                ],
                "count": len(versions),
            }
        )

    def _parse_version(self, params: Dict[str, Any]) -> ActionResult:
        """Parse a version string."""
        version_str = params.get("version", "")

        parsed = self._parse_version_string(version_str)

        if not parsed:
            return ActionResult(success=False, message=f"Invalid version string: {version_str}")

        major, minor, patch = parsed

        return ActionResult(
            success=True,
            data={
                "version": version_str,
                "major": major,
                "minor": minor,
                "patch": patch,
                "semantic": True,
            }
        )

    def _bump_version(self, params: Dict[str, Any]) -> ActionResult:
        """Bump a version number."""
        version_str = params.get("version", "1.0.0")
        bump_type = params.get("bump_type", "patch")

        parsed = self._parse_version_string(version_str)

        if not parsed:
            return ActionResult(success=False, message=f"Invalid version string: {version_str}")

        major, minor, patch = parsed

        if bump_type == "major":
            major += 1
            minor = 0
            patch = 0
        elif bump_type == "minor":
            minor += 1
            patch = 0
        elif bump_type == "patch":
            patch += 1

        new_version = f"{major}.{minor}.{patch}"

        return ActionResult(
            success=True,
            message=f"Version bumped: {version_str} -> {new_version}",
            data={
                "old_version": version_str,
                "new_version": new_version,
                "bump_type": bump_type,
            }
        )

    def _validate_version_string(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a version string format."""
        version_str = params.get("version", "")

        parsed = self._parse_version_string(version_str)
        is_valid = parsed is not None

        return ActionResult(
            success=is_valid,
            message="Valid" if is_valid else "Invalid",
            data={
                "version": version_str,
                "valid": is_valid,
                "parsed": {
                    "major": parsed[0] if parsed else None,
                    "minor": parsed[1] if parsed else None,
                    "patch": parsed[2] if parsed else None,
                } if parsed else None,
            }
        )

    def _parse_version_string(self, version: str) -> Optional[Tuple[int, int, int]]:
        """Parse a semantic version string."""
        import re

        match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version)
        if not match:
            return None

        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))

    def _determine_change_type(
        self,
        v1: Tuple[int, int, int],
        v2: Tuple[int, int, int],
    ) -> ChangeType:
        """Determine the type of change between versions."""
        if v2[0] > v1[0]:
            return ChangeType.MAJOR
        elif v2[1] > v1[1]:
            return ChangeType.MINOR
        elif v2[2] > v1[2]:
            return ChangeType.PATCH
        else:
            return ChangeType.BREAKING

    def _calculate_version_distance(
        self,
        v1: Tuple[int, int, int],
        v2: Tuple[int, int, int],
    ) -> int:
        """Calculate the distance between two versions."""
        return (
            (v2[0] - v1[0]) * 10000 +
            (v2[1] - v1[1]) * 100 +
            (v2[2] - v1[2])
        )

    def _generate_migration_steps(
        self,
        from_ver: str,
        to_ver: str,
        v1: Tuple[int, int, int],
        v2: Tuple[int, int, int],
    ) -> List[Dict[str, Any]]:
        """Generate migration steps between versions."""
        steps = []

        if v2[0] > v1[0]:
            steps.append({
                "type": "breaking_change",
                "description": "Major version change - review breaking changes",
                "action": "Review migration guide",
            })
            steps.append({
                "type": "endpoint_update",
                "description": "Update endpoint URLs from v{old} to v{new}".format(old=v1[0], new=v2[0]),
                "action": "Update base URL",
            })

        if v2[1] > v1[1]:
            steps.append({
                "type": "new_features",
                "description": "New features added in minor version",
                "action": "Review release notes",
            })

        if v2[2] > v1[2]:
            steps.append({
                "type": "patch",
                "description": "Patch release - backward compatible",
                "action": "Update to latest patch",
            })

        steps.append({
            "type": "test",
            "description": "Test integration",
            "action": "Run regression tests",
        })

        return steps

    def get_active_versions(self) -> List[APIVersion]:
        """Get all active (non-deprecated) versions."""
        return [
            v for v in self._versions.values()
            if v.status in (DeprecationPhase.ANNOUNCED, DeprecationPhase.ACTIVE)
        ]

    def get_versions_needing_migration(self) -> List[APIVersion]:
        """Get versions that need migration attention."""
        now = time.time()
        needing_migration = []

        for version in self._versions.values():
            if version.status == DeprecationPhase.GRACE_PERIOD:
                needing_migration.append(version)
            elif version.sunset_date and version.sunset_date < now + 86400 * 30:
                needing_migration.append(version)

        return needing_migration
