"""Version action module for RabAI AutoClick.

Provides version comparison utilities:
- Version: Semantic version
- VersionComparator: Compare versions
- VersionBumper: Bump version numbers
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import re
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Version:
    """Semantic version."""

    def __init__(self, version_str: str):
        self.original = version_str
        self.major, self.minor, self.patch, self.prerelease, self.build = self._parse(version_str)

    def _parse(self, version_str: str) -> Tuple[int, int, int, str, str]:
        """Parse version string."""
        pattern = r"^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$"
        match = re.match(pattern, version_str)

        if not match:
            raise ValueError(f"Invalid version: {version_str}")

        major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
        prerelease = match.group(4) or ""
        build = match.group(5) or ""

        return major, minor, patch, prerelease, build

    def __str__(self) -> str:
        """String representation."""
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            version += f"-{self.prerelease}"
        if self.build:
            version += f"+{self.build}"
        return version

    def __repr__(self) -> str:
        """Debug representation."""
        return f"Version('{str(self)}')"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, Version):
            return False
        return self._tuple() == other._tuple()

    def __lt__(self, other: "Version") -> bool:
        """Less than comparison."""
        return self._tuple() < other._tuple()

    def __le__(self, other: "Version") -> bool:
        """Less than or equal."""
        return self == other or self < other

    def __gt__(self, other: "Version") -> bool:
        """Greater than comparison."""
        return self._tuple() > other._tuple()

    def __ge__(self, other: "Version") -> bool:
        """Greater than or equal."""
        return self == other or self > other

    def _tuple(self) -> Tuple:
        """Get comparison tuple."""
        return (self.major, self.minor, self.patch, self.prerelease, self.build)


class VersionComparator:
    """Compare versions."""

    @staticmethod
    def compare(v1: str, v2: str) -> int:
        """Compare two versions. Returns -1, 0, or 1."""
        ver1 = Version(v1)
        ver2 = Version(v2)

        if ver1 < ver2:
            return -1
        elif ver1 > ver2:
            return 1
        else:
            return 0

    @staticmethod
    def is_compatible(current: str, required: str) -> bool:
        """Check if versions are compatible (same major version)."""
        cur = Version(current)
        req = Version(required)
        return cur.major == req.major

    @staticmethod
    def is_newer(new: str, old: str) -> bool:
        """Check if new is newer than old."""
        return Version(new) > Version(old)


class VersionBumper:
    """Bump version numbers."""

    @staticmethod
    def bump_major(version: str) -> str:
        """Bump major version."""
        ver = Version(version)
        return f"{ver.major + 1}.0.0"

    @staticmethod
    def bump_minor(version: str) -> str:
        """Bump minor version."""
        ver = Version(version)
        return f"{ver.major}.{ver.minor + 1}.0"

    @staticmethod
    def bump_patch(version: str) -> str:
        """Bump patch version."""
        ver = Version(version)
        return f"{ver.major}.{ver.minor}.{ver.patch + 1}"

    @staticmethod
    def set_prerelease(version: str, prerelease: str) -> str:
        """Set prerelease."""
        ver = Version(version)
        return f"{ver.major}.{ver.minor}.{ver.patch}-{prerelease}"

    @staticmethod
    def remove_prerelease(version: str) -> str:
        """Remove prerelease."""
        ver = Version(version)
        return f"{ver.major}.{ver.minor}.{ver.patch}"


class VersionAction(BaseAction):
    """Version management action."""
    action_type = "version"
    display_name = "版本管理"
    description = "语义版本"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compare")

            if operation == "compare":
                return self._compare(params)
            elif operation == "bump":
                return self._bump(params)
            elif operation == "is_compatible":
                return self._is_compatible(params)
            elif operation == "is_newer":
                return self._is_newer(params)
            elif operation == "parse":
                return self._parse(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Version error: {str(e)}")

    def _compare(self, params: Dict[str, Any]) -> ActionResult:
        """Compare two versions."""
        v1 = params.get("v1")
        v2 = params.get("v2")

        if not v1 or not v2:
            return ActionResult(success=False, message="v1 and v2 are required")

        try:
            result = VersionComparator.compare(v1, v2)
            comparison = {-1: "less than", 0: "equal to", 1: "greater than"}[result]
            return ActionResult(success=True, message=f"{v1} is {comparison} {v2}", data={"result": result})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _bump(self, params: Dict[str, Any]) -> ActionResult:
        """Bump version."""
        version = params.get("version")
        bump_type = params.get("type", "patch")
        prerelease = params.get("prerelease")

        if not version:
            return ActionResult(success=False, message="version is required")

        try:
            if bump_type == "major":
                new_version = VersionBumper.bump_major(version)
            elif bump_type == "minor":
                new_version = VersionBumper.bump_minor(version)
            elif bump_type == "patch":
                new_version = VersionBumper.bump_patch(version)
            elif bump_type == "prerelease":
                if prerelease:
                    new_version = VersionBumper.set_prerelease(version, prerelease)
                else:
                    new_version = VersionBumper.remove_prerelease(version)
            else:
                return ActionResult(success=False, message=f"Unknown bump type: {bump_type}")

            return ActionResult(success=True, message=f"{version} -> {new_version}", data={"version": new_version})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _is_compatible(self, params: Dict[str, Any]) -> ActionResult:
        """Check compatibility."""
        current = params.get("current")
        required = params.get("required")

        if not current or not required:
            return ActionResult(success=False, message="current and required are required")

        try:
            compatible = VersionComparator.is_compatible(current, required)
            return ActionResult(success=True, message="Compatible" if compatible else "Not compatible", data={"compatible": compatible})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _is_newer(self, params: Dict[str, Any]) -> ActionResult:
        """Check if newer."""
        new = params.get("new")
        old = params.get("old")

        if not new or not old:
            return ActionResult(success=False, message="new and old are required")

        try:
            newer = VersionComparator.is_newer(new, old)
            return ActionResult(success=True, message=f"{new} is {'newer' if newer else 'not newer'} than {old}", data={"newer": newer})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _parse(self, params: Dict[str, Any]) -> ActionResult:
        """Parse version."""
        version = params.get("version")

        if not version:
            return ActionResult(success=False, message="version is required")

        try:
            ver = Version(version)
            return ActionResult(
                success=True,
                message=f"Parsed: {version}",
                data={
                    "major": ver.major,
                    "minor": ver.minor,
                    "patch": ver.patch,
                    "prerelease": ver.prerelease,
                    "build": ver.build,
                },
            )
        except ValueError as e:
            return ActionResult(success=False, message=str(e))
