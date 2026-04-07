"""version_action module for rabai_autoclick.

Provides version handling utilities: semantic versioning,
version comparison, requirements parsing, and compatibility checks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

__all__ = [
    "Version",
    "VersionRange",
    "Requirement",
    "parse_version",
    "compare_versions",
    "is_compatible",
    "VersionError",
    "VersionRangeError",
]


class VersionError(Exception):
    """Raised when version parsing fails."""
    pass


class VersionRangeError(Exception):
    """Raised when version range parsing fails."""
    pass


@dataclass
class Version:
    """Semantic version representation."""
    major: int = 0
    minor: int = 0
    patch: int = 0
    prerelease: str = ""
    build: str = ""

    def __str__(self) -> str:
        v = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            v += f"-{self.prerelease}"
        if self.build:
            v += f"+{self.build}"
        return v

    def __repr__(self) -> str:
        return f"Version({self.major}, {self.minor}, {self.patch}, '{self.prerelease}', '{self.build}')"

    def __lt__(self, other: "Version") -> bool:
        return self._compare(other) < 0

    def __le__(self, other: "Version") -> bool:
        return self._compare(other) <= 0

    def __gt__(self, other: "Version") -> bool:
        return self._compare(other) > 0

    def __ge__(self, other: "Version") -> bool:
        return self._compare(other) >= 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return False
        return self._compare(other) == 0

    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch, self.prerelease, self.build))

    def _compare(self, other: "Version") -> int:
        """Compare versions: returns -1, 0, or 1."""
        self_tuple = (self.major, self.minor, self.patch)
        other_tuple = (other.major, other.minor, other.patch)
        if self_tuple < other_tuple:
            return -1
        if self_tuple > other_tuple:
            return 1
        if self.prerelease and not other.prerelease:
            return -1
        if not self.prerelease and other.prerelease:
            return 1
        if self.prerelease < other.prerelease:
            return -1
        if self.prerelease > other.prerelease:
            return 1
        return 0

    def is_prerelease(self) -> bool:
        """Check if version is a prerelease."""
        return bool(self.prerelease)

    def is_stable(self) -> bool:
        """Check if version is stable (no prerelease)."""
        return not self.prerelease


def parse_version(version_str: str) -> Version:
    """Parse version string into Version object.

    Supports:
        - MAJOR.MINOR.PATCH (1.2.3)
        - MAJOR.MINOR.PATCH-prerelease (1.2.3-beta)
        - MAJOR.MINOR.PATCH+build (1.2.3+build)
        - MAJOR.MINOR.PATCH-prerelease+build (1.2.3-beta+build)

    Args:
        version_str: Version string.

    Returns:
        Version object.

    Raises:
        VersionError: If version string is invalid.
    """
    version_str = version_str.strip()

    pattern = r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+](\w[\w\.-]*))?(?:\+(\w[\w\.-]*))?$"
    match = re.match(pattern, version_str)

    if not match:
        raise VersionError(f"Invalid version string: {version_str}")

    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) else 0
    patch = int(match.group(3)) if match.group(3) else 0
    prerelease = match.group(4) or ""
    build = match.group(5) or ""

    return Version(major=major, minor=minor, patch=patch, prerelease=prerelease, build=build)


def compare_versions(v1: str, v2: str) -> int:
    """Compare two version strings.

    Args:
        v1: First version string.
        v2: Second version string.

    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
    """
    ver1 = parse_version(v1)
    ver2 = parse_version(v2)
    return ver1._compare(ver2)


class VersionRange:
    """Version range specification (e.g., >=1.0,<2.0)."""

    def __init__(self, spec: str) -> None:
        self.spec = spec
        self._constraints: List[Tuple[str, Version]] = []
        self._parse(spec)

    def _parse(self, spec: str) -> None:
        """Parse version range specification."""
        spec = spec.strip()
        parts = spec.split(",")
        for part in parts:
            part = part.strip()
            if not part:
                continue
            match = re.match(r"^([><=!]+)\s*([\d.]+)", part)
            if not match:
                raise VersionRangeError(f"Invalid constraint: {part}")
            op = match.group(1)
            ver_str = match.group(2)
            version = parse_version(ver_str)
            self._constraints.append((op, version))

    def contains(self, version: Union[str, Version]) -> bool:
        """Check if version satisfies the range."""
        if isinstance(version, str):
            version = parse_version(version)
        for op, constraint in self._constraints:
            if not self._check_op(version, op, constraint):
                return False
        return True

    def _check_op(self, version: Version, op: str, constraint: Version) -> bool:
        """Check single operator."""
        if op == ">":
            return version > constraint
        if op == ">=":
            return version >= constraint
        if op == "<":
            return version < constraint
        if op == "<=":
            return version <= constraint
        if op == "==":
            return version == constraint
        if op == "!=":
            return version != constraint
        return False

    def __str__(self) -> str:
        return self.spec


@dataclass
class Requirement:
    """Package requirement specification."""
    name: str
    version_spec: Optional[str] = None
    extras: List[str] = None
    url: Optional[str] = None
    markers: Optional[str] = None

    def __post_init__(self) -> None:
        if self.extras is None:
            self.extras = []

    def __str__(self) -> str:
        result = self.name
        if self.extras:
            result += f"[{','.join(self.extras)}]"
        if self.version_spec:
            result += f" {self.version_spec}"
        if self.url:
            result += f"; {self.url}"
        if self.markers:
            result += f" ; {self.markers}"
        return result

    def is_satisfied_by(self, version: Union[str, Version]) -> bool:
        """Check if requirement is satisfied by version."""
        if isinstance(version, str):
            version = parse_version(version)
        if self.version_spec:
            r = VersionRange(self.version_spec)
            return r.contains(version)
        return True


def parse_requirement(req_str: str) -> Requirement:
    """Parse requirement string.

    Args:
        req_str: Requirement string (e.g., "package>=1.0,<2.0").

    Returns:
        Requirement object.
    """
    req_str = req_str.strip()

    extras_match = re.match(r"^([a-zA-Z0-9_-]+)\[([^\]]+)\]", req_str)
    extras: List[str] = []
    name_part = req_str
    if extras_match:
        name_part = extras_match.group(1)
        extras = extras_match.group(2).split(",")

    parts = name_part.split()
    name = parts[0]
    version_spec = parts[1] if len(parts) > 1 else None

    url_match = re.search(r";\s*url\s*==\s*['\"]([^'\"]+)['\"]", req_str)
    url = url_match.group(1) if url_match else None

    markers_match = re.search(r";\s*(.+?)(?:\s*;|$)", req_str)
    markers = markers_match.group(1).strip() if markers_match else None

    return Requirement(
        name=name,
        version_spec=version_spec,
        extras=extras,
        url=url,
        markers=markers,
    )


def is_compatible(
    version: str,
    requirement: str,
) -> bool:
    """Check if version satisfies requirement.

    Args:
        version: Version string.
        requirement: Requirement string.

    Returns:
        True if compatible.
    """
    req = parse_requirement(requirement)
    return req.is_satisfied_by(version)
