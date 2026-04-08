"""
Version comparison and parsing utilities.

Provides semantic versioning (SemVer) and other version
string parsing with comparison, ordering, and constraint checking.

Example:
    >>> from utils.version_utils_v2 import Version, parse_version
    >>> v1 = Version("1.2.3")
    >>> v2 = Version("2.0.0")
    >>> v1 < v2
    True
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import total_ordering
from typing import List, Optional, Union


@total_ordering
class Version:
    """
    Semantic version representation with comparison support.

    Supports SemVer 2.0.0 specification including pre-release
    and build metadata.

    Attributes:
        major: Major version number.
        minor: Minor version number.
        patch: Patch version number.
        prerelease: Pre-release identifier.
        build: Build metadata identifier.
    """

    def __init__(
        self,
        major: int = 0,
        minor: int = 0,
        patch: int = 0,
        prerelease: Optional[str] = None,
        build: Optional[str] = None,
    ) -> None:
        """
        Initialize a version.

        Args:
            major: Major version number.
            minor: Minor version number.
            patch: Patch version number.
            prerelease: Pre-release identifier (e.g., "alpha.1").
            build: Build metadata (e.g., "20240101").
        """
        self.major = major
        self.minor = minor
        self.patch = patch
        self.prerelease = prerelease
        self.build = build

    @classmethod
    def parse(cls, version_string: str) -> "Version":
        """
        Parse a version string.

        Args:
            version_string: Version string (e.g., "1.2.3-alpha").

        Returns:
            Version object.

        Raises:
            ValueError: If version string is invalid.
        """
        version_string = version_string.strip()

        pattern = r"^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$"
        match = re.match(pattern, version_string)

        if not match:
            raise ValueError(f"Invalid version string: {version_string}")

        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3))
        prerelease = match.group(4)
        build = match.group(5)

        return cls(major, minor, patch, prerelease, build)

    def __str__(self) -> str:
        """Get string representation."""
        parts = [f"{self.major}.{self.minor}.{self.patch}"]
        if self.prerelease:
            parts.append(f"-{self.prerelease}")
        if self.build:
            parts.append(f"+{self.build}")
        return "".join(parts)

    def __repr__(self) -> str:
        """Get detailed string representation."""
        return f"Version({self.major}, {self.minor}, {self.patch}, {self.prerelease!r}, {self.build!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) == 0

    def __lt__(self, other: "Version") -> bool:
        """Check less than."""
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) < 0

    def _compare(self, other: "Version") -> int:
        """Compare versions, returning -1, 0, or 1."""
        if self.major != other.major:
            return -1 if self.major < other.major else 1
        if self.minor != other.minor:
            return -1 if self.minor < other.minor else 1
        if self.patch != other.patch:
            return -1 if self.patch < other.patch else 1

        if self.prerelease and not other.prerelease:
            return -1
        if not self.prerelease and other.prerelease:
            return 1
        if self.prerelease and other.prerelease:
            return self._compare_prerelease(self.prerelease, other.prerelease)

        return 0

    @staticmethod
    def _compare_prerelease(a: str, b: str) -> int:
        """Compare pre-release identifiers."""
        a_parts = a.split(".")
        b_parts = b.split(".")

        for i in range(max(len(a_parts), len(b_parts))):
            a_part = a_parts[i] if i < len(a_parts) else None
            b_part = b_parts[i] if i < len(b_parts) else None

            if a_part is None:
                return -1
            if b_part is None:
                return 1

            a_is_num = a_part.isdigit()
            b_is_num = b_part.isdigit()

            if a_is_num and b_is_num:
                a_num = int(a_part)
                b_num = int(b_part)
                if a_num != b_num:
                    return -1 if a_num < b_num else 1
            elif a_is_num:
                return -1
            elif b_is_num:
                return 1
            else:
                if a_part != b_part:
                    return -1 if a_part < b_part else 1

        return 0

    def bump_major(self) -> "Version":
        """Return a new version with major bumped."""
        return Version(self.major + 1, 0, 0)

    def bump_minor(self) -> "Version":
        """Return a new version with minor bumped."""
        return Version(self.major, self.minor + 1, 0)

    def bump_patch(self) -> "Version":
        """Return a new version with patch bumped."""
        return Version(self.major, self.minor, self.patch + 1)

    def to_tuple(self) -> tuple:
        """Convert to tuple for ordering."""
        return (self.major, self.minor, self.patch)


class VersionConstraint:
    """Base class for version constraints."""

    def satisfied_by(self, version: Version) -> bool:
        """Check if a version satisfies this constraint."""
        raise NotImplementedError


class ExactConstraint(VersionConstraint):
    """Exact version match constraint."""

    def __init__(self, version: Version) -> None:
        """Initialize with exact version."""
        self.version = version

    def satisfied_by(self, version: Version) -> bool:
        """Check if version matches exactly."""
        return version == self.version

    def __str__(self) -> str:
        return f"=={self.version}"


class GreaterThanConstraint(VersionConstraint):
    """Greater than or equal to constraint."""

    def __init__(self, version: Version, inclusive: bool = False) -> None:
        """Initialize with version threshold."""
        self.version = version
        self.inclusive = inclusive

    def satisfied_by(self, version: Version) -> bool:
        """Check if version satisfies threshold."""
        if self.inclusive:
            return version >= self.version
        return version > self.version

    def __str__(self) -> str:
        op = ">=" if self.inclusive else ">"
        return f"{op}{self.version}"


class LessThanConstraint(VersionConstraint):
    """Less than constraint."""

    def __init__(self, version: Version, inclusive: bool = False) -> None:
        """Initialize with version threshold."""
        self.version = version
        self.inclusive = inclusive

    def satisfied_by(self, version: Version) -> bool:
        """Check if version satisfies threshold."""
        if self.inclusive:
            return version <= self.version
        return version < self.version

    def __str__(self) -> str:
        op = "<=" if self.inclusive else "<"
        return f"{op}{self.version}"


class RangeConstraint(VersionConstraint):
    """Version range constraint."""

    def __init__(
        self,
        min_version: Optional[Version] = None,
        max_version: Optional[Version] = None,
        min_inclusive: bool = False,
        max_inclusive: bool = False,
    ) -> None:
        """Initialize with range bounds."""
        self.min_version = min_version
        self.max_version = max_version
        self.min_inclusive = min_inclusive
        self.max_inclusive = max_inclusive

    def satisfied_by(self, version: Version) -> bool:
        """Check if version is within range."""
        if self.min_version:
            if self.min_inclusive:
                if version < self.min_version:
                    return False
            else:
                if version <= self.min_version:
                    return False

        if self.max_version:
            if self.max_inclusive:
                if version > self.max_version:
                    return False
            else:
                if version >= self.max_version:
                    return False

        return True

    def __str__(self) -> str:
        parts = []
        if self.min_version:
            op = ">=" if self.min_inclusive else ">"
            parts.append(f"{op}{self.min_version}")
        if self.max_version:
            op = "<=" if self.max_inclusive else "<"
            parts.append(f"{op}{self.max_version}")
        return " ".join(parts)


class VersionRange:
    """
    Parses and evaluates version range expressions.

    Supports operators like ~=, ^, >=, <=, ==, !=
    and common patterns like "1.2.x", "1.x", etc.
    """

    _OPERATOR_PATTERN = re.compile(
        r"^(~=|^|>=|<=|==|!=|<|>)?\s*v?(\d+(?:\.\d+)?(?:\.\d+)?(?:-[a-zA-Z0-9.-]+)?)"
    )

    def __init__(self, constraint_string: str) -> None:
        """
        Initialize with constraint string.

        Args:
            constraint_string: Version constraint (e.g., ">=1.0,<2.0").
        """
        self.constraint_string = constraint_string
        self._constraints: List[VersionConstraint] = self._parse(constraint_string)

    def _parse(self, constraint_string: str) -> List[VersionConstraint]:
        """Parse a constraint string into constraints."""
        constraints: List[VersionConstraint] = []
        parts = constraint_string.split(",")

        for part in parts:
            part = part.strip()
            match = self._OPERATOR_PATTERN.match(part)
            if not match:
                continue

            operator = match.group(1) or ">="
            version_str = match.group(2)

            try:
                version = Version.parse(version_str)
            except ValueError:
                continue

            if operator == "==":
                constraints.append(ExactConstraint(version))
            elif operator == ">":
                constraints.append(GreaterThanConstraint(version, inclusive=False))
            elif operator == ">=":
                constraints.append(GreaterThanConstraint(version, inclusive=True))
            elif operator == "<":
                constraints.append(LessThanConstraint(version, inclusive=False))
            elif operator == "<=":
                constraints.append(LessThanConstraint(version, inclusive=True))
            elif operator == "!=":
                constraints.append(LessThanConstraint(version, inclusive=False))
                constraints.append(GreaterThanConstraint(version, inclusive=False))
            elif operator == "~=":
                constraints.append(
                    RangeConstraint(
                        min_version=version,
                        min_inclusive=True,
                        max_version=version.bump_minor(),
                        max_inclusive=False,
                    )
                )
            elif operator == "^":
                constraints.append(
                    RangeConstraint(
                        min_version=version,
                        min_inclusive=True,
                        max_version=Version(version.major + 1, 0, 0),
                        max_inclusive=False,
                    )
                )

        return constraints

    def satisfied_by(self, version: Version) -> bool:
        """Check if a version satisfies all constraints."""
        return all(c.satisfied_by(version) for c in self._constraints)


def parse_version(version_string: str) -> Version:
    """
    Convenience function to parse a version string.

    Args:
        version_string: Version string.

    Returns:
        Version object.
    """
    return Version.parse(version_string)


def compare_versions(
    v1: Union[str, Version],
    v2: Union[str, Version],
) -> int:
    """
    Compare two versions.

    Args:
        v1: First version.
        v2: Second version.

    Returns:
        -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2.
    """
    if isinstance(v1, str):
        v1 = Version.parse(v1)
    if isinstance(v2, str):
        v2 = Version.parse(v2)

    if v1 < v2:
        return -1
    if v1 > v2:
        return 1
    return 0
