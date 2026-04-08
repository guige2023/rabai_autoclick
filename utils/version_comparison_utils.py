"""
Version comparison utilities.

Provides semantic version parsing, comparison,
and compatibility checking.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass
class Version:
    """Semantic version representation."""
    major: int
    minor: int
    patch: int
    prerelease: str = ""
    build: str = ""

    def __str__(self) -> str:
        v = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            v += f"-{self.prerelease}"
        if self.build:
            v += f"+{self.build}"
        return v

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) < 0

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) <= 0

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) > 0

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) >= 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._compare(other) == 0

    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch, self.prerelease, self.build))

    def _compare(self, other: "Version") -> int:
        for a, b in [
            (self.major, other.major),
            (self.minor, other.minor),
            (self.patch, other.patch),
        ]:
            if a < b:
                return -1
            if a > b:
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


def parse_version(version_str: str) -> Version:
    """
    Parse version string into Version object.

    Supports:
      - 1.2.3
      - 1.2.3-beta
      - 1.2.3+build
      - 1.2.3-beta+build
      - v1.2.3 (strips v prefix)
      - 1.2 (treats patch as 0)

    Args:
        version_str: Version string

    Returns:
        Version object

    Raises:
        ValueError: If version string is invalid
    """
    version_str = version_str.strip().lstrip("v")
    pattern = r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+])(.+)$"
    match = re.match(pattern, version_str)

    if not match:
        parts = version_str.split("-")[0].split("+")[0].split(".")
        while len(parts) < 3:
            parts.append("0")
        try:
            major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
        except ValueError:
            raise ValueError(f"Invalid version: {version_str}")
        prerelease = ""
        build = ""
        if "-" in version_str:
            prerelease = version_str.split("-", 1)[1].split("+")[0]
        if "+" in version_str:
            build = version_str.split("+")[1]
        return Version(major, minor, patch, prerelease, build)

    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) else 0
    patch = int(match.group(3)) if match.group(3) else 0
    rest = match.group(4)
    prerelease = ""
    build = ""
    if "-" in rest:
        prerelease, build = rest.split("-", 1)
    elif "+" in rest:
        build = rest.split("+")[1]
    else:
        prerelease = rest

    return Version(major, minor, patch, prerelease, build)


def is_compatible(
    version: Version,
    constraint: str,
) -> bool:
    """
    Check if version satisfies constraint.

    Constraint formats:
      - "1.2.3" - exact match
      - "^1.2.3" - compatible (major same)
      - "~1.2.3" - roughly equivalent (major.minor same)
      - ">=1.2.3" - greater than or equal
      - ">1.2.3" - greater than
      - "<=1.2.3" - less than or equal
      - "<1.2.3" - less than
      - "1.2.x" - any patch with given major.minor

    Args:
        version: Version to check
        constraint: Constraint string

    Returns:
        True if compatible
    """
    constraint = constraint.strip()

    if constraint.startswith("^"):
        min_ver = parse_version(constraint[1:])
        return (
            version.major == min_ver.major
            and (version.minor > min_ver.minor
                 or (version.minor == min_ver.minor and version.patch >= min_ver.patch))
        )

    if constraint.startswith("~"):
        min_ver = parse_version(constraint[1:])
        return (
            version.major == min_ver.major
            and version.minor == min_ver.minor
            and version.patch >= min_ver.patch
        )

    operators = [">=", "<=", ">", "<", "="]
    for op in operators:
        if constraint.startswith(op):
            target = parse_version(constraint[len(op):])
            if op == ">=":
                return version >= target
            if op == "<=":
                return version <= target
            if op == ">":
                return version > target
            if op == "<":
                return version < target
            if op == "=":
                return version == target

    if "x" in constraint.lower() or "." in constraint:
        parts = constraint.replace("x", "0").split(".")
        while len(parts) < 3:
            parts.append("0")
        target = Version(int(parts[0]), int(parts[1]), int(parts[2]))
        return version.major == target.major and version.minor == target.minor

    try:
        target = parse_version(constraint)
        return version == target
    except ValueError:
        return False


def compare_versions(
    v1: str | Version,
    v2: str | Version,
) -> Literal[-1, 0, 1]:
    """
    Compare two versions.

    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2
    """
    if isinstance(v1, str):
        v1 = parse_version(v1)
    if isinstance(v2, str):
        v2 = parse_version(v2)

    if v1 < v2:
        return -1
    if v1 > v2:
        return 1
    return 0
