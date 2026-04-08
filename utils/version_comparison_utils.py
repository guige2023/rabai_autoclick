"""Version comparison utilities for RabAI AutoClick.

Provides:
- Semantic version comparison
- Version parsing
- Version requirement checking
"""

from __future__ import annotations

import re
from typing import (
    Any,
    List,
    Optional,
    Tuple,
)


class Version:
    """A parsed version."""

    def __init__(self, version_string: str) -> None:
        self._string = version_string
        parts = re.match(r"(\d+)\.(\d+)\.(\d+)", version_string)
        if parts:
            self.major = int(parts.group(1))
            self.minor = int(parts.group(2))
            self.patch = int(parts.group(3))
        else:
            self.major = self.minor = self.patch = 0

    def __str__(self) -> str:
        return self._string

    def __repr__(self) -> str:
        return f"Version({self._string})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return False
        return (self.major, self.minor, self.patch) == (
            other.major, other.minor, other.patch
        )

    def __lt__(self, other: "Version") -> bool:
        return (self.major, self.minor, self.patch) < (
            other.major, other.minor, other.patch
        )

    def __le__(self, other: "Version") -> bool:
        return self == other or self < other

    def __gt__(self, other: "Version") -> bool:
        return other < self

    def __ge__(self, other: "Version") -> bool:
        return not self < other


def parse_version(version_string: str) -> Version:
    """Parse a version string.

    Args:
        version_string: Version like '1.2.3'.

    Returns:
        Version object.
    """
    return Version(version_string)


def compare_versions(a: str, b: str) -> int:
    """Compare two version strings.

    Args:
        a: First version.
        b: Second version.

    Returns:
        -1 if a < b, 0 if equal, 1 if a > b.
    """
    va = Version(a)
    vb = Version(b)
    if va < vb:
        return -1
    elif va > vb:
        return 1
    return 0


def is_compatible(
    version: str,
    requirement: str,
) -> bool:
    """Check if version satisfies a requirement.

    Args:
        version: Version string.
        requirement: Requirement like '>=1.0.0', '==2.0.0', '>1.5.0'.

    Returns:
        True if version satisfies requirement.
    """
    v = Version(version)
    match = re.match(r"(>=|<=|==|!=|>|<)(\d+)\.(\d+)\.(\d+)", requirement)
    if not match:
        return False

    op = match.group(1)
    req_v = Version(requirement)

    if op == ">=":
        return v >= req_v
    elif op == "<=":
        return v <= req_v
    elif op == "==":
        return v == req_v
    elif op == "!=":
        return v != req_v
    elif op == ">":
        return v > req_v
    elif op == "<":
        return v < req_v
    return False


__all__ = [
    "Version",
    "parse_version",
    "compare_versions",
    "is_compatible",
]
