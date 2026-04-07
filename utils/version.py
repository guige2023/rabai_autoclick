"""Version utilities for RabAI AutoClick.

Provides:
- Version parsing and comparison
- Version constraints checking
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union


@dataclass
class Version:
    """Semantic version representation.

    Supports semantic versioning (MAJOR.MINOR.PATCH) with optional pre-release.
    """

    major: int
    minor: int
    patch: int
    prerelease: Optional[str] = None

    def __str__(self) -> str:
        """String representation."""
        v = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            v += f"-{self.prerelease}"
        return v

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"Version({self.major}, {self.minor}, {self.patch}, {self.prerelease!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Version):
            return NotImplemented
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
        )

    def __lt__(self, other: 'Version') -> bool:
        """Less than comparison."""
        if not isinstance(other, Version):
            return NotImplemented

        for v1, v2 in [(self.major, other.major), (self.minor, other.minor), (self.patch, other.patch)]:
            if v1 < v2:
                return True
            if v1 > v2:
                return False

        # No prerelease > prerelease in semver
        if self.prerelease and other.prerelease:
            return self.prerelease < other.prerelease
        if self.prerelease:
            return True  # 1.0.0-alpha < 1.0.0
        if other.prerelease:
            return False

        return False

    def __le__(self, other: 'Version') -> bool:
        """Less than or equal."""
        return self == other or self < other

    def __gt__(self, other: 'Version') -> bool:
        """Greater than."""
        return other < self

    def __ge__(self, other: 'Version') -> bool:
        """Greater than or equal."""
        return self == other or self > other

    def __hash__(self) -> int:
        """Hash for use in dicts."""
        return hash((self.major, self.minor, self.patch, self.prerelease))


def parse_version(version_str: str) -> Optional[Version]:
    """Parse version string.

    Args:
        version_str: Version string (e.g., "1.2.3", "1.2.3-beta").

    Returns:
        Version object or None if invalid.
    """
    # Match semantic version
    pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?$'
    match = re.match(pattern, version_str.strip())

    if not match:
        return None

    major, minor, patch, prerelease = match.groups()

    return Version(
        major=int(major),
        minor=int(minor),
        patch=int(patch),
        prerelease=prerelease,
    )


def compare_versions(
    v1: Union[str, Version],
    v2: Union[str, Version],
) -> int:
    """Compare two versions.

    Args:
        v1: First version.
        v2: Second version.

    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
    """
    if isinstance(v1, str):
        v1 = parse_version(v1)
    if isinstance(v2, str):
        v2 = parse_version(v2)

    if v1 is None or v2 is None:
        raise ValueError("Invalid version string")

    if v1 < v2:
        return -1
    elif v1 > v2:
        return 1
    return 0


class VersionConstraint:
    """Represents a version constraint.

    Supports constraints like:
    - ">=1.0.0"
    - "<2.0.0"
    - ">=1.0.0,<2.0.0"
    - "^1.0.0" (compatible)
    - "~1.0.0" (patch compatible)
    """

    def __init__(self, constraint: str) -> None:
        """Initialize constraint.

        Args:
            constraint: Constraint string.
        """
        self._constraints: List[Tuple[str, Version]] = []
        self._parse_constraint(constraint)

    def _parse_constraint(self, constraint: str) -> None:
        """Parse constraint string."""
        for part in constraint.split(','):
            part = part.strip()
            if not part:
                continue

            # Extract operator and version
            match = re.match(r'^([<>=^~]+)?(.+)$', part)
            if not match:
                continue

            op, version_str = match.groups()
            version = parse_version(version_str.strip())
            if version:
                self._constraints.append((op or '==', version))

    def matches(self, version: Union[str, Version]) -> bool:
        """Check if version matches constraint.

        Args:
            version: Version to check.

        Returns:
            True if version satisfies constraint.
        """
        if isinstance(version, str):
            version = parse_version(version)
        if version is None:
            return False

        for op, constraint_version in self._constraints:
            if not self._check_op(version, op, constraint_version):
                return False

        return True

    def _check_op(self, version: Version, op: str, constraint: Version) -> bool:
        """Check single operator."""
        if op == '==':
            return version == constraint
        elif op == '!=':
            return version != constraint
        elif op == '>=':
            return version >= constraint
        elif op == '<=':
            return version <= constraint
        elif op == '>':
            return version > constraint
        elif op == '<':
            return version < constraint
        elif op == '^':  # Compatible
            return (
                version.major == constraint.major
                and version >= constraint
            )
        elif op == '~':  # Patch compatible
            return (
                version.major == constraint.major
                and version.minor == constraint.minor
                and version >= constraint
            )
        return False

    def __str__(self) -> str:
        """String representation."""
        return ', '.join(f"{op}{v}" for op, v in self._constraints)


def check_version(
    current: Union[str, Version],
    required: str,
) -> Tuple[bool, Optional[str]]:
    """Check if current version meets requirement.

    Args:
        current: Current version.
        required: Required version/constraint.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        constraint = VersionConstraint(required)
        if not constraint.matches(current):
            return False, f"Version {current} does not satisfy {required}"
        return True, None
    except Exception as e:
        return False, f"Invalid version constraint: {e}"