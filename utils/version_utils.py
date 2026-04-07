"""Version utilities for RabAI AutoClick.

Provides:
- Version parsing
- Version comparison
- Version constraints
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Version:
    """Semantic version."""

    major: int
    minor: int
    patch: int
    prerelease: Optional[str] = None
    build: Optional[str] = None

    @classmethod
    def parse(cls, version_str: str) -> Version:
        """Parse version string.

        Args:
            version_str: Version string like "1.2.3" or "1.2.3-alpha".

        Returns:
            Version object.
        """
        pattern = r"(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?"
        match = re.match(pattern, version_str)
        if not match:
            raise ValueError(f"Invalid version string: {version_str}")

        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
            prerelease=match.group(4),
            build=match.group(5),
        )

    def __str__(self) -> str:
        result = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            result += f"-{self.prerelease}"
        if self.build:
            result += f"+{self.build}"
        return result

    def __lt__(self, other: Version) -> bool:
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.patch != other.patch:
            return self.patch < other.patch
        if self.prerelease != other.prerelease:
            if self.prerelease is None:
                return False
            if other.prerelease is None:
                return True
            return self.prerelease < other.prerelease
        return False

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return False
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
        )

    def is_compatible(self, other: Version, strict: bool = False) -> bool:
        """Check if versions are compatible."""
        if strict:
            return self == other
        return self.major == other.major and self.minor >= other.minor
