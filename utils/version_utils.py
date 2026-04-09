"""Version and compatibility utilities.

Provides version parsing, comparison, and feature
flag management for cross-version compatibility.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union


@dataclass
class Version:
    """Semantic version representation.

    Example:
        v1 = Version.parse("1.2.3")
        v2 = Version.parse("1.3.0")
        if v1 < v2:
            print("v1 is older")
    """
    major: int
    minor: int
    patch: int
    prerelease: str = ""
    build: str = ""

    @classmethod
    def parse(cls, version: str) -> "Version":
        """Parse version string.

        Args:
            version: Version string like "1.2.3-beta.1".

        Returns:
            Version object.
        """
        pattern = r"(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?(?:\+([a-zA-Z0-9.]+))?"
        match = re.match(pattern, version)

        if not match:
            return cls(0, 0, 0)

        major, minor, patch, prerelease, build = match.groups()
        return cls(
            major=int(major),
            minor=int(minor),
            patch=int(patch),
            prerelease=prerelease or "",
            build=build or "",
        )

    def __str__(self) -> str:
        """String representation."""
        v = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            v += f"-{self.prerelease}"
        if self.build:
            v += f"+{self.build}"
        return v

    def __lt__(self, other: "Version") -> bool:
        """Less than comparison."""
        return self._compare(other) < 0

    def __le__(self, other: "Version") -> bool:
        """Less than or equal comparison."""
        return self._compare(other) <= 0

    def __gt__(self, other: "Version") -> bool:
        """Greater than comparison."""
        return self._compare(other) > 0

    def __ge__(self, other: "Version") -> bool:
        """Greater than or equal comparison."""
        return self._compare(other) >= 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return False
        return self._compare(other) == 0

    def _compare(self, other: "Version") -> int:
        """Compare versions returning -1, 0, or 1."""
        self_tuple = (self.major, self.minor, self.patch)
        other_tuple = (other.major, other.minor, other.patch)

        if self_tuple != other_tuple:
            return -1 if self_tuple < other_tuple else 1

        if self.prerelease and not other.prerelease:
            return -1
        if not self.prerelease and other.prerelease:
            return 1
        if self.prerelease and other.prerelease:
            return -1 if self.prerelease < other.prerelease else 1

        return 0

    def is_compatible_with(self, min_version: "Version", max_version: Optional["Version"] = None) -> bool:
        """Check if version is within range.

        Args:
            min_version: Minimum required version.
            max_version: Maximum allowed version (exclusive).

        Returns:
            True if compatible.
        """
        if self < min_version:
            return False
        if max_version and self >= max_version:
            return False
        return True


class FeatureFlag:
    """Feature flag with version-based enablement.

    Example:
        flags = FeatureFlagRegistry()
        flags.register("dark_mode", min_version="2.0.0")
        flags.register("beta_feature", min_version="2.5.0", max_version="3.0.0")

        if flags.is_enabled("dark_mode", current_version="2.1.0"):
            show_dark_mode()
    """

    def __init__(
        self,
        name: str,
        min_version: Optional[Union[str, Version]] = None,
        max_version: Optional[Union[str, Version]] = None,
        default_enabled: bool = False,
    ) -> None:
        self.name = name
        self.min_version = Version.parse(str(min_version)) if min_version else None
        self.max_version = Version.parse(str(max_version)) if max_version else None
        self.default_enabled = default_enabled
        self._enabled: Optional[bool] = None

    def is_enabled(self, current_version: Union[str, Version]) -> bool:
        """Check if feature is enabled for version.

        Args:
            current_version: Current application version.

        Returns:
            True if feature should be enabled.
        """
        if self._enabled is not None:
            return self._enabled

        v = Version.parse(str(current_version))

        if self.min_version and v < self.min_version:
            return self.default_enabled

        if self.max_version and v >= self.max_version:
            return self.default_enabled

        return not self.default_enabled

    def enable(self) -> None:
        """Force enable feature."""
        self._enabled = True

    def disable(self) -> None:
        """Force disable feature."""
        self._enabled = False

    def reset(self) -> None:
        """Reset to version-based determination."""
        self._enabled = None


class FeatureFlagRegistry:
    """Registry for managing multiple feature flags.

    Example:
        registry = FeatureFlagRegistry()
        registry.register("new_ui", min_version="1.5.0")
        registry.register("beta_api", min_version="2.0.0", default_enabled=False)

        if registry.is_enabled("new_ui"):
            use_new_ui()
    """

    def __init__(self) -> None:
        self._flags: dict[str, FeatureFlag] = {}

    def register(
        self,
        name: str,
        **kwargs: Any,
    ) -> FeatureFlag:
        """Register a feature flag.

        Args:
            name: Feature name.
            **kwargs: Passed to FeatureFlag constructor.

        Returns:
            Created or existing flag.
        """
        if name in self._flags:
            flag = self._flags[name]
            if kwargs:
                self._flags[name] = FeatureFlag(name, **kwargs)
            return self._flags[name]

        flag = FeatureFlag(name, **kwargs)
        self._flags[name] = flag
        return flag

    def unregister(self, name: str) -> bool:
        """Remove a feature flag.

        Args:
            name: Feature name.

        Returns:
            True if flag existed.
        """
        if name in self._flags:
            del self._flags[name]
            return True
        return False

    def is_enabled(
        self,
        name: str,
        current_version: Optional[Union[str, Version]] = None,
    ) -> bool:
        """Check if feature is enabled.

        Args:
            name: Feature name.
            current_version: Version to check against.

        Returns:
            True if enabled or default.
        """
        if name not in self._flags:
            return False

        flag = self._flags[name]

        if current_version is None:
            return flag.default_enabled

        return flag.is_enabled(current_version)

    def enable(self, name: str) -> None:
        """Force enable a feature."""
        if name in self._flags:
            self._flags[name].enable()

    def disable(self, name: str) -> None:
        """Force disable a feature."""
        if name in self._flags:
            self._flags[name].disable()

    def list_flags(self) -> List[str]:
        """List all registered feature names."""
        return list(self._flags.keys())

    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get flag object."""
        return self._flags.get(name)


def check_compatibility(
    required_version: str,
    current_version: str,
) -> Tuple[bool, str]:
    """Check version compatibility.

    Args:
        required_version: Minimum required version.
        current_version: Current version.

    Returns:
        Tuple of (is_compatible, message).
    """
    required = Version.parse(required_version)
    current = Version.parse(current_version)

    if current < required:
        return False, f"Version {current} is below required {required}"

    return True, f"Compatible with {current}"


def parse_version_tuple(version: str) -> Tuple[int, int, int]:
    """Parse version string to tuple for simple comparison.

    Args:
        version: Version string.

    Returns:
        Tuple of (major, minor, patch).
    """
    v = Version.parse(version)
    return (v.major, v.minor, v.patch)


from typing import Any
