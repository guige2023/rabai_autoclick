"""API versioning helper action module.

Provides URL path and header-based API versioning utilities.
Supports version negotiation, deprecation warnings, and migration helpers.
"""

from __future__ import annotations

import re
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from packaging import version as pkg_version

logger = logging.getLogger(__name__)


@dataclass
class APIVersion:
    """Represents an API version."""
    major: int
    minor: int = 0
    patch: int = 0

    def __str__(self) -> str:
        """Return version string (e.g., 'v1.2.3')."""
        return f"v{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: "APIVersion") -> bool:
        """Compare versions for sorting."""
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, APIVersion):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    @classmethod
    def parse(cls, v: str) -> "APIVersion":
        """Parse version from string like 'v1.2', 'v2.0.1', '1.2'."""
        v = v.strip().lstrip("v")
        parts = v.split(".")
        major = int(parts[0]) if parts else 0
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
        return cls(major=major, minor=minor, patch=patch)

    @property
    def is_major(self) -> bool:
        """Check if this is a major version bump."""
        return self.minor == 0 and self.patch == 0

    def matches(self, constraint: str) -> bool:
        """Check if version matches a constraint like '>=1.2.0', '==2.0', '<3.0.0'."""
        match = re.match(r"([><=!]+)([\d.]+)", constraint)
        if not match:
            return False
        op, ver_str = match.groups()
        other = APIVersion.parse(ver_str)
        if op == ">":
            return self > other
        elif op == ">=":
            return self >= other
        elif op == "<":
            return self < other
        elif op == "<=":
            return self <= other
        elif op == "==":
            return self == other
        elif op == "!=":
            return self != other
        return False


@dataclass
class VersionDeprecation:
    """Represents a version deprecation notice."""
    version: APIVersion
    sunset_date: Optional[str] = None
    migration_guide: Optional[str] = None
    replacement_version: Optional[APIVersion] = None
    is_critical: bool = False


class APIVersionAction:
    """API versioning utilities.

    Supports path-based (/v1/, /v2/) and header-based (Accept: application/vnd.api+json;version=2)
    versioning strategies. Provides version negotiation and deprecation tracking.

    Example:
        versioner = APIVersionAction()
        versioner.add_version("v1", deprecated=VersionDeprecation(version=APIVersion.parse("v1"), sunset_date="2025-01-01"))
        latest = versioner.get_latest_version()
        headers = versioner.build_version_headers(APIVersion.parse("v2"))
    """

    def __init__(
        self,
        current_version: Optional[str] = None,
        supported_versions: Optional[List[str]] = None,
        default_version: Optional[str] = None,
    ) -> None:
        """Initialize API versioner.

        Args:
            current_version: Current API version string.
            supported_versions: List of supported version strings.
            default_version: Default version for negotiation.
        """
        self.current = APIVersion.parse(current_version) if current_version else None
        self._supported: Dict[str, APIVersion] = {}
        self._deprecated: Dict[str, VersionDeprecation] = {}
        self._default = APIVersion.parse(default_version) if default_version else None

        if supported_versions:
            for v in supported_versions:
                self.add_version(v)

    def add_version(self, version_str: str, deprecation: Optional[VersionDeprecation] = None) -> None:
        """Register a supported API version.

        Args:
            version_str: Version string like 'v1.2'.
            deprecation: Optional deprecation info.
        """
        ver = APIVersion.parse(version_str)
        self._supported[version_str] = ver
        if deprecation:
            self._deprecated[version_str] = deprecation
        if self._default is None:
            self._default = ver

    def get_supported_versions(self) -> List[APIVersion]:
        """Get list of supported versions sorted newest first."""
        versions = sorted(self._supported.values(), reverse=True)
        return versions

    def get_latest_version(self) -> Optional[APIVersion]:
        """Get the latest non-deprecated version."""
        supported = [v for v in self._supported.values() if v not in self._deprecated]
        return max(supported) if supported else None

    def is_supported(self, version_str: str) -> bool:
        """Check if a version is supported."""
        return version_str in self._supported

    def is_deprecated(self, version_str: str) -> bool:
        """Check if a version is deprecated."""
        return version_str in self._deprecated

    def get_deprecation(self, version_str: str) -> Optional[VersionDeprecation]:
        """Get deprecation info for a version."""
        return self._deprecated.get(version_str)

    def negotiate_version(
        self,
        requested: Optional[str] = None,
        accept_header: Optional[str] = None,
        default: bool = True,
    ) -> Optional[APIVersion]:
        """Negotiate the best available version.

        Args:
            requested: Explicitly requested version string.
            accept_header: Accept header value (application/vnd.api+json;version=2).
            default: Fall back to default version if no match.

        Returns:
            Negotiated APIVersion or None.
        """
        if requested:
            ver = self._supported.get(requested)
            if ver:
                return ver
            logger.warning("Requested version %s not supported", requested)

        if accept_header:
            ver = self._parse_accept_header(accept_header)
            if ver and ver in self._supported.values():
                return ver

        return self._default if default else None

    def build_version_headers(
        self,
        ver: APIVersion,
        content_type: str = "application/json",
    ) -> Dict[str, str]:
        """Build request/response headers for a version.

        Args:
            ver: API version.
            content_type: Base content type.

        Returns:
            Dict of headers with Content-Type and API-Version.
        """
        base_type = content_type.split(";")[0]
        versioned_type = f"{base_type};version={ver.major}"
        headers = {
            "Content-Type": versioned_type,
            "Accept": versioned_type,
            "API-Version": str(ver),
        }

        if str(ver) in self._deprecated:
            dep = self._deprecated[str(ver)]
            if dep.sunset_date:
                headers["Sunset"] = dep.sunset_date
            if dep.migration_guide:
                headers["Deprecation"] = f'{str(ver)}; rel="deprecation"; type="{dep.migration_guide}"'

        return headers

    def build_version_url(self, base_url: str, ver: APIVersion, strip_version: bool = False) -> str:
        """Build a versioned URL.

        Args:
            base_url: Base URL path.
            ver: API version to use.
            strip_version: Remove version prefix instead of adding.

        Returns:
            Versioned URL string.
        """
        base_url = base_url.rstrip("/")
        if strip_version:
            pattern = r"/v\d+(\.\d+)?"
            return re.sub(pattern, "", base_url)
        return f"{base_url}/{str(ver)}"

    def check_migration(
        self,
        from_version: str,
        to_version: str,
    ) -> List[str]:
        """Generate migration steps between versions.

        Args:
            from_version: Current version string.
            to_version: Target version string.

        Returns:
            List of migration step descriptions.
        """
        steps = []
        from_ver = APIVersion.parse(from_version)
        to_ver = APIVersion.parse(to_version)

        if to_ver.major > from_ver.major:
            steps.append(f"Migration from v{from_ver.major} to v{to_ver.major} is a major version change.")
            steps.append("Review the full migration guide for breaking changes.")

        if to_ver.minor > from_ver.minor:
            steps.append(f"Minor version update: {from_ver.minor} -> {to_ver.minor}.")
            steps.append("New optional fields may be available.")

        if to_ver.patch > from_ver.patch:
            steps.append(f"Patch update: {from_ver.patch} -> {to_ver.patch}.")
            steps.append("This should be a non-breaking change.")

        return steps

    def _parse_accept_header(self, header: str) -> Optional[APIVersion]:
        """Parse version from Accept header.

        Supports application/vnd.api+json;version=2 format.
        """
        match = re.search(r'version=([v\d.]+)', header)
        if match:
            return APIVersion.parse(match.group(1))
        return None
