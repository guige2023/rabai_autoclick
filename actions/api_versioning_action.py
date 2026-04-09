"""
API Versioning and Migration Module.

Provides API versioning strategies, migration handling,
deprecation management, and version compatibility checking
for maintaining backwards compatibility.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, TypeVar, Union, Pattern
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import re
import logging
from functools import cmp_to_key

logger = logging.getLogger(__name__)


class VersionFormat(Enum):
    """API version string formats."""
    SEMVER = auto()      # 1.2.3
    MAJOR_MINOR = auto() # 1.2
    DATE = auto()        # 2024-01-15
    HEADER = auto()      # v1, v2
    QUERY = auto()       # ?version=1.2


@dataclass
class APIVersion:
    """Represents an API version."""
    major: int
    minor: int = 0
    patch: int = 0
    raw: str = ""
    
    def __str__(self) -> str:
        if self.patch > 0:
            return f"{self.major}.{self.minor}.{self.patch}"
        elif self.minor > 0:
            return f"{self.major}.{self.minor}"
        return f"{self.major}"
    
    def __lt__(self, other: "APIVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, APIVersion):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch))
    
    @property
    def is_major(self) -> bool:
        return self.minor == 0 and self.patch == 0
    
    def is_compatible_with(self, other: "APIVersion") -> bool:
        """Check if versions are API compatible."""
        return self.major == other.major


@dataclass
class EndpointVersion:
    """Version information for an endpoint."""
    path: str
    method: str
    deprecated_in: Optional[APIVersion] = None
    removed_in: Optional[APIVersion] = None
    introduced_in: APIVersion = field(default_factory=lambda: APIVersion(1))
    sunset_date: Optional[datetime] = None
    migration_guide: Optional[str] = None
    
    def is_deprecated(self, current_version: APIVersion) -> bool:
        """Check if endpoint is deprecated."""
        if self.deprecated_in is None:
            return False
        return current_version >= self.deprecated_in
    
    def is_removed(self, current_version: APIVersion) -> bool:
        """Check if endpoint is removed."""
        if self.removed_in is None:
            return False
        return current_version >= self.removed_in


@dataclass
class DeprecationInfo:
    """Deprecation information for API elements."""
    element: str
    deprecated_in: APIVersion
    sunset_date: Optional[datetime] = None
    alternative: Optional[str] = None
    migration_steps: List[str] = field(default_factory=list)
    severity: str = "warning"  # info, warning, error


class VersionParser:
    """Parses API version strings."""
    
    SEMVER_PATTERN: Pattern = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
    MAJOR_MINOR_PATTERN: Pattern = re.compile(r"^(\d+)\.(\d+)$")
    MAJOR_ONLY_PATTERN: Pattern = re.compile(r"^(\d+)$")
    DATE_PATTERN: Pattern = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
    HEADER_PATTERN: Pattern = re.compile(r"^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?$")
    
    @classmethod
    def parse(
        cls,
        version_str: str,
        format_hint: Optional[VersionFormat] = None
    ) -> Optional[APIVersion]:
        """
        Parse version string to APIVersion.
        
        Args:
            version_str: Version string
            format_hint: Optional format hint
            
        Returns:
            APIVersion or None if invalid
        """
        version_str = version_str.strip()
        
        # Try format hint first
        if format_hint == VersionFormat.SEMVER:
            return cls._parse_semver(version_str)
        elif format_hint == VersionFormat.DATE:
            return cls._parse_date(version_str)
        
        # Try each format
        for fmt in [VersionFormat.SEMVER, VersionFormat.MAJOR_MINOR, VersionFormat.HEADER]:
            result = cls._try_parse(version_str, fmt)
            if result:
                return result
        
        return None
    
    @classmethod
    def _try_parse(cls, version_str: str, format_hint: VersionFormat) -> Optional[APIVersion]:
        """Try parsing with specific format."""
        try:
            if format_hint == VersionFormat.SEMVER:
                return cls._parse_semver(version_str)
            elif format_hint == VersionFormat.MAJOR_MINOR:
                return cls._parse_major_minor(version_str)
            elif format_hint == VersionFormat.HEADER:
                return cls._parse_header(version_str)
            elif format_hint == VersionFormat.DATE:
                return cls._parse_date(version_str)
        except Exception:
            return None
        return None
    
    @classmethod
    def _parse_semver(cls, version_str: str) -> Optional[APIVersion]:
        match = cls.SEMVER_PATTERN.match(version_str)
        if match:
            return APIVersion(
                major=int(match.group(1)),
                minor=int(match.group(2)),
                patch=int(match.group(3)),
                raw=version_str
            )
        return None
    
    @classmethod
    def _parse_major_minor(cls, version_str: str) -> Optional[APIVersion]:
        match = cls.MAJOR_MINOR_PATTERN.match(version_str)
        if match:
            return APIVersion(
                major=int(match.group(1)),
                minor=int(match.group(2)),
                raw=version_str
            )
        return None
    
    @classmethod
    def _parse_header(cls, version_str: str) -> Optional[APIVersion]:
        version_str = version_str.lstrip("v")
        match = cls.MAJOR_ONLY_PATTERN.match(version_str)
        if match:
            return APIVersion(
                major=int(match.group(1)),
                raw=version_str
            )
        return None
    
    @classmethod
    def _parse_date(cls, version_str: str) -> Optional[APIVersion]:
        match = cls.DATE_PATTERN.match(version_str)
        if match:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            # Convert to major.minor representation
            return APIVersion(
                major=year,
                minor=month * 100 + day,
                raw=version_str
            )
        return None


class VersionRouter:
    """Routes requests to appropriate API version handlers."""
    
    def __init__(self, default_version: APIVersion) -> None:
        self.default_version = default_version
        self._version_handlers: Dict[APIVersion, Callable] = {}
        self._endpoints: List[EndpointVersion] = []
    
    def register_handler(
        self,
        version: APIVersion,
        handler: Callable
    ) -> "VersionRouter":
        """Register handler for a version."""
        self._version_handlers[version] = handler
        return self
    
    def register_endpoint(self, endpoint: EndpointVersion) -> "VersionRouter":
        """Register endpoint version information."""
        self._endpoints.append(endpoint)
        return self
    
    def route(
        self,
        path: str,
        method: str,
        version: APIVersion,
        request: Any
    ) -> Any:
        """
        Route request to appropriate handler.
        
        Args:
            path: Request path
            method: HTTP method
            version: API version
            request: Request data
            
        Returns:
            Response from handler
        """
        # Find handler
        handler = self._find_handler(version)
        if not handler:
            raise ValueError(f"No handler for version {version}")
        
        # Check deprecation
        deprecation_info = self.get_deprecation_info(path, method, version)
        if deprecation_info:
            logger.warning(
                f"Using deprecated endpoint: {deprecation_info.element} "
                f"(deprecated in {deprecation_info.deprecated_in})"
            )
        
        # Call handler
        return handler(request)
    
    def _find_handler(self, version: APIVersion) -> Optional[Callable]:
        """Find best matching handler for version."""
        # Exact match
        if version in self._version_handlers:
            return self._version_handlers[version]
        
        # Find latest compatible version
        compatible_versions = [
            v for v in self._version_handlers.keys()
            if v.is_compatible_with(version)
        ]
        
        if compatible_versions:
            latest = max(compatible_versions)
            if latest <= version:
                return self._version_handlers[latest]
        
        return self._version_handlers.get(self.default_version)
    
    def get_deprecation_info(
        self,
        path: str,
        method: str,
        version: APIVersion
    ) -> Optional[DeprecationInfo]:
        """Get deprecation info for endpoint."""
        for endpoint in self._endpoints:
            if endpoint.path == path and endpoint.method == method:
                if endpoint.is_deprecated(version):
                    return DeprecationInfo(
                        element=f"{method} {path}",
                        deprecated_in=endpoint.deprecated_in,
                        sunset_date=endpoint.sunset_date,
                        alternative=endpoint.migration_guide
                    )
        return None


class APIVersionManager:
    """
    Manages API versioning lifecycle.
    
    Tracks versions, deprecations, migrations, and provides
    version compatibility checking.
    """
    
    def __init__(self) -> None:
        self.supported_versions: List[APIVersion] = []
        self.deprecated_versions: List[APIVersion] = []
        self._version_registry: Dict[str, APIVersion] = {}
        self._deprecation_notices: List[DeprecationInfo] = []
    
    def register_version(
        self,
        version: APIVersion,
        release_date: Optional[datetime] = None,
        support_end_date: Optional[datetime] = None
    ) -> "APIVersionManager":
        """Register a supported API version."""
        self.supported_versions.append(version)
        self.supported_versions.sort()
        self._version_registry[str(version)] = version
        
        if support_end_date and support_end_date < datetime.now():
            self.deprecated_versions.append(version)
        
        return self
    
    def deprecate_version(
        self,
        version: APIVersion,
        sunset_date: datetime,
        alternative: Optional[str] = None,
        migration_steps: Optional[List[str]] = None
    ) -> "APIVersionManager":
        """Mark a version as deprecated."""
        if version not in self.deprecated_versions:
            self.deprecated_versions.append(version)
        
        notice = DeprecationInfo(
            element=f"API v{version}",
            deprecated_in=version,
            sunset_date=sunset_date,
            alternative=alternative,
            migration_steps=migration_steps or [],
            severity="warning"
        )
        self._deprecation_notices.append(notice)
        
        return self
    
    def get_active_deprecations(
        self,
        current_version: APIVersion
    ) -> List[DeprecationInfo]:
        """Get all active deprecations for a version."""
        return [
            d for d in self._deprecation_notices
            if d.deprecated_in < current_version
        ]
    
    def is_version_supported(self, version: APIVersion) -> bool:
        """Check if version is still supported."""
        if version in self.deprecated_versions:
            return False
        return version in self.supported_versions
    
    def get_latest_version(self) -> Optional[APIVersion]:
        """Get the latest supported version."""
        active = [v for v in self.supported_versions if v not in self.deprecated_versions]
        return max(active) if active else None
    
    def get_migration_path(
        self,
        from_version: APIVersion,
        to_version: APIVersion
    ) -> List[APIVersion]:
        """Get migration path between versions."""
        if from_version > to_version:
            # Downgrade - no standard path
            return []
        
        path = []
        current = from_version
        
        while current < to_version:
            if current.minor == 0:
                # Major version bump
                current = APIVersion(current.major + 1)
            else:
                # Minor version bump
                current = APIVersion(current.major, current.minor + 1)
            
            if current in self.supported_versions:
                path.append(current)
        
        return path
    
    def generate_openapi_versions(
        self,
        base_spec: Dict[str, Any],
        versions: List[APIVersion]
    ) -> Dict[str, Dict[str, Any]]:
        """Generate OpenAPI specs for multiple versions."""
        specs = {}
        
        for version in versions:
            spec = dict(base_spec)
            spec["info"]["version"] = str(version)
            spec["info"]["title"] = f"{base_spec.get('title', 'API')} v{version}"
            
            # Add deprecation headers
            if version in self.deprecated_versions:
                spec["info"]["description"] = (
                    f"⚠️ **DEPRECATED** - This version will be removed. "
                    f"Please migrate to a newer version."
                )
            
            specs[str(version)] = spec
        
        return specs


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== API Versioning Demo ===")
    
    # Parse versions
    versions = ["1.0.0", "1.2", "v2", "2024-03-15"]
    
    print("\nParsed versions:")
    for v_str in versions:
        version = VersionParser.parse(v_str)
        print(f"  '{v_str}' -> {version}")
    
    # Version manager
    manager = APIVersionManager()
    
    manager.register_version(APIVersion(1, 0), support_end_date=datetime.now() + timedelta(days=30))
    manager.register_version(APIVersion(1, 1))
    manager.register_version(APIVersion(2, 0))
    
    manager.deprecate_version(
        APIVersion(1, 0),
        sunset_date=datetime.now() + timedelta(days=30),
        alternative="v1.1 or v2.0",
        migration_steps=["Update endpoint URLs", "Refresh authentication token"]
    )
    
    print("\nVersion Support:")
    print(f"  Latest: {manager.get_latest_version()}")
    print(f"  v1.0 supported: {manager.is_version_supported(APIVersion(1, 0))}")
    print(f"  v1.1 supported: {manager.is_version_supported(APIVersion(1, 1))}")
    
    # Migration path
    path = manager.get_migration_path(APIVersion(1, 0), APIVersion(2, 0))
    print(f"\nMigration v1.0 -> v2.0: {[str(v) for v in path]}")
    
    # Endpoint versioning
    endpoint = EndpointVersion(
        path="/users",
        method="GET",
        introduced_in=APIVersion(1, 0),
        deprecated_in=APIVersion(2, 0),
        removed_in=APIVersion(3, 0)
    )
    
    print(f"\nEndpoint /users:")
    print(f"  Deprecated in v2: {endpoint.is_deprecated(APIVersion(2, 0))}")
    print(f"  Removed in v2: {endpoint.is_removed(APIVersion(2, 0))}")
