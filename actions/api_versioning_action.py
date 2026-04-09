"""API Versioning Action Module.

Provides API versioning utilities: version negotiation, deprecation handling,
migration helpers, and changelog management.

Example:
    result = execute(context, {"action": "negotiate_version", "versions": ["v1", "v2"]})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class APIVersion:
    """API version definition."""
    
    version: str
    release_date: datetime
    sunset_date: Optional[datetime] = None
    deprecation_warning: Optional[str] = None
    breaking_changes: list[str] = field(default_factory=list)
    features: list[str] = field(default_factory=list)
    
    def is_deprecated(self) -> bool:
        """Check if version is deprecated."""
        if self.sunset_date is None:
            return False
        return datetime.now() > self.sunset_date
    
    def is_sunset(self) -> bool:
        """Check if version has passed sunset date."""
        if self.sunset_date is None:
            return False
        return datetime.now() > self.sunset_date
    
    def days_until_sunset(self) -> Optional[int]:
        """Get days until sunset date."""
        if self.sunset_date is None:
            return None
        delta = self.sunset_date - datetime.now()
        return max(0, delta.days)


class VersionNegotiator:
    """Handles API version negotiation from client requests."""
    
    HEADER_NAME = "Accept"
    HEADER_VALUE_PREFIX = "application/vnd.api+json;version="
    
    def __init__(self, supported_versions: list[str]) -> None:
        """Initialize version negotiator.
        
        Args:
            supported_versions: List of supported version strings (e.g., ["v1", "v2"])
        """
        self.supported_versions = supported_versions
        self.default_version = supported_versions[-1] if supported_versions else "v1"
    
    def negotiate(self, client_version: Optional[str]) -> str:
        """Negotiate best matching version.
        
        Args:
            client_version: Version requested by client
            
        Returns:
            Agreed upon version string
        """
        if client_version is None:
            return self.default_version
        
        clean_version = client_version.strip().lower()
        
        if clean_version in self.supported_versions:
            return clean_version
        
        for supported in self.supported_versions:
            if self._versions_compatible(clean_version, supported):
                return supported
        
        return self.default_version
    
    def negotiate_from_header(self, accept_header: str) -> str:
        """Negotiate version from Accept header.
        
        Args:
            accept_header: Accept header value
            
        Returns:
            Agreed upon version string
        """
        if not accept_header:
            return self.default_version
        
        if self.HEADER_VALUE_PREFIX in accept_header:
            parts = accept_header.split(self.HEADER_VALUE_PREFIX)
            if len(parts) > 1:
                version = parts[1].split(";")[0].strip()
                return self.negotiate(version)
        
        return self.default_version
    
    @staticmethod
    def _versions_compatible(requested: str, supported: str) -> bool:
        """Check if two versions are compatible."""
        req_parts = requested.lstrip("v").split(".")
        sup_parts = supported.lstrip("v").split(".")
        
        if not req_parts or not sup_parts:
            return False
        
        return req_parts[0] == sup_parts[0]


class DeprecationManager:
    """Manages API version deprecation lifecycle."""
    
    def __init__(self) -> None:
        """Initialize deprecation manager."""
        self._versions: dict[str, APIVersion] = {}
        self._migration_guides: dict[str, str] = {}
    
    def register_version(
        self,
        version: str,
        release_date: datetime,
        sunset_date: Optional[datetime] = None,
        deprecation_warning: Optional[str] = None,
        breaking_changes: Optional[list[str]] = None,
        features: Optional[list[str]] = None,
    ) -> None:
        """Register an API version.
        
        Args:
            version: Version string
            release_date: Release date
            sunset_date: Planned deprecation date
            deprecation_warning: Warning message
            breaking_changes: List of breaking changes
            features: List of new features
        """
        self._versions[version] = APIVersion(
            version=version,
            release_date=release_date,
            sunset_date=sunset_date,
            deprecation_warning=deprecation_warning,
            breaking_changes=breaking_changes or [],
            features=features or [],
        )
    
    def get_version_info(self, version: str) -> Optional[dict[str, Any]]:
        """Get version information."""
        api_version = self._versions.get(version)
        if api_version is None:
            return None
        
        return {
            "version": api_version.version,
            "release_date": api_version.release_date.isoformat(),
            "sunset_date": api_version.sunset_date.isoformat() if api_version.sunset_date else None,
            "is_deprecated": api_version.is_deprecated(),
            "is_sunset": api_version.is_sunset(),
            "days_until_sunset": api_version.days_until_sunset(),
            "deprecation_warning": api_version.deprecation_warning,
            "breaking_changes": api_version.breaking_changes,
            "features": api_version.features,
        }
    
    def get_deprecated_versions(self) -> list[str]:
        """Get list of deprecated versions."""
        return [
            v for v, api_ver in self._versions.items()
            if api_ver.is_deprecated() and not api_ver.is_sunset()
        ]
    
    def get_sunset_versions(self) -> list[str]:
        """Get list of sunset versions."""
        return [
            v for v, api_ver in self._versions.items()
            if api_ver.is_sunset()
        ]
    
    def register_migration_guide(self, from_version: str, to_version: str, guide: str) -> None:
        """Register migration guide between versions.
        
        Args:
            from_version: Source version
            to_version: Target version
            guide: Migration guide text
        """
        key = f"{from_version}->{to_version}"
        self._migration_guides[key] = guide
    
    def get_migration_guide(self, from_version: str, to_version: str) -> Optional[str]:
        """Get migration guide between versions."""
        key = f"{from_version}->{to_version}"
        return self._migration_guides.get(key)


class ChangelogManager:
    """Manages API changelog entries."""
    
    def __init__(self) -> None:
        """Initialize changelog manager."""
        self._entries: list[dict[str, Any]] = []
    
    def add_entry(
        self,
        version: str,
        date: datetime,
        entry_type: str,
        description: str,
        breaking: bool = False,
        deprecated: Optional[str] = None,
    ) -> None:
        """Add changelog entry.
        
        Args:
            version: Version string
            date: Entry date
            entry_type: Type (added, changed, deprecated, removed, fixed)
            description: Change description
            breaking: Is breaking change
            deprecated: What this deprecates
        """
        self._entries.append({
            "version": version,
            "date": date.isoformat(),
            "type": entry_type,
            "description": description,
            "breaking": breaking,
            "deprecated": deprecated,
        })
    
    def get_changelog(self, version: Optional[str] = None) -> list[dict[str, Any]]:
        """Get changelog entries.
        
        Args:
            version: Optional version filter
            
        Returns:
            List of changelog entries
        """
        if version is None:
            return self._entries.copy()
        
        return [e for e in self._entries if e["version"] == version]
    
    def get_latest_changes(self, count: int = 10) -> list[dict[str, Any]]:
        """Get latest changelog entries."""
        sorted_entries = sorted(
            self._entries,
            key=lambda x: (x["version"], x["date"]),
            reverse=True,
        )
        return sorted_entries[:count]


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute API versioning action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "negotiate":
        supported = params.get("supported_versions", ["v1", "v2"])
        negotiator = VersionNegotiator(supported)
        client_version = params.get("client_version")
        agreed = negotiator.negotiate(client_version)
        result["data"] = {"agreed_version": agreed}
    
    elif action == "negotiate_header":
        supported = params.get("supported_versions", ["v1", "v2"])
        negotiator = VersionNegotiator(supported)
        header = params.get("accept_header", "")
        agreed = negotiator.negotiate_from_header(header)
        result["data"] = {"agreed_version": agreed}
    
    elif action == "register_version":
        manager = DeprecationManager()
        manager.register_version(
            version=params.get("version", "v1"),
            release_date=datetime.now(),
            sunset_date=None,
        )
        result["data"] = {"registered": True}
    
    elif action == "version_info":
        manager = DeprecationManager()
        info = manager.get_version_info(params.get("version", ""))
        result["data"] = info or {"error": "Version not found"}
    
    elif action == "deprecated_list":
        manager = DeprecationManager()
        result["data"] = {"deprecated": manager.get_deprecated_versions()}
    
    elif action == "sunset_list":
        manager = DeprecationManager()
        result["data"] = {"sunset": manager.get_sunset_versions()}
    
    elif action == "add_changelog":
        manager = ChangelogManager()
        manager.add_entry(
            version=params.get("version", "v1"),
            date=datetime.now(),
            entry_type=params.get("type", "changed"),
            description=params.get("description", ""),
            breaking=params.get("breaking", False),
        )
        result["data"] = {"added": True}
    
    elif action == "get_changelog":
        manager = ChangelogManager()
        entries = manager.get_changelog(params.get("version"))
        result["data"] = {"entries": entries}
    
    elif action == "migration_guide":
        manager = DeprecationManager()
        guide = manager.get_migration_guide(
            params.get("from_version", ""),
            params.get("to_version", ""),
        )
        result["data"] = {"guide": guide or "No migration guide available"}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
