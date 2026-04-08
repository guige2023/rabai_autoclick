"""
API Versioning Action Module

Provides API versioning, deprecation management, and version negotiation.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import re


class VersionScheme(Enum):
    """API versioning scheme."""
    URI_PATH = "uri_path"  # /v1/resource
    HEADER = "header"  # Accept: application/vnd.api+json; version=1
    QUERY_PARAM = "query_param"  # ?version=1
    CONTENT_NEGOTIATION = "content_negotiation"


@dataclass
class APIVersion:
    """API version definition."""
    version: str
    status: str  # active, deprecated, sunset, retired
    release_date: datetime
    sunset_date: Optional[datetime] = None
    retirement_date: Optional[datetime] = None
    migration_guide: Optional[str] = None
    breaking_changes: list[str] = field(default_factory=list)


@dataclass
class VersionedEndpoint:
    """A versioned API endpoint."""
    path: str
    method: str
    versions: dict[str, Callable]  # version -> handler
    default_version: Optional[str] = None
    deprecated: bool = False
    sunset_date: Optional[datetime] = None


@dataclass
class VersionNegotiationResult:
    """Result of version negotiation."""
    selected_version: str
    content_type: Optional[str] = None
    warning: Optional[str] = None
    deprecation_notice: Optional[str] = None


@dataclass
class VersionMigrationStep:
    """A single migration step."""
    from_version: str
    to_version: str
    step_order: int
    description: str
    transform: Optional[Callable[[dict], dict]] = None
    rollback: Optional[Callable[[dict], dict]] = None


class ApiVersioningAction:
    """Main API versioning action handler."""
    
    def __init__(self, scheme: VersionScheme = VersionScheme.URI_PATH):
        self.scheme = scheme
        self._versions: dict[str, APIVersion] = {}
        self._endpoints: dict[str, VersionedEndpoint] = {}
        self._migrations: list[VersionMigrationStep] = []
        self._current_stable = "v1"
    
    def register_version(
        self,
        version: str,
        status: str = "active",
        release_date: Optional[datetime] = None,
        sunset_date: Optional[datetime] = None,
        retirement_date: Optional[datetime] = None,
        migration_guide: Optional[str] = None,
        breaking_changes: Optional[list[str]] = None
    ) -> "ApiVersioningAction":
        """Register an API version."""
        self._versions[version] = APIVersion(
            version=version,
            status=status,
            release_date=release_date or datetime.now(),
            sunset_date=sunset_date,
            retirement_date=retirement_date,
            migration_guide=migration_guide,
            breaking_changes=breaking_changes or []
        )
        return self
    
    def register_endpoint(
        self,
        path: str,
        method: str,
        handler: Callable,
        version: str,
        set_as_default: bool = False
    ) -> "ApiVersioningAction":
        """Register a versioned endpoint handler."""
        key = f"{method}:{path}"
        
        if key not in self._endpoints:
            self._endpoints[key] = VersionedEndpoint(
                path=path,
                method=method,
                versions={}
            )
        
        self._endpoints[key].versions[version] = handler
        
        if set_as_default or not self._endpoints[key].default_version:
            self._endpoints[key].default_version = version
        
        return self
    
    def add_migration_step(
        self,
        from_version: str,
        to_version: str,
        step_order: int,
        description: str,
        transform: Optional[Callable[[dict], dict]] = None,
        rollback: Optional[Callable[[dict], dict]] = None
    ) -> "ApiVersioningAction":
        """Add a migration step between versions."""
        step = VersionMigrationStep(
            from_version=from_version,
            to_version=to_version,
            step_order=step_order,
            description=description,
            transform=transform,
            rollback=rollback
        )
        self._migrations.append(step)
        self._migrations.sort(key=lambda s: s.step_order)
        return self
    
    async def negotiate_version(
        self,
        request: dict[str, Any],
        default_version: Optional[str] = None
    ) -> VersionNegotiationResult:
        """
        Negotiate API version from request.
        
        Args:
            request: Request data containing version info
            default_version: Fallback version
            
        Returns:
            VersionNegotiationResult with selected version
        """
        requested_version = None
        warning = None
        deprecation_notice = None
        
        if self.scheme == VersionScheme.URI_PATH:
            path = request.get("path", "")
            match = re.match(r"(/v\d+(?:\.\d+)?)", path)
            if match:
                requested_version = match.group(1)
        
        elif self.scheme == VersionScheme.HEADER:
            headers = request.get("headers", {})
            accept = headers.get("Accept", "")
            
            # Parse version from Accept header
            match = re.search(r"version=(\d+)", accept)
            if match:
                requested_version = f"v{match.group(1)}"
            
            # Check for content type
            content_type_match = re.search(r"application/vnd\.(\w+)\+json", accept)
            content_type = content_type_match.group(1) if content_type_match else None
        
        elif self.scheme == VersionScheme.QUERY_PARAM:
            query = request.get("query", {})
            requested_version = query.get("version") or query.get("v")
        
        # Validate requested version
        if requested_version:
            if requested_version in self._versions:
                version_info = self._versions[requested_version]
                
                if version_info.status == "deprecated":
                    deprecation_notice = (
                        f"API version {requested_version} is deprecated. "
                        f"Please migrate to a newer version."
                    )
                    if version_info.sunset_date:
                        deprecation_notice += f" Sunset date: {version_info.sunset_date.strftime('%Y-%m-%d')}"
                
                elif version_info.status == "sunset":
                    warning = f"API version {requested_version} has reached sunset date."
                
                elif version_info.status == "retired":
                    warning = f"API version {requested_version} has been retired."
                    requested_version = None  # Force downgrade
            
            else:
                warning = f"Requested version {requested_version} not found."
                requested_version = None
        
        # Fall back to default
        final_version = requested_version or default_version or self._current_stable
        
        return VersionNegotiationResult(
            selected_version=final_version,
            warning=warning,
            deprecation_notice=deprecation_notice
        )
    
    async def get_handler(
        self,
        path: str,
        method: str,
        version: str
    ) -> Optional[Callable]:
        """Get handler for specific endpoint and version."""
        key = f"{method}:{path}"
        
        if key not in self._endpoints:
            return None
        
        endpoint = self._endpoints[key]
        
        # Try exact version match
        if version in endpoint.versions:
            return endpoint.versions[version]
        
        # Try compatible version
        for v in sorted(endpoint.versions.keys()):
            if v.startswith(version.split(".")[0]):  # Major version match
                return endpoint.versions[v]
        
        # Fall back to default
        if endpoint.default_version:
            return endpoint.versions.get(endpoint.default_version)
        
        return None
    
    async def get_version_info(self, version: str) -> Optional[APIVersion]:
        """Get information about a specific version."""
        return self._versions.get(version)
    
    async def list_versions(
        self,
        status: Optional[str] = None,
        include_retired: bool = False
    ) -> list[APIVersion]:
        """List all registered versions."""
        versions = list(self._versions.values())
        
        if status:
            versions = [v for v in versions if v.status == status]
        
        if not include_retired:
            versions = [v for v in versions if v.status != "retired"]
        
        return sorted(versions, key=lambda v: v.version, reverse=True)
    
    async def get_migration_path(
        self,
        from_version: str,
        to_version: str
    ) -> list[VersionMigrationStep]:
        """Get migration steps between versions."""
        return [
            step for step in self._migrations
            if step.from_version == from_version and step.to_version == to_version
        ]
    
    async def execute_migration(
        self,
        data: dict[str, Any],
        from_version: str,
        to_version: str
    ) -> dict[str, Any]:
        """Execute migration from one version to another."""
        steps = await self.get_migration_path(from_version, to_version)
        
        result = dict(data)
        for step in steps:
            if step.transform:
                try:
                    result = step.transform(result)
                except Exception as e:
                    raise Exception(f"Migration step {step.step_order} failed: {e}")
        
        return result
    
    async def rollback_migration(
        self,
        data: dict[str, Any],
        from_version: str,
        to_version: str
    ) -> dict[str, Any]:
        """Rollback migration from one version to another."""
        steps = await self.get_migration_path(from_version, to_version)
        
        result = dict(data)
        # Apply rollbacks in reverse order
        for step in reversed(steps):
            if step.rollback:
                try:
                    result = step.rollback(result)
                except Exception as e:
                    raise Exception(f"Rollback step {step.step_order} failed: {e}")
        
        return result
    
    def deprecate_version(
        self,
        version: str,
        sunset_date: Optional[datetime] = None,
        migration_guide: Optional[str] = None
    ) -> bool:
        """Mark a version as deprecated."""
        if version not in self._versions:
            return False
        
        v = self._versions[version]
        v.status = "deprecated"
        v.sunset_date = sunset_date
        v.migration_guide = migration_guide
        
        return True
    
    def sunset_version(
        self,
        version: str,
        retirement_date: Optional[datetime] = None
    ) -> bool:
        """Mark a version as sunset."""
        if version not in self._versions:
            return False
        
        v = self._versions[version]
        v.status = "sunset"
        v.retirement_date = retirement_date
        
        return True
    
    def retire_version(self, version: str) -> bool:
        """Retire a version completely."""
        if version not in self._versions:
            return False
        
        self._versions[version].status = "retired"
        return True
    
    async def get_deprecation_notice(
        self,
        version: str,
        format: str = "header"
    ) -> Optional[str]:
        """Generate deprecation notice for a version."""
        if version not in self._versions:
            return None
        
        v = self._versions[version]
        
        if format == "header":
            if v.status == "deprecated":
                notice = f"API version {version} is deprecated"
                if v.sunset_date:
                    notice += f". Sunset date: {v.sunset_date.strftime('%Y-%m-%d')}"
                if v.migration_guide:
                    notice += f". Migration guide: {v.migration_guide}"
                return notice
            elif v.status == "sunset":
                return f"API version {version} has reached sunset date."
            elif v.status == "retired":
                return f"API version {version} has been retired."
        
        return None
    
    def get_version_compatibility(
        self,
        version_a: str,
        version_b: str
    ) -> dict[str, Any]:
        """Check compatibility between two versions."""
        if version_a not in self._versions or version_b not in self._versions:
            return {"compatible": False, "reason": "Version not found"}
        
        v_a = self._versions[version_a]
        v_b = self._versions[version_b]
        
        # Extract major version
        major_a = version_a.split(".")[0]
        major_b = version_b.split(".")[0]
        
        if major_a == major_b:
            return {
                "compatible": True,
                "level": "minor",
                "breaking_changes": []
            }
        
        # Different major versions - check if migration exists
        migration_steps = asyncio.run(
            self.get_migration_path(version_a, version_b)
        )
        
        if migration_steps:
            return {
                "compatible": True,
                "level": "major",
                "migration_steps": len(migration_steps),
                "breaking_changes": v_a.breaking_changes
            }
        
        return {
            "compatible": False,
            "reason": "No migration path available",
            "breaking_changes": v_a.breaking_changes
        }
