"""
API Versioning Action Module.

Handles API version negotiation, deprecation warnings, and version migration
strategies for evolving APIs.
"""

from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import logging

logger = logging.getLogger(__name__)


class VersionStrategy(Enum):
    """API versioning strategy types."""
    URI_PATH = "uri_path"          # /v1/resource
    HEADER = "header"              # Accept: application/vnd.api+v1
    QUERY_PARAM = "query"           # ?version=1
    CONTENT_NEGOTIATION = "content" # Accept header with vendor type


@dataclass
class APIVersion:
    """Represents an API version with its metadata."""
    version: str
    deprecation_date: Optional[float] = None
    sunset_date: Optional[float] = None
    migration_guide: Optional[str] = None
    breaking_changes: List[str] = field(default_factory=list)
    is_default: bool = False


@dataclass
class VersionedEndpoint:
    """An endpoint registered under specific API versions."""
    path: str
    method: str
    handler: Callable
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    deprecated_in: Optional[str] = None
    removed_in: Optional[str] = None


class APIVersioningManager:
    """
    Manages API versioning across multiple versions with deprecation handling.
    
    Example:
        manager = APIVersioningManager(VersionStrategy.URI_PATH)
        manager.register_version("v1", is_default=True)
        manager.register_version("v2", deprecation_date=time.time() + 86400*30)
        manager.register_endpoint("/users", "GET", handler, min_version="v1")
    """
    
    def __init__(self, strategy: VersionStrategy = VersionStrategy.URI_PATH):
        self.strategy = strategy
        self.versions: Dict[str, APIVersion] = {}
        self.endpoints: List[VersionedEndpoint] = []
        self.deprecation_callbacks: Dict[str, List[Callable]] = {}
        
    def register_version(
        self,
        version: str,
        is_default: bool = False,
        deprecation_date: Optional[float] = None,
        sunset_date: Optional[float] = None,
        migration_guide: Optional[str] = None,
        breaking_changes: Optional[List[str]] = None,
    ) -> None:
        """
        Register a new API version.
        
        Args:
            version: Version identifier (e.g., "v1", "v2")
            is_default: Whether this is the default version
            deprecation_date: Unix timestamp when version is deprecated
            sunset_date: Unix timestamp when version is removed
            migration_guide: URL or path to migration documentation
            breaking_changes: List of breaking changes in this version
        """
        if is_default:
            for v in self.versions.values():
                v.is_default = False
                
        self.versions[version] = APIVersion(
            version=version,
            deprecation_date=deprecation_date,
            sunset_date=sunset_date,
            migration_guide=migration_guide,
            breaking_changes=breaking_changes or [],
            is_default=is_default,
        )
        logger.info(f"Registered API version: {version} (default={is_default})")
        
    def register_endpoint(
        self,
        path: str,
        method: str,
        handler: Callable,
        min_version: Optional[str] = None,
        max_version: Optional[str] = None,
        deprecated_in: Optional[str] = None,
        removed_in: Optional[str] = None,
    ) -> None:
        """
        Register an endpoint with version constraints.
        
        Args:
            path: API path (e.g., "/users")
            method: HTTP method (e.g., "GET", "POST")
            handler: Function to handle requests
            min_version: Minimum supported version
            max_version: Maximum supported version
            deprecated_in: Version when endpoint was deprecated
            removed_in: Version when endpoint was removed
        """
        endpoint = VersionedEndpoint(
            path=path,
            method=method.upper(),
            handler=handler,
            min_version=min_version,
            max_version=max_version,
            deprecated_in=deprecated_in,
            removed_in=removed_in,
        )
        self.endpoints.append(endpoint)
        
    def get_version_from_request(self, request_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract API version from incoming request.
        
        Args:
            request_data: Request information (URI, headers, query params)
            
        Returns:
            Version string if found, None otherwise
        """
        if self.strategy == VersionStrategy.URI_PATH:
            uri = request_data.get("uri", "")
            for version in self.versions:
                if f"/{version}/" in uri or uri.startswith(f"/{version}"):
                    return version
                    
        elif self.strategy == VersionStrategy.HEADER:
            accept = request_data.get("headers", {}).get("Accept", "")
            for version in self.versions:
                if f"vnd.api+{version}" in accept or f"vnd.{version}" in accept:
                    return version
                    
        elif self.strategy == VersionStrategy.QUERY_PARAM:
            return request_data.get("query_params", {}).get("version")
            
        # Return default version if no version specified
        for v in self.versions.values():
            if v.is_default:
                return v.version
        return None
        
    def find_endpoint(
        self,
        path: str,
        method: str,
        version: Optional[str] = None,
    ) -> Optional[VersionedEndpoint]:
        """
        Find matching endpoint for request.
        
        Args:
            path: Request path
            method: HTTP method
            version: Requested API version
            
        Returns:
            Matching VersionedEndpoint or None
        """
        method = method.upper()
        
        for endpoint in self.endpoints:
            if endpoint.path != path or endpoint.method != method:
                continue
                
            if version:
                if endpoint.min_version and version < endpoint.min_version:
                    continue
                if endpoint.max_version and version > endpoint.max_version:
                    continue
                    
            if endpoint.removed_in and version and version >= endpoint.removed_in:
                continue
                
            return endpoint
            
        return None
        
    def check_deprecation_status(self, version: str) -> Dict[str, Any]:
        """
        Check deprecation status for a version.
        
        Args:
            version: Version to check
            
        Returns:
            Dict with deprecation info including days_until_sunset
        """
        if version not in self.versions:
            return {"exists": False}
            
        v = self.versions[version]
        now = time.time()
        
        status = {
            "exists": True,
            "version": version,
            "is_deprecated": v.deprecation_date is not None and now >= v.deprecation_date,
            "is_sunset": v.sunset_date is not None and now >= v.sunset_date,
            "days_until_deprecation": None,
            "days_until_sunset": None,
            "migration_guide": v.migration_guide,
            "breaking_changes": v.breaking_changes,
        }
        
        if v.deprecation_date:
            status["days_until_deprecation"] = max(0, (v.deprecation_date - now) / 86400)
            
        if v.sunset_date:
            status["days_until_sunset"] = max(0, (v.sunset_date - now) / 86400)
            
        return status
        
    def execute_with_version_fallback(
        self,
        path: str,
        method: str,
        request_data: Dict[str, Any],
        handlers: Dict[str, Callable],
    ) -> Any:
        """
        Execute request with automatic version fallback.
        
        Args:
            path: Request path
            method: HTTP method
            request_data: Full request data
            handlers: Map of version -> handler function
            
        Returns:
            Handler response or error dict
        """
        version = self.get_version_from_request(request_data)
        
        if version in handlers:
            return handlers[version](request_data)
            
        # Try default version
        for v in self.versions.values():
            if v.is_default and v.version in handlers:
                return handlers[v.version](request_data)
                
        # Try any available version
        available = set(handlers.keys()) & set(self.versions.keys())
        if available:
            fallback = sorted(available)[-1]
            return handlers[fallback](request_data)
            
        return {"error": "No handler found", "version": version}
        
    def register_deprecation_callback(
        self,
        version: str,
        callback: Callable[[], None],
    ) -> None:
        """
        Register callback to be called when version is deprecated.
        
        Args:
            version: Version being deprecated
            callback: Function to call on deprecation
        """
        if version not in self.deprecation_callbacks:
            self.deprecation_callbacks[version] = []
        self.deprecation_callbacks[version].append(callback)
        
    def trigger_deprecation_callbacks(self, version: str) -> None:
        """Trigger all callbacks for a deprecated version."""
        for callback in self.deprecation_callbacks.get(version, []):
            try:
                callback()
            except Exception as e:
                logger.error(f"Deprecation callback error: {e}")


def create_versioning_manager(
    strategy: str = "uri_path",
    default_version: str = "v1",
) -> APIVersioningManager:
    """
    Factory function to create a configured versioning manager.
    
    Args:
        strategy: Versioning strategy name
        default_version: Default version identifier
        
    Returns:
        Configured APIVersioningManager instance
    """
    strategy_map = {
        "uri_path": VersionStrategy.URI_PATH,
        "header": VersionStrategy.HEADER,
        "query": VersionStrategy.QUERY_PARAM,
        "content": VersionStrategy.CONTENT_NEGOTIATION,
    }
    
    manager = APIVersioningManager(strategy_map.get(strategy, VersionStrategy.URI_PATH))
    manager.register_version(default_version, is_default=True)
    return manager
