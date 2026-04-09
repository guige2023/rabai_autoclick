"""API Endpoint Registry Action Module.

Registry for managing API endpoint metadata, versioning,
deprecation schedules, and routing rules.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)


class EndpointStatus(Enum):
    """Status of an API endpoint."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    RETIRED = "retired"


@dataclass
class APIEndpoint:
    """Metadata for a registered API endpoint."""
    path: str
    method: str
    version: str
    status: EndpointStatus
    description: str = ""
    rate_limit: Optional[int] = None  # requests per minute
    timeout_sec: float = 30.0
    tags: List[str] = field(default_factory=list)
    deprecated_at: Optional[float] = None
    sunset_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIEndpointRegistryAction:
    """Registry for managing API endpoint metadata.
    
    Provides registration, lookup, versioning, and deprecation
    management for API endpoints.
    """

    def __init__(self) -> None:
        self._endpoints: Dict[str, APIEndpoint] = {}
        self._version_index: Dict[str, List[str]] = {}  # version -> [endpoint_keys]
        self._tag_index: Dict[str, Set[str]] = {}  # tag -> set of endpoint_keys
        self._stats: Dict[str, int] = {"registered": 0, "lookups": 0, "not_found": 0}

    def _make_key(self, path: str, method: str, version: str) -> str:
        return f"{version}:{method.upper()}:{path}"

    def register(
        self,
        path: str,
        method: str = "GET",
        version: str = "v1",
        description: str = "",
        rate_limit: Optional[int] = None,
        timeout_sec: float = 30.0,
        tags: Optional[List[str]] = None,
    ) -> str:
        """Register an API endpoint.
        
        Args:
            path: URL path.
            method: HTTP method.
            version: API version.
            description: Endpoint description.
            rate_limit: Optional rate limit (req/min).
            timeout_sec: Timeout in seconds.
            tags: Optional list of tags.
        
        Returns:
            The generated endpoint key.
        """
        key = self._make_key(path, method, version)
        endpoint = APIEndpoint(
            path=path,
            method=method.upper(),
            version=version,
            status=EndpointStatus.ACTIVE,
            description=description,
            rate_limit=rate_limit,
            timeout_sec=timeout_sec,
            tags=tags or [],
        )
        self._endpoints[key] = endpoint
        self._version_index.setdefault(version, []).append(key)
        for tag in endpoint.tags:
            self._tag_index.setdefault(tag, set()).add(key)
        self._stats["registered"] += 1
        logger.debug("Registered endpoint: %s", key)
        return key

    def lookup(
        self,
        path: str,
        method: str = "GET",
        version: Optional[str] = None,
    ) -> Optional[APIEndpoint]:
        """Look up an endpoint by path and method.
        
        Args:
            path: URL path.
            method: HTTP method.
            version: Optional specific version (defaults to latest).
        
        Returns:
            APIEndpoint if found, None otherwise.
        """
        self._stats["lookups"] += 1
        if version:
            key = self._make_key(path, method, version)
            endpoint = self._endpoints.get(key)
            if endpoint:
                return endpoint
        else:
            for ver in sorted(self._version_index.keys(), reverse=True):
                key = self._make_key(path, method, ver)
                if key in self._endpoints:
                    return self._endpoints[key]
        self._stats["not_found"] += 1
        return None

    def deprecate(
        self,
        path: str,
        method: str,
        version: str,
        sunset_days: int = 30,
    ) -> bool:
        """Mark an endpoint as deprecated.
        
        Args:
            path: URL path.
            method: HTTP method.
            version: API version.
            sunset_days: Days until sunset.
        
        Returns:
            True if endpoint was found and deprecated.
        """
        key = self._make_key(path, method, version)
        endpoint = self._endpoints.get(key)
        if not endpoint:
            return False
        endpoint.status = EndpointStatus.DEPRECATED
        endpoint.deprecated_at = time.time()
        endpoint.sunset_at = time.time() + sunset_days * 86400
        logger.info("Deprecated endpoint: %s (sunset in %d days)", key, sunset_days)
        return True

    def get_active_endpoints(
        self,
        version: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> List[APIEndpoint]:
        """Get active endpoints, optionally filtered.
        
        Args:
            version: Filter by version.
            tag: Filter by tag.
        
        Returns:
            List of matching active endpoints.
        """
        results: List[APIEndpoint] = []
        if version:
            keys = self._version_index.get(version, [])
        elif tag:
            keys = list(self._tag_index.get(tag, set()))
        else:
            keys = list(self._endpoints.keys())

        for key in keys:
            ep = self._endpoints.get(key)
            if ep and ep.status == EndpointStatus.ACTIVE:
                results.append(ep)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        status_counts: Dict[str, int] = {}
        for ep in self._endpoints.values():
            status_counts[ep.status.value] = status_counts.get(ep.status.value, 0) + 1
        return {
            "total_registered": len(self._endpoints),
            "status_counts": status_counts,
            "versions": list(self._version_index.keys()),
            "lookups": self._stats["lookups"],
            "not_found": self._stats["not_found"],
        }
