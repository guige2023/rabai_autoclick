"""
API Endpoint Registry Action Module.

Service registry for API endpoint discovery and management.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class EndpointStatus(Enum):
    """Endpoint status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    DRAINING = "draining"


@dataclass
class Endpoint:
    """An API endpoint."""
    name: str
    url: str
    status: EndpointStatus = EndpointStatus.UNKNOWN
    weight: int = 1
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_check: float = field(default_factory=time.time)
    failure_count: int = 0
    latency_avg_ms: float = 0.0


@dataclass
class RegistryConfig:
    """Registry configuration."""
    health_check_interval_seconds: float = 30.0
    failure_threshold: int = 3
    success_threshold: int = 2


class ApiEndpointRegistryAction:
    """
    Service registry for API endpoints.

    Supports registration, discovery, health tracking, and load balancing.
    """

    def __init__(
        self,
        config: Optional[RegistryConfig] = None,
    ) -> None:
        self.config = config or RegistryConfig()
        self._endpoints: Dict[str, Endpoint] = {}
        self._by_tag: Dict[str, Set[str]] = {}

    def register(
        self,
        name: str,
        url: str,
        weight: int = 1,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Endpoint:
        """
        Register an endpoint.

        Args:
            name: Endpoint name
            url: Endpoint URL
            weight: Load balancing weight
            tags: Optional tags
            metadata: Optional metadata

        Returns:
            Created endpoint
        """
        endpoint = Endpoint(
            name=name,
            url=url,
            weight=weight,
            tags=tags or set(),
            metadata=metadata or {},
        )

        self._endpoints[name] = endpoint

        for tag in endpoint.tags:
            if tag not in self._by_tag:
                self._by_tag[tag] = set()
            self._by_tag[tag].add(name)

        return endpoint

    def unregister(self, name: str) -> bool:
        """
        Unregister an endpoint.

        Args:
            name: Endpoint name

        Returns:
            True if unregistered
        """
        if name not in self._endpoints:
            return False

        endpoint = self._endpoints.pop(name)

        for tag in endpoint.tags:
            if tag in self._by_tag:
                self._by_tag[tag].discard(name)

        return True

    def get(self, name: str) -> Optional[Endpoint]:
        """Get endpoint by name."""
        return self._endpoints.get(name)

    def get_all(self) -> List[Endpoint]:
        """Get all endpoints."""
        return list(self._endpoints.values())

    def get_by_tag(self, tag: str) -> List[Endpoint]:
        """Get endpoints with a specific tag."""
        names = self._by_tag.get(tag, set())
        return [self._endpoints[n] for n in names if n in self._endpoints]

    def get_healthy(self) -> List[Endpoint]:
        """Get all healthy endpoints."""
        return [e for e in self._endpoints.values() if e.status == EndpointStatus.HEALTHY]

    def update_status(
        self,
        name: str,
        healthy: bool,
        latency_ms: Optional[float] = None,
    ) -> bool:
        """
        Update endpoint health status.

        Args:
            name: Endpoint name
            healthy: Whether endpoint is healthy
            latency_ms: Optional latency

        Returns:
            True if updated
        """
        endpoint = self._endpoints.get(name)
        if not endpoint:
            return False

        endpoint.last_check = time.time()

        if latency_ms is not None:
            if endpoint.latency_avg_ms == 0:
                endpoint.latency_avg_ms = latency_ms
            else:
                endpoint.latency_avg_ms = (endpoint.latency_avg_ms + latency_ms) / 2

        if healthy:
            endpoint.failure_count = 0
            if endpoint.status != EndpointStatus.HEALTHY:
                endpoint.failure_count = 0
                endpoint.status = EndpointStatus.HEALTHY
        else:
            endpoint.failure_count += 1
            if endpoint.failure_count >= self.config.failure_threshold:
                endpoint.status = EndpointStatus.UNHEALTHY

        return True

    def set_status(
        self,
        name: str,
        status: EndpointStatus,
    ) -> bool:
        """Set endpoint status directly."""
        endpoint = self._endpoints.get(name)
        if not endpoint:
            return False

        endpoint.status = status
        endpoint.last_check = time.time()

        if status == EndpointStatus.DRAINING:
            endpoint.failure_count = 0

        return True

    def drain(self, name: str) -> bool:
        """Start draining an endpoint."""
        return self.set_status(name, EndpointStatus.DRAINING)

    def find(
        self,
        tags: Optional[Set[str]] = None,
        status: Optional[EndpointStatus] = None,
    ) -> List[Endpoint]:
        """
        Find endpoints matching criteria.

        Args:
            tags: Required tags (all must match)
            status: Required status

        Returns:
            Matching endpoints
        """
        results = list(self._endpoints.values())

        if status is not None:
            results = [e for e in results if e.status == status]

        if tags:
            results = [e for e in results if tags.issubset(e.tags)]

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        by_status: Dict[str, int] = {}

        for endpoint in self._endpoints.values():
            status_name = endpoint.status.value
            by_status[status_name] = by_status.get(status_name, 0) + 1

        return {
            "total_endpoints": len(self._endpoints),
            "by_status": by_status,
            "tags": list(self._by_tag.keys()),
            "healthy_count": len(self.get_healthy()),
        }
