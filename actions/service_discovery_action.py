"""Service discovery and registry for API endpoints.

This module provides service discovery:
- Service registration and deregistration
- Health monitoring
- Service lookup by name/tag
- Load balancing integration

Example:
    >>> from actions.service_discovery_action import ServiceRegistry
    >>> registry = ServiceRegistry()
    >>> registry.register("user-service", "localhost:8001")
    >>> endpoints = registry.discover("user-service")
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class ServiceInstance:
    """A service instance."""
    id: str
    name: str
    url: str
    port: int
    host: str
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    health_check_url: Optional[str] = None
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    is_healthy: bool = True


class ServiceRegistry:
    """Service registry for managing service instances.

    Example:
        >>> registry = ServiceRegistry()
        >>> registry.register("api", "localhost", 8000)
        >>> services = registry.discover("api")
    """

    def __init__(
        self,
        ttl: float = 30.0,
        cleanup_interval: float = 10.0,
    ) -> None:
        self.ttl = ttl
        self.cleanup_interval = cleanup_interval
        self._services: dict[str, dict[str, ServiceInstance]] = defaultdict(dict)
        self._lock = threading.RLock()
        self._running = True
        self._cleanup_thread: Optional[threading.Thread] = None
        self._start_cleanup_thread()
        logger.info("ServiceRegistry initialized")

    def _start_cleanup_thread(self) -> None:
        """Start background cleanup thread."""
        def cleanup_loop() -> None:
            while self._running:
                time.sleep(self.cleanup_interval)
                self._cleanup_expired()
        self._cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def _cleanup_expired(self) -> None:
        """Remove expired service instances."""
        with self._lock:
            now = time.time()
            for service_name in list(self._services.keys()):
                instances = self._services[service_name]
                expired = [
                    instance_id
                    for instance_id, inst in instances.items()
                    if now - inst.last_heartbeat > self.ttl
                ]
                for instance_id in expired:
                    del instances[instance_id]
                    logger.info(f"Removed expired instance: {instance_id}")

    def register(
        self,
        name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        health_check_url: Optional[str] = None,
    ) -> ServiceInstance:
        """Register a service instance.

        Args:
            name: Service name.
            host: Service host.
            port: Service port.
            instance_id: Optional instance ID.
            tags: Optional list of tags.
            metadata: Optional metadata.
            health_check_url: Optional health check URL.

        Returns:
            The registered ServiceInstance.
        """
        import uuid
        instance = ServiceInstance(
            id=instance_id or str(uuid.uuid4())[:8],
            name=name,
            url=f"http://{host}:{port}",
            host=host,
            port=port,
            tags=tags or [],
            metadata=metadata or {},
            health_check_url=health_check_url,
        )
        with self._lock:
            self._services[name][instance.id] = instance
        logger.info(f"Registered service: {name} ({instance.url})")
        return instance

    def deregister(self, name: str, instance_id: str) -> bool:
        """Deregister a service instance.

        Args:
            name: Service name.
            instance_id: Instance ID.

        Returns:
            True if deregistered successfully.
        """
        with self._lock:
            if name in self._services and instance_id in self._services[name]:
                del self._services[name][instance_id]
                logger.info(f"Deregistered service: {name}/{instance_id}")
                return True
        return False

    def heartbeat(self, name: str, instance_id: str) -> bool:
        """Send heartbeat for a service instance.

        Args:
            name: Service name.
            instance_id: Instance ID.

        Returns:
            True if heartbeat was recorded.
        """
        with self._lock:
            if name in self._services and instance_id in self._services[name]:
                self._services[name][instance_id].last_heartbeat = time.time()
                return True
        return False

    def discover(
        self,
        name: str,
        tags: Optional[list[str]] = None,
        healthy_only: bool = True,
    ) -> list[ServiceInstance]:
        """Discover service instances.

        Args:
            name: Service name to discover.
            tags: Optional tags to filter by.
            healthy_only: Only return healthy instances.

        Returns:
            List of matching ServiceInstances.
        """
        with self._lock:
            if name not in self._services:
                return []
            instances = list(self._services[name].values())
            if healthy_only:
                instances = [i for i in instances if i.is_healthy]
            if tags:
                instances = [
                    i for i in instances
                    if any(tag in i.tags for tag in tags)
                ]
            return instances

    def set_health(self, name: str, instance_id: str, is_healthy: bool) -> bool:
        """Set health status for a service instance."""
        with self._lock:
            if name in self._services and instance_id in self._services[name]:
                self._services[name][instance_id].is_healthy = is_healthy
                return True
        return False

    def get_all_services(self) -> dict[str, list[ServiceInstance]]:
        """Get all registered services."""
        with self._lock:
            return {
                name: list(instances.values())
                for name, instances in self._services.items()
            }

    def stop(self) -> None:
        """Stop the registry and cleanup thread."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=1.0)
        logger.info("ServiceRegistry stopped")


class ServiceDiscovery:
    """Client-side service discovery with caching."""

    def __init__(
        self,
        registry: ServiceRegistry,
        cache_ttl: float = 10.0,
    ) -> None:
        self.registry = registry
        self.cache_ttl = cache_ttl
        self._cache: dict[str, tuple[list[ServiceInstance], float]] = {}
        self._lock = threading.RLock()

    def discover(
        self,
        name: str,
        tags: Optional[list[str]] = None,
        use_cache: bool = True,
    ) -> list[ServiceInstance]:
        """Discover service with caching.

        Args:
            name: Service name.
            tags: Optional tags filter.
            use_cache: Whether to use cached results.

        Returns:
            List of ServiceInstances.
        """
        cache_key = f"{name}:{','.join(tags or [])}"
        with self._lock:
            if use_cache and cache_key in self._cache:
                instances, cached_at = self._cache[cache_key]
                if time.time() - cached_at < self.cache_ttl:
                    return instances
            instances = self.registry.discover(name, tags)
            self._cache[cache_key] = (instances, time.time())
            return instances

    def clear_cache(self) -> None:
        """Clear the discovery cache."""
        with self._lock:
            self._cache.clear()
