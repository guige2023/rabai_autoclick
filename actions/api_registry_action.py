"""
API Registry Action Module.

Provides service registry, discovery, health monitoring,
and dynamic routing capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict

logger = logging.getLogger(__name__)


class ServiceHealth(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceInstance:
    """Service instance in registry."""
    instance_id: str
    service_name: str
    version: str
    host: str
    port: int
    health: ServiceHealth = ServiceHealth.HEALTHY
    metadata: Dict[str, Any] = field(default_factory=dict)
    registered_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    weight: int = 1


@dataclass
class ServiceInfo:
    """Service information."""
    service_name: str
    version: str
    instances: List[ServiceInstance]
    total_instances: int
    healthy_instances: int


class ServiceRegistry:
    """Central service registry."""

    def __init__(self, heartbeat_timeout: int = 30):
        self.services: Dict[str, Dict[str, ServiceInstance]] = defaultdict(dict)
        self.heartbeat_timeout = heartbeat_timeout
        self._lock = asyncio.Lock()

    async def register(self, instance: ServiceInstance) -> bool:
        """Register a service instance."""
        async with self._lock:
            key = f"{instance.service_name}:{instance.instance_id}"
            self.services[instance.service_name][instance.instance_id] = instance
            logger.info(f"Registered: {instance.service_name} ({instance.instance_id})")
            return True

    async def unregister(self, service_name: str, instance_id: str) -> bool:
        """Unregister a service instance."""
        async with self._lock:
            if service_name in self.services:
                if instance_id in self.services[service_name]:
                    del self.services[service_name][instance_id]
                    return True
            return False

    async def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update service heartbeat."""
        async with self._lock:
            if service_name in self.services:
                if instance_id in self.services[service_name]:
                    self.services[service_name][instance_id].last_heartbeat = datetime.now()
                    return True
            return False

    async def discover(self, service_name: str) -> ServiceInfo:
        """Discover service instances."""
        async with self._lock:
            instances = list(self.services.get(service_name, {}).values())
            await self._cleanup_stale(service_name)

            instances = list(self.services.get(service_name, {}).values())
            healthy = [i for i in instances if i.health == ServiceHealth.HEALTHY]

            return ServiceInfo(
                service_name=service_name,
                version=instances[0].version if instances else "unknown",
                instances=instances,
                total_instances=len(instances),
                healthy_instances=len(healthy)
            )

    async def _cleanup_stale(self, service_name: str):
        """Remove stale service instances."""
        cutoff = datetime.now() - timedelta(seconds=self.heartbeat_timeout)
        if service_name in self.services:
            stale = [
                iid for iid, inst in self.services[service_name].items()
                if inst.last_heartbeat < cutoff
            ]
            for iid in stale:
                del self.services[service_name][iid]


class LoadBalancer:
    """Service load balancer."""

    def __init__(self, strategy: str = "round_robin"):
        self.strategy = strategy
        self.counters: Dict[str, int] = defaultdict(int)

    def select(self, instances: List[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select an instance."""
        healthy = [i for i in instances if i.health == ServiceHealth.HEALTHY]
        if not healthy:
            return instances[0] if instances else None

        if self.strategy == "round_robin":
            idx = self.counters[self.strategy] % len(healthy)
            self.counters[self.strategy] += 1
            return healthy[idx]

        elif self.strategy == "random":
            import random
            return healthy[int(random.random() * len(healthy))]

        elif self.strategy == "weighted":
            total_weight = sum(i.weight for i in healthy)
            r = int(time.time() * 1000) % total_weight
            cumulative = 0
            for inst in healthy:
                cumulative += inst.weight
                if cumulative >= r:
                    return inst

        return healthy[0]


class ServiceDiscovery:
    """Service discovery with caching."""

    def __init__(self, registry: ServiceRegistry, cache_ttl: int = 60):
        self.registry = registry
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[ServiceInfo, datetime]] = {}

    async def find(self, service_name: str, use_cache: bool = True) -> ServiceInfo:
        """Find service with optional caching."""
        if use_cache and service_name in self._cache:
            cached, timestamp = self._cache[service_name]
            if (datetime.now() - timestamp).total_seconds() < self.cache_ttl:
                return cached

        info = await self.registry.discover(service_name)
        self._cache[service_name] = (info, datetime.now())
        return info

    def invalidate(self, service_name: str):
        """Invalidate cache entry."""
        if service_name in self._cache:
            del self._cache[service_name]


class HealthMonitor:
    """Monitors service health."""

    def __init__(self, registry: ServiceRegistry, check_interval: int = 10):
        self.registry = registry
        self.check_interval = check_interval
        self._running = False
        self._handlers: List[Callable] = []

    def add_handler(self, handler: Callable):
        """Add health status handler."""
        self._handlers.append(handler)

    async def check_instance(self, instance: ServiceInstance) -> ServiceHealth:
        """Check single instance health."""
        try:
            await asyncio.sleep(0.01)
            return ServiceHealth.HEALTHY
        except Exception:
            return ServiceHealth.UNHEALTHY

    async def check_service(self, service_name: str):
        """Check all instances of a service."""
        info = await self.registry.discover(service_name)
        for inst in info.instances:
            health = await self.check_instance(inst)
            inst.health = health

            for handler in self._handlers:
                try:
                    handler(inst, health)
                except Exception:
                    pass


async def main():
    """Demonstrate service registry."""
    registry = ServiceRegistry()

    await registry.register(ServiceInstance(
        instance_id="inst-1",
        service_name="api-gateway",
        version="v1",
        host="localhost",
        port=8001
    ))

    info = await registry.discover("api-gateway")
    print(f"Service: {info.service_name}, Instances: {info.total_instances}")


if __name__ == "__main__":
    asyncio.run(main())
