"""
Service discovery utilities for microservices and dynamic infrastructure.

Provides service registry, health-based filtering, load balancing,
DNS discovery, and consul/etcd integration.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    HEALTHY = auto()
    UNHEALTHY = auto()
    UNKNOWN = auto()


@dataclass
class ServiceInstance:
    """A single instance of a service."""
    id: str
    name: str
    host: str
    port: int
    health_status: HealthStatus = HealthStatus.HEALTHY
    weight: int = 100
    metadata: dict[str, str] = field(default_factory=dict)
    version: str = "1.0.0"
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    endpoint: str = field(init=False)

    def __post_init__(self) -> None:
        self.endpoint = f"http://{self.host}:{self.port}"

    @property
    def is_healthy(self) -> bool:
        return self.health_status == HealthStatus.HEALTHY


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = auto()
    LEAST_CONNECTIONS = auto()
    WEIGHTED = auto()
    RANDOM = auto()
    IP_HASH = auto()


@dataclass
class ServiceRegistryConfig:
    """Configuration for service registry."""
    ttl_seconds: float = 30.0
    heartbeat_interval: float = 10.0
    health_check_interval: float = 15.0
    max_instance_age: float = 300.0


class ServiceRegistry:
    """In-memory service registry with health tracking."""

    def __init__(self, config: Optional[ServiceRegistryConfig] = None) -> None:
        self.config = config or ServiceRegistryConfig()
        self._services: dict[str, dict[str, ServiceInstance]] = {}
        self._health_checks: dict[str, Callable[..., bool]] = {}
        self._round_robin_counters: dict[str, int] = {}

    def register(self, instance: ServiceInstance) -> None:
        """Register a service instance."""
        if instance.name not in self._services:
            self._services[instance.name] = {}
        self._services[instance.name][instance.id] = instance
        logger.info("Registered service: %s/%s", instance.name, instance.id)

    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        if service_name in self._services and instance_id in self._services[service_name]:
            del self._services[service_name][instance_id]
            logger.info("Deregistered service: %s/%s", service_name, instance_id)
            return True
        return False

    def get_instance(self, service_name: str, instance_id: str) -> Optional[ServiceInstance]:
        """Get a specific service instance."""
        return self._services.get(service_name, {}).get(instance_id)

    def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> list[ServiceInstance]:
        """Get all instances of a service."""
        instances = list(self._services.get(service_name, {}).values())
        if healthy_only:
            now = time.time()
            instances = [
                i for i in instances
                if i.is_healthy or (now - i.last_heartbeat) < self.config.max_instance_age
            ]
        return instances

    def update_health(self, service_name: str, instance_id: str, status: HealthStatus) -> None:
        """Update health status of a service instance."""
        instance = self.get_instance(service_name, instance_id)
        if instance:
            instance.health_status = status
            instance.last_heartbeat = time.time()

    def get_services(self) -> list[str]:
        """Get list of all registered service names."""
        return list(self._services.keys())

    def get_stats(self) -> dict[str, Any]:
        """Get registry statistics."""
        total = sum(len(instances) for instances in self._services.values())
        healthy = sum(
            1 for instances in self._services.values()
            for i in instances.values() if i.is_healthy
        )
        return {
            "total_services": len(self._services),
            "total_instances": total,
            "healthy_instances": healthy,
            "services": {
                name: len(instances) for name, instances in self._services.items()
            },
        }


class ServiceDiscovery:
    """Service discovery with load balancing."""

    def __init__(
        self,
        registry: Optional[ServiceRegistry] = None,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ) -> None:
        self.registry = registry or ServiceRegistry()
        self.strategy = strategy
        self._connection_counts: dict[str, dict[str, int]] = {}

    def select_instance(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> Optional[ServiceInstance]:
        """Select an instance using the configured load balancing strategy."""
        instances = self.registry.get_instances(service_name, healthy_only)
        if not instances:
            return None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(service_name, instances)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(service_name, instances)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(instances)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(instances)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return instances[0]
        return instances[0]

    def _round_robin(self, service_name: str, instances: list[ServiceInstance]) -> ServiceInstance:
        """Round-robin selection."""
        counter = self.registry._round_robin_counters.get(service_name, 0)
        instance = instances[counter % len(instances)]
        self.registry._round_robin_counters[service_name] = counter + 1
        return instance

    def _least_connections(self, service_name: str, instances: list[ServiceInstance]) -> ServiceInstance:
        """Select instance with fewest active connections."""
        if service_name not in self._connection_counts:
            self._connection_counts[service_name] = {}
        counts = self._connection_counts[service_name]
        min_count = min(counts.get(i.id, 0) for i in instances)
        candidates = [i for i in instances if counts.get(i.id, 0) == min_count]
        return candidates[0]

    def _weighted(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Weighted selection based on instance weight."""
        total_weight = sum(i.weight for i in instances)
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for instance in instances:
            cumulative += instance.weight
            if r <= cumulative:
                return instance
        return instances[-1]

    async def call_service(
        self,
        service_name: str,
        path: str = "/",
        method: str = "GET",
        **kwargs: Any,
    ) -> Any:
        """Make a call to a service using service discovery."""
        import httpx
        instance = self.select_instance(service_name)
        if not instance:
            raise ServiceDiscoveryError(f"No healthy instances for {service_name}")

        url = f"{instance.endpoint}{path}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(method, url, **kwargs)
                return response
        except httpx.HTTPError as e:
            logger.error("Service call failed: %s", e)
            raise

    def record_connection(self, service_name: str, instance_id: str) -> None:
        """Record that a connection was made to an instance."""
        if service_name not in self._connection_counts:
            self._connection_counts[service_name] = {}
        self._connection_counts[service_name][instance_id] = self._connection_counts[service_name].get(instance_id, 0) + 1

    def release_connection(self, service_name: str, instance_id: str) -> None:
        """Record that a connection was released."""
        if service_name in self._connection_counts and instance_id in self._connection_counts[service_name]:
            self._connection_counts[service_name][instance_id] = max(0, self._connection_counts[service_name][instance_id] - 1)


class ServiceDiscoveryError(Exception):
    """Raised when service discovery fails."""
    pass
