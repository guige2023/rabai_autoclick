"""
API Registry Action Module.

Service registry for API endpoints with health monitoring,
automatic discovery, and load balancing.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import time
import asyncio
from enum import Enum

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceInstance:
    """Single service instance."""
    instance_id: str
    host: str
    port: int
    weight: int = 1
    status: ServiceStatus = ServiceStatus.UNKNOWN
    metadata: dict[str, Any] = field(default_factory=dict)
    last_health_check: float = 0.0
    failure_count: int = 0


@dataclass
class RegisteredService:
    """Registered API service."""
    name: str
    instances: list[ServiceInstance] = field(default_factory=list)
    health_check_url: Optional[str] = None
    health_check_interval: float = 30.0


class APIRegistryAction:
    """
    API service registry with health monitoring.

    Manages service instances, health checks,
    and load-balanced endpoint selection.

    Example:
        registry = APIRegistryAction()
        registry.register("user-service", "localhost", 8000)
        instance = registry.get_instance("user-service")
    """

    def __init__(
        self,
        health_check_interval: float = 30.0,
        unhealthy_threshold: int = 3,
    ) -> None:
        self.health_check_interval = health_check_interval
        self.unhealthy_threshold = unhealthy_threshold
        self._services: dict[str, RegisteredService] = {}
        self._health_check_tasks: dict[str, asyncio.Task] = {}

    def register(
        self,
        service_name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
        weight: int = 1,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Register a service instance."""
        if service_name not in self._services:
            self._services[service_name] = RegisteredService(
                name=service_name,
            )

        instance_id = instance_id or f"{host}:{port}"
        instance = ServiceInstance(
            instance_id=instance_id,
            host=host,
            port=port,
            weight=weight,
            metadata=metadata or {},
        )

        self._services[service_name].instances.append(instance)
        logger.info("Registered %s:%d as %s", host, port, service_name)

        return instance_id

    def unregister(
        self,
        service_name: str,
        instance_id: str,
    ) -> bool:
        """Unregister a service instance."""
        if service_name not in self._services:
            return False

        instances = self._services[service_name].instances
        for i, inst in enumerate(instances):
            if inst.instance_id == instance_id:
                del instances[i]
                return True

        return False

    def get_instance(
        self,
        service_name: str,
        strategy: str = "round_robin",
    ) -> Optional[ServiceInstance]:
        """Get a service instance using specified strategy."""
        if service_name not in self._services:
            return None

        instances = self._services[service_name].instances
        healthy = [i for i in instances if i.status == ServiceStatus.HEALTHY]

        if not healthy:
            return None

        if strategy == "random":
            import random
            return random.choice(healthy)

        elif strategy == "weighted":
            total_weight = sum(i.weight for i in healthy)
            import random
            r = random.uniform(0, total_weight)
            cumulative = 0
            for inst in healthy:
                cumulative += inst.weight
                if r <= cumulative:
                    return inst
            return healthy[-1]

        else:
            return healthy[0]

    def get_all_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> list[ServiceInstance]:
        """Get all instances for a service."""
        if service_name not in self._services:
            return []

        instances = self._services[service_name].instances

        if healthy_only:
            instances = [i for i in instances if i.status == ServiceStatus.HEALTHY]

        return instances

    def update_status(
        self,
        service_name: str,
        instance_id: str,
        status: ServiceStatus,
    ) -> bool:
        """Update instance health status."""
        if service_name not in self._services:
            return False

        for instance in self._services[service_name].instances:
            if instance.instance_id == instance_id:
                instance.status = status
                instance.last_health_check = time.time()
                return True

        return False

    def record_failure(
        self,
        service_name: str,
        instance_id: str,
    ) -> bool:
        """Record a failure for an instance."""
        if service_name not in self._services:
            return False

        for instance in self._services[service_name].instances:
            if instance.instance_id == instance_id:
                instance.failure_count += 1
                if instance.failure_count >= self.unhealthy_threshold:
                    instance.status = ServiceStatus.UNHEALTHY
                return True

        return False

    def record_success(
        self,
        service_name: str,
        instance_id: str,
    ) -> bool:
        """Record a success for an instance."""
        if service_name not in self._services:
            return False

        for instance in self._services[service_name].instances:
            if instance.instance_id == instance_id:
                instance.failure_count = 0
                instance.status = ServiceStatus.HEALTHY
                instance.last_health_check = time.time()
                return True

        return False

    def get_service_url(
        self,
        service_name: str,
    ) -> Optional[str]:
        """Get URL for a service instance."""
        instance = self.get_instance(service_name)
        if instance:
            return f"http://{instance.host}:{instance.port}"
        return None

    @property
    def service_names(self) -> list[str]:
        """List all registered service names."""
        return list(self._services.keys())
