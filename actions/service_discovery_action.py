"""Service Discovery Action Module.

Dynamic service discovery with registration and health checking.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .workflow_routing_action import RouteTarget


class ServiceStatus(Enum):
    """Service instance status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    DRAINING = "draining"


@dataclass
class ServiceInstance:
    """Service instance."""
    instance_id: str
    service_name: str
    host: str
    port: int
    status: ServiceStatus = ServiceStatus.UNKNOWN
    metadata: dict = field(default_factory=dict)
    registered_at: float = 0.0
    last_heartbeat: float = 0.0
    health_check_url: str | None = None


@dataclass
class ServiceHealth:
    """Service health info."""
    instance_id: str
    is_healthy: bool
    latency_ms: float | None = None
    checked_at: float


class ServiceRegistry:
    """Service registry with health checking."""

    def __init__(
        self,
        health_check_interval: float = 30.0,
        unhealthy_threshold: int = 3
    ) -> None:
        self.health_check_interval = health_check_interval
        self.unhealthy_threshold = unhealthy_threshold
        self._services: dict[str, dict[str, ServiceInstance]] = {}
        self._health_checks: dict[str, asyncio.Task] = {}
        self._failure_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def register(self, instance: ServiceInstance) -> None:
        """Register a service instance."""
        async with self._lock:
            if instance.service_name not in self._services:
                self._services[instance.service_name] = {}
            instance.registered_at = time.time()
            instance.last_heartbeat = time.time()
            self._services[instance.service_name][instance.instance_id] = instance
            self._failure_counts[instance.instance_id] = 0

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        async with self._lock:
            if service_name in self._services:
                if instance_id in self._services[service_name]:
                    del self._services[service_name][instance_id]
                    self._failure_counts.pop(instance_id, None)
                    return True
        return False

    async def get_instances(
        self,
        service_name: str,
        status: ServiceStatus | None = None
    ) -> list[ServiceInstance]:
        """Get healthy instances for a service."""
        async with self._lock:
            instances = self._services.get(service_name, {}).values()
            if status:
                instances = [i for i in instances if i.status == status]
            return list(instances)

    async def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Record heartbeat for instance."""
        async with self._lock:
            instance = self._services.get(service_name, {}).get(instance_id)
            if instance:
                instance.last_heartbeat = time.time()
                self._failure_counts[instance_id] = 0
                instance.status = ServiceStatus.HEALTHY
                return True
            return False

    async def start_health_checks(self) -> None:
        """Start health check loop."""
        asyncio.create_task(self._health_check_loop())

    async def _health_check_loop(self) -> None:
        """Periodically check health of all instances."""
        while True:
            async with self._lock:
                all_instances = [
                    inst for instances in self._services.values()
                    for inst in instances.values()
                ]
            for instance in all_instances:
                asyncio.create_task(self._check_instance(instance))
            await asyncio.sleep(self.health_check_interval)

    async def _check_instance(self, instance: ServiceInstance) -> None:
        """Check health of single instance."""
        if not instance.health_check_url:
            return
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                start = time.monotonic()
                async with session.get(
                    instance.health_check_url,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    latency = (time.monotonic() - start) * 1000
                    is_healthy = 200 <= resp.status < 300
                    async with self._lock:
                        self._failure_counts[instance.instance_id] = (
                            0 if is_healthy
                            else self._failure_counts.get(instance.instance_id, 0) + 1
                        )
                        instance.status = (
                            ServiceStatus.HEALTHY
                            if is_healthy
                            else ServiceStatus.UNHEALTHY
                        )
        except Exception:
            async with self._lock:
                instance.status = ServiceStatus.UNHEALTHY
                self._failure_counts[instance.instance_id] = (
                    self._failure_counts.get(instance.instance_id, 0) + 1
                )

    def get_stats(self) -> dict[str, Any]:
        """Get registry statistics."""
        total = sum(len(instances) for instances in self._services.values())
        healthy = sum(
            1 for instances in self._services.values()
            for inst in instances.values() if inst.status == ServiceStatus.HEALTHY
        )
        return {
            "total_instances": total,
            "healthy_instances": healthy,
            "unhealthy_instances": total - healthy,
            "services": len(self._services),
        }
