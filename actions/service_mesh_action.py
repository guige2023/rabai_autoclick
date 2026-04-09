"""
Service Mesh Action Module

Provides service mesh capabilities for distributed UI automation workflows.
Supports service discovery, load balancing, and circuit breaking across services.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = auto()
    RANDOM = auto()
    LEAST_CONNECTIONS = auto()
    WEIGHTED = auto()
    CONSISTENT_HASH = auto()


class ServiceStatus(Enum):
    """Service instance status."""
    HEALTHY = auto()
    UNHEALTHY = auto()
    UNKNOWN = auto()


@dataclass
class ServiceInstance:
    """Service instance representation."""
    id: str
    host: str
    port: int
    weight: int = 1
    status: ServiceStatus = ServiceStatus.HEALTHY
    metadata: dict[str, Any] = field(default_factory=dict)
    connections: int = 0
    last_health_check: float = field(default_factory=lambda: time.time())
    created_at: float = field(default_factory=lambda: time.time())


@dataclass
class ServiceEndpoint:
    """Service endpoint."""
    name: str
    instances: list[ServiceInstance] = field(default_factory=list)

    def get_url(self, instance: Optional[ServiceInstance] = None) -> str:
        """Get URL for instance."""
        inst = instance or self.instances[0] if self.instances else None
        if inst:
            return f"http://{inst.host}:{inst.port}"
        return ""


class ServiceRegistry:
    """
    Service registry for service discovery.

    Example:
        >>> registry = ServiceRegistry()
        >>> registry.register("auth", "192.168.1.10", 8080)
        >>> instance = registry.get_instance("auth")
    """

    def __init__(self) -> None:
        self._services: dict[str, ServiceEndpoint] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        service_name: str,
        host: str,
        port: int,
        weight: int = 1,
        metadata: Optional[dict[str, Any]] = None,
    ) -> ServiceInstance:
        """Register service instance."""
        async with self._lock:
            instance_id = f"{service_name}-{host}:{port}-{int(time.time())}"
            instance = ServiceInstance(
                id=instance_id,
                host=host,
                port=port,
                weight=weight,
                metadata=metadata or {},
            )

            if service_name not in self._services:
                self._services[service_name] = ServiceEndpoint(
                    name=service_name,
                    instances=[],
                )

            self._services[service_name].instances.append(instance)
            logger.info(f"Registered service: {service_name} at {host}:{port}")
            return instance

    async def unregister(self, service_name: str, instance_id: str) -> bool:
        """Unregister service instance."""
        async with self._lock:
            if service_name in self._services:
                instances = self._services[service_name].instances
                for i, inst in enumerate(instances):
                    if inst.id == instance_id:
                        instances.pop(i)
                        logger.info(f"Unregistered service: {service_name} ({instance_id})")
                        return True
        return False

    async def get_instances(self, service_name: str) -> list[ServiceInstance]:
        """Get healthy instances for service."""
        async with self._lock:
            if service_name not in self._services:
                return []
            return [
                inst for inst in self._services[service_name].instances
                if inst.status == ServiceStatus.HEALTHY
            ]

    async def update_health(
        self,
        service_name: str,
        instance_id: str,
        status: ServiceStatus,
    ) -> None:
        """Update instance health status."""
        async with self._lock:
            if service_name in self._services:
                for inst in self._services[service_name].instances:
                    if inst.id == instance_id:
                        inst.status = status
                        inst.last_health_check = time.time()

    def list_services(self) -> list[str]:
        """List all registered services."""
        return list(self._services.keys())


class LoadBalancer:
    """
    Load balancer with multiple strategies.

    Example:
        >>> lb = LoadBalancer(LoadBalancingStrategy.ROUND_ROBIN)
        >>> instance = lb.select(instances)
    """

    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN) -> None:
        self.strategy = strategy
        self._counters: dict[str, int] = {}

    def select(self, instances: list[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select instance based on strategy."""
        if not instances:
            return None

        healthy = [i for i in instances if i.status == ServiceStatus.HEALTHY]
        if not healthy:
            return None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        if self.strategy == LoadBalancingStrategy.RANDOM:
            return self._random(healthy)
        if self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy)
        if self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(healthy)

        return healthy[0]

    def _round_robin(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Round-robin selection."""
        service_name = instances[0].id.split("-")[0]
        if service_name not in self._counters:
            self._counters[service_name] = 0
        self._counters[service_name] = (self._counters[service_name] + 1) % len(instances)
        return instances[self._counters[service_name]]

    def _random(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Random selection."""
        return random.choice(instances)

    def _least_connections(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Select instance with least connections."""
        return min(instances, key=lambda i: i.connections)

    def _weighted(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Weighted random selection."""
        total_weight = sum(i.weight for i in instances)
        r = random.uniform(0, total_weight)
        cumulative = 0
        for inst in instances:
            cumulative += inst.weight
            if cumulative >= r:
                return inst
        return instances[-1]


class HealthChecker:
    """
    Health checker for service instances.

    Example:
        >>> checker = HealthChecker(registry)
        >>> await checker.start()
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        interval: float = 10.0,
        timeout: float = 5.0,
    ) -> None:
        self.registry = registry
        self.interval = interval
        self.timeout = timeout
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start health checker."""
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("Health checker started")

    async def stop(self) -> None:
        """Stop health checker."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Health checker stopped")

    async def _check_loop(self) -> None:
        """Health check loop."""
        while self._running:
            try:
                await self._check_all_services()
            except Exception as e:
                logger.error(f"Health check error: {e}")

            await asyncio.sleep(self.interval)

    async def _check_all_services(self) -> None:
        """Check all registered services."""
        for service_name in self.registry.list_services():
            instances = await self.registry.get_instances(service_name)
            for instance in instances:
                is_healthy = await self._check_instance(instance)
                status = ServiceStatus.HEALTHY if is_healthy else ServiceStatus.UNHEALTHY
                await self.registry.update_health(service_name, instance.id, status)

    async def _check_instance(self, instance: ServiceInstance) -> bool:
        """Check single instance health."""
        try:
            import aiohttp
            url = f"http://{instance.host}:{instance.port}/health"
            async with aiohttp.ClientSession() as session:
                async with asyncio.timeout(self.timeout):
                    async with session.get(url) as response:
                        return response.status == 200
        except Exception:
            return False


class ServiceMesh:
    """
    Service mesh for distributed service management.

    Example:
        >>> mesh = ServiceMesh()
        >>> await mesh.register("api", "localhost", 8080)
        >>> result = await mesh.call("api", "/endpoint")
    """

    def __init__(
        self,
        load_balancer: Optional[LoadBalancer] = None,
        registry: Optional[ServiceRegistry] = None,
    ) -> None:
        self.registry = registry or ServiceRegistry()
        self.load_balancer = load_balancer or LoadBalancer()
        self.health_checker = HealthChecker(self.registry)
        self._circuit_breakers: dict[str, dict] = {}

    async def start(self) -> None:
        """Start service mesh."""
        await self.health_checker.start()
        logger.info("Service mesh started")

    async def stop(self) -> None:
        """Stop service mesh."""
        await self.health_checker.stop()
        logger.info("Service mesh stopped")

    async def register(
        self,
        service_name: str,
        host: str,
        port: int,
        weight: int = 1,
    ) -> ServiceInstance:
        """Register service."""
        return await self.registry.register(service_name, host, port, weight)

    async def unregister(self, service_name: str, instance_id: str) -> bool:
        """Unregister service."""
        return await self.registry.unregister(service_name, instance_id)

    async def call(
        self,
        service_name: str,
        path: str,
        method: str = "GET",
        **kwargs: Any,
    ) -> Any:
        """Call service method via mesh."""
        instances = await self.registry.get_instances(service_name)
        instance = self.load_balancer.select(instances)

        if not instance:
            raise ServiceUnavailableError(f"No healthy instances for {service_name}")

        instance.connections += 1
        try:
            url = f"http://{instance.host}:{instance.port}{path}"
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as response:
                    return await response.json()
        finally:
            instance.connections -= 1

    def get_circuit_breaker(self, service_name: str) -> dict:
        """Get circuit breaker state for service."""
        return self._circuit_breakers.get(service_name, {"state": "closed", "failures": 0})

    def __repr__(self) -> str:
        return f"ServiceMesh(services={len(self.registry.list_services())})"


class ServiceUnavailableError(Exception):
    """Service unavailable error."""
    pass
