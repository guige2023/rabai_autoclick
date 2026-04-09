"""Service registry action for service discovery and registration.

Provides service registration, health monitoring, and
lookup functionality for microservice architectures.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceInstance:
    instance_id: str
    service_name: str
    host: str
    port: int
    status: ServiceStatus
    metadata: dict[str, Any] = field(default_factory=dict)
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    health_check_url: Optional[str] = None


@dataclass
class ServiceDefinition:
    name: str
    version: str
    description: str = ""
    instances: list[ServiceInstance] = field(default_factory=list)


class ServiceRegistryAction:
    """Service registry with health checking and discovery.

    Args:
        heartbeat_interval: Heartbeat interval in seconds.
        unhealthy_threshold: Missed heartbeats before marking unhealthy.
        enable_dns: Enable DNS-style discovery.
    """

    def __init__(
        self,
        heartbeat_interval: float = 30.0,
        unhealthy_threshold: int = 3,
        enable_dns: bool = True,
    ) -> None:
        self._services: dict[str, ServiceDefinition] = {}
        self._instances: dict[str, ServiceInstance] = {}
        self._heartbeat_interval = heartbeat_interval
        self._unhealthy_threshold = unhealthy_threshold
        self._enable_dns = enable_dns
        self._change_handlers: list[Callable] = []

    def register_service(
        self,
        service_name: str,
        version: str,
        description: str = "",
    ) -> bool:
        """Register a service definition.

        Args:
            service_name: Service name.
            version: Service version.
            description: Service description.

        Returns:
            True if registered successfully.
        """
        if service_name in self._services:
            logger.warning(f"Service already registered: {service_name}")
            return False

        self._services[service_name] = ServiceDefinition(
            name=service_name,
            version=version,
            description=description,
        )
        logger.debug(f"Registered service: {service_name}")
        return True

    def register_instance(
        self,
        instance_id: str,
        service_name: str,
        host: str,
        port: int,
        metadata: Optional[dict[str, Any]] = None,
        health_check_url: Optional[str] = None,
    ) -> bool:
        """Register a service instance.

        Args:
            instance_id: Unique instance ID.
            service_name: Service name.
            host: Instance host.
            port: Instance port.
            metadata: Instance metadata.
            health_check_url: Optional health check URL.

        Returns:
            True if registered successfully.
        """
        if instance_id in self._instances:
            logger.warning(f"Instance already registered: {instance_id}")
            return False

        if service_name not in self._services:
            self.register_service(service_name, "1.0.0")

        instance = ServiceInstance(
            instance_id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            status=ServiceStatus.HEALTHY,
            metadata=metadata or {},
            health_check_url=health_check_url,
        )

        self._instances[instance_id] = instance
        self._services[service_name].instances.append(instance)

        for handler in self._change_handlers:
            try:
                handler("instance_registered", instance)
            except Exception as e:
                logger.error(f"Change handler error: {e}")

        logger.debug(f"Registered instance: {instance_id} for {service_name}")
        return True

    def deregister_instance(self, instance_id: str) -> bool:
        """Deregister a service instance.

        Args:
            instance_id: Instance ID.

        Returns:
            True if deregistered.
        """
        instance = self._instances.get(instance_id)
        if not instance:
            return False

        service = self._services.get(instance.service_name)
        if service:
            service.instances = [i for i in service.instances if i.instance_id != instance_id]

        del self._instances[instance_id]

        for handler in self._change_handlers:
            try:
                handler("instance_deregistered", instance)
            except Exception as e:
                logger.error(f"Change handler error: {e}")

        logger.debug(f"Deregistered instance: {instance_id}")
        return True

    def heartbeat(self, instance_id: str) -> bool:
        """Record a heartbeat for an instance.

        Args:
            instance_id: Instance ID.

        Returns:
            True if heartbeat recorded.
        """
        instance = self._instances.get(instance_id)
        if not instance:
            return False

        instance.last_heartbeat = time.time()
        instance.status = ServiceStatus.HEALTHY
        return True

    def get_instance(self, instance_id: str) -> Optional[ServiceInstance]:
        """Get a service instance by ID.

        Args:
            instance_id: Instance ID.

        Returns:
            Service instance or None.
        """
        return self._instances.get(instance_id)

    def get_service_instances(
        self,
        service_name: str,
        status_filter: Optional[ServiceStatus] = None,
    ) -> list[ServiceInstance]:
        """Get all instances for a service.

        Args:
            service_name: Service name.
            status_filter: Filter by status.

        Returns:
            List of service instances.
        """
        service = self._services.get(service_name)
        if not service:
            return []

        instances = service.instances
        if status_filter:
            instances = [i for i in instances if i.status == status_filter]

        return instances

    def discover_service(
        self,
        service_name: str,
        strategy: str = "random",
    ) -> Optional[ServiceInstance]:
        """Discover a service instance.

        Args:
            service_name: Service name.
            strategy: Discovery strategy ('random', 'round_robin', 'least_connections').

        Returns:
            Discovered service instance or None.
        """
        instances = self.get_service_instances(service_name, ServiceStatus.HEALTHY)
        if not instances:
            return None

        if strategy == "random":
            import random
            return random.choice(instances)
        elif strategy == "round_robin":
            return instances[0]
        elif strategy == "least_connections":
            return min(instances, key=lambda i: i.metadata.get("connections", 0))

        return instances[0]

    def check_health(self, instance_id: str) -> ServiceStatus:
        """Check health of an instance.

        Args:
            instance_id: Instance ID.

        Returns:
            Health status.
        """
        instance = self._instances.get(instance_id)
        if not instance:
            return ServiceStatus.UNKNOWN

        elapsed = time.time() - instance.last_heartbeat
        if elapsed > self._heartbeat_interval * self._unhealthy_threshold:
            instance.status = ServiceStatus.UNHEALTHY
            return ServiceStatus.UNHEALTHY

        if instance.health_check_url:
            try:
                import urllib.request
                req = urllib.request.Request(instance.health_check_url)
                urllib.request.urlopen(req, timeout=5.0)
                instance.status = ServiceStatus.HEALTHY
            except Exception:
                instance.status = ServiceStatus.UNHEALTHY

        return instance.status

    def check_all_health(self) -> dict[str, ServiceStatus]:
        """Check health of all instances.

        Returns:
            Dictionary mapping instance IDs to health status.
        """
        results = {}
        for instance_id in self._instances:
            results[instance_id] = self.check_health(instance_id)
        return results

    def cleanup_unhealthy(self) -> int:
        """Remove unhealthy instances.

        Returns:
            Number of instances removed.
        """
        unhealthy = [
            iid for iid, inst in self._instances.items()
            if inst.status == ServiceStatus.UNHEALTHY
        ]

        for iid in unhealthy:
            self.deregister_instance(iid)

        return len(unhealthy)

    def register_change_handler(self, handler: Callable) -> None:
        """Register a handler for registry changes.

        Args:
            handler: Callback function.
        """
        self._change_handlers.append(handler)

    def get_stats(self) -> dict[str, Any]:
        """Get service registry statistics.

        Returns:
            Dictionary with stats.
        """
        total_instances = len(self._instances)
        healthy = sum(1 for i in self._instances.values() if i.status == ServiceStatus.HEALTHY)

        return {
            "total_services": len(self._services),
            "total_instances": total_instances,
            "healthy_instances": healthy,
            "unhealthy_instances": total_instances - healthy,
            "heartbeat_interval": self._heartbeat_interval,
            "unhealthy_threshold": self._unhealthy_threshold,
        }
