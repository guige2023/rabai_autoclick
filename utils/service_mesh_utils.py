"""
Service mesh utilities for microservices.

Provides service registry, sidecar helpers, and mesh-aware client utilities.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class ServiceInstance:
    """A single service instance."""
    id: str
    host: str
    port: int
    metadata: dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    weight: int = 1
    last_heartbeat: float = field(default_factory=time.time)


class ServiceRegistry:
    """In-memory service registry."""

    def __init__(self):
        self._lock = threading.Lock()
        self._services: dict[str, dict[str, ServiceInstance]] = {}

    def register(
        self,
        service_name: str,
        instance: ServiceInstance,
    ) -> None:
        """Register a service instance."""
        with self._lock:
            if service_name not in self._services:
                self._services[service_name] = {}
            self._services[service_name][instance.id] = instance

    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        with self._lock:
            if service_name in self._services:
                if instance_id in self._services[service_name]:
                    del self._services[service_name][instance_id]
                    return True
        return False

    def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> list[ServiceInstance]:
        """Get all instances for a service."""
        with self._lock:
            instances = list(self._services.get(service_name, {}).values())
        if healthy_only:
            instances = [i for i in instances if i.healthy]
        return instances

    def update_heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update heartbeat for an instance."""
        with self._lock:
            if service_name in self._services:
                if instance_id in self._services[service_name]:
                    self._services[service_name][instance_id].last_heartbeat = time.time()
                    return True
        return False

    def mark_unhealthy(self, service_name: str, instance_id: str) -> bool:
        """Mark an instance as unhealthy."""
        with self._lock:
            if service_name in self._services:
                if instance_id in self._services[service_name]:
                    self._services[service_name][instance_id].healthy = False
                    return True
        return False

    def get_all_services(self) -> list[str]:
        """List all registered services."""
        with self._lock:
            return list(self._services.keys())


class SidecarHelper:
    """Helper for sidecar proxy operations."""

    def __init__(self, registry: ServiceRegistry):
        self.registry = registry

    def build_upstream_config(
        self,
        service_name: str,
        timeout_ms: int = 30000,
    ) -> dict:
        """Build Envoy/HAProxy upstream configuration."""
        instances = self.registry.get_instances(service_name)
        hosts = [
            {"host": i.host, "port": i.port, "weight": i.weight}
            for i in instances
        ]
        return {
            "service": service_name,
            "hosts": hosts,
            "timeout_ms": timeout_ms,
            "health_check": {
                "enabled": True,
                "interval_ms": 5000,
                "unhealthy_threshold": 3,
                "healthy_threshold": 2,
            },
        }

    def register_with_sidecar(
        self,
        service_name: str,
        instance: ServiceInstance,
        sidecar_port: int = 15000,
    ) -> None:
        """Register service with local sidecar."""
        self.registry.register(service_name, instance)
        # In real mesh, would POST to sidecar admin API
        # e.g., http://localhost:{sidecar_port}/v1/registration


class MeshAwareClient:
    """HTTP client with service mesh awareness."""

    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self._lock = threading.Lock()
        self._balancer_state: dict[str, int] = {}

    def _next_index(self, service_name: str) -> int:
        with self._lock:
            idx = self._balancer_state.get(service_name, 0)
            self._balancer_state[service_name] = (idx + 1) % 1000
            return idx

    def get_url(self, service_name: str, path: str = "/") -> str | None:
        """Get URL for a service using round-robin."""
        instances = self.registry.get_instances(service_name)
        if not instances:
            return None
        idx = self._next_index(service_name)
        inst = instances[idx % len(instances)]
        return f"http://{inst.host}:{inst.port}{path}"

    def get_all_urls(self, service_name: str, path: str = "/") -> list[str]:
        """Get all URLs for a service."""
        instances = self.registry.get_instances(service_name)
        return [
            f"http://{i.host}:{i.port}{path}"
            for i in instances
        ]


def create_service_instance(
    host: str,
    port: int,
    service_name: str,
    metadata: dict[str, str] | None = None,
) -> ServiceInstance:
    """Factory to create a service instance."""
    import uuid
    return ServiceInstance(
        id=f"{service_name}-{uuid.uuid4().hex[:8]}",
        host=host,
        port=port,
        metadata=metadata or {},
    )
