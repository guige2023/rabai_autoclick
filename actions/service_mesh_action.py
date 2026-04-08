"""
Service mesh module for microservices traffic management.

Supports service discovery, load balancing, circuit breaking,
retries, timeouts, and traffic routing.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ServiceStatus(Enum):
    """Service instance status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    UNKNOWN = "unknown"


@dataclass
class ServiceInstance:
    """A service instance."""
    id: str
    service_name: str
    host: str
    port: int
    weight: int = 100
    status: ServiceStatus = ServiceStatus.HEALTHY
    metadata: dict = field(default_factory=dict)
    health_check_url: Optional[str] = None
    consecutive_failures: int = 0
    last_health_check: Optional[float] = None
    registered_at: float = field(default_factory=time.time)


@dataclass
class ServiceEndpoint:
    """A service endpoint with multiple instances."""
    name: str
    instances: list[ServiceInstance] = field(default_factory=list)
    health_check_interval: int = 30
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2


@dataclass
class TrafficRoute:
    """A traffic routing rule."""
    destination: str
    weight: int = 100
    subset: Optional[str] = None
    header_match: Optional[dict] = None


@dataclass
class MeshConfig:
    """Service mesh configuration."""
    enable_circuit_breaker: bool = True
    enable_retries: bool = True
    enable_timeout: bool = True
    default_timeout_seconds: int = 30
    max_retries: int = 3
    circuit_breaker_threshold: int = 5


class ServiceMesh:
    """
    Service mesh for microservices traffic management.

    Provides service discovery, load balancing, circuit breaking,
    retries, timeouts, and traffic routing.
    """

    def __init__(self, config: Optional[MeshConfig] = None):
        self.config = config or MeshConfig()
        self._services: dict[str, ServiceEndpoint] = {}
        self._circuit_breakers: dict[str, dict] = {}
        self._routes: dict[str, list[TrafficRoute]] = {}
        self._request_logs: list[dict] = []

    def register_service(
        self,
        service_name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
        weight: int = 100,
        metadata: Optional[dict] = None,
    ) -> ServiceInstance:
        """Register a service instance."""
        if service_name not in self._services:
            self._services[service_name] = ServiceEndpoint(name=service_name)

        instance = ServiceInstance(
            id=instance_id or str(uuid.uuid4())[:8],
            service_name=service_name,
            host=host,
            port=port,
            weight=weight,
            metadata=metadata or {},
        )

        self._services[service_name].instances.append(instance)
        return instance

    def deregister_service(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        endpoint = self._services.get(service_name)
        if not endpoint:
            return False

        endpoint.instances = [
            i for i in endpoint.instances if i.id != instance_id
        ]
        return True

    def get_service_endpoint(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> Optional[ServiceInstance]:
        """Get a service endpoint using load balancing."""
        endpoint = self._services.get(service_name)
        if not endpoint:
            return None

        instances = endpoint.instances
        if healthy_only:
            instances = [i for i in instances if i.status == ServiceStatus.HEALTHY]

        if not instances:
            return None

        total_weight = sum(i.weight for i in instances)
        if total_weight == 0:
            return instances[0]

        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for instance in instances:
            cumulative += instance.weight
            if r <= cumulative:
                return instance

        return instances[0]

    def health_check(self, service_name: str) -> dict:
        """Perform health check on all instances of a service."""
        endpoint = self._services.get(service_name)
        if not endpoint:
            return {"service": service_name, "status": "not_found"}

        results = {
            "service": service_name,
            "total_instances": len(endpoint.instances),
            "healthy_instances": 0,
            "unhealthy_instances": 0,
            "instance_statuses": [],
        }

        for instance in endpoint.instances:
            is_healthy = self._check_instance_health(instance)

            if is_healthy:
                instance.status = ServiceStatus.HEALTHY
                instance.consecutive_failures = 0
                results["healthy_instances"] += 1
            else:
                instance.consecutive_failures += 1
                if instance.consecutive_failures >= endpoint.unhealthy_threshold:
                    instance.status = ServiceStatus.UNHEALTHY
                    results["unhealthy_instances"] += 1

            instance.last_health_check = time.time()

            results["instance_statuses"].append({
                "id": instance.id,
                "host": instance.host,
                "port": instance.port,
                "status": instance.status.value,
                "consecutive_failures": instance.consecutive_failures,
            })

        return results

    def _check_instance_health(self, instance: ServiceInstance) -> bool:
        """Check if a service instance is healthy."""
        import socket

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((instance.host, instance.port))
            sock.close()
            return result == 0
        except Exception:
            return False

    def check_circuit_breaker(self, service_name: str) -> tuple[bool, str]:
        """Check circuit breaker status for a service."""
        cb = self._circuit_breakers.get(service_name, {
            "state": "closed",
            "failures": 0,
            "last_failure": None,
        })

        if cb["state"] == "open":
            if cb.get("next_retry") and time.time() >= cb["next_retry"]:
                cb["state"] = "half_open"
                return True, "half_open"
            return False, "open"

        return True, cb["state"]

    def record_success(self, service_name: str) -> None:
        """Record a successful request."""
        if service_name not in self._circuit_breakers:
            self._circuit_breakers[service_name] = {
                "state": "closed",
                "failures": 0,
            }

        cb = self._circuit_breakers[service_name]
        cb["failures"] = 0
        if cb["state"] == "half_open":
            cb["state"] = "closed"

    def record_failure(self, service_name: str) -> None:
        """Record a failed request."""
        if service_name not in self._circuit_breakers:
            self._circuit_breakers[service_name] = {
                "state": "closed",
                "failures": 0,
            }

        cb = self._circuit_breakers[service_name]
        cb["failures"] += 1
        cb["last_failure"] = time.time()

        if cb["failures"] >= self.config.circuit_breaker_threshold:
            cb["state"] = "open"
            cb["next_retry"] = time.time() + 30

    def add_route(
        self,
        from_service: str,
        to_service: str,
        weight: int = 100,
        subset: Optional[str] = None,
    ) -> None:
        """Add a traffic route."""
        if from_service not in self._routes:
            self._routes[from_service] = []

        route = TrafficRoute(
            destination=to_service,
            weight=weight,
            subset=subset,
        )
        self._routes[from_service].append(route)

    def get_route(
        self,
        from_service: str,
        headers: Optional[dict] = None,
    ) -> Optional[str]:
        """Get the destination service for a route."""
        routes = self._routes.get(from_service, [])
        if not routes:
            return from_service

        if len(routes) == 1:
            return routes[0].destination

        total_weight = sum(r.weight for r in routes)
        import random
        r = random.randint(1, total_weight)
        cumulative = 0

        for route in routes:
            cumulative += route.weight
            if r <= cumulative:
                return route.destination

        return routes[0].destination

    def log_request(
        self,
        service_name: str,
        instance_id: str,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        """Log a request."""
        log = {
            "id": str(uuid.uuid4())[:8],
            "service": service_name,
            "instance": instance_id,
            "method": method,
            "path": path,
            "status": status_code,
            "latency_ms": latency_ms,
            "timestamp": time.time(),
        }
        self._request_logs.append(log)

        if len(self._request_logs) > 10000:
            self._request_logs = self._request_logs[-5000:]

    def get_service_stats(self, service_name: str) -> dict:
        """Get statistics for a service."""
        endpoint = self._services.get(service_name)
        if not endpoint:
            return {}

        logs = [l for l in self._request_logs if l["service"] == service_name]

        return {
            "service": service_name,
            "total_instances": len(endpoint.instances),
            "healthy_instances": sum(
                1 for i in endpoint.instances if i.status == ServiceStatus.HEALTHY
            ),
            "unhealthy_instances": sum(
                1 for i in endpoint.instances if i.status == ServiceStatus.UNHEALTHY
            ),
            "circuit_breaker_state": self._circuit_breakers.get(service_name, {}).get("state", "closed"),
            "total_requests": len(logs),
            "failed_requests": sum(1 for l in logs if l["status"] >= 500),
            "avg_latency_ms": sum(l["latency_ms"] for l in logs) / len(logs) if logs else 0,
        }

    def list_services(self) -> list[str]:
        """List all registered services."""
        return list(self._services.keys())

    def get_service_instances(self, service_name: str) -> list[ServiceInstance]:
        """Get all instances of a service."""
        endpoint = self._services.get(service_name)
        return endpoint.instances if endpoint else []
