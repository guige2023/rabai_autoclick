"""Service mesh utilities: load balancing, service registry, and traffic management."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "ServiceInstance",
    "ServiceRegistry",
    "LoadBalancer",
    "CircuitBreaker",
]


@dataclass
class ServiceInstance:
    """A service instance in the mesh."""

    id: str
    name: str
    host: str
    port: int
    healthy: bool = True
    weight: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


class ServiceRegistry:
    """Thread-safe service registry."""

    def __init__(self) -> None:
        self._services: dict[str, list[ServiceInstance]] = {}
        self._lock = threading.RLock()

    def register(self, instance: ServiceInstance) -> None:
        with self._lock:
            self._services.setdefault(instance.name, []).append(instance)

    def deregister(self, name: str, instance_id: str) -> bool:
        with self._lock:
            instances = self._services.get(name, [])
            self._services[name] = [i for i in instances if i.id != instance_id]
            return True

    def get_all(self, name: str) -> list[ServiceInstance]:
        with self._lock:
            return list(self._services.get(name, []))

    def get_healthy(self, name: str) -> list[ServiceInstance]:
        with self._lock:
            return [i for i in self._services.get(name, []) if i.healthy]


class LoadBalancer:
    """Load balancer with multiple strategies."""

    def __init__(self, registry: ServiceRegistry) -> None:
        self.registry = registry
        self._round_robin_index: dict[str, int] = {}
        self._lock = threading.Lock()

    def pick(self, service_name: str, strategy: str = "round_robin") -> ServiceInstance | None:
        instances = self.registry.get_healthy(service_name)
        if not instances:
            return None

        if strategy == "random":
            import random
            return random.choice(instances)

        elif strategy == "round_robin":
            with self._lock:
                idx = self._round_robin_index.get(service_name, 0)
                self._round_robin_index[service_name] = (idx + 1) % len(instances)
                return instances[idx]

        elif strategy == "weighted":
            total_weight = sum(i.weight for i in instances)
            import random
            r = random.randint(1, total_weight)
            cumulative = 0
            for inst in instances:
                cumulative += inst.weight
                if cumulative >= r:
                    return inst
            return instances[-1]

        return instances[0]


class CircuitBreaker:
    """Circuit breaker for service mesh fault tolerance."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = "closed"
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == "open":
                if self._last_failure_time and (
                    time.time() - self._last_failure_time
                ) >= self.recovery_timeout:
                    self._state = "half_open"
            return self._state

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = "closed"

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = "open"

    def allow_request(self) -> bool:
        return self.state != "open"
