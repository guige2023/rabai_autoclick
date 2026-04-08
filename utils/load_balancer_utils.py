"""
Load balancing strategy implementations.

Provides round-robin, weighted, least-connections, and consistent hashing.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from typing import Callable, Protocol


class Backend(Protocol):
    """Protocol for backend servers."""
    @property
    def weight(self) -> int: ...


@dataclass
class Server:
    """Simple backend server representation."""
    host: str
    port: int
    weight: int = 1

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class BackendStats:
    """Statistics for a backend."""
    host: str
    port: int
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0


class RoundRobinBalancer:
    """Round-robin load balancer."""

    def __init__(self, backends: list[Server]):
        self.backends = backends
        self._index = 0
        self._lock = threading.Lock()

    def select(self) -> Server:
        """Select next backend in round-robin order."""
        with self._lock:
            backend = self.backends[self._index]
            self._index = (self._index + 1) % len(self.backends)
            return backend

    def add_backend(self, backend: Server) -> None:
        self.backends.append(backend)

    def remove_backend(self, host: str, port: int) -> bool:
        for i, b in enumerate(self.backends):
            if b.host == host and b.port == port:
                self.backends.pop(i)
                return True
        return False


class WeightedRoundRobinBalancer:
    """Weighted round-robin load balancer."""

    def __init__(self, backends: list[Server]):
        self.backends = backends
        self._current_weights: dict[int, int] = {i: b.weight for i, b in enumerate(backends)}
        self._index = 0
        self._lock = threading.Lock()

    def select(self) -> Server:
        """Select backend using weighted round-robin."""
        with self._lock:
            while True:
                total_weight = sum(b.weight for b in self.backends)
                if total_weight == 0:
                    return self.backends[self._index % len(self.backends)]

                idx = self._index % len(self.backends)
                weight_sum = sum(self.backends[i].weight for i in range(idx + 1))

                selected = idx
                for i in range(len(self.backends)):
                    candidate = (idx + i) % len(self.backends)
                    if self._current_weights[candidate] > 0:
                        selected = candidate
                        self._current_weights[candidate] -= 1
                        break

                self._index += 1
                if self._current_weights[selected] > 0 or sum(self._current_weights.values()) == 0:
                    return self.backends[selected]

                if all(w == 0 for w in self._current_weights.values()):
                    self._current_weights = {i: b.weight for i, b in enumerate(self.backends)}
                    return self.backends[selected]


class LeastConnectionsBalancer:
    """Least connections load balancer."""

    def __init__(self, backends: list[Server]):
        self.backends = backends
        self._stats: dict[str, int] = {str(b): 0 for b in backends}
        self._lock = threading.Lock()

    def select(self) -> Server:
        """Select backend with fewest active connections."""
        with self._lock:
            backend = min(self.backends, key=lambda b: self._stats[str(b)])
            self._stats[str(backend)] += 1
            return backend

    def release(self, backend: Server) -> None:
        """Decrement connection count on release."""
        with self._lock:
            key = str(backend)
            if self._stats.get(key, 0) > 0:
                self._stats[key] -= 1


class ConsistentHashBalancer:
    """Consistent hashing load balancer."""

    def __init__(self, backends: list[Server], virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, Server] = {}
        self._sorted_keys: list[int] = []
        self._backends = backends
        self._build_ring()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self) -> None:
        self._ring.clear()
        for backend in self._backends:
            for i in range(self.virtual_nodes):
                hash_key = self._hash(f"{backend.host}:{backend.port}:{i}")
                self._ring[hash_key] = backend
        self._sorted_keys = sorted(self._ring.keys())

    def select(self, key: str | None = None) -> Server:
        """Select backend for a given key."""
        if key is None:
            key = str(id(object()))
        hash_val = self._hash(key)
        for k in self._sorted_keys:
            if k >= hash_val:
                return self._ring[k]
        return self._ring[self._sorted_keys[0]]

    def add_backend(self, backend: Server) -> None:
        self._backends.append(backend)
        self._build_ring()

    def remove_backend(self, host: str, port: int) -> bool:
        for i, b in enumerate(self._backends):
            if b.host == host and b.port == port:
                self._backends.pop(i)
                self._build_ring()
                return True
        return False


class HealthCheckBalancer:
    """Load balancer with health check awareness."""

    def __init__(
        self,
        backends: list[Server],
        health_check: Callable[[Server], bool],
        strategy: Literal["round-robin", "least-conn"] = "round-robin",
    ):
        self.health_check = health_check
        self._healthy_backends: list[Server] = []
        self._lock = threading.Lock()

        if strategy == "round-robin":
            self._balancer = RoundRobinBalancer(backends)
        else:
            self._balancer = LeastConnectionsBalancer(backends)

        self._refresh_healthy()

    def _refresh_healthy(self) -> None:
        healthy = [b for b in self._balancer.backends if self.health_check(b)]
        with self._lock:
            self._healthy_backends = healthy

    def select(self) -> Server | None:
        """Select a healthy backend."""
        self._refresh_healthy()
        if not self._healthy_backends:
            return None
        with self._lock:
            import random
            return random.choice(self._healthy_backends)
