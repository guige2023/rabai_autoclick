"""
Load balancer module for distributing traffic across multiple backends.

Supports multiple load balancing algorithms, health checks,
active/passive failover, and traffic weighting.
"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class BalanceMode(Enum):
    """Load balancing modes."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOAD = "least_load"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    WEIGHTED = "weighted"
    FAIR = "fair"


@dataclass
class Backend:
    """A backend server."""
    id: str
    host: str
    port: int
    weight: int = 100
    max_connections: int = 100
    current_connections: int = 0
    healthy: bool = True
    latency_ms: float = 0.0
    failures: int = 0
    last_failure: Optional[float] = None
    metadata: dict = field(default_factory=dict)

    def url(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class BackendPool:
    """A pool of backend servers."""
    name: str
    backends: list[Backend] = field(default_factory=list)
    health_check_interval: int = 30
    health_check_url: Optional[str] = None
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    backend_id: str
    healthy: bool
    latency_ms: float
    status_code: Optional[int] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class Request:
    """A load-balanced request."""
    id: str
    client_ip: str
    method: str
    path: str
    backend: Optional[Backend] = None
    status_code: Optional[int] = None
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)


class LoadBalancer:
    """
    Load balancer for distributing traffic across backends.

    Supports multiple load balancing algorithms, health checks,
    and failover.
    """

    def __init__(self, mode: BalanceMode = BalanceMode.ROUND_ROBIN):
        self.mode = mode
        self._pools: dict[str, BackendPool] = {}
        self._round_robin_index: dict[str, int] = defaultdict(int)
        self._requests: list[Request] = []

    def create_pool(self, name: str) -> BackendPool:
        """Create a new backend pool."""
        pool = BackendPool(name=name)
        self._pools[name] = pool
        return pool

    def add_backend(
        self,
        pool_name: str,
        host: str,
        port: int,
        weight: int = 100,
        backend_id: Optional[str] = None,
    ) -> Optional[Backend]:
        """Add a backend to a pool."""
        pool = self._pools.get(pool_name)
        if not pool:
            return None

        backend = Backend(
            id=backend_id or str(uuid.uuid4())[:8],
            host=host,
            port=port,
            weight=weight,
        )
        pool.backends.append(backend)
        return backend

    def remove_backend(self, pool_name: str, backend_id: str) -> bool:
        """Remove a backend from a pool."""
        pool = self._pools.get(pool_name)
        if not pool:
            return False

        pool.backends = [b for b in pool.backends if b.id != backend_id]
        return True

    def select_backend(
        self,
        pool_name: str,
        client_ip: str = "",
    ) -> Optional[Backend]:
        """Select a backend using the configured algorithm."""
        pool = self._pools.get(pool_name)
        if not pool:
            return None

        healthy_backends = [b for b in pool.backends if b.healthy]
        if not healthy_backends:
            return None

        if self.mode == BalanceMode.ROUND_ROBIN:
            idx = self._round_robin_index[pool_name] % len(healthy_backends)
            self._round_robin_index[pool_name] += 1
            return healthy_backends[idx]

        elif self.mode == BalanceMode.LEAST_LOAD:
            return min(healthy_backends, key=lambda b: b.current_connections)

        elif self.mode == BalanceMode.IP_HASH:
            hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
            return healthy_backends[hash_val % len(healthy_backends)]

        elif self.mode == BalanceMode.RANDOM:
            return random.choice(healthy_backends)

        elif self.mode == BalanceMode.WEIGHTED:
            total_weight = sum(b.weight for b in healthy_backends)
            r = random.randint(1, total_weight)
            cumulative = 0
            for backend in healthy_backends:
                cumulative += backend.weight
                if r <= cumulative:
                    return backend
            return healthy_backends[-1]

        return healthy_backends[0]

    def health_check(
        self,
        pool_name: str,
        health_checker: Optional[Callable] = None,
    ) -> list[HealthCheckResult]:
        """Perform health check on all backends in a pool."""
        pool = self._pools.get(pool_name)
        if not pool:
            return []

        results = []

        for backend in pool.backends:
            if health_checker:
                result = health_checker(backend)
            else:
                result = self._default_health_check(backend)

            results.append(result)

            if result.healthy:
                backend.healthy = True
                backend.failures = 0
                backend.latency_ms = result.latency_ms
            else:
                backend.failures += 1
                backend.last_failure = time.time()
                if backend.failures >= pool.unhealthy_threshold:
                    backend.healthy = False

        return results

    def _default_health_check(self, backend: Backend) -> HealthCheckResult:
        """Default TCP health check."""
        import socket

        start = time.time()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((backend.host, backend.port))
            sock.close()

            latency = (time.time() - start) * 1000

            return HealthCheckResult(
                backend_id=backend.id,
                healthy=(result == 0),
                latency_ms=latency,
            )
        except Exception as e:
            return HealthCheckResult(
                backend_id=backend.id,
                healthy=False,
                latency_ms=(time.time() - start) * 1000,
                error=str(e),
            )

    def record_request_start(self, pool_name: str, client_ip: str, method: str, path: str) -> Request:
        """Record the start of a request."""
        backend = self.select_backend(pool_name, client_ip)

        request = Request(
            id=str(uuid.uuid4())[:8],
            client_ip=client_ip,
            method=method,
            path=path,
            backend=backend,
        )

        if backend:
            backend.current_connections += 1

        self._requests.append(request)
        return request

    def record_request_end(
        self,
        request_id: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        """Record the end of a request."""
        for req in reversed(self._requests):
            if req.id == request_id:
                req.status_code = status_code
                req.latency_ms = latency_ms

                if req.backend:
                    req.backend.current_connections = max(0, req.backend.current_connections - 1)

                    if status_code >= 500:
                        req.backend.failures += 1
                    else:
                        req.backend.failures = 0

                break

        max_requests = 10000
        if len(self._requests) > max_requests:
            self._requests = self._requests[-max_requests // 2:]

    def get_pool_stats(self, pool_name: str) -> dict:
        """Get statistics for a pool."""
        pool = self._pools.get(pool_name)
        if not pool:
            return {}

        requests = [r for r in self._requests if r.backend and r.backend.id in [b.id for b in pool.backends]]

        total_requests = len(requests)
        failed_requests = sum(1 for r in requests if r.status_code and r.status_code >= 500)

        return {
            "pool": pool_name,
            "total_backends": len(pool.backends),
            "healthy_backends": sum(1 for b in pool.backends if b.healthy),
            "total_requests": total_requests,
            "failed_requests": failed_requests,
            "failure_rate": failed_requests / total_requests if total_requests > 0 else 0,
            "avg_latency_ms": sum(r.latency_ms for r in requests) / total_requests if requests else 0,
        }

    def get_backend_stats(self, pool_name: str, backend_id: str) -> dict:
        """Get statistics for a specific backend."""
        pool = self._pools.get(pool_name)
        if not pool:
            return {}

        backend = next((b for b in pool.backends if b.id == backend_id), None)
        if not backend:
            return {}

        requests = [r for r in self._requests if r.backend and r.backend.id == backend_id]
        total = len(requests)
        failed = sum(1 for r in requests if r.status_code and r.status_code >= 500)

        return {
            "backend_id": backend_id,
            "host": backend.host,
            "port": backend.port,
            "healthy": backend.healthy,
            "weight": backend.weight,
            "current_connections": backend.current_connections,
            "total_requests": total,
            "failed_requests": failed,
            "failure_rate": failed / total if total > 0 else 0,
            "avg_latency_ms": sum(r.latency_ms for r in requests) / total if requests else 0,
        }

    def list_pools(self) -> list[str]:
        """List all pool names."""
        return list(self._pools.keys())

    def get_pool(self, pool_name: str) -> Optional[BackendPool]:
        """Get a pool by name."""
        return self._pools.get(pool_name)
