"""
Load balancer action for distributing requests across multiple backends.

This module provides actions for load balancing strategies including
round-robin, least connections, IP hash, and weighted distribution.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class LoadBalancingStrategy(Enum):
    """Load balancing algorithm strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    WEIGHTED = "weighted"
    RANDOM = "random"
    LEAST_RESPONSE_TIME = "least_response_time"
    CONSISTENT_HASH = "consistent_hash"


class BackendStatus(Enum):
    """Status of a backend server."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    MAINTENANCE = "maintenance"


@dataclass
class Backend:
    """Represents a backend server."""
    id: str
    host: str
    port: int
    weight: int = 1
    max_connections: Optional[int] = None
    status: BackendStatus = BackendStatus.HEALTHY
    current_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_response_time_ms: float = 0.0
    last_health_check: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def avg_response_time_ms(self) -> float:
        """Calculate average response time."""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time_ms / self.total_requests

    @property
    def health_score(self) -> float:
        """Calculate health score (0.0 to 1.0)."""
        if self.total_requests == 0:
            return 1.0
        success_rate = 1.0 - (self.failed_requests / self.total_requests)
        return max(0.0, success_rate)

    def to_dict(self) -> Dict[str, Any]:
        """Convert backend to dictionary."""
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "weight": self.weight,
            "status": self.status.value,
            "current_connections": self.current_connections,
            "total_requests": self.total_requests,
            "avg_response_time_ms": self.avg_response_time_ms,
            "health_score": self.health_score,
            "last_health_check": (
                self.last_health_check.isoformat()
                if self.last_health_check else None
            ),
        }


@dataclass
class Request:
    """Represents a request to be load balanced."""
    client_ip: str
    path: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoadBalancerConfig:
    """Configuration for the load balancer."""
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    health_check_interval: int = 30
    health_check_timeout: int = 5
    max_retries: int = 3
    retry_on_failure: bool = True
    connection_timeout: int = 10
    read_timeout: int = 60
    enable_metrics: bool = True


class LoadBalancer:
    """
    Load balancer with multiple distribution strategies.

    Supports round-robin, least connections, IP hash, and weighted
    distribution with health checking and metrics.
    """

    def __init__(self, config: Optional[LoadBalancerConfig] = None):
        """
        Initialize the load balancer.

        Args:
            config: Load balancer configuration.
        """
        self.config = config or LoadBalancerConfig()
        self._backends: Dict[str, Backend] = {}
        self._lock = threading.RLock()
        self._round_robin_index = 0
        self._request_counts: Dict[str, int] = defaultdict(int)
        self._active_requests: Dict[str, Dict[str, Any]] = {}

    def add_backend(
        self,
        backend_id: str,
        host: str,
        port: int,
        weight: int = 1,
        max_connections: Optional[int] = None,
    ) -> Backend:
        """
        Add a backend server.

        Args:
            backend_id: Unique identifier.
            host: Backend host.
            port: Backend port.
            weight: Weight for weighted strategies.
            max_connections: Maximum concurrent connections.

        Returns:
            The created Backend.
        """
        with self._lock:
            backend = Backend(
                id=backend_id,
                host=host,
                port=port,
                weight=weight,
                max_connections=max_connections,
            )
            self._backends[backend_id] = backend
            return backend

    def remove_backend(self, backend_id: str) -> bool:
        """Remove a backend server."""
        with self._lock:
            if backend_id in self._backends:
                del self._backends[backend_id]
                return True
            return False

    def update_backend(
        self,
        backend_id: str,
        weight: Optional[int] = None,
        status: Optional[BackendStatus] = None,
        max_connections: Optional[int] = None,
    ) -> Optional[Backend]:
        """Update a backend's configuration."""
        with self._lock:
            backend = self._backends.get(backend_id)
            if not backend:
                return None

            if weight is not None:
                backend.weight = weight
            if status is not None:
                backend.status = status
            if max_connections is not None:
                backend.max_connections = max_connections

            return backend

    def get_backend(self, backend_id: str) -> Optional[Backend]:
        """Get a backend by ID."""
        with self._lock:
            return self._backends.get(backend_id)

    def list_backends(
        self,
        status: Optional[BackendStatus] = None,
    ) -> List[Backend]:
        """List backends, optionally filtered by status."""
        with self._lock:
            backends = list(self._backends.values())
            if status:
                backends = [b for b in backends if b.status == status]
            return backends

    def select_backend(self, request: Optional[Request] = None) -> Optional[Backend]:
        """
        Select a backend using the configured strategy.

        Args:
            request: Optional request for context (IP hash, etc.).

        Returns:
            Selected Backend or None if no healthy backends.
        """
        with self._lock:
            healthy = [
                b for b in self._backends.values()
                if b.status == BackendStatus.HEALTHY
            ]

            if not healthy:
                return None

            if self.config.strategy == LoadBalancingStrategy.ROUND_ROBIN:
                return self._select_round_robin(healthy)
            elif self.config.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
                return self._select_least_connections(healthy)
            elif self.config.strategy == LoadBalancingStrategy.IP_HASH:
                return self._select_ip_hash(healthy, request)
            elif self.config.strategy == LoadBalancingStrategy.WEIGHTED:
                return self._select_weighted(healthy)
            elif self.config.strategy == LoadBalancingStrategy.RANDOM:
                return self._select_random(healthy)
            elif self.config.strategy == LoadBalancingStrategy.LEAST_RESPONSE_TIME:
                return self._select_least_response_time(healthy)
            else:
                return self._select_round_robin(healthy)

    def _select_round_robin(self, backends: List[Backend]) -> Backend:
        """Round-robin selection."""
        if not backends:
            return None
        index = self._round_robin_index % len(backends)
        self._round_robin_index += 1
        return backends[index]

    def _select_least_connections(self, backends: List[Backend]) -> Backend:
        """Select backend with fewest active connections."""
        if not backends:
            return None
        return min(backends, key=lambda b: b.current_connections)

    def _select_ip_hash(self, backends: List[Backend], request: Optional[Request]) -> Backend:
        """IP hash-based selection for session affinity."""
        if not backends:
            return None
        if not request:
            return backends[0]

        ip = request.client_ip
        hash_val = int(hashlib.md5(ip.encode()).hexdigest(), 16)
        index = hash_val % len(backends)
        return backends[index]

    def _select_weighted(self, backends: List[Backend]) -> Backend:
        """Weighted selection based on backend weights."""
        if not backends:
            return None

        total_weight = sum(b.weight for b in backends)
        if total_weight == 0:
            return backends[0]

        import random
        rand_val = random.randint(1, total_weight)

        cumulative = 0
        for backend in backends:
            cumulative += backend.weight
            if rand_val <= cumulative:
                return backend

        return backends[-1]

    def _select_random(self, backends: List[Backend]) -> Backend:
        """Random selection."""
        if not backends:
            return None
        import random
        return random.choice(backends)

    def _select_least_response_time(self, backends: List[Backend]) -> Backend:
        """Select backend with lowest average response time."""
        if not backends:
            return None
        return min(backends, key=lambda b: b.avg_response_time_ms)

    def record_request(
        self,
        backend_id: str,
        response_time_ms: float,
        success: bool = True,
    ) -> None:
        """
        Record a request for a backend.

        Args:
            backend_id: Backend that handled the request.
            response_time_ms: Response time in milliseconds.
            success: Whether the request was successful.
        """
        with self._lock:
            backend = self._backends.get(backend_id)
            if not backend:
                return

            backend.total_requests += 1
            backend.total_response_time_ms += response_time_ms
            if not success:
                backend.failed_requests += 1

    def increment_connections(self, backend_id: str) -> bool:
        """Increment connection count for a backend."""
        with self._lock:
            backend = self._backends.get(backend_id)
            if not backend:
                return False
            if backend.max_connections and backend.current_connections >= backend.max_connections:
                return False
            backend.current_connections += 1
            return True

    def decrement_connections(self, backend_id: str) -> None:
        """Decrement connection count for a backend."""
        with self._lock:
            backend = self._backends.get(backend_id)
            if backend and backend.current_connections > 0:
                backend.current_connections -= 1

    def health_check(self, backend_id: str) -> bool:
        """
        Perform health check on a backend.

        Args:
            backend_id: Backend to check.

        Returns:
            True if backend is healthy.
        """
        import socket

        backend = self._backends.get(backend_id)
        if not backend:
            return False

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.config.health_check_timeout)
            result = sock.connect_ex((backend.host, backend.port))
            sock.close()

            is_healthy = result == 0
            backend.last_health_check = datetime.now()

            if backend.status != BackendStatus.DRAINING:
                backend.status = (
                    BackendStatus.HEALTHY if is_healthy else BackendStatus.UNHEALTHY
                )

            return is_healthy

        except Exception:
            backend.status = BackendStatus.UNHEALTHY
            backend.last_health_check = datetime.now()
            return False

    def health_check_all(self) -> Dict[str, bool]:
        """Perform health checks on all backends."""
        results = {}
        for backend_id in self._backends:
            results[backend_id] = self.health_check(backend_id)
        return results

    def get_metrics(self) -> Dict[str, Any]:
        """Get load balancer metrics."""
        with self._lock:
            backends = list(self._backends.values())
            total_requests = sum(b.total_requests for b in backends)
            total_failed = sum(b.failed_requests for b in backends)

            avg_response_time = 0.0
            if backends:
                avg_response_time = sum(b.avg_response_time_ms for b in backends) / len(backends)

            return {
                "total_backends": len(backends),
                "healthy_backends": sum(1 for b in backends if b.status == BackendStatus.HEALTHY),
                "unhealthy_backends": sum(1 for b in backends if b.status == BackendStatus.UNHEALTHY),
                "total_requests": total_requests,
                "failed_requests": total_failed,
                "success_rate": (
                    (total_requests - total_failed) / total_requests * 100
                    if total_requests > 0 else 100.0
                ),
                "avg_response_time_ms": avg_response_time,
                "strategy": self.config.strategy.value,
                "backends": [b.to_dict() for b in backends],
            }


def load_balancer_add_backend_action(
    backend_id: str,
    host: str,
    port: int,
    weight: int = 1,
) -> Dict[str, Any]:
    """Add a backend to the load balancer."""
    lb = LoadBalancer()
    backend = lb.add_backend(backend_id, host, port, weight)
    return backend.to_dict()


def load_balancer_select_action(
    client_ip: Optional[str] = None,
    strategy: str = "round_robin",
) -> Optional[Dict[str, Any]]:
    """Select a backend using the specified strategy."""
    strategy_map = {
        "round_robin": LoadBalancingStrategy.ROUND_ROBIN,
        "least_connections": LoadBalancingStrategy.LEAST_CONNECTIONS,
        "ip_hash": LoadBalancingStrategy.IP_HASH,
        "weighted": LoadBalancingStrategy.WEIGHTED,
        "random": LoadBalancingStrategy.RANDOM,
        "least_response_time": LoadBalancingStrategy.LEAST_RESPONSE_TIME,
    }

    if strategy.lower() not in strategy_map:
        raise ValueError(f"Unknown strategy: {strategy}")

    config = LoadBalancerConfig(strategy=strategy_map[strategy.lower()])
    lb = LoadBalancer(config)

    request = None
    if client_ip:
        request = Request(client_ip=client_ip, path="/", method="GET")

    backend = lb.select_backend(request)
    if backend:
        return backend.to_dict()
    return None


def load_balancer_metrics_action() -> Dict[str, Any]:
    """Get load balancer metrics."""
    lb = LoadBalancer()
    return lb.get_metrics()
