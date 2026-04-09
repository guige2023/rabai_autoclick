"""API load balancer and traffic distribution action."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class BalanceStrategy(str, Enum):
    """Load balancing strategy."""

    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"
    LATENCY = "latency"


@dataclass
class Endpoint:
    """An API endpoint."""

    url: str
    weight: float = 1.0
    max_connections: int = 100
    active_connections: int = 0
    last_latency_ms: float = 0
    consecutive_failures: int = 0
    is_healthy: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BalanceResult:
    """Result of load balancing decision."""

    endpoint: Endpoint
    strategy: BalanceStrategy
    timestamp: float = field(default_factory=time.time)


@dataclass
class EndpointStats:
    """Statistics for an endpoint."""

    url: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0
    avg_latency_ms: float = 0
    current_concurrency: int = 0


class APILoadBalancerAction:
    """Distributes API traffic across multiple endpoints."""

    def __init__(
        self,
        strategy: BalanceStrategy = BalanceStrategy.ROUND_ROBIN,
        health_check_interval_seconds: float = 30.0,
        failure_threshold: int = 3,
    ):
        """Initialize load balancer.

        Args:
            strategy: Default balancing strategy.
            health_check_interval_seconds: Interval between health checks.
            failure_threshold: Failures before marking endpoint unhealthy.
        """
        self._strategy = strategy
        self._endpoints: dict[str, Endpoint] = {}
        self._round_robin_index: int = 0
        self._request_counts: dict[str, int] = {}
        self._health_check_interval = health_check_interval_seconds
        self._failure_threshold = failure_threshold
        self._on_endpoint_unhealthy: Optional[Callable[[Endpoint], None]] = None
        self._on_endpoint_healthy: Optional[Callable[[Endpoint], None]] = None

    def add_endpoint(self, endpoint: Endpoint) -> None:
        """Add an endpoint to the pool."""
        self._endpoints[endpoint.url] = endpoint
        self._request_counts[endpoint.url] = 0

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint from the pool."""
        self._request_counts.pop(url, None)
        return self._endpoints.pop(url, None) is not None

    def get_endpoint(self, client_ip: Optional[str] = None) -> BalanceResult:
        """Get the best endpoint based on strategy.

        Args:
            client_ip: Client IP for IP_HASH strategy.

        Returns:
            BalanceResult with selected endpoint.
        """
        healthy = [e for e in self._endpoints.values() if e.is_healthy]
        if not healthy:
            raise RuntimeError("No healthy endpoints available")

        endpoint: Optional[Endpoint] = None

        if self._strategy == BalanceStrategy.ROUND_ROBIN:
            endpoint = self._round_robin(healthy)
        elif self._strategy == BalanceStrategy.LEAST_CONNECTIONS:
            endpoint = self._least_connections(healthy)
        elif self._strategy == BalanceStrategy.RANDOM:
            endpoint = random.choice(healthy)
        elif self._strategy == BalanceStrategy.WEIGHTED:
            endpoint = self._weighted(healthy)
        elif self._strategy == BalanceStrategy.IP_HASH:
            endpoint = self._ip_hash(healthy, client_ip or "")
        elif self._strategy == BalanceStrategy.LATENCY:
            endpoint = self._latency_based(healthy)

        if not endpoint:
            endpoint = healthy[0]

        self._request_counts[endpoint.url] += 1
        endpoint.active_connections += 1

        return BalanceResult(endpoint=endpoint, strategy=self._strategy)

    def _round_robin(self, endpoints: list[Endpoint]) -> Endpoint:
        """Round-robin selection."""
        index = self._round_robin_index % len(endpoints)
        self._round_robin_index += 1
        return endpoints[index]

    def _least_connections(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select endpoint with least active connections."""
        return min(endpoints, key=lambda e: e.active_connections)

    def _weighted(self, endpoints: list[Endpoint]) -> Endpoint:
        """Weighted random selection."""
        weights = [e.weight for e in endpoints]
        total = sum(weights)
        normalized = [w / total for w in weights]
        return random.choices(endpoints, weights=normalized, k=1)[0]

    def _ip_hash(self, endpoints: list[Endpoint], client_ip: str) -> Endpoint:
        """Consistent hashing based on client IP."""
        hash_val = hash(client_ip) % len(endpoints)
        return endpoints[hash_val]

    def _latency_based(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select endpoint with lowest latency."""
        return min(endpoints, key=lambda e: e.last_latency_ms)

    def release_endpoint(self, endpoint: Endpoint) -> None:
        """Release an endpoint after request completes."""
        endpoint.active_connections = max(0, endpoint.active_connections - 1)

    def record_success(self, endpoint: Endpoint, latency_ms: float) -> None:
        """Record a successful request."""
        endpoint.last_latency_ms = latency_ms
        endpoint.consecutive_failures = 0
        if endpoint.active_connections > 0:
            endpoint.active_connections -= 1

    def record_failure(self, endpoint: Endpoint) -> None:
        """Record a failed request."""
        endpoint.consecutive_failures += 1
        endpoint.active_connections = max(0, endpoint.active_connections - 1)

        if endpoint.consecutive_failures >= self._failure_threshold:
            endpoint.is_healthy = False
            if self._on_endpoint_unhealthy:
                self._on_endpoint_unhealthy(endpoint)

    def record_latency(self, endpoint: Endpoint, latency_ms: float) -> None:
        """Record latency for an endpoint."""
        endpoint.last_latency_ms = latency_ms

    def get_stats(self) -> dict[str, EndpointStats]:
        """Get statistics for all endpoints."""
        stats = {}
        for url, endpoint in self._endpoints.items():
            stats[url] = EndpointStats(
                url=url,
                total_requests=self._request_counts.get(url, 0),
                current_concurrency=endpoint.active_connections,
                avg_latency_ms=endpoint.last_latency_ms,
            )
        return stats

    def get_healthy_count(self) -> int:
        """Get count of healthy endpoints."""
        return sum(1 for e in self._endpoints.values() if e.is_healthy)

    def get_total_requests(self) -> int:
        """Get total requests processed."""
        return sum(self._request_counts.values())

    def set_strategy(self, strategy: BalanceStrategy) -> None:
        """Change balancing strategy."""
        self._strategy = strategy
