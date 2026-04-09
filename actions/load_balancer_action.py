"""Load balancer for distributing API requests.

This module provides load balancing strategies:
- Round robin
- Least connections
- Weighted distribution
- Health-aware routing

Example:
    >>> from actions.load_balancer_action import LoadBalancer
    >>> lb = LoadBalancer(endpoints=["api1:8000", "api2:8000"])
    >>> endpoint = lb.get_endpoint()
"""

from __future__ import annotations

import time
import random
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class Endpoint:
    """An API endpoint."""
    url: str
    weight: int = 1
    max_connections: int = 100
    health_check_url: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EndpointStats:
    """Statistics for an endpoint."""
    url: str
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    last_used: float = 0.0
    last_failure: Optional[float] = None
    is_healthy: bool = True


class LoadBalancer:
    """Load balancer for distributing requests across endpoints.

    Example:
        >>> lb = LoadBalancer(strategy="least_connections")
        >>> lb.add_endpoint("api1:8000", weight=2)
        >>> lb.add_endpoint("api2:8000")
        >>> endpoint = lb.get_endpoint()
    """

    STRATEGIES = [
        "round_robin",
        "random",
        "least_connections",
        "weighted",
        "health_aware",
        "ip_hash",
    ]

    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: float = 30.0,
    ) -> None:
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}")
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        self._endpoints: dict[str, Endpoint] = {}
        self._stats: dict[str, EndpointStats] = {}
        self._round_robin_index = 0
        self._lock = threading.RLock()
        logger.info(f"LoadBalancer initialized with strategy: {strategy}")

    def add_endpoint(
        self,
        url: str,
        weight: int = 1,
        max_connections: int = 100,
        health_check_url: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add an endpoint to the load balancer.

        Args:
            url: Endpoint URL.
            weight: Endpoint weight for weighted distribution.
            max_connections: Maximum concurrent connections.
            health_check_url: URL for health checks.
            metadata: Additional metadata.
        """
        endpoint = Endpoint(
            url=url,
            weight=weight,
            max_connections=max_connections,
            health_check_url=health_check_url,
            metadata=metadata or {},
        )
        self._endpoints[url] = endpoint
        self._stats[url] = EndpointStats(url=url)
        logger.info(f"Added endpoint: {url} (weight={weight})")

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint.

        Returns:
            True if removed successfully.
        """
        with self._lock:
            if url in self._endpoints:
                del self._endpoints[url]
                del self._stats[url]
                return True
            return False

    def get_endpoint(self, key: Optional[str] = None) -> Optional[str]:
        """Get an endpoint based on the strategy.

        Args:
            key: Optional key for consistent hashing strategies.

        Returns:
            Selected endpoint URL or None.
        """
        with self._lock:
            healthy = self._get_healthy_endpoints()
            if not healthy:
                return None
            if self.strategy == "round_robin":
                return self._round_robin(healthy)
            elif self.strategy == "random":
                return self._random(healthy)
            elif self.strategy == "least_connections":
                return self._least_connections(healthy)
            elif self.strategy == "weighted":
                return self._weighted(healthy)
            elif self.strategy == "health_aware":
                return self._health_aware(healthy)
            elif self.strategy == "ip_hash":
                return self._ip_hash(healthy, key or "")
            return healthy[0] if healthy else None

    def _get_healthy_endpoints(self) -> list[str]:
        """Get list of healthy endpoints."""
        return [
            url for url, stats in self._stats.items()
            if stats.is_healthy and stats.active_connections < self._endpoints[url].max_connections
        ]

    def _round_robin(self, endpoints: list[str]) -> str:
        """Round-robin selection."""
        endpoint = endpoints[self._round_robin_index % len(endpoints)]
        self._round_robin_index += 1
        return endpoint

    def _random(self, endpoints: list[str]) -> str:
        """Random selection."""
        return random.choice(endpoints)

    def _least_connections(self, endpoints: list[str]) -> str:
        """Select endpoint with least active connections."""
        return min(endpoints, key=lambda url: self._stats[url].active_connections)

    def _weighted(self, endpoints: list[str]) -> str:
        """Weighted selection based on endpoint weights."""
        weights = [self._endpoints[url].weight for url in endpoints]
        total = sum(weights)
        r = random.randint(1, total)
        cumulative = 0
        for i, url in enumerate(endpoints):
            cumulative += weights[i]
            if r <= cumulative:
                return url
        return endpoints[-1]

    def _health_aware(self, endpoints: list[str]) -> str:
        """Health-aware selection favoring healthier endpoints."""
        scores = {}
        for url in endpoints:
            stats = self._stats[url]
            if stats.total_requests == 0:
                scores[url] = 1.0
            else:
                error_rate = stats.failed_requests / stats.total_requests
                avg_latency = stats.total_latency / stats.total_requests
                health_score = (1 - error_rate) / (1 + avg_latency / 1000)
                scores[url] = health_score
        return max(endpoints, key=lambda url: scores[url])

    def _ip_hash(self, endpoints: list[str], key: str) -> str:
        """Consistent hash based on key."""
        if not key:
            return random.choice(endpoints)
        import hashlib
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return endpoints[hash_val % len(endpoints)]

    def record_request_start(self, url: str) -> None:
        """Record the start of a request to an endpoint."""
        with self._lock:
            if url in self._stats:
                self._stats[url].active_connections += 1
                self._stats[url].total_requests += 1
                self._stats[url].last_used = time.time()

    def record_request_end(
        self,
        url: str,
        success: bool = True,
        latency: float = 0.0,
    ) -> None:
        """Record the end of a request to an endpoint."""
        with self._lock:
            if url in self._stats:
                stats = self._stats[url]
                stats.active_connections = max(0, stats.active_connections - 1)
                stats.total_latency += latency
                if not success:
                    stats.failed_requests += 1
                    stats.last_failure = time.time()
                    if stats.failed_requests >= 10:
                        stats.is_healthy = False

    def set_endpoint_health(self, url: str, is_healthy: bool) -> None:
        """Manually set endpoint health status."""
        with self._lock:
            if url in self._stats:
                self._stats[url].is_healthy = is_healthy

    def get_stats(self, url: str) -> Optional[EndpointStats]:
        """Get statistics for an endpoint."""
        return self._stats.get(url)

    def get_all_stats(self) -> dict[str, EndpointStats]:
        """Get all endpoint statistics."""
        return dict(self._stats)
