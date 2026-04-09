"""API Load Balancer.

This module provides load balancing for API endpoints:
- Round-robin, least-connected, random strategies
- Health-aware routing
- Weight-based distribution
- Connection pooling

Example:
    >>> from actions.api_load_balancer_action import LoadBalancer
    >>> lb = LoadBalancer(strategy="round_robin")
    >>> endpoint = lb.get_endpoint()
"""

from __future__ import annotations

import random
import threading
import logging
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"


@dataclass
class Endpoint:
    """An API endpoint."""
    url: str
    weight: int = 1
    max_connections: int = 100
    current_connections: int = 0
    is_healthy: bool = True
    last_health_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class LoadBalancer:
    """Load balancer for API endpoints."""

    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: float = 30.0,
        failure_threshold: int = 3,
    ) -> None:
        """Initialize the load balancer.

        Args:
            strategy: Balancing strategy name.
            health_check_interval: Seconds between health checks.
            failure_threshold: Failures before marking unhealthy.
        """
        self._strategy = LoadBalanceStrategy(strategy)
        self._endpoints: dict[str, Endpoint] = {}
        self._round_robin_index = 0
        self._lock = threading.RLock()
        self._health_check_interval = health_check_interval
        self._failure_threshold = failure_threshold
        self._health_checker: Optional[Callable[[str], bool]] = None
        self._stats = {"requests": 0, "errors": 0}

    def add_endpoint(
        self,
        url: str,
        weight: int = 1,
        max_connections: int = 100,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Endpoint:
        """Add an endpoint to the load balancer.

        Args:
            url: Endpoint URL.
            weight: Weight for weighted strategies.
            max_connections: Maximum concurrent connections.
            metadata: Additional endpoint metadata.

        Returns:
            The created Endpoint.
        """
        endpoint = Endpoint(
            url=url,
            weight=weight,
            max_connections=max_connections,
            metadata=metadata or {},
        )
        with self._lock:
            self._endpoints[url] = endpoint
            logger.info("Added endpoint: %s (weight=%d)", url, weight)
        return endpoint

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint.

        Args:
            url: Endpoint URL.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if url in self._endpoints:
                del self._endpoints[url]
                logger.info("Removed endpoint: %s", url)
                return True
            return False

    def get_endpoint(self, client_ip: Optional[str] = None) -> Optional[Endpoint]:
        """Get the next endpoint based on strategy.

        Args:
            client_ip: Client IP for IP hash strategy.

        Returns:
            Selected Endpoint or None if no healthy endpoints.
        """
        with self._lock:
            self._stats["requests"] += 1
            healthy = [e for e in self._endpoints.values() if e.is_healthy and e.current_connections < e.max_connections]

            if not healthy:
                self._stats["errors"] += 1
                return None

            if self._strategy == LoadBalanceStrategy.ROUND_ROBIN:
                return self._round_robin(healthy)
            elif self._strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
                return min(healthy, key=lambda e: e.current_connections)
            elif self._strategy == LoadBalanceStrategy.RANDOM:
                return random.choice(healthy)
            elif self._strategy == LoadBalanceStrategy.WEIGHTED:
                return self._weighted(healthy)
            elif self._strategy == LoadBalanceStrategy.IP_HASH:
                return self._ip_hash(healthy, client_ip or "")
            else:
                return self._round_robin(healthy)

    def _round_robin(self, endpoints: list[Endpoint]) -> Endpoint:
        """Round-robin selection."""
        if self._round_robin_index >= len(endpoints):
            self._round_robin_index = 0
        endpoint = endpoints[self._round_robin_index]
        self._round_robin_index += 1
        return endpoint

    def _weighted(self, endpoints: list[Endpoint]) -> Endpoint:
        """Weighted selection."""
        total_weight = sum(e.weight for e in endpoints)
        if total_weight == 0:
            return endpoints[0]
        r = random.randint(1, total_weight)
        cumsum = 0
        for e in endpoints:
            cumsum += e.weight
            if r <= cumsum:
                return e
        return endpoints[-1]

    def _ip_hash(self, endpoints: list[Endpoint], client_ip: str) -> Endpoint:
        """IP-based hash selection."""
        if not client_ip:
            return endpoints[0]
        hash_val = sum(ord(c) for c in client_ip)
        idx = hash_val % len(endpoints)
        return endpoints[idx]

    def connection_start(self, url: str) -> None:
        """Mark a connection starting to an endpoint.

        Args:
            url: Endpoint URL.
        """
        with self._lock:
            endpoint = self._endpoints.get(url)
            if endpoint:
                endpoint.current_connections += 1

    def connection_end(self, url: str) -> None:
        """Mark a connection ending to an endpoint.

        Args:
            url: Endpoint URL.
        """
        with self._lock:
            endpoint = self._endpoints.get(url)
            if endpoint and endpoint.current_connections > 0:
                endpoint.current_connections -= 1

    def mark_failure(self, url: str) -> None:
        """Mark an endpoint failure.

        Args:
            url: Endpoint URL.
        """
        with self._lock:
            endpoint = self._endpoints.get(url)
            if endpoint:
                endpoint.consecutive_failures += 1
                if endpoint.consecutive_failures >= self._failure_threshold:
                    endpoint.is_healthy = False
                    logger.warning("Endpoint marked unhealthy: %s", url)

    def mark_success(self, url: str) -> None:
        """Mark an endpoint success (recovery).

        Args:
            url: Endpoint URL.
        """
        with self._lock:
            endpoint = self._endpoints.get(url)
            if endpoint:
                endpoint.consecutive_failures = 0
                if not endpoint.is_healthy:
                    endpoint.is_healthy = True
                    logger.info("Endpoint recovered: %s", url)

    def list_endpoints(self) -> list[Endpoint]:
        """List all endpoints."""
        with self._lock:
            return list(self._endpoints.values())

    def get_stats(self) -> dict[str, Any]:
        """Get load balancer statistics."""
        with self._lock:
            healthy = sum(1 for e in self._endpoints.values() if e.is_healthy)
            total_conn = sum(e.current_connections for e in self._endpoints.values())
            return {
                **self._stats,
                "total_endpoints": len(self._endpoints),
                "healthy_endpoints": healthy,
                "total_connections": total_conn,
            }
