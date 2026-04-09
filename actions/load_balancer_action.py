"""Load balancer action for distributing requests across endpoints.

Provides round-robin, least-connected, and weighted load balancing
with health checking.
"""

import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    RANDOM = "random"
    IP_HASH = "ip_hash"


@dataclass
class Endpoint:
    url: str
    weight: int = 1
    max_connections: int = 100
    active_connections: int = 0
    is_healthy: bool = True
    last_health_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0


@dataclass
class HealthCheckResult:
    endpoint_url: str
    is_healthy: bool
    latency_ms: float
    error: Optional[str] = None


class LoadBalancerAction:
    """Load balancer with multiple strategies and health checking.

    Args:
        strategy: Load balancing strategy.
        health_check_interval: Health check interval in seconds.
        unhealthy_threshold: Failures before marking unhealthy.
    """

    def __init__(
        self,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        health_check_interval: float = 30.0,
        unhealthy_threshold: int = 3,
    ) -> None:
        self._endpoints: dict[str, Endpoint] = {}
        self._strategy = strategy
        self._health_check_interval = health_check_interval
        self._unhealthy_threshold = unhealthy_threshold
        self._current_index: int = 0
        self._request_count: int = 0
        self._health_check_handlers: list[Callable] = []

    def add_endpoint(
        self,
        url: str,
        weight: int = 1,
        max_connections: int = 100,
    ) -> bool:
        """Add an endpoint to the load balancer.

        Args:
            url: Endpoint URL.
            weight: Endpoint weight.
            max_connections: Maximum concurrent connections.

        Returns:
            True if added successfully.
        """
        if url in self._endpoints:
            logger.warning(f"Endpoint already exists: {url}")
            return False

        self._endpoints[url] = Endpoint(
            url=url,
            weight=weight,
            max_connections=max_connections,
        )
        logger.debug(f"Added endpoint: {url}")
        return True

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint from the load balancer.

        Args:
            url: Endpoint URL.

        Returns:
            True if removed.
        """
        if url in self._endpoints:
            del self._endpoints[url]
            return True
        return False

    def select_endpoint(self, client_ip: Optional[str] = None) -> Optional[str]:
        """Select an endpoint based on load balancing strategy.

        Args:
            client_ip: Optional client IP for IP hash.

        Returns:
            Selected endpoint URL or None.
        """
        healthy = [e for e in self._endpoints.values() if e.is_healthy and e.active_connections < e.max_connections]
        if not healthy:
            logger.warning("No healthy endpoints available")
            return None

        if self._strategy == LoadBalanceStrategy.ROUND_ROBIN:
            return self._round_robin_select(healthy)
        elif self._strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return self._least_connections_select(healthy)
        elif self._strategy == LoadBalanceStrategy.WEIGHTED:
            return self._weighted_select(healthy)
        elif self._strategy == LoadBalanceStrategy.RANDOM:
            return random.choice(healthy).url
        elif self._strategy == LoadBalanceStrategy.IP_HASH:
            return self._ip_hash_select(healthy, client_ip or "")

        return None

    def _round_robin_select(self, endpoints: list[Endpoint]) -> str:
        """Select endpoint using round-robin.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint URL.
        """
        endpoint = endpoints[self._current_index % len(endpoints)]
        self._current_index += 1
        return endpoint.url

    def _least_connections_select(self, endpoints: list[Endpoint]) -> str:
        """Select endpoint with least connections.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint URL.
        """
        return min(endpoints, key=lambda e: e.active_connections).url

    def _weighted_select(self, endpoints: list[Endpoint]) -> str:
        """Select endpoint using weighted random.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint URL.
        """
        total_weight = sum(e.weight for e in endpoints)
        rand = random.uniform(0, total_weight)
        cumulative = 0
        for e in endpoints:
            cumulative += e.weight
            if rand <= cumulative:
                return e.url
        return endpoints[-1].url

    def _ip_hash_select(self, endpoints: list[Endpoint], client_ip: str) -> str:
        """Select endpoint using IP hash.

        Args:
            endpoints: List of healthy endpoints.
            client_ip: Client IP address.

        Returns:
            Selected endpoint URL.
        """
        hash_value = hash(client_ip) % len(endpoints)
        return endpoints[hash_value].url

    def increment_connections(self, url: str) -> bool:
        """Increment active connections for an endpoint.

        Args:
            url: Endpoint URL.

        Returns:
            True if incremented.
        """
        endpoint = self._endpoints.get(url)
        if endpoint:
            endpoint.active_connections += 1
            return True
        return False

    def decrement_connections(self, url: str) -> bool:
        """Decrement active connections for an endpoint.

        Args:
            url: Endpoint URL.

        Returns:
            True if decremented.
        """
        endpoint = self._endpoints.get(url)
        if endpoint and endpoint.active_connections > 0:
            endpoint.active_connections -= 1
            return True
        return False

    def record_failure(self, url: str) -> None:
        """Record a failure for an endpoint.

        Args:
            url: Endpoint URL.
        """
        endpoint = self._endpoints.get(url)
        if endpoint:
            endpoint.consecutive_failures += 1
            if endpoint.consecutive_failures >= self._unhealthy_threshold:
                endpoint.is_healthy = False
                logger.warning(f"Endpoint marked unhealthy: {url}")

    def record_success(self, url: str) -> None:
        """Record a success for an endpoint.

        Args:
            url: Endpoint URL.
        """
        endpoint = self._endpoints.get(url)
        if endpoint:
            endpoint.consecutive_failures = 0
            if not endpoint.is_healthy:
                endpoint.is_healthy = True
                logger.info(f"Endpoint marked healthy: {url}")

    def check_health(
        self,
        url: str,
        timeout: float = 5.0,
    ) -> HealthCheckResult:
        """Perform health check on an endpoint.

        Args:
            url: Endpoint URL.
            timeout: Check timeout.

        Returns:
            Health check result.
        """
        endpoint = self._endpoints.get(url)
        if not endpoint:
            return HealthCheckResult(url, False, 0, "Endpoint not found")

        start = time.time()
        try:
            import urllib.request
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=timeout)
            latency = (time.time() - start) * 1000
            return HealthCheckResult(url, True, latency)
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheckResult(url, False, latency, str(e))

    def check_all_health(self) -> list[HealthCheckResult]:
        """Check health of all endpoints.

        Returns:
            List of health check results.
        """
        results = []
        for url in self._endpoints:
            result = self.check_health(url)
            results.append(result)

            endpoint = self._endpoints[url]
            endpoint.last_health_check = time.time()

            if result.is_healthy:
                self.record_success(url)
            else:
                self.record_failure(url)

        return results

    def register_health_check_handler(self, handler: Callable) -> None:
        """Register a handler for health check events.

        Args:
            handler: Callback function.
        """
        self._health_check_handlers.append(handler)

    def get_endpoint(self, url: str) -> Optional[Endpoint]:
        """Get endpoint information.

        Args:
            url: Endpoint URL.

        Returns:
            Endpoint object or None.
        """
        return self._endpoints.get(url)

    def get_all_endpoints(self) -> list[Endpoint]:
        """Get all endpoints.

        Returns:
            List of endpoints.
        """
        return list(self._endpoints.values())

    def get_stats(self) -> dict[str, Any]:
        """Get load balancer statistics.

        Returns:
            Dictionary with stats.
        """
        healthy = sum(1 for e in self._endpoints.values() if e.is_healthy)
        total_connections = sum(e.active_connections for e in self._endpoints.values())

        return {
            "total_endpoints": len(self._endpoints),
            "healthy_endpoints": healthy,
            "unhealthy_endpoints": len(self._endpoints) - healthy,
            "total_connections": total_connections,
            "strategy": self._strategy.value,
            "total_requests": self._request_count,
        }
