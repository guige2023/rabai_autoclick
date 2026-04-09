"""API load balancer action for distributing requests across endpoints.

Distributes API requests across multiple endpoints using
various load balancing strategies with health awareness.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"


@dataclass
class Endpoint:
    """An API endpoint for load balancing."""
    url: str
    weight: int = 1
    max_connections: int = 100
    current_connections: int = 0
    healthy: bool = True
    last_health_check: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LoadBalancerStats:
    """Statistics for load balancer operations."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    endpoint_stats: dict[str, int] = field(default_factory=dict)


class APILoadBalancerAction:
    """Distribute requests across multiple API endpoints.

    Args:
        strategy: Load balancing strategy to use.

    Example:
        >>> lb = APILoadBalancerAction()
        >>> lb.add_endpoint("http://api1.example.com")
        >>> lb.add_endpoint("http://api2.example.com")
        >>> result = await lb.execute(request_fn, {"url": "/api/data"})
    """

    def __init__(
        self,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ) -> None:
        self.strategy = strategy
        self._endpoints: list[Endpoint] = []
        self._current_index = 0
        self._stats = LoadBalancerStats()

    def add_endpoint(
        self,
        url: str,
        weight: int = 1,
        max_connections: int = 100,
    ) -> "APILoadBalancerAction":
        """Add an endpoint to the pool.

        Args:
            url: Endpoint URL.
            weight: Relative weight for weighted strategies.
            max_connections: Maximum concurrent connections.

        Returns:
            Self for method chaining.
        """
        endpoint = Endpoint(
            url=url,
            weight=weight,
            max_connections=max_connections,
        )
        self._endpoints.append(endpoint)
        self._stats.endpoint_stats[url] = 0
        return self

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint from the pool.

        Args:
            url: Endpoint URL to remove.

        Returns:
            True if endpoint was found and removed.
        """
        for i, ep in enumerate(self._endpoints):
            if ep.url == url:
                del self._endpoints[i]
                return True
        return False

    def get_endpoint_count(self) -> int:
        """Get number of endpoints.

        Returns:
            Endpoint count.
        """
        return len(self._endpoints)

    def _select_endpoint(self, context: Optional[dict[str, Any]] = None) -> Optional[Endpoint]:
        """Select an endpoint based on strategy.

        Args:
            context: Optional context for selection (e.g., client IP).

        Returns:
            Selected endpoint or None.
        """
        healthy = [ep for ep in self._endpoints if ep.healthy]
        if not healthy:
            logger.warning("No healthy endpoints available")
            return self._endpoints[0] if self._endpoints else None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_select(healthy)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_select(healthy)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            return self._random_select(healthy)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted_select(healthy)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return self._ip_hash_select(healthy, context)

        return healthy[0]

    def _round_robin_select(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select using round-robin.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint.
        """
        endpoint = endpoints[self._current_index % len(endpoints)]
        self._current_index += 1
        return endpoint

    def _least_connections_select(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select endpoint with least connections.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint.
        """
        return min(endpoints, key=lambda ep: ep.current_connections)

    def _random_select(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select endpoint randomly.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint.
        """
        import random
        return endpoints[int(random.random() * len(endpoints))]

    def _weighted_select(self, endpoints: list[Endpoint]) -> Endpoint:
        """Select endpoint using weighted probability.

        Args:
            endpoints: List of healthy endpoints.

        Returns:
            Selected endpoint.
        """
        import random
        total_weight = sum(ep.weight for ep in endpoints)
        r = random.random() * total_weight

        cumulative = 0
        for ep in endpoints:
            cumulative += ep.weight
            if r <= cumulative:
                return ep

        return endpoints[-1]

    def _ip_hash_select(
        self,
        endpoints: list[Endpoint],
        context: Optional[dict[str, Any]],
    ) -> Endpoint:
        """Select endpoint using IP hash.

        Args:
            endpoints: List of healthy endpoints.
            context: Request context with client info.

        Returns:
            Selected endpoint.
        """
        if not context:
            return endpoints[self._current_index % len(endpoints)]

        client_ip = context.get("client_ip", "unknown")
        hash_val = hash(client_ip) % len(endpoints)
        return endpoints[hash_val]

    async def execute(
        self,
        request_fn: Callable[..., Any],
        request_data: dict[str, Any],
        context: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute request through load balancer.

        Args:
            request_fn: Function to make the request.
            request_data: Request parameters.
            context: Optional context (client IP, etc.).

        Returns:
            Response from selected endpoint.
        """
        self._stats.total_requests += 1

        endpoint = self._select_endpoint(context)
        if not endpoint:
            raise Exception("No available endpoints")

        endpoint.current_connections += 1
        self._stats.endpoint_stats[endpoint.url] += 1

        try:
            request_data["url"] = endpoint.url + request_data.get("path", "")
            result = await request_fn(request_data)
            self._stats.successful_requests += 1
            return result

        except Exception as e:
            self._stats.failed_requests += 1
            logger.error(f"Request to {endpoint.url} failed: {e}")
            endpoint.healthy = False
            endpoint.last_health_check = time.time()
            raise

        finally:
            endpoint.current_connections = max(0, endpoint.current_connections - 1)

    def mark_unhealthy(self, url: str) -> None:
        """Mark an endpoint as unhealthy.

        Args:
            url: Endpoint URL.
        """
        for ep in self._endpoints:
            if ep.url == url:
                ep.healthy = False
                logger.warning(f"Endpoint marked unhealthy: {url}")

    def mark_healthy(self, url: str) -> None:
        """Mark an endpoint as healthy.

        Args:
            url: Endpoint URL.
        """
        for ep in self._endpoints:
            if ep.url == url:
                ep.healthy = True
                logger.info(f"Endpoint marked healthy: {url}")

    def get_stats(self) -> LoadBalancerStats:
        """Get load balancer statistics.

        Returns:
            Current statistics.
        """
        return self._stats

    def get_endpoints(self) -> list[Endpoint]:
        """Get list of all endpoints.

        Returns:
            List of endpoints.
        """
        return self._endpoints.copy()
