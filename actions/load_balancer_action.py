"""
Load Balancer Action Module.

Provides multi-endpoint load distribution with
multiple balancing algorithms.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class BalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    IP_HASH = "ip_hash"
    LEAST_RESPONSE_TIME = "least_response_time"


@dataclass
class Endpoint:
    """Load balancer endpoint."""
    id: str
    address: str
    weight: int = 1
    healthy: bool = True
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    last_used: float = 0.0
    metadata: dict = field(default_factory=dict)

    @property
    def avg_response_time(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests

    @property
    def failure_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests


@dataclass
class LoadBalancerConfig:
    """Load balancer configuration."""
    strategy: BalancingStrategy = BalancingStrategy.ROUND_ROBIN
    health_check_interval: float = 30.0
    health_check_timeout: float = 5.0
    max_failures: int = 3
    circuit_breaker_threshold: int = 5


@dataclass
class LoadBalancerStats:
    """Load balancer statistics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    redirected_requests: int = 0
    active_endpoints: int = 0


class RoundRobinBalancer:
    """Round-robin balancer."""

    def __init__(self):
        self._current: int = 0
        self._lock = asyncio.Lock()

    async def select(self, endpoints: list[Endpoint]) -> Optional[Endpoint]:
        """Select endpoint."""
        if not endpoints:
            return None

        healthy = [e for e in endpoints if e.healthy]
        if not healthy:
            return None

        async with self._lock:
            selected = healthy[self._current % len(healthy)]
            self._current += 1
            return selected


class WeightedRoundRobinBalancer:
    """Weighted round-robin balancer."""

    def __init__(self):
        self._counters: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def select(self, endpoints: list[Endpoint]) -> Optional[Endpoint]:
        """Select endpoint based on weight."""
        if not endpoints:
            return None

        healthy = [e for e in endpoints if e.healthy]
        if not healthy:
            return None

        async with self._lock:
            selected = None
            for endpoint in healthy:
                counter = self._counters.get(endpoint.id, 0)
                if counter < endpoint.weight:
                    selected = endpoint
                    self._counters[endpoint.id] = counter + 1
                    break

            if selected is None:
                self._counters = {e.id: 0 for e in healthy}
                selected = healthy[0]
                self._counters[selected.id] = 1

            return selected


class LeastConnectionsBalancer:
    """Least connections balancer."""

    async def select(self, endpoints: list[Endpoint]) -> Optional[Endpoint]:
        """Select endpoint with least connections."""
        if not endpoints:
            return None

        healthy = [e for e in endpoints if e.healthy]
        if not healthy:
            return None

        return min(healthy, key=lambda e: e.active_connections)


class RandomBalancer:
    """Random selection balancer."""

    async def select(self, endpoints: list[Endpoint]) -> Optional[Endpoint]:
        """Select random endpoint."""
        if not endpoints:
            return None

        healthy = [e for e in endpoints if e.healthy]
        if not healthy:
            return None

        return random.choice(healthy)


class IPHashBalancer:
    """IP hash balancer."""

    def __init__(self):
        self._node_count = 0
        self._endpoints: list[Endpoint] = []

    async def select(
        self,
        endpoints: list[Endpoint],
        client_ip: str = ""
    ) -> Optional[Endpoint]:
        """Select endpoint based on IP hash."""
        if not endpoints:
            return None

        self._endpoints = [e for e in endpoints if e.healthy]
        if not self._endpoints:
            return None

        hash_value = sum(ord(c) for c in client_ip)
        index = hash_value % len(self._endpoints)
        return self._endpoints[index]


class LeastResponseTimeBalancer:
    """Least response time balancer."""

    async def select(self, endpoints: list[Endpoint]) -> Optional[Endpoint]:
        """Select endpoint with fastest response time."""
        if not endpoints:
            return None

        healthy = [e for e in endpoints if e.healthy]
        if not healthy:
            return None

        return min(healthy, key=lambda e: e.avg_response_time)


class LoadBalancerAction:
    """
    Multi-endpoint load balancer.

    Example:
        lb = LoadBalancerAction(
            strategy=BalancingStrategy.LEAST_CONNECTIONS
        )

        await lb.add_endpoint("api1", "http://api1.example.com", weight=2)
        await lb.add_endpoint("api2", "http://api2.example.com", weight=1)

        endpoint = await lb.select()
        result = await lb.route_request(endpoint, api_call)
    """

    def __init__(
        self,
        strategy: BalancingStrategy = BalancingStrategy.ROUND_ROBIN
    ):
        self.config = LoadBalancerConfig(strategy=strategy)
        self._endpoints: dict[str, Endpoint] = {}
        self._stats = LoadBalancerStats()
        self._lock = asyncio.Lock()

        if strategy == BalancingStrategy.ROUND_ROBIN:
            self._balancer = RoundRobinBalancer()
        elif strategy == BalancingStrategy.WEIGHTED_ROUND_ROBIN:
            self._balancer = WeightedRoundRobinBalancer()
        elif strategy == BalancingStrategy.LEAST_CONNECTIONS:
            self._balancer = LeastConnectionsBalancer()
        elif strategy == BalancingStrategy.RANDOM:
            self._balancer = RandomBalancer()
        elif strategy == BalancingStrategy.IP_HASH:
            self._balancer = IPHashBalancer()
        elif strategy == BalancingStrategy.LEAST_RESPONSE_TIME:
            self._balancer = LeastResponseTimeBalancer()
        else:
            self._balancer = RoundRobinBalancer()

    async def add_endpoint(
        self,
        endpoint_id: str,
        address: str,
        weight: int = 1,
        metadata: Optional[dict] = None
    ) -> None:
        """Add endpoint."""
        async with self._lock:
            endpoint = Endpoint(
                id=endpoint_id,
                address=address,
                weight=weight,
                metadata=metadata or {}
            )
            self._endpoints[endpoint_id] = endpoint
            self._stats.active_endpoints = sum(1 for e in self._endpoints.values() if e.healthy)

    async def remove_endpoint(self, endpoint_id: str) -> None:
        """Remove endpoint."""
        async with self._lock:
            if endpoint_id in self._endpoints:
                del self._endpoints[endpoint_id]
                self._stats.active_endpoints = sum(1 for e in self._endpoints.values() if e.healthy)

    async def select(
        self,
        client_ip: str = ""
    ) -> Optional[Endpoint]:
        """Select endpoint."""
        endpoints = list(self._endpoints.values())

        if isinstance(self._balancer, IPHashBalancer):
            return await self._balancer.select(endpoints, client_ip)

        return await self._balancer.select(endpoints)

    async def record_success(
        self,
        endpoint_id: str,
        response_time: float
    ) -> None:
        """Record successful request."""
        async with self._lock:
            if endpoint_id in self._endpoints:
                e = self._endpoints[endpoint_id]
                e.active_connections = max(0, e.active_connections - 1)
                e.total_requests += 1
                e.total_response_time += response_time
                e.last_used = time.time()
            self._stats.total_requests += 1
            self._stats.successful_requests += 1

    async def record_failure(self, endpoint_id: str) -> None:
        """Record failed request."""
        async with self._lock:
            if endpoint_id in self._endpoints:
                e = self._endpoints[endpoint_id]
                e.active_connections = max(0, e.active_connections - 1)
                e.total_requests += 1
                e.failed_requests += 1
                e.last_used = time.time()

                if e.failed_requests >= self.config.circuit_breaker_threshold:
                    e.healthy = False

            self._stats.total_requests += 1
            self._stats.failed_requests += 1

    async def route_request(
        self,
        endpoint: Endpoint,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Route request to endpoint."""
        endpoint.active_connections += 1
        start = time.monotonic()

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(endpoint.address, *args, **kwargs)
            else:
                result = func(endpoint.address, *args, **kwargs)

            await self.record_success(endpoint.id, time.monotonic() - start)
            return result

        except Exception as e:
            await self.record_failure(endpoint.id)
            raise

    def get_stats(self) -> LoadBalancerStats:
        """Get load balancer statistics."""
        return self._stats

    def get_endpoints(self) -> list[Endpoint]:
        """Get all endpoints."""
        return list(self._endpoints.values())
