"""
Load Balancer Action Module.

Provides load balancing strategies including round-robin,
least connections, weighted, and adaptive algorithms.

Author: rabai_autoclick team
"""

import time
import asyncio
import logging
from typing import (
    Optional, Dict, Any, List, Callable, Awaitable,
    Union
)
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Algorithm(Enum):
    """Load balancing algorithms."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    RANDOM = "random"
    IP_HASH = "ip_hash"
    ADAPTIVE = "adaptive"
    POWER_OF_TWO = "power_of_two"


@dataclass
class Endpoint:
    """Represents a backend endpoint."""
    id: str
    url: str
    weight: int = 1
    max_connections: int = 100
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_available(self) -> bool:
        """Check if endpoint is available."""
        return self.metadata.get("healthy", True)

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class EndpointStats:
    """Statistics for an endpoint."""
    endpoint_id: str
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    last_used: float = 0.0
    consecutive_failures: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_requests == 0:
            return 1.0
        return 1.0 - (self.failed_requests / self.total_requests)

    @property
    def avg_latency(self) -> float:
        """Calculate average latency."""
        if self.total_requests == 0:
            return 0.0
        return self.total_latency / self.total_requests


class LoadBalancingStrategy(ABC):
    """Base class for load balancing strategies."""

    @abstractmethod
    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        """Select an endpoint based on strategy."""
        pass


class RoundRobinStrategy(LoadBalancingStrategy):
    """Round-robin load balancing."""

    def __init__(self):
        self._counter: Dict[str, int] = {}

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        key = context.get("pool_id", "default")
        self._counter[key] = (self._counter.get(key, 0) + 1) % len(available)
        return available[self._counter[key]]


class LeastConnectionsStrategy(LoadBalancingStrategy):
    """Least connections load balancing."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        min_connections = float("inf")
        selected = None

        for endpoint in available:
            endpoint_stats = stats.get(endpoint.id, EndpointStats(endpoint_id=endpoint.id))
            if endpoint_stats.active_connections < min_connections:
                min_connections = endpoint_stats.active_connections
                selected = endpoint

        return selected


class WeightedStrategy(LoadBalancingStrategy):
    """Weighted load balancing."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        total_weight = sum(e.weight for e in available)
        import random
        rand = random.uniform(0, total_weight)

        cumulative = 0
        for endpoint in available:
            cumulative += endpoint.weight
            if cumulative >= rand:
                return endpoint

        return available[-1]


class RandomStrategy(LoadBalancingStrategy):
    """Random selection load balancing."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None
        import random
        return random.choice(available)


class IPHashStrategy(LoadBalancingStrategy):
    """IP hash-based load balancing."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        client_ip = context.get("client_ip", "0.0.0.0")
        hash_value = hash(client_ip) % len(available)
        return available[hash_value]


class PowerOfTwoStrategy(LoadBalancingStrategy):
    """Power of two choices (load balancer lottery)."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        if len(available) < 2:
            return available[0]

        import random
        candidates = random.sample(available, 2)

        stats1 = stats.get(candidates[0].id, EndpointStats(endpoint_id=candidates[0].id))
        stats2 = stats.get(candidates[1].id, EndpointStats(endpoint_id=candidates[1].id))

        if stats1.avg_latency <= stats2.avg_latency:
            return candidates[0]
        return candidates[1]


class AdaptiveStrategy(LoadBalancingStrategy):
    """Adaptive load balancing based on real metrics."""

    async def select(
        self,
        endpoints: List[Endpoint],
        stats: Dict[str, EndpointStats],
        context: Dict[str, Any],
    ) -> Optional[Endpoint]:
        available = [e for e in endpoints if e.is_available]
        if not available:
            return None

        scores: Dict[str, float] = {}

        for endpoint in available:
            endpoint_stats = stats.get(endpoint.id, EndpointStats(endpoint_id=endpoint.id))

            success_weight = 0.4
            latency_weight = 0.3
            capacity_weight = 0.3

            success_score = endpoint_stats.success_rate
            latency_score = 1.0 / (1.0 + endpoint_stats.avg_latency / 1000)
            capacity_score = 1.0 - (
                endpoint_stats.active_connections / endpoint.max_connections
            )

            score = (
                success_weight * success_score
                + latency_weight * latency_score
                + capacity_weight * capacity_score
            )

            scores[endpoint.id] = score

        if not scores:
            return available[0]

        return max(available, key=lambda e: scores.get(e.id, 0))


class LoadBalancerAction:
    """
    Load Balancer Implementation.

    Provides multiple load balancing strategies with
    health checking, circuit breaking, and metrics.

    Example:
        >>> lb = LoadBalancerAction(algorithm=Algorithm.LEAST_CONNECTIONS)
        >>> lb.add_endpoint(Endpoint(id="1", url="http://backend1:8080"))
        >>> endpoint = await lb.get_endpoint()
    """

    def __init__(
        self,
        algorithm: Algorithm = Algorithm.ROUND_ROBIN,
        health_check_interval: float = 30.0,
    ):
        self.algorithm = algorithm
        self.health_check_interval = health_check_interval
        self._endpoints: Dict[str, Endpoint] = {}
        self._stats: Dict[str, EndpointStats] = {}
        self._strategy: LoadBalancingStrategy = self._create_strategy(algorithm)
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    def _create_strategy(self, algorithm: Algorithm) -> LoadBalancingStrategy:
        """Create strategy instance based on algorithm."""
        strategies = {
            Algorithm.ROUND_ROBIN: RoundRobinStrategy,
            Algorithm.LEAST_CONNECTIONS: LeastConnectionsStrategy,
            Algorithm.WEIGHTED: WeightedStrategy,
            Algorithm.RANDOM: RandomStrategy,
            Algorithm.IP_HASH: IPHashStrategy,
            Algorithm.POWER_OF_TWO: PowerOfTwoStrategy,
            Algorithm.ADAPTIVE: AdaptiveStrategy,
        }
        strategy_class = strategies.get(algorithm, RoundRobinStrategy)
        return strategy_class()

    def add_endpoint(self, endpoint: Endpoint) -> None:
        """
        Add a backend endpoint.

        Args:
            endpoint: Endpoint to add
        """
        self._endpoints[endpoint.id] = endpoint
        self._stats[endpoint.id] = EndpointStats(endpoint_id=endpoint.id)
        self._circuit_breakers[endpoint.id] = CircuitBreaker(endpoint.id)
        logger.info(f"Added endpoint: {endpoint.id} ({endpoint.url})")

    def remove_endpoint(self, endpoint_id: str) -> bool:
        """
        Remove an endpoint.

        Args:
            endpoint_id: Endpoint ID

        Returns:
            True if removed
        """
        if endpoint_id in self._endpoints:
            del self._endpoints[endpoint_id]
            logger.info(f"Removed endpoint: {endpoint_id}")
            return True
        return False

    def update_endpoint(self, endpoint_id: str, **kwargs) -> bool:
        """Update endpoint configuration."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint:
            for key, value in kwargs.items():
                if hasattr(endpoint, key):
                    setattr(endpoint, key, value)
            return True
        return False

    async def get_endpoint(
        self,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Endpoint]:
        """
        Get next endpoint based on load balancing algorithm.

        Args:
            context: Optional context (client_ip, etc.)

        Returns:
            Selected endpoint or None
        """
        context = context or {}

        endpoints = list(self._endpoints.values())
        if not endpoints:
            return None

        selected = await self._strategy.select(endpoints, self._stats, context)

        if selected:
            cb = self._circuit_breakers.get(selected.id)
            if cb and cb.is_open:
                return await self._strategy.select(
                    [e for e in endpoints if e.id != selected.id],
                    self._stats,
                    context,
                )

        return selected

    async def record_request(
        self,
        endpoint_id: str,
        success: bool,
        latency: float,
    ) -> None:
        """
        Record request result for an endpoint.

        Args:
            endpoint_id: Endpoint ID
            success: Whether request succeeded
            latency: Request latency in seconds
        """
        async with self._lock:
            stats = self._stats.get(endpoint_id)
            if not stats:
                stats = EndpointStats(endpoint_id=endpoint_id)
                self._stats[endpoint_id] = stats

            stats.total_requests += 1
            stats.total_latency += latency
            stats.last_used = time.time()

            if success:
                stats.consecutive_failures = 0
            else:
                stats.failed_requests += 1
                stats.consecutive_failures += 1

                cb = self._circuit_breakers.get(endpoint_id)
                if cb:
                    cb.record_failure()

    async def release_connection(self, endpoint_id: str) -> None:
        """Release a connection (decrement active count)."""
        async with self._lock:
            stats = self._stats.get(endpoint_id)
            if stats and stats.active_connections > 0:
                stats.active_connections -= 1

    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        return {
            "algorithm": self.algorithm.value,
            "total_endpoints": len(self._endpoints),
            "available_endpoints": sum(1 for e in self._endpoints.values() if e.is_available),
            "endpoint_stats": {
                eid: {
                    "active_connections": stats.active_connections,
                    "total_requests": stats.total_requests,
                    "failed_requests": stats.failed_requests,
                    "success_rate": stats.success_rate,
                    "avg_latency": stats.avg_latency,
                }
                for eid, stats in self._stats.items()
            },
        }


class CircuitBreaker:
    """Circuit breaker for endpoint failure protection."""

    def __init__(
        self,
        endpoint_id: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ):
        self.endpoint_id = endpoint_id
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = "closed"

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        if self._state == "open":
            if (
                self._last_failure_time
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = "half_open"
                return False
            return True
        return False

    def record_failure(self) -> None:
        """Record a failure."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._failure_count >= self.failure_threshold:
            self._state = "open"
            logger.warning(f"Circuit breaker opened for {self.endpoint_id}")

    def record_success(self) -> None:
        """Record a success."""
        self._failure_count = 0
        self._state = "closed"
