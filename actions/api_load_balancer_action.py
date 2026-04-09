"""
API load balancer for distributing requests across multiple endpoints.

This module provides intelligent load balancing strategies including
round-robin, weighted routing, least connections, and health-aware distribution.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum, auto
from collections import defaultdict

logger = logging.getLogger(__name__)


class EndpointStatus(Enum):
    """Health status of an endpoint."""
    HEALTHY = auto()
    DEGRADED = auto()
    UNHEALTHY = auto()
    DRAINING = auto()


@dataclass
class Endpoint:
    """Represents a backend endpoint."""
    url: str
    weight: float = 1.0
    max_connections: int = 100
    timeout: float = 30.0
    health_check_path: str = "/health"
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: EndpointStatus = EndpointStatus.HEALTHY
    current_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    last_health_check: Optional[float] = None
    last_failure: Optional[float] = None
    consecutive_failures: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate request success rate."""
        if self.total_requests == 0:
            return 1.0
        return 1.0 - (self.failed_requests / self.total_requests)

    @property
    def average_latency(self) -> float:
        """Calculate average request latency."""
        if self.total_requests == 0:
            return 0.0
        return self.total_latency / self.total_requests

    @property
    def is_available(self) -> bool:
        """Check if endpoint is available for requests."""
        return (
            self.status == EndpointStatus.HEALTHY
            and self.current_connections < self.max_connections
        )

    def record_request(self, success: bool, latency: float) -> None:
        """Record a request result."""
        self.total_requests += 1
        self.total_latency += latency
        if not success:
            self.failed_requests += 1
            self.consecutive_failures += 1
            self.last_failure = time.time()
        else:
            self.consecutive_failures = 0


class LoadBalancingStrategy(ABC):
    """Base class for load balancing strategies."""

    @abstractmethod
    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        """Select an endpoint based on the strategy."""
        pass

    def filter_available(self, endpoints: List[Endpoint]) -> List[Endpoint]:
        """Filter to only available endpoints."""
        return [ep for ep in endpoints if ep.is_available]


class RoundRobinStrategy(LoadBalancingStrategy):
    """Round-robin distribution across endpoints."""

    def __init__(self):
        self._index = 0

    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        available = self.filter_available(endpoints)
        if not available:
            return None
        ep = available[self._index % len(available)]
        self._index += 1
        return ep


class WeightedRoundRobinStrategy(LoadBalancingStrategy):
    """Weighted round-robin distribution."""

    def __init__(self):
        self._counters: Dict[str, int] = defaultdict(int)

    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        available = self.filter_available(endpoints)
        if not available:
            return None

        total_weight = sum(ep.weight for ep in available)
        rand = random.uniform(0, total_weight)

        cumulative = 0.0
        for ep in available:
            cumulative += ep.weight
            if rand <= cumulative:
                return ep
        return available[-1]


class LeastConnectionsStrategy(LoadBalancingStrategy):
    """Route to endpoint with fewest active connections."""

    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        available = self.filter_available(endpoints)
        if not available:
            return None
        return min(available, key=lambda ep: ep.current_connections)


class ResponseTimeStrategy(LoadBalancingStrategy):
    """Route to endpoint with lowest average latency."""

    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        available = self.filter_available(endpoints)
        if not available:
            return None
        return min(available, key=lambda ep: ep.average_latency)


class HealthAwareStrategy(LoadBalancingStrategy):
    """Route based on health score combining success rate and latency."""

    def select(self, endpoints: List[Endpoint]) -> Optional[Endpoint]:
        available = self.filter_available(endpoints)
        if not available:
            return None

        def health_score(ep: Endpoint) -> float:
            success_weight = 0.7
            latency_weight = 0.3
            latency_score = max(0, 1.0 - (ep.average_latency / 10.0))
            return (success_weight * ep.success_rate) + (latency_weight * latency_score)

        return max(available, key=health_score)


class LoadBalancer:
    """
    API load balancer with multiple distribution strategies.

    Features:
    - Multiple load balancing strategies
    - Health checking and automatic failover
    - Connection tracking per endpoint
    - Request/response recording
    - Configurable thresholds

    Example:
        >>> lb = LoadBalancer(strategy="round_robin")
        >>> lb.add_endpoint("http://api1.example.com", weight=2)
        >>> lb.add_endpoint("http://api2.example.com", weight=1)
        >>> endpoint = lb.get_endpoint()
        >>> response = make_request(endpoint.url)
        >>> lb.record_response(endpoint.url, success=True, latency=0.1)
    """

    STRATEGIES = {
        "round_robin": RoundRobinStrategy,
        "weighted": WeightedRoundRobinStrategy,
        "least_connections": LeastConnectionsStrategy,
        "response_time": ResponseTimeStrategy,
        "health_aware": HealthAwareStrategy,
    }

    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: float = 30.0,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
    ):
        """
        Initialize the load balancer.

        Args:
            strategy: Load balancing strategy name
            health_check_interval: Seconds between health checks
            failure_threshold: Failures before marking unhealthy
            recovery_threshold: Successes before marking healthy
        """
        self.endpoints: Dict[str, Endpoint] = {}
        self.strategy_name = strategy
        self.strategy = self.STRATEGIES.get(strategy, RoundRobinStrategy)()
        self.health_check_interval = health_check_interval
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_urls: Dict[str, Callable[[str], bool]] = {}
        logger.info(f"LoadBalancer initialized with strategy: {strategy}")

    def add_endpoint(
        self,
        url: str,
        weight: float = 1.0,
        max_connections: int = 100,
        timeout: float = 30.0,
        health_check_path: str = "/health",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Endpoint:
        """
        Add an endpoint to the load balancer.

        Args:
            url: Full endpoint URL
            weight: Relative weight for weighted strategies
            max_connections: Maximum concurrent connections
            timeout: Request timeout in seconds
            health_check_path: Path for health checks
            metadata: Additional endpoint metadata

        Returns:
            The created Endpoint
        """
        if url in self.endpoints:
            raise ValueError(f"Endpoint '{url}' already exists")

        endpoint = Endpoint(
            url=url,
            weight=weight,
            max_connections=max_connections,
            timeout=timeout,
            health_check_path=health_check_path,
            metadata=metadata or {},
        )
        self.endpoints[url] = endpoint
        logger.info(f"Endpoint added: {url} (weight={weight})")
        return endpoint

    def remove_endpoint(self, url: str) -> bool:
        """Remove an endpoint from the load balancer."""
        if url in self.endpoints:
            del self.endpoints[url]
            logger.info(f"Endpoint removed: {url}")
            return True
        return False

    def get_endpoint(self) -> Optional[Endpoint]:
        """Get the next endpoint based on load balancing strategy."""
        endpoint = self.strategy.select(list(self.endpoints.values()))
        if endpoint:
            endpoint.current_connections += 1
            logger.debug(f"Selected endpoint: {endpoint.url}")
        else:
            logger.warning("No available endpoints")
        return endpoint

    def record_response(
        self,
        url: str,
        success: bool,
        latency: float,
    ) -> None:
        """
        Record the result of a request to an endpoint.

        Args:
            url: The endpoint URL
            success: Whether the request succeeded
            latency: Request latency in seconds
        """
        endpoint = self.endpoints.get(url)
        if not endpoint:
            return

        endpoint.current_connections = max(0, endpoint.current_connections - 1)
        endpoint.record_request(success, latency)

        if not success and endpoint.consecutive_failures >= self.failure_threshold:
            endpoint.status = EndpointStatus.UNHEALTHY
            logger.warning(
                f"Endpoint {url} marked UNHEALTHY "
                f"(failures={endpoint.consecutive_failures})"
            )
        elif success and endpoint.consecutive_failures == 0:
            if endpoint.status == EndpointStatus.UNHEALTHY:
                endpoint.status = EndpointStatus.HEALTHY
                logger.info(f"Endpoint {url} recovered to HEALTHY")

    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        return {
            "strategy": self.strategy_name,
            "total_endpoints": len(self.endpoints),
            "available_endpoints": sum(1 for ep in self.endpoints.values() if ep.is_available),
            "endpoints": {
                url: {
                    "status": ep.status.name,
                    "connections": ep.current_connections,
                    "total_requests": ep.total_requests,
                    "success_rate": ep.success_rate,
                    "average_latency": ep.average_latency,
                }
                for url, ep in self.endpoints.items()
            },
        }

    def reset_stats(self) -> None:
        """Reset all endpoint statistics."""
        for ep in self.endpoints.values():
            ep.total_requests = 0
            ep.failed_requests = 0
            ep.total_latency = 0.0
            ep.consecutive_failures = 0
        logger.info("LoadBalancer statistics reset")
