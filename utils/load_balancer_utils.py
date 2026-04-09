"""
Load Balancing Utilities.

Implements various load balancing algorithms including round-robin,
weighted distribution, least connections, and health-aware routing.

Example:
    >>> lb = LoadBalancer(servers=["s1:8080", "s2:8080", "s3:8080"])
    >>> server = lb.get_next()
    >>> lb.report_failure(server)
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class LoadBalancerStrategy(Enum):
    """Load balancing strategy types."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    IP_HASH = "ip_hash"
    LEAST_RESPONSE_TIME = "least_response_time"


@dataclass
class Server:
    """Represents a server in the load balancer pool."""
    address: str
    weight: int = 1
    max_connections: int = 1000
    current_connections: int = 0
    is_healthy: bool = True
    is_enabled: bool = True
    last_health_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    total_requests: int = 0
    total_failures: int = 0
    total_response_time: float = 0.0

    @property
    def avg_response_time(self) -> float:
        """Calculate average response time."""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_requests == 0:
            return 0.0
        return self.total_failures / self.total_requests


class LoadBalancerStrategyBase(ABC):
    """Base class for load balancing strategies."""

    @abstractmethod
    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select a server based on the strategy."""
        pass

    @abstractmethod
    def record_success(self, server: Server, response_time: float) -> None:
        """Record a successful request."""
        pass

    @abstractmethod
    def record_failure(self, server: Server) -> None:
        """Record a failed request."""
        pass


class RoundRobinStrategy(LoadBalancerStrategyBase):
    """Round-robin load balancing."""

    def __init__(self):
        self._index = 0
        self._lock = threading.Lock()

    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select next server in round-robin order."""
        if not servers:
            return None

        healthy = [s for s in servers if s.is_healthy and s.is_enabled]
        if not healthy:
            return None

        with self._lock:
            selected = healthy[self._index % len(healthy)]
            self._index += 1
            return selected

    def record_success(self, server: Server, response_time: float) -> None:
        """Record successful request."""
        server.total_requests += 1
        server.total_response_time += response_time

    def record_failure(self, server: Server) -> None:
        """Record failed request."""
        server.total_requests += 1
        server.total_failures += 1


class WeightedRoundRobinStrategy(LoadBalancerStrategyBase):
    """Weighted round-robin load balancing."""

    def __init__(self):
        self._current_index = 0
        self._current_weight = 0
        self._lock = threading.Lock()

    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select server based on weight."""
        if not servers:
            return None

        enabled = [s for s in servers if s.is_enabled]
        if not enabled:
            return None

        with self._lock:
            while True:
                if self._current_weight <= 0:
                    max_weight = max(s.weight for s in enabled)
                    if max_weight <= 0:
                        max_weight = 1
                    self._current_weight = max_weight

                idx = self._current_index % len(enabled)
                server = enabled[idx]

                if server.is_healthy and server.current_connections < server.max_connections:
                    if self._current_weight == max_weight:
                        self._current_index = 0
                    else:
                        self._current_index += 1
                    return server

                self._current_weight -= 1
                self._current_index += 1

                if all(not s.is_healthy or s.current_connections >= s.max_connections for s in enabled):
                    return None

    def record_success(self, server: Server, response_time: float) -> None:
        """Record successful request."""
        server.total_requests += 1
        server.total_response_time += response_time
        with self._lock:
            self._current_weight -= 1

    def record_failure(self, server: Server) -> None:
        """Record failed request."""
        server.total_requests += 1
        server.total_failures += 1
        with self._lock:
            self._current_weight = 0


class LeastConnectionsStrategy(LoadBalancerStrategyBase):
    """Least connections load balancing."""

    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select server with fewest active connections."""
        if not servers:
            return None

        healthy = [s for s in servers if s.is_healthy and s.is_enabled]
        if not healthy:
            return None

        return min(healthy, key=lambda s: s.current_connections)

    def record_success(self, server: Server, response_time: float) -> None:
        """Record successful request."""
        server.current_connections = max(0, server.current_connections - 1)
        server.total_requests += 1
        server.total_response_time += response_time

    def record_failure(self, server: Server) -> None:
        """Record failed request."""
        server.current_connections = max(0, server.current_connections - 1)
        server.total_requests += 1
        server.total_failures += 1


class LeastResponseTimeStrategy(LoadBalancerStrategyBase):
    """Least response time load balancing."""

    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select server with lowest response time."""
        if not servers:
            return None

        healthy = [s for s in servers if s.is_healthy and s.is_enabled]
        if not healthy:
            return None

        return min(healthy, key=lambda s: s.avg_response_time if s.avg_response_time > 0 else float('inf'))

    def record_success(self, server: Server, response_time: float) -> None:
        """Record successful request."""
        server.total_requests += 1
        server.total_response_time += response_time

    def record_failure(self, server: Server) -> None:
        """Record failed request."""
        server.total_requests += 1
        server.total_failures += 1
        server.consecutive_failures += 1


class RandomStrategy(LoadBalancerStrategyBase):
    """Random selection load balancing."""

    def select(self, servers: list[Server]) -> Optional[Server]:
        """Select random server."""
        import random
        if not servers:
            return None

        healthy = [s for s in servers if s.is_healthy and s.is_enabled]
        if not healthy:
            return None

        return random.choice(healthy)

    def record_success(self, server: Server, response_time: float) -> None:
        """Record successful request."""
        server.total_requests += 1
        server.total_response_time += response_time

    def record_failure(self, server: Server) -> None:
        """Record failed request."""
        server.total_requests += 1
        server.total_failures += 1


class LoadBalancer:
    """
    Load balancer with multiple strategy options.

    Provides server selection, health tracking, and request
    statistics management.
    """

    def __init__(
        self,
        servers: Optional[list[str]] = None,
        strategy: LoadBalancerStrategy = LoadBalancerStrategy.ROUND_ROBIN,
        health_check_interval: float = 30.0,
        health_check_timeout: float = 5.0
    ):
        """
        Initialize load balancer.

        Args:
            servers: List of server addresses
            strategy: Load balancing strategy
            health_check_interval: Seconds between health checks
            health_check_timeout: Timeout for health check requests
        """
        self._servers: dict[str, Server] = {}
        self._strategy = self._create_strategy(strategy)
        self._health_check_interval = health_check_interval
        self._health_check_timeout = health_check_timeout
        self._lock = threading.Lock()

        if servers:
            for addr in servers:
                self.add_server(addr)

    def _create_strategy(self, strategy: LoadBalancerStrategy) -> LoadBalancerStrategyBase:
        """Create strategy instance from enum."""
        strategies = {
            LoadBalancerStrategy.ROUND_ROBIN: RoundRobinStrategy,
            LoadBalancerStrategy.WEIGHTED_ROUND_ROBIN: WeightedRoundRobinStrategy,
            LoadBalancerStrategy.LEAST_CONNECTIONS: LeastConnectionsStrategy,
            LoadBalancerStrategy.RANDOM: RandomStrategy,
            LoadBalancerStrategy.LEAST_RESPONSE_TIME: LeastResponseTimeStrategy,
        }
        return strategies.get(strategy, RoundRobinStrategy)()

    def add_server(self, address: str, weight: int = 1) -> None:
        """Add a server to the pool."""
        with self._lock:
            if address not in self._servers:
                self._servers[address] = Server(address=address, weight=weight)

    def remove_server(self, address: str) -> bool:
        """Remove a server from the pool."""
        with self._lock:
            if address in self._servers:
                del self._servers[address]
                return True
        return False

    def get_server_list(self) -> list[Server]:
        """Get list of all servers."""
        with self._lock:
            return list(self._servers.values())

    def get_next(self) -> Optional[str]:
        """
        Get next server address for request.

        Returns:
            Server address or None if no healthy servers
        """
        with self._lock:
            servers = list(self._servers.values())
            selected = self._strategy.select(servers)
            if selected:
                selected.current_connections += 1
                return selected.address
            return None

    def report_success(self, address: str, response_time: float = 0.0) -> None:
        """Report successful request."""
        with self._lock:
            if address in self._servers:
                server = self._servers[address]
                server.current_connections = max(0, server.current_connections - 1)
                self._strategy.record_success(server, response_time)

    def report_failure(self, address: str) -> None:
        """Report failed request."""
        with self._lock:
            if address in self._servers:
                server = self._servers[address]
                server.current_connections = max(0, server.current_connections - 1)
                self._strategy.record_failure(server)

    def set_healthy(self, address: str, is_healthy: bool) -> None:
        """Set server health status."""
        with self._lock:
            if address in self._servers:
                self._servers[address].is_healthy = is_healthy
                self._servers[address].last_health_check = time.time()

    def set_enabled(self, address: str, is_enabled: bool) -> None:
        """Enable or disable a server."""
        with self._lock:
            if address in self._servers:
                self._servers[address].is_enabled = is_enabled
