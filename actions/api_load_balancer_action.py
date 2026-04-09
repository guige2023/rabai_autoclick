"""
API Load Balancer Action Module.

Provides load balancing strategies including round-robin,
weighted round-robin, least connections, IP hashing, and
adaptive load balancing for API gateways.
"""

import random
import threading
import time
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class LoadBalancingStrategy(Enum):
    """Load balancing algorithm types."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    ADAPTIVE = "adaptive"


@dataclass
class Server:
    """Represents a backend server."""
    id: str
    host: str
    port: int
    weight: int = 1  # for weighted strategies
    max_connections: int = 1000
    is_healthy: bool = True
    is_draining: bool = False  # for graceful shutdown

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class ServerStats:
    """Statistics for a server."""
    server_id: str
    requests: int = 0
    successes: int = 0
    failures: int = 0
    total_response_time: float = 0.0
    current_connections: int = 0
    last_used: float = field(default_factory=time.time)

    @property
    def avg_response_time(self) -> float:
        if self.successes == 0:
            return 0
        return self.total_response_time / self.successes

    @property
    def failure_rate(self) -> float:
        total = self.successes + self.failures
        if total == 0:
            return 0
        return self.failures / total


@dataclass
class LoadBalancerConfig:
    """Configuration for load balancer."""
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    health_check_interval: float = 30.0  # seconds
    health_check_timeout: float = 5.0
    max_failures: int = 3  # failures before marking unhealthy
    circuit_breaker_threshold: int = 5  # failures to open circuit
    circuit_breaker_timeout: float = 60.0  # seconds to try again
    slow_start_time: float = 0.0  # seconds for slow start
    health_check_url: Optional[str] = None


class CircuitBreaker:
    """Circuit breaker for server protection."""

    def __init__(
        self,
        threshold: int = 5,
        timeout: float = 60.0,
    ):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half_open

    def record_failure(self) -> None:
        """Record a failure."""
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.threshold:
            self.state = "open"

    def record_success(self) -> None:
        """Record a success."""
        self.failures = 0
        self.state = "closed"

    def can_attempt(self) -> bool:
        """Check if request can be attempted."""
        if self.state == "closed":
            return True
        if self.state == "open":
            if time.time() - self.last_failure_time >= self.timeout:
                self.state = "half_open"
                return True
            return False
        return True  # half_open


class APILoadBalancerAction:
    """
    Load balancing action with multiple strategies.

    Provides intelligent request distribution across backend
    servers with health checking and circuit breaker support.
    """

    def __init__(self, config: Optional[LoadBalancerConfig] = None):
        self.config = config or LoadBalancerConfig()
        self._servers: Dict[str, Server] = {}
        self._server_stats: Dict[str, ServerStats] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._server_sequence: List[str] = []

    def add_server(
        self,
        server_id: str,
        host: str,
        port: int,
        weight: int = 1,
        max_connections: int = 1000,
    ) -> "APILoadBalancerAction":
        """Add a server to the pool."""
        with self._lock:
            server = Server(
                id=server_id,
                host=host,
                port=port,
                weight=weight,
                max_connections=max_connections,
            )
            self._servers[server_id] = server
            self._server_stats[server_id] = ServerStats(server_id=server_id)
            self._circuit_breakers[server_id] = CircuitBreaker(
                threshold=self.config.circuit_breaker_threshold,
                timeout=self.config.circuit_breaker_timeout,
            )
            self._server_sequence = sorted(
                self._servers.keys(),
                key=lambda s: self._servers[s].weight,
                reverse=True,
            )
        return self

    def remove_server(self, server_id: str) -> "APILoadBalancerAction":
        """Remove a server from the pool."""
        with self._lock:
            self._servers.pop(server_id, None)
            self._server_stats.pop(server_id, None)
            self._circuit_breakers.pop(server_id, None)
        return self

    def get_available_servers(self) -> List[str]:
        """Get list of available server IDs."""
        with self._lock:
            return [
                sid for sid, server in self._servers.items()
                if server.is_healthy and not server.is_draining
                and self._circuit_breakers[sid].can_attempt()
                and self._server_stats[sid].current_connections < server.max_connections
            ]

    def _select_round_robin(self) -> Optional[str]:
        """Select server using round-robin."""
        available = self.get_available_servers()
        if not available:
            return None

        # Find next index
        while True:
            idx = self._round_robin_index[self._server_sequence[0] if self._server_sequence else ""]
            if idx >= len(self._server_sequence):
                self._round_robin_index[self._server_sequence[0]] = 0
                idx = 0

            server_id = self._server_sequence[idx]
            if server_id in available:
                self._round_robin_index[self._server_sequence[0]] = idx + 1
                return server_id
            else:
                self._round_robin_index[self._server_sequence[0]] = idx + 1

    def _select_weighted_round_robin(self) -> Optional[str]:
        """Select server using weighted round-robin."""
        available = self.get_available_servers()
        if not available:
            return None

        # Build weighted sequence
        weighted_list = []
        for sid in self._server_sequence:
            if sid in available:
                server = self._servers[sid]
                weighted_list.extend([sid] * server.weight)

        if not weighted_list:
            return None

        idx = self._round_robin_index["weighted"] % len(weighted_list)
        self._round_robin_index["weighted"] = idx + 1
        return weighted_list[idx]

    def _select_least_connections(self) -> Optional[str]:
        """Select server with least connections."""
        available = self.get_available_servers()
        if not available:
            return None

        return min(
            available,
            key=lambda sid: self._server_stats[sid].current_connections,
        )

    def _select_least_response_time(self) -> Optional[str]:
        """Select server with least average response time."""
        available = self.get_available_servers()
        if not available:
            return None

        return min(
            available,
            key=lambda sid: self._server_stats[sid].avg_response_time,
        )

    def _select_ip_hash(self, client_ip: str) -> Optional[str]:
        """Select server using IP hash."""
        available = self.get_available_servers()
        if not available:
            return None

        hash_val = hash(client_ip) % len(available)
        return available[hash_val]

    def _select_random(self) -> Optional[str]:
        """Select random server."""
        available = self.get_available_servers()
        if not available:
            return None
        return random.choice(available)

    def _select_adaptive(self) -> Optional[str]:
        """Select server using adaptive algorithm."""
        available = self.get_available_servers()
        if not available:
            return None

        # Score servers based on multiple factors
        scores = {}
        for sid in available:
            stats = self._server_stats[sid]
            server = self._servers[sid]

            # Lower is better: connections ratio, response time, failure rate
            connection_score = stats.current_connections / server.max_connections
            response_score = stats.avg_response_time / 1000  # normalize to seconds
            failure_score = stats.failure_rate

            # Weighted combination
            total_score = (
                connection_score * 0.4 +
                response_score * 0.4 +
                failure_score * 0.2
            )
            scores[sid] = total_score

        return min(scores, key=scores.get)

    def select_server(self, client_ip: Optional[str] = None) -> Optional[str]:
        """
        Select a server based on configured strategy.

        Args:
            client_ip: Client IP for IP hash strategy

        Returns:
            Selected server ID or None
        """
        strategy = self.config.strategy

        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._select_round_robin()
        elif strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._select_weighted_round_robin()
        elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._select_least_connections()
        elif strategy == LoadBalancingStrategy.LEAST_RESPONSE_TIME:
            return self._select_least_response_time()
        elif strategy == LoadBalancingStrategy.IP_HASH:
            return self._select_ip_hash(client_ip or "unknown")
        elif strategy == LoadBalancingStrategy.RANDOM:
            return self._select_random()
        elif strategy == LoadBalancingStrategy.ADAPTIVE:
            return self._select_adaptive()

        return self._select_round_robin()

    def record_request_start(self, server_id: str) -> None:
        """Record start of request to server."""
        with self._lock:
            if server_id in self._server_stats:
                self._server_stats[server_id].requests += 1
                self._server_stats[server_id].current_connections += 1
                self._server_stats[server_id].last_used = time.time()

    def record_request_end(
        self,
        server_id: str,
        success: bool,
        response_time: float,
    ) -> None:
        """Record end of request to server."""
        with self._lock:
            if server_id not in self._server_stats:
                return

            stats = self._server_stats[server_id]
            stats.current_connections = max(0, stats.current_connections - 1)
            stats.total_response_time += response_time

            if success:
                stats.successes += 1
                self._circuit_breakers[server_id].record_success()
            else:
                stats.failures += 1
                self._circuit_breakers[server_id].record_failure()

    def get_server(self, server_id: str) -> Optional[Server]:
        """Get server by ID."""
        return self._servers.get(server_id)

    def get_all_servers(self) -> List[Server]:
        """Get all servers."""
        return list(self._servers.values())

    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        with self._lock:
            total_requests = sum(s.requests for s in self._server_stats.values())
            total_successes = sum(s.successes for s in self._server_stats.values())
            total_failures = sum(s.failures for s in self._server_stats.values())

            return {
                "strategy": self.config.strategy.value,
                "total_servers": len(self._servers),
                "available_servers": len(self.get_available_servers()),
                "total_requests": total_requests,
                "total_successes": total_successes,
                "total_failures": total_failures,
                "overall_failure_rate": total_failures / total_requests if total_requests > 0 else 0,
                "servers": {
                    sid: {
                        "requests": stats.requests,
                        "successes": stats.successes,
                        "failures": stats.failures,
                        "avg_response_time_ms": stats.avg_response_time,
                        "current_connections": stats.current_connections,
                        "circuit_breaker_state": self._circuit_breakers[sid].state,
                    }
                    for sid, stats in self._server_stats.items()
                },
            }

    def health_check(self, server_id: str) -> bool:
        """Perform health check on server."""
        import socket
        server = self._servers.get(server_id)
        if not server:
            return False

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.config.health_check_timeout)
            result = sock.connect_ex((server.host, server.port))
            sock.close()
            is_healthy = result == 0
            with self._lock:
                server.is_healthy = is_healthy
            return is_healthy
        except Exception:
            with self._lock:
                server.is_healthy = False
            return False
