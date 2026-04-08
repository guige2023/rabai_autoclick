"""
API Gateway Load Balancer Action Module.

Distributes incoming API requests across multiple backend servers
using round-robin, weighted, least-connections, or IP-hash strategies.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class BackendServer:
    """A backend server in the pool."""
    url: str
    weight: int = 1
    max_connections: int = 100
    current_connections: int = 0
    healthy: bool = True


@dataclass
class LoadBalanceResult:
    """Result of load balancing decision."""
    selected_server: Optional[str]
    strategy: str
    error: Optional[str] = None


class APIGatewayLBAction(BaseAction):
    """Load balancer for distributing requests across backend servers."""

    def __init__(self) -> None:
        super().__init__("api_gateway_lb")
        self._servers: list[BackendServer] = []
        self._strategy = "round_robin"
        self._current_index = 0
        self._connection_counts: dict[str, int] = {}

    def execute(self, context: dict, params: dict) -> LoadBalanceResult:
        """
        Select a backend server based on load balancing strategy.

        Args:
            context: Execution context
            params: Parameters:
                - strategy: round_robin, weighted, least_conn, ip_hash
                - client_ip: Client IP for ip_hash strategy
                - exclude_unhealthy: Exclude unhealthy servers (default: True)

        Returns:
            LoadBalanceResult with selected server URL
        """
        strategy = params.get("strategy", "round_robin")
        client_ip = params.get("client_ip", "")
        exclude_unhealthy = params.get("exclude_unhealthy", True)

        if not self._servers:
            return LoadBalanceResult(None, strategy, "No servers configured")

        healthy = [s for s in self._servers if not exclude_unhealthy or s.healthy]
        if not healthy:
            return LoadBalanceResult(None, strategy, "No healthy servers available")

        if strategy == "round_robin":
            selected = self._round_robin(healthy)
        elif strategy == "weighted":
            selected = self._weighted(healthy)
        elif strategy == "least_conn":
            selected = self._least_connections(healthy)
        elif strategy == "ip_hash":
            selected = self._ip_hash(healthy, client_ip)
        else:
            selected = self._round_robin(healthy)

        if selected:
            self._current_index = (self._current_index + 1) % len(healthy)
        return LoadBalanceResult(selected, strategy)

    def _round_robin(self, servers: list[BackendServer]) -> Optional[str]:
        """Round robin selection."""
        if not servers:
            return None
        return servers[self._current_index % len(servers)].url

    def _weighted(self, servers: list[BackendServer]) -> Optional[str]:
        """Weighted selection based on server weight."""
        total_weight = sum(s.weight for s in servers)
        if total_weight == 0:
            return servers[0].url
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for s in servers:
            cumulative += s.weight
            if r <= cumulative:
                return s.url
        return servers[-1].url

    def _least_connections(self, servers: list[BackendServer]) -> Optional[str]:
        """Select server with fewest active connections."""
        return min(servers, key=lambda s: self._connection_counts.get(s.url, 0)).url

    def _ip_hash(self, servers: list[BackendServer], client_ip: str) -> Optional[str]:
        """Consistent hash based on client IP."""
        if not client_ip:
            return servers[0].url
        import hashlib
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return servers[hash_val % len(servers)].url

    def add_server(self, url: str, weight: int = 1, max_connections: int = 100) -> None:
        """Add a backend server to the pool."""
        self._servers.append(BackendServer(url=url, weight=weight, max_connections=max_connections))

    def remove_server(self, url: str) -> None:
        """Remove a backend server from the pool."""
        self._servers = [s for s in self._servers if s.url != url]

    def set_healthy(self, url: str, healthy: bool) -> None:
        """Set server healthy/unhealthy status."""
        for s in self._servers:
            if s.url == url:
                s.healthy = healthy
                break
