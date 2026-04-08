"""API Load Balancer Action Module.

Provides round-robin, weighted, and least-connection load balancing
strategies for API endpoint distribution.
"""
from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar
from random import random, choice
from heapq import heappush, heappop

T = TypeVar("T")


class LoadBalanceStrategy(Enum):
    """Load balancing strategy."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    FAIR = "fair"


@dataclass
class Endpoint:
    """API endpoint."""
    url: str
    weight: int = 1
    max_connections: int = 100
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EndpointStats:
    """Endpoint statistics."""
    endpoint: Endpoint
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    last_used: float = 0.0


class APILoadBalancerAction:
    """Load balancer for API endpoints.

    Example:
        lb = APILoadBalancerAction()
        lb.add_endpoint(Endpoint("http://api1.example.com", weight=2))
        lb.add_endpoint(Endpoint("http://api2.example.com", weight=1))

        result = await lb.execute(make_request, "/api/data")
    """

    def __init__(
        self,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
    ) -> None:
        self.strategy = strategy
        self.endpoints: List[Endpoint] = []
        self._stats: Dict[str, EndpointStats] = {}
        self._round_robin_index = 0
        self._fair_heap: List[tuple] = []
        self._lock = asyncio.Lock()

    def add_endpoint(self, endpoint: Endpoint) -> None:
        """Add endpoint to load balancer."""
        self.endpoints.append(endpoint)
        self._stats[endpoint.url] = EndpointStats(endpoint=endpoint)

    def remove_endpoint(self, url: str) -> None:
        """Remove endpoint from load balancer."""
        self.endpoints = [e for e in self.endpoints if e.url != url]
        if url in self._stats:
            del self._stats[url]

    async def execute(
        self,
        func: Callable[..., T],
        path: str,
        *args: Any,
        client_ip: Optional[str] = None,
        **kwargs: Any,
    ) -> T:
        """Execute request through load balancer.

        Args:
            func: Request function(endpoint, path, *args, **kwargs)
            path: API path
            *args: Additional positional args
            client_ip: Client IP for IP_HASH strategy
            **kwargs: Additional keyword args

        Returns:
            Result from selected endpoint
        """
        endpoint = await self._select_endpoint(client_ip)
        async with self._lock:
            self._stats[endpoint.url].active_connections += 1
            self._stats[endpoint.url].total_requests += 1
            self._stats[endpoint.url].last_used = asyncio.get_event_loop().time()

        try:
            result = await asyncio.wait_for(
                func(endpoint, path, *args, **kwargs),
                timeout=endpoint.timeout
            )
            return result
        except Exception as e:
            async with self._lock:
                self._stats[endpoint.url].failed_requests += 1
            raise
        finally:
            async with self._lock:
                self._stats[endpoint.url].active_connections -= 1

    async def _select_endpoint(self, client_ip: Optional[str]) -> Endpoint:
        """Select endpoint based on strategy."""
        if not self.endpoints:
            raise NoEndpointsAvailableError("No endpoints available")

        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            return self._round_robin()
        elif self.strategy == LoadBalanceStrategy.WEIGHTED:
            return await self._weighted_select()
        elif self.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return self._least_connections()
        elif self.strategy == LoadBalanceStrategy.IP_HASH:
            return self._ip_hash(client_ip or "unknown")
        elif self.strategy == LoadBalanceStrategy.RANDOM:
            return choice(self.endpoints)
        elif self.strategy == LoadBalanceStrategy.FAIR:
            return await self._fair_select()
        return self._round_robin()

    def _round_robin(self) -> Endpoint:
        """Round-robin selection."""
        endpoint = self.endpoints[self._round_robin_index]
        self._round_robin_index = (self._round_robin_index + 1) % len(self.endpoints)
        return endpoint

    async def _weighted_select(self) -> Endpoint:
        """Weighted selection."""
        total_weight = sum(e.weight for e in self.endpoints)
        r = random() * total_weight
        cumulative = 0

        for endpoint in self.endpoints:
            cumulative += endpoint.weight
            if r <= cumulative:
                return endpoint
        return self.endpoints[-1]

    def _least_connections(self) -> Endpoint:
        """Select endpoint with least active connections."""
        return min(
            self.endpoints,
            key=lambda e: self._stats[e.url].active_connections
        )

    def _ip_hash(self, ip: str) -> Endpoint:
        """IP hash based selection."""
        hash_value = int(hashlib.md5(ip.encode()).hexdigest(), 16)
        index = hash_value % len(self.endpoints)
        return self.endpoints[index]

    async def _fair_select(self) -> Endpoint:
        """Fair scheduling selection."""
        await self._update_fair_heap()
        if not self._fair_heap:
            return choice(self.endpoints)

        _, endpoint_url = heappop(self._fair_heap)
        endpoint = next(e for e in self.endpoints if e.url == endpoint_url)

        latency = self._stats[endpoint_url].total_latency / max(1, self._stats[endpoint_url].total_requests)
        heappush(self._fair_heap, (latency, endpoint_url))

        return endpoint

    async def _update_fair_heap(self) -> None:
        """Update fair scheduling heap."""
        self._fair_heap.clear()
        for endpoint in self.endpoints:
            stats = self._stats[endpoint.url]
            avg_latency = stats.total_latency / max(1, stats.total_requests)
            heappush(self._fair_heap, (avg_latency, endpoint.url))

    def get_healthy_endpoints(self) -> List[Endpoint]:
        """Get list of healthy endpoints."""
        return [
            e for e in self.endpoints
            if self._stats[e.url].active_connections < e.max_connections
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        return {
            "strategy": self.strategy.value,
            "total_endpoints": len(self.endpoints),
            "healthy_endpoints": len(self.get_healthy_endpoints()),
            "endpoints": [
                {
                    "url": stats.endpoint.url,
                    "active_connections": stats.active_connections,
                    "total_requests": stats.total_requests,
                    "failed_requests": stats.failed_requests,
                    "success_rate": (
                        (stats.total_requests - stats.failed_requests)
                        / max(1, stats.total_requests)
                    ),
                }
                for stats in self._stats.values()
            ],
        }


class NoEndpointsAvailableError(Exception):
    """Raised when no endpoints are available."""
    pass
