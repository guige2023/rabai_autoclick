"""
API Federation Action - Unified interface for multiple API endpoints.

This module provides API federation capabilities including endpoint
discovery, load balancing, and failover across multiple API backends.
"""

from __future__ import annotations

import asyncio
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from collections import defaultdict


class LoadBalancingStrategy(Enum):
    """Strategy for distributing requests across endpoints."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"
    HASH = "hash"


@dataclass
class FederationEndpoint:
    """A federated API endpoint."""
    name: str
    url: str
    weight: int = 1
    health_check_url: str | None = None
    timeout: float = 30.0
    max_retries: int = 3
    healthy: bool = True
    last_health_check: float = 0.0
    consecutive_failures: int = 0


@dataclass
class FederationConfig:
    """Configuration for API federation."""
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    health_check_interval: float = 60.0
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2
    retry_on_failover: bool = True


@dataclass
class FederationResult:
    """Result of a federated API request."""
    success: bool
    endpoint: str | None
    response: Any
    duration_ms: float
    attempts: int = 1
    error: str | None = None


class HealthChecker:
    """Health checker for federation endpoints."""
    
    def __init__(
        self,
        unhealthy_threshold: int = 3,
        healthy_threshold: int = 2,
    ) -> None:
        self.unhealthy_threshold = unhealthy_threshold
        self.healthy_threshold = healthy_threshold
        self._health_counts: dict[str, int] = defaultdict(int)
    
    async def check(self, endpoint: FederationEndpoint) -> bool:
        """Check if endpoint is healthy."""
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                check_url = endpoint.health_check_url or endpoint.url
                async with session.get(check_url) as response:
                    healthy = response.status < 500
        except Exception:
            healthy = False
        
        if healthy:
            self._health_counts[endpoint.name] = max(
                self._health_counts[endpoint.name] + 1,
                1
            )
            return self._health_counts[endpoint.name] >= self.healthy_threshold
        else:
            self._health_counts[endpoint.name] = min(
                self._health_counts[endpoint.name] - 1,
                -self.unhealthy_threshold
            )
            return False


class LoadBalancer:
    """Load balancer for federation endpoints."""
    
    def __init__(
        self,
        strategy: LoadBalancingStrategy,
    ) -> None:
        self.strategy = strategy
        self._round_robin_index: dict[str, int] = defaultdict(int)
        self._connection_counts: dict[str, int] = defaultdict(int)
        self._hash_counts: dict[str, int] = defaultdict(int)
    
    def select(
        self,
        endpoints: list[FederationEndpoint],
        key: str | None = None,
    ) -> FederationEndpoint | None:
        """Select an endpoint based on load balancing strategy."""
        healthy = [e for e in endpoints if e.healthy]
        
        if not healthy:
            return None
        
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(healthy)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(healthy)
        elif self.strategy == LoadBalancingStrategy.HASH:
            return self._hash(healthy, key)
        
        return healthy[0]
    
    def _round_robin(self, endpoints: list[FederationEndpoint]) -> FederationEndpoint:
        """Round robin selection."""
        name = ",".join(e.name for e in endpoints)
        index = self._round_robin_index[name] % len(endpoints)
        self._round_robin_index[name] += 1
        return endpoints[index]
    
    def _least_connections(self, endpoints: list[FederationEndpoint]) -> FederationEndpoint:
        """Select endpoint with least connections."""
        return min(endpoints, key=lambda e: self._connection_counts[e.name])
    
    def _weighted(self, endpoints: list[FederationEndpoint]) -> FederationEndpoint:
        """Weighted selection based on endpoint weight."""
        total_weight = sum(e.weight for e in endpoints)
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for endpoint in endpoints:
            cumulative += endpoint.weight
            if r <= cumulative:
                return endpoint
        return endpoints[-1]
    
    def _hash(self, endpoints: list[FederationEndpoint], key: str | None) -> FederationEndpoint:
        """Consistent hashing selection."""
        if not key:
            return endpoints[0]
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return endpoints[hash_val % len(endpoints)]
    
    def increment_connections(self, endpoint_name: str) -> None:
        """Increment connection count for endpoint."""
        self._connection_counts[endpoint_name] += 1
    
    def decrement_connections(self, endpoint_name: str) -> None:
        """Decrement connection count for endpoint."""
        self._connection_counts[endpoint_name] = max(
            0,
            self._connection_counts[endpoint_name] - 1
        )


class APIFederationAction:
    """
    API Federation for unified access to multiple backends.
    
    Example:
        federation = APIFederationAction()
        federation.add_endpoint(FederationEndpoint(name="api1", url="https://api1.example.com"))
        federation.add_endpoint(FederationEndpoint(name="api2", url="https://api2.example.com"))
        result = await federation.request("/data", {"key": "value"})
    """
    
    def __init__(self, config: FederationConfig | None = None) -> None:
        self.config = config or FederationConfig()
        self._endpoints: dict[str, FederationEndpoint] = {}
        self._health_checker = HealthChecker(
            self.config.unhealthy_threshold,
            self.config.healthy_threshold,
        )
        self._load_balancer = LoadBalancer(self.config.strategy)
        self._lock = asyncio.Lock()
    
    def add_endpoint(self, endpoint: FederationEndpoint) -> None:
        """Add an endpoint to the federation."""
        self._endpoints[endpoint.name] = endpoint
    
    def remove_endpoint(self, name: str) -> bool:
        """Remove an endpoint from the federation."""
        if name in self._endpoints:
            del self._endpoints[name]
            return True
        return False
    
    async def request(
        self,
        path: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        hash_key: str | None = None,
    ) -> FederationResult:
        """
        Make a federated request to an available endpoint.
        
        Args:
            path: API path
            method: HTTP method
            headers: Request headers
            params: Query parameters
            body: Request body
            hash_key: Key for consistent hashing
            
        Returns:
            FederationResult with response data
        """
        start_time = time.time()
        attempts = 0
        tried_endpoints: set[str] = set()
        
        while attempts < len(self._endpoints):
            attempts += 1
            endpoint = self._load_balancer.select(
                list(self._endpoints.values()),
                hash_key,
            )
            
            if endpoint is None:
                return FederationResult(
                    success=False,
                    endpoint=None,
                    response=None,
                    duration_ms=(time.time() - start_time) * 1000,
                    attempts=attempts,
                    error="No healthy endpoints available",
                )
            
            if endpoint.name in tried_endpoints:
                continue
            
            tried_endpoints.add(endpoint.name)
            self._load_balancer.increment_connections(endpoint.name)
            
            try:
                result = await self._do_request(endpoint, path, method, headers, params, body)
                self._load_balancer.decrement_connections(endpoint.name)
                
                if result[0] < 500:
                    endpoint.consecutive_failures = 0
                    return FederationResult(
                        success=True,
                        endpoint=endpoint.name,
                        response=result[1],
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts,
                    )
                else:
                    endpoint.consecutive_failures += 1
                    if endpoint.consecutive_failures >= self.config.unhealthy_threshold:
                        endpoint.healthy = False
            
            except Exception as e:
                self._load_balancer.decrement_connections(endpoint.name)
                endpoint.consecutive_failures += 1
                if endpoint.consecutive_failures >= self.config.unhealthy_threshold:
                    endpoint.healthy = False
                
                if not self.config.retry_on_failover:
                    return FederationResult(
                        success=False,
                        endpoint=endpoint.name,
                        response=None,
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts,
                        error=str(e),
                    )
        
        return FederationResult(
            success=False,
            endpoint=None,
            response=None,
            duration_ms=(time.time() - start_time) * 1000,
            attempts=attempts,
            error="All endpoints failed",
        )
    
    async def _do_request(
        self,
        endpoint: FederationEndpoint,
        path: str,
        method: str,
        headers: dict[str, str] | None,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
    ) -> tuple[int, Any]:
        """Execute HTTP request to endpoint."""
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=endpoint.timeout)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{endpoint.url.rstrip('/')}/{path.lstrip('/')}"
            async with session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=body,
            ) as response:
                try:
                    data = await response.json()
                except Exception:
                    data = await response.text()
                return response.status, data
    
    async def health_check_all(self) -> dict[str, bool]:
        """Run health check on all endpoints."""
        results = {}
        for endpoint in self._endpoints.values():
            is_healthy = await self._health_checker.check(endpoint)
            endpoint.healthy = is_healthy
            endpoint.last_health_check = time.time()
            results[endpoint.name] = is_healthy
        return results
    
    def get_endpoint_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all endpoints."""
        return {
            name: {
                "healthy": e.healthy,
                "consecutive_failures": e.consecutive_failures,
                "last_health_check": e.last_health_check,
                "weight": e.weight,
            }
            for name, e in self._endpoints.items()
        }


# Export public API
__all__ = [
    "LoadBalancingStrategy",
    "FederationEndpoint",
    "FederationConfig",
    "FederationResult",
    "HealthChecker",
    "LoadBalancer",
    "APIFederationAction",
]
