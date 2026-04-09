"""
API Load Balancer Action Module

Load balancing across multiple API endpoints with strategies
(round-robin, weighted, least connections, health-aware).

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    """Load balancing strategies."""
    
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    RANDOM = "random"
    IP_HASH = "ip_hash"


@dataclass
class Endpoint:
    """API endpoint definition."""
    
    id: str
    url: str
    weight: int = 1
    max_connections: int = 100
    timeout_seconds: float = 30
    health_check_path: str = "/health"
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    avg_response_time_ms: float = 0
    last_health_check: Optional[float] = None
    healthy: bool = True


@dataclass
class Request:
    """Balanced request."""
    
    id: str
    endpoint: Endpoint
    timestamp: float = field(default_factory=time.time)


@dataclass
class LoadBalanceResult:
    """Result of load balancing decision."""
    
    endpoint: Endpoint
    strategy: LoadBalanceStrategy
    request_id: str


class HealthChecker:
    """Monitors endpoint health."""
    
    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self._lock = asyncio.Lock()
    
    async def check(self, endpoint: Endpoint, client: Callable) -> bool:
        """Check endpoint health."""
        try:
            response = await asyncio.wait_for(
                client({"method": "GET", "url": endpoint.url + endpoint.health_check_path}),
                timeout=self.timeout
            )
            healthy = 200 <= response.get("status_code", 500) < 300
            return healthy
        except Exception as e:
            logger.warning(f"Health check failed for {endpoint.url}: {e}")
            return False


class LoadBalancer:
    """Core load balancing logic."""
    
    def __init__(self, strategy: LoadBalanceStrategy):
        self.strategy = strategy
        self._round_robin_counters: Dict[str, int] = defaultdict(int)
        self._request_history: List[Request] = []
    
    def select(
        self,
        endpoints: List[Endpoint],
        request_context: Optional[Dict] = None
    ) -> Optional[Endpoint]:
        """Select an endpoint based on strategy."""
        enabled = [e for e in endpoints if e.enabled and e.healthy]
        
        if not enabled:
            return None
        
        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            return self._round_robin(enabled)
        
        if self.strategy == LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin(enabled)
        
        if self.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return self._least_connections(enabled)
        
        if self.strategy == LoadBalanceStrategy.LEAST_RESPONSE_TIME:
            return self._least_response_time(enabled)
        
        if self.strategy == LoadBalanceStrategy.RANDOM:
            import random
            return random.choice(enabled)
        
        if self.strategy == LoadBalanceStrategy.IP_HASH:
            client_ip = request_context.get("client_ip", "") if request_context else ""
            return self._ip_hash(enabled, client_ip)
        
        return enabled[0]
    
    def _round_robin(self, endpoints: List[Endpoint]) -> Endpoint:
        """Round-robin selection."""
        counter = self._round_robin_counters["default"]
        self._round_robin_counters["default"] = (counter + 1) % len(endpoints)
        return endpoints[counter]
    
    def _weighted_round_robin(self, endpoints: List[Endpoint]) -> Endpoint:
        """Weighted round-robin selection."""
        weighted_list = []
        for ep in endpoints:
            weighted_list.extend([ep] * ep.weight)
        
        counter = self._round_robin_counters["weighted"]
        self._round_robin_counters["weighted"] = (counter + 1) % len(weighted_list)
        return weighted_list[counter]
    
    def _least_connections(self, endpoints: List[Endpoint]) -> Endpoint:
        """Select endpoint with least active connections."""
        return min(endpoints, key=lambda e: e.active_connections)
    
    def _least_response_time(self, endpoints: List[Endpoint]) -> Endpoint:
        """Select endpoint with lowest average response time."""
        return min(endpoints, key=lambda e: e.avg_response_time_ms)
    
    def _ip_hash(self, endpoints: List[Endpoint], client_ip: str) -> Endpoint:
        """IP hash-based selection."""
        if not client_ip:
            return endpoints[0]
        
        hash_value = sum(ord(c) for c in client_ip)
        index = hash_value % len(endpoints)
        return endpoints[index]


class APILoadBalancerAction:
    """
    Main API load balancer action handler.
    
    Provides intelligent load balancing across multiple API endpoints
    with health checking and configurable strategies.
    """
    
    def __init__(
        self,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN
    ):
        self.strategy = strategy
        self.load_balancer = LoadBalancer(strategy)
        self.health_checker = HealthChecker()
        self._endpoints: Dict[str, Endpoint] = {}
        self._client: Optional[Callable] = None
        self._middleware: List[Callable] = []
        self._stats = {
            "total_requests": 0,
            "failed_requests": 0,
            "by_endpoint": defaultdict(lambda: {"requests": 0, "failures": 0})
        }
    
    def add_endpoint(
        self,
        url: str,
        weight: int = 1,
        max_connections: int = 100,
        metadata: Optional[Dict] = None
    ) -> str:
        """Add an endpoint to the load balancer."""
        endpoint_id = str(uuid.uuid4())
        endpoint = Endpoint(
            id=endpoint_id,
            url=url.rstrip("/"),
            weight=weight,
            max_connections=max_connections,
            metadata=metadata or {}
        )
        
        self._endpoints[endpoint_id] = endpoint
        return endpoint_id
    
    def remove_endpoint(self, endpoint_id: str) -> bool:
        """Remove an endpoint from the load balancer."""
        if endpoint_id in self._endpoints:
            del self._endpoints[endpoint_id]
            return True
        return False
    
    def get_endpoint(self, endpoint_id: str) -> Optional[Endpoint]:
        """Get endpoint by ID."""
        return self._endpoints.get(endpoint_id)
    
    def set_endpoint_enabled(self, endpoint_id: str, enabled: bool) -> None:
        """Enable or disable an endpoint."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint:
            endpoint.enabled = enabled
    
    def set_client(self, client: Callable) -> None:
        """Set the HTTP client for requests and health checks."""
        self._client = client
    
    async def select_endpoint(
        self,
        request_context: Optional[Dict] = None
    ) -> Optional[Endpoint]:
        """Select an endpoint for the next request."""
        endpoints = list(self._endpoints.values())
        endpoint = self.load_balancer.select(endpoints, request_context)
        
        if endpoint:
            endpoint.active_connections += 1
            endpoint.total_requests += 1
            self._stats["total_requests"] += 1
        
        return endpoint
    
    async def health_check_all(self) -> Dict[str, bool]:
        """Perform health check on all endpoints."""
        results = {}
        
        for endpoint in self._endpoints.values():
            if self._client:
                healthy = await self.health_checker.check(endpoint, self._client)
            else:
                healthy = True
            
            endpoint.healthy = healthy
            endpoint.last_health_check = time.time()
            results[endpoint.id] = healthy
        
        return results
    
    async def record_response(
        self,
        endpoint_id: str,
        success: bool,
        response_time_ms: float
    ) -> None:
        """Record response metrics for an endpoint."""
        endpoint = self._endpoints.get(endpoint_id)
        if not endpoint:
            return
        
        endpoint.active_connections = max(0, endpoint.active_connections - 1)
        
        if not success:
            endpoint.failed_requests += 1
            self._stats["failed_requests"] += 1
            self._stats["by_endpoint"][endpoint_id]["failures"] += 1
            
            failure_rate = endpoint.failed_requests / max(endpoint.total_requests, 1)
            if failure_rate > 0.5:
                endpoint.healthy = False
        
        n = endpoint.total_requests
        endpoint.avg_response_time_ms = (
            (endpoint.avg_response_time_ms * (n - 1) + response_time_ms) / n
        )
        
        self._stats["by_endpoint"][endpoint_id]["requests"] += 1
    
    async def forward_request(
        self,
        request: Dict,
        request_context: Optional[Dict] = None
    ) -> Tuple[Optional[Dict], Optional[Endpoint]]:
        """Forward request to selected endpoint."""
        endpoint = await self.select_endpoint(request_context)
        
        if not endpoint:
            return None, None
        
        if self._client:
            try:
                response = await self._client({
                    **request,
                    "url": endpoint.url + request.get("path", "/")
                })
                return response, endpoint
            except Exception as e:
                await self.record_response(endpoint.id, False, 0)
                raise
        
        return None, endpoint
    
    def get_endpoints_status(self) -> List[Dict]:
        """Get status of all endpoints."""
        return [
            {
                "id": ep.id,
                "url": ep.url,
                "enabled": ep.enabled,
                "healthy": ep.healthy,
                "active_connections": ep.active_connections,
                "total_requests": ep.total_requests,
                "failed_requests": ep.failed_requests,
                "avg_response_time_ms": ep.avg_response_time_ms,
                "last_health_check": ep.last_health_check,
                "weight": ep.weight
            }
            for ep in self._endpoints.values()
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        total_requests = self._stats["total_requests"]
        failed_requests = self._stats["failed_requests"]
        
        return {
            "strategy": self.strategy.value,
            "total_endpoints": len(self._endpoints),
            "enabled_endpoints": sum(1 for e in self._endpoints.values() if e.enabled),
            "healthy_endpoints": sum(1 for e in self._endpoints.values() if e.healthy),
            "total_requests": total_requests,
            "failed_requests": failed_requests,
            "success_rate": f"{(1 - failed_requests / max(total_requests, 1)) * 100:.1f}%",
            "total_active_connections": sum(
                e.active_connections for e in self._endpoints.values()
            )
        }
