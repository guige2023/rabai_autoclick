"""API Service Mesh Action Module.

Implements service mesh patterns for API traffic management including:
- Traffic routing and splitting
- Service discovery integration
- Load balancing strategies
- Circuit breaking per service

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = auto()
    LEAST_CONNECTIONS = auto()
    RANDOM = auto()
    WEIGHTED = auto()
    CONSISTENT_HASH = auto()


@dataclass
class ServiceEndpoint:
    """Represents a service endpoint."""
    id: str
    host: str
    port: int
    weight: int = 100
    healthy: bool = True
    latency_ms: float = 0.0
    connections: int = 0
    last_health_check: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceConfig:
    """Configuration for a service in the mesh."""
    name: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    health_check_path: Optional[str] = None
    health_check_interval: float = 30.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 30.0


class ServiceMeshRouter:
    """Routes traffic within a service mesh.
    
    Provides:
    - Load balancing across endpoints
    - Health-based routing
    - Traffic splitting
    - Consistent hashing
    """
    
    def __init__(
        self,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    ):
        self.strategy = strategy
        self._services: Dict[str, ServiceConfig] = {}
        self._round_robin_counters: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    def register_service(self, config: ServiceConfig) -> None:
        """Register a service with the mesh.
        
        Args:
            config: Service configuration
        """
        self._services[config.name] = config
        self._round_robin_counters[config.name] = 0
        logger.info(f"Registered service: {config.name}")
    
    def add_endpoint(self, service_name: str, endpoint: ServiceEndpoint) -> None:
        """Add an endpoint to a service.
        
        Args:
            service_name: Service name
            endpoint: Endpoint to add
        """
        if service_name not in self._services:
            self._services[service_name] = ServiceConfig(name=service_name)
        
        self._services[service_name].endpoints.append(endpoint)
        logger.info(f"Added endpoint {endpoint.id} to service {service_name}")
    
    def remove_endpoint(self, service_name: str, endpoint_id: str) -> bool:
        """Remove an endpoint from a service.
        
        Args:
            service_name: Service name
            endpoint_id: Endpoint ID to remove
            
        Returns:
            True if removed
        """
        if service_name not in self._services:
            return False
        
        endpoints = self._services[service_name].endpoints
        for i, ep in enumerate(endpoints):
            if ep.id == endpoint_id:
                endpoints.pop(i)
                return True
        return False
    
    async def route(
        self,
        service_name: str,
        request_key: Optional[str] = None
    ) -> Optional[ServiceEndpoint]:
        """Route to an endpoint using the configured strategy.
        
        Args:
            service_name: Service to route to
            request_key: Optional key for consistent hashing
            
        Returns:
            Selected endpoint or None
        """
        if service_name not in self._services:
            logger.warning(f"Service not found: {service_name}")
            return None
        
        service = self._services[service_name]
        healthy_endpoints = [ep for ep in service.endpoints if ep.healthy]
        
        if not healthy_endpoints:
            logger.warning(f"No healthy endpoints for service: {service_name}")
            return None
        
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(service_name, healthy_endpoints)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy_endpoints)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            return random.choice(healthy_endpoints)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted_choice(healthy_endpoints)
        elif self.strategy == LoadBalancingStrategy.CONSISTENT_HASH:
            return self._consistent_hash(service_name, request_key, healthy_endpoints)
        
        return healthy_endpoints[0]
    
    def _round_robin(
        self,
        service_name: str,
        endpoints: List[ServiceEndpoint]
    ) -> ServiceEndpoint:
        """Round robin load balancing."""
        counter = self._round_robin_counters.get(service_name, 0)
        selected = endpoints[counter % len(endpoints)]
        self._round_robin_counters[service_name] = counter + 1
        return selected
    
    def _least_connections(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Select endpoint with fewest connections."""
        return min(endpoints, key=lambda ep: ep.connections)
    
    def _weighted_choice(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Weighted random selection."""
        total_weight = sum(ep.weight for ep in endpoints)
        r = random.uniform(0, total_weight)
        cumulative = 0
        
        for ep in endpoints:
            cumulative += ep.weight
            if r <= cumulative:
                return ep
        
        return endpoints[-1]
    
    def _consistent_hash(
        self,
        service_name: str,
        request_key: Optional[str],
        endpoints: List[ServiceEndpoint]
    ) -> ServiceEndpoint:
        """Consistent hashing for request affinity."""
        if not request_key:
            return random.choice(endpoints)
        
        hash_value = int(hashlib.md5(f"{service_name}:{request_key}".encode()).hexdigest(), 16)
        index = hash_value % len(endpoints)
        return endpoints[index]
    
    async def update_endpoint_health(
        self,
        service_name: str,
        endpoint_id: str,
        healthy: bool
    ) -> None:
        """Update endpoint health status.
        
        Args:
            service_name: Service name
            endpoint_id: Endpoint ID
            healthy: Health status
        """
        if service_name not in self._services:
            return
        
        for ep in self._services[service_name].endpoints:
            if ep.id == endpoint_id:
                ep.healthy = healthy
                ep.last_health_check = time.time()
                status = "healthy" if healthy else "unhealthy"
                logger.info(f"Endpoint {endpoint_id} marked {status}")
                break
    
    def get_service_stats(self, service_name: str) -> Dict[str, Any]:
        """Get statistics for a service.
        
        Args:
            service_name: Service name
            
        Returns:
            Service statistics
        """
        if service_name not in self._services:
            return {"error": "Service not found"}
        
        service = self._services[service_name]
        total_connections = sum(ep.connections for ep in service.endpoints)
        healthy_count = sum(1 for ep in service.endpoints if ep.healthy)
        
        return {
            "name": service_name,
            "total_endpoints": len(service.endpoints),
            "healthy_endpoints": healthy_count,
            "total_connections": total_connections,
            "endpoints": [
                {
                    "id": ep.id,
                    "host": ep.host,
                    "port": ep.port,
                    "healthy": ep.healthy,
                    "connections": ep.connections,
                    "latency_ms": ep.latency_ms
                }
                for ep in service.endpoints
            ]
        }


class TrafficSplitter:
    """Splits traffic between multiple service versions.
    
    Supports:
    - Percentage-based splitting
    - Header-based routing
    - Weight-based traffic distribution
    """
    
    def __init__(self, router: ServiceMeshRouter):
        self.router = router
        self._rules: List[Dict[str, Any]] = []
    
    def add_weighted_rule(
        self,
        service_name: str,
        version_weights: Dict[str, float]
    ) -> None:
        """Add weighted traffic split rule.
        
        Args:
            service_name: Service name
            version_weights: Dict mapping version to weight (0-100)
        """
        self._rules.append({
            "type": "weighted",
            "service": service_name,
            "weights": version_weights
        })
    
    def add_header_rule(
        self,
        service_name: str,
        header_name: str,
        header_values: Dict[str, str]
    ) -> None:
        """Add header-based routing rule.
        
        Args:
            service_name: Service name
            header_name: Header to match
            header_values: Dict mapping header value to version
        """
        self._rules.append({
            "type": "header",
            "service": service_name,
            "header": header_name,
            "values": header_values
        })
    
    async def route_with_splits(
        self,
        service_name: str,
        headers: Optional[Dict[str, str]] = None,
        request_key: Optional[str] = None
    ) -> Optional[ServiceEndpoint]:
        """Route considering traffic split rules.
        
        Args:
            service_name: Service name
            headers: Request headers
            request_key: Request key for hashing
            
        Returns:
            Selected endpoint
        """
        for rule in self._rules:
            if rule["service"] != service_name:
                continue
            
            if rule["type"] == "header" and headers:
                header_value = headers.get(rule["header"])
                if header_value and header_value in rule["values"]:
                    version = rule["values"][header_value]
                    endpoint_id = f"{service_name}-{version}"
                    service = self.router._services.get(service_name)
                    if service:
                        for ep in service.endpoints:
                            if ep.id == endpoint_id:
                                return ep
            
            elif rule["type"] == "weighted":
                r = random.uniform(0, 100)
                cumulative = 0
                
                for version, weight in rule["weights"].items():
                    cumulative += weight
                    if r <= cumulative:
                        endpoint_id = f"{service_name}-{version}"
                        service = self.router._services.get(service_name)
                        if service:
                            for ep in service.endpoints:
                                if ep.id == endpoint_id:
                                    return ep
        
        return await self.router.route(service_name, request_key)


class CircuitBreakerPerService:
    """Circuit breaker for individual services.
    
    Tracks failures per service and trips the circuit
    when thresholds are exceeded.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: float = 30.0,
        half_open_attempts: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_attempts = half_open_attempts
        self._circuits: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def record_success(self, service_name: str) -> None:
        """Record a successful call.
        
        Args:
            service_name: Service name
        """
        async with self._lock:
            if service_name not in self._circuits:
                return
            
            circuit = self._circuits[service_name]
            circuit["success_count"] += 1
            circuit["consecutive_failures"] = 0
            
            if circuit["state"] == "half_open":
                if circuit["success_count"] >= self.half_open_attempts:
                    circuit["state"] = "closed"
                    circuit["success_count"] = 0
                    logger.info(f"Circuit closed for service: {service_name}")
    
    async def record_failure(self, service_name: str) -> None:
        """Record a failed call.
        
        Args:
            service_name: Service name
        """
        async with self._lock:
            if service_name not in self._circuits:
                self._circuits[service_name] = {
                    "state": "closed",
                    "consecutive_failures": 0,
                    "success_count": 0,
                    "last_failure_time": None
                }
            
            circuit = self._circuits[service_name]
            circuit["consecutive_failures"] += 1
            circuit["last_failure_time"] = time.time()
            
            if circuit["state"] == "half_open":
                circuit["state"] = "open"
                logger.warning(f"Circuit reopened for service: {service_name}")
            elif circuit["consecutive_failures"] >= self.failure_threshold:
                circuit["state"] = "open"
                logger.warning(f"Circuit opened for service: {service_name}")
    
    async def is_circuit_open(self, service_name: str) -> bool:
        """Check if circuit is open for a service.
        
        Args:
            service_name: Service name
            
        Returns:
            True if circuit is open
        """
        async with self._lock:
            if service_name not in self._circuits:
                return False
            
            circuit = self._circuits[service_name]
            
            if circuit["state"] == "open":
                if circuit["last_failure_time"]:
                    elapsed = time.time() - circuit["last_failure_time"]
                    if elapsed >= self.timeout_seconds:
                        circuit["state"] = "half_open"
                        circuit["success_count"] = 0
                        logger.info(f"Circuit half-open for service: {service_name}")
                        return False
                return True
            
            return False
    
    def get_circuit_state(self, service_name: str) -> str:
        """Get current circuit state for a service."""
        if service_name not in self._circuits:
            return "closed"
        return self._circuits[service_name]["state"]
