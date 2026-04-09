"""
API Service Discovery Action Module.

Provides service discovery capabilities including service registry,
health checking, load balancing, and failover for API microservices.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import random
from datetime import datetime, timedelta
from collections import defaultdict


class HealthStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"


@dataclass
class ServiceEndpoint:
    """Represents a service endpoint."""
    id: str
    host: str
    port: int
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    health_status: HealthStatus = HealthStatus.UNKNOWN
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0
    latency_avg: float = 0.0


@dataclass
class Service:
    """Represents a registered service."""
    name: str
    version: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    endpoint_id: str
    healthy: bool
    latency: float
    timestamp: datetime
    error: Optional[str] = None


class HealthChecker:
    """
    Health checker for service endpoints.
    
    Example:
        checker = HealthChecker()
        result = await checker.check(endpoint)
    """
    
    def __init__(
        self,
        timeout: float = 5.0,
        failure_threshold: int = 3
    ):
        self.timeout = timeout
        self.failure_threshold = failure_threshold
        self.health_check_fn: Optional[Callable] = None
    
    def set_health_check(self, fn: Callable[[ServiceEndpoint], bool]):
        """Set custom health check function."""
        self.health_check_fn = fn
    
    async def check(self, endpoint: ServiceEndpoint) -> HealthCheckResult:
        """Perform health check on endpoint."""
        start = time.monotonic()
        
        try:
            if self.health_check_fn:
                healthy = await self._async_health_check(endpoint)
            else:
                healthy = await self._default_check(endpoint)
            
            latency = time.monotonic() - start
            
            return HealthCheckResult(
                endpoint_id=endpoint.id,
                healthy=healthy,
                latency=latency,
                timestamp=datetime.now()
            )
        except Exception as e:
            return HealthCheckResult(
                endpoint_id=endpoint.id,
                healthy=False,
                latency=time.monotonic() - start,
                timestamp=datetime.now(),
                error=str(e)
            )
    
    async def _default_check(self, endpoint: ServiceEndpoint) -> bool:
        """Default TCP health check."""
        import asyncio
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(endpoint.host, endpoint.port),
                timeout=self.timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False
    
    async def _async_health_check(self, endpoint: ServiceEndpoint) -> bool:
        """Run custom health check."""
        if asyncio.iscoroutinefunction(self.health_check_fn):
            return await self.health_check_fn(endpoint)
        return self.health_check_fn(endpoint)


class LoadBalancer:
    """
    Load balancer with multiple strategies.
    
    Example:
        balancer = LoadBalancer(strategy=LoadBalancingStrategy.ROUND_ROBIN)
        endpoint = balancer.select(service.endpoints)
    """
    
    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._counters: Dict[str, int] = defaultdict(int)
        self._connections: Dict[str, int] = defaultdict(int)
        self._locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
    
    def select(
        self,
        endpoints: List[ServiceEndpoint],
        key: Optional[str] = None
    ) -> Optional[ServiceEndpoint]:
        """Select an endpoint based on strategy."""
        if not endpoints:
            return None
        
        healthy = [e for e in endpoints if e.health_status == HealthStatus.HEALTHY]
        if not healthy:
            return None
        
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            return self._random(healthy)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(healthy)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return self._ip_hash(healthy, key or "")
        else:
            return healthy[0]
    
    def _round_robin(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Round robin selection."""
        service_name = endpoints[0].id.split(":")[0] if endpoints else "default"
        idx = self._counters[service_name] % len(endpoints)
        self._counters[service_name] += 1
        return endpoints[idx]
    
    def _random(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Random selection."""
        return random.choice(endpoints)
    
    def _least_connections(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Select endpoint with least connections."""
        min_connections = min(
            self._connections.get(e.id, 0) for e in endpoints
        )
        candidates = [
            e for e in endpoints
            if self._connections.get(e.id, 0) == min_connections
        ]
        return random.choice(candidates)
    
    def _weighted(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Weighted selection based on endpoint weight."""
        weights = [e.weight for e in endpoints]
        total = sum(weights)
        r = random.uniform(0, total)
        
        cumsum = 0
        for i, endpoint in enumerate(endpoints):
            cumsum += weights[i]
            if r <= cumsum:
                return endpoint
        return endpoints[-1]
    
    def _ip_hash(self, endpoints: List[ServiceEndpoint], key: str) -> ServiceEndpoint:
        """IP hash based selection."""
        hash_value = hash(key) % len(endpoints)
        return endpoints[hash_value]
    
    def record_connection(self, endpoint_id: str):
        """Record a new connection to endpoint."""
        self._connections[endpoint_id] += 1
    
    def release_connection(self, endpoint_id: str):
        """Release a connection from endpoint."""
        if self._connections.get(endpoint_id, 0) > 0:
            self._connections[endpoint_id] -= 1


class ServiceRegistry:
    """
    Service registry for managing service endpoints.
    
    Example:
        registry = ServiceRegistry()
        registry.register("user-service", "localhost", 8000)
        endpoint = registry.discover("user-service")
    """
    
    def __init__(self):
        self.services: Dict[str, Service] = {}
        self._lock = threading.RLock()
    
    def register(
        self,
        name: str,
        host: str,
        port: int,
        version: str = "v1",
        weight: int = 1,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ServiceEndpoint:
        """Register a new service endpoint."""
        with self._lock:
            if name not in self.services:
                self.services[name] = Service(name=name, version=version)
            
            endpoint_id = f"{name}:{host}:{port}"
            endpoint = ServiceEndpoint(
                id=endpoint_id,
                host=host,
                port=port,
                weight=weight,
                metadata=metadata or {}
            )
            
            self.services[name].endpoints.append(endpoint)
            return endpoint
    
    def deregister(self, name: str, host: str, port: int) -> bool:
        """Deregister a service endpoint."""
        with self._lock:
            if name not in self.services:
                return False
            
            endpoint_id = f"{name}:{host}:{port}"
            endpoints = self.services[name].endpoints
            self.services[name].endpoints = [
                e for e in endpoints if e.id != endpoint_id
            ]
            return True
    
    def discover(
        self,
        name: str,
        healthy_only: bool = True
    ) -> List[ServiceEndpoint]:
        """Discover service endpoints."""
        with self._lock:
            if name not in self.services:
                return []
            
            endpoints = self.services[name].endpoints
            if healthy_only:
                endpoints = [e for e in endpoints if e.health_status == HealthStatus.HEALTHY]
            
            return endpoints
    
    def get_all_services(self) -> List[str]:
        """Get all registered service names."""
        with self._lock:
            return list(self.services.keys())
    
    def update_health(
        self,
        name: str,
        endpoint_id: str,
        status: HealthStatus
    ):
        """Update health status of an endpoint."""
        with self._lock:
            if name not in self.services:
                return
            
            for endpoint in self.services[name].endpoints:
                if endpoint.id == endpoint_id:
                    endpoint.health_status = status
                    endpoint.last_health_check = datetime.now()
                    break


class ServiceDiscovery:
    """
    Complete service discovery system with registry and load balancing.
    
    Example:
        discovery = ServiceDiscovery()
        discovery.register("api", "localhost", 8000)
        
        endpoint = discovery.get_endpoint("api")
        if endpoint:
            make_request(endpoint)
    """
    
    def __init__(self):
        self.registry = ServiceRegistry()
        self.health_checker = HealthChecker()
        self.load_balancer = LoadBalancer()
    
    def register(
        self,
        name: str,
        host: str,
        port: int,
        **kwargs
    ) -> ServiceEndpoint:
        """Register a service."""
        return self.registry.register(name, host, port, **kwargs)
    
    def get_endpoint(
        self,
        name: str,
        strategy: Optional[LoadBalancingStrategy] = None
    ) -> Optional[ServiceEndpoint]:
        """Get an endpoint using configured load balancing."""
        if strategy:
            self.load_balancer.strategy = strategy
        
        endpoints = self.registry.discover(name)
        return self.load_balancer.select(endpoints)
    
    def get_all_endpoints(self, name: str) -> List[ServiceEndpoint]:
        """Get all endpoints for a service."""
        return self.registry.discover(name)


import asyncio


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APIServiceDiscoveryAction(BaseAction):
    """
    Service discovery action for API microservices.
    
    Parameters:
        operation: Operation type (register/discover/deregister)
        service_name: Name of the service
        host: Service host
        port: Service port
    
    Example:
        action = APIServiceDiscoveryAction()
        result = action.execute({}, {
            "operation": "register",
            "service_name": "user-api",
            "host": "localhost",
            "port": 8000
        })
    """
    
    _registry: Optional[ServiceRegistry] = None
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute service discovery operation."""
        operation = params.get("operation", "register")
        service_name = params.get("service_name")
        host = params.get("host")
        port = params.get("port")
        version = params.get("version", "v1")
        weight = params.get("weight", 1)
        
        with self._lock:
            if self._registry is None:
                self._registry = ServiceRegistry()
        
        if operation == "register":
            endpoint = self._registry.register(
                service_name, host, port, version, weight
            )
            return {
                "success": True,
                "operation": "register",
                "endpoint_id": endpoint.id,
                "service_name": service_name,
                "registered_at": datetime.now().isoformat()
            }
        
        elif operation == "discover":
            endpoints = self._registry.discover(service_name)
            return {
                "success": True,
                "operation": "discover",
                "service_name": service_name,
                "endpoint_count": len(endpoints),
                "endpoints": [
                    {"id": e.id, "host": e.host, "port": e.port}
                    for e in endpoints
                ]
            }
        
        elif operation == "deregister":
            success = self._registry.deregister(service_name, host, port)
            return {
                "success": success,
                "operation": "deregister",
                "service_name": service_name
            }
        
        else:
            return {
                "success": False,
                "error": f"Unknown operation: {operation}"
            }
