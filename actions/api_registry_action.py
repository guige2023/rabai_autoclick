"""
API Registry Action.

Provides API service registry and discovery.
Supports:
- Service registration/deregistration
- Health checking
-负载均衡策略
- Service metadata
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import logging
import hashlib
import time

logger = logging.getLogger(__name__)


@dataclass
class ServiceEndpoint:
    """Service endpoint information."""
    service_id: str
    service_name: str
    host: str
    port: int
    protocol: str = "http"
    path_prefix: str = ""
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    health_check_url: Optional[str] = None
    status: str = "healthy"
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    unhealthy_count: int = 0
    
    @property
    def url(self) -> str:
        """Get full URL."""
        base = f"{self.protocol}://{self.host}:{self.port}"
        return f"{base}{self.path_prefix}" if self.path_prefix else base


@dataclass
class ServiceMetadata:
    """Service metadata for discovery."""
    service_name: str
    version: str
    description: str = ""
    owner: str = ""
    dependencies: List[str] = field(default_factory=list)
    schemas: Dict[str, str] = field(default_factory=dict)  # endpoint -> schema URL


class ApiRegistryAction:
    """
    API Registry Action.
    
    Provides service registry with support for:
    - Service registration and deregistration
    - Health monitoring
    - Multiple load balancing strategies
    - Service metadata management
    """
    
    STRATEGIES = ["round_robin", "random", "weighted", "least_connections", "ip_hash"]
    
    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: int = 30,
        unhealthy_threshold: int = 3,
        ttl: int = 300
    ):
        """
        Initialize the API Registry Action.
        
        Args:
            strategy: Load balancing strategy
            health_check_interval: Health check interval in seconds
            unhealthy_threshold: Threshold for marking unhealthy
            ttl: Service TTL in seconds
        """
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        self.unhealthy_threshold = unhealthy_threshold
        self.ttl = ttl
        
        self._services: Dict[str, ServiceEndpoint] = {}
        self._metadata: Dict[str, ServiceMetadata] = {}
        self._connections: Dict[str, int] = {}  # For least_connections
        self._last_requests: Dict[str, List[datetime]] = {}  # For sliding window
        self._lock = threading.RLock()
        self._strategy_counters: Dict[str, int] = {}  # For round_robin
        self._running = False
        self._health_thread: Optional[threading.Thread] = None
    
    def register(
        self,
        service_name: str,
        host: str,
        port: int,
        protocol: str = "http",
        path_prefix: str = "",
        service_id: Optional[str] = None,
        weight: int = 1,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        health_check_url: Optional[str] = None
    ) -> ServiceEndpoint:
        """
        Register a service endpoint.
        
        Args:
            service_name: Name of the service
            host: Service host
            port: Service port
            protocol: Protocol (http/https)
            path_prefix: URL path prefix
            service_id: Unique service ID (auto-generated if not provided)
            weight: Service weight for load balancing
            tags: Service tags
            metadata: Additional metadata
            health_check_url: Health check URL
        
        Returns:
            Registered ServiceEndpoint
        """
        if service_id is None:
            content = f"{service_name}:{host}:{port}:{time.time()}"
            service_id = hashlib.md5(content.encode()).hexdigest()[:16]
        
        endpoint = ServiceEndpoint(
            service_id=service_id,
            service_name=service_name,
            host=host,
            port=port,
            protocol=protocol,
            path_prefix=path_prefix,
            weight=weight,
            tags=tags or [],
            metadata=metadata or {},
            health_check_url=health_check_url
        )
        
        with self._lock:
            self._services[service_id] = endpoint
        
        logger.info(f"Registered service: {service_name} ({service_id}) at {endpoint.url}")
        return endpoint
    
    def deregister(self, service_id: str) -> bool:
        """
        Deregister a service.
        
        Args:
            service_id: Service ID to deregister
        
        Returns:
            True if deregistered, False if not found
        """
        with self._lock:
            if service_id in self._services:
                service = self._services.pop(service_id)
                if service_id in self._connections:
                    del self._connections[service_id]
                logger.info(f"Deregistered service: {service.service_name} ({service_id})")
                return True
        return False
    
    def heartbeat(self, service_id: str) -> bool:
        """
        Update service heartbeat.
        
        Args:
            service_id: Service ID
        
        Returns:
            True if heartbeat updated, False if not found
        """
        with self._lock:
            if service_id in self._services:
                self._services[service_id].last_heartbeat = datetime.utcnow()
                self._services[service_id].status = "healthy"
                self._services[service_id].unhealthy_count = 0
                return True
        return False
    
    def discover(
        self,
        service_name: str,
        tags: Optional[List[str]] = None,
        healthy_only: bool = True
    ) -> Optional[ServiceEndpoint]:
        """
        Discover a service endpoint using the configured strategy.
        
        Args:
            service_name: Name of the service
            tags: Optional tags to filter by
            healthy_only: Only return healthy endpoints
        
        Returns:
            A ServiceEndpoint or None
        """
        endpoints = self.get_endpoints(service_name, tags, healthy_only)
        
        if not endpoints:
            return None
        
        return self._select_endpoint(endpoints)
    
    def get_endpoints(
        self,
        service_name: str,
        tags: Optional[List[str]] = None,
        healthy_only: bool = True
    ) -> List[ServiceEndpoint]:
        """
        Get all endpoints for a service.
        
        Args:
            service_name: Name of the service
            tags: Optional tags to filter by
            healthy_only: Only return healthy endpoints
        
        Returns:
            List of ServiceEndpoints
        """
        with self._lock:
            endpoints = [
                s for s in self._services.values()
                if s.service_name == service_name
            ]
        
        if tags:
            endpoints = [
                e for e in endpoints
                if any(tag in e.tags for tag in tags)
            ]
        
        if healthy_only:
            endpoints = [e for e in endpoints if e.status == "healthy"]
        
        return endpoints
    
    def _select_endpoint(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Select an endpoint based on the configured strategy."""
        if not endpoints:
            raise ValueError("No endpoints available")
        
        if self.strategy == "round_robin":
            service_name = endpoints[0].service_name
            if service_name not in self._strategy_counters:
                self._strategy_counters[service_name] = 0
            
            index = self._strategy_counters[service_name] % len(endpoints)
            self._strategy_counters[service_name] += 1
            return endpoints[index]
        
        elif self.strategy == "random":
            import random
            return random.choice(endpoints)
        
        elif self.strategy == "weighted":
            total_weight = sum(e.weight for e in endpoints)
            import random
            r = random.uniform(0, total_weight)
            cumulative = 0
            for endpoint in endpoints:
                cumulative += endpoint.weight
                if r <= cumulative:
                    return endpoint
            return endpoints[-1]
        
        elif self.strategy == "least_connections":
            # Select endpoint with fewest active connections
            with self._lock:
                return min(
                    endpoints,
                    key=lambda e: self._connections.get(e.service_id, 0)
                )
        
        elif self.strategy == "ip_hash":
            # Placeholder - would hash client IP
            return endpoints[0]
        
        return endpoints[0]
    
    def record_request(self, service_id: str) -> None:
        """Record a request to a service (for statistics)."""
        with self._lock:
            if service_id not in self._connections:
                self._connections[service_id] = 0
            self._connections[service_id] += 1
            
            if service_id not in self._last_requests:
                self._last_requests[service_id] = []
            self._last_requests[service_id].append(datetime.utcnow())
    
    def record_response(self, service_id: str) -> None:
        """Record a response from a service."""
        with self._lock:
            if service_id in self._connections and self._connections[service_id] > 0:
                self._connections[service_id] -= 1
    
    def set_metadata(
        self,
        service_name: str,
        version: str,
        description: str = "",
        owner: str = "",
        dependencies: Optional[List[str]] = None,
        schemas: Optional[Dict[str, str]] = None
    ) -> ServiceMetadata:
        """Set service metadata."""
        metadata = ServiceMetadata(
            service_name=service_name,
            version=version,
            description=description,
            owner=owner,
            dependencies=dependencies or [],
            schemas=schemas or {}
        )
        
        with self._lock:
            self._metadata[service_name] = metadata
        
        return metadata
    
    def get_metadata(self, service_name: str) -> Optional[ServiceMetadata]:
        """Get service metadata."""
        with self._lock:
            return self._metadata.get(service_name)
    
    def list_services(self) -> List[str]:
        """List all registered service names."""
        with self._lock:
            return list(set(s.service_name for s in self._services.values()))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        with self._lock:
            services_by_name: Dict[str, List] = {}
            for s in self._services.values():
                if s.service_name not in services_by_name:
                    services_by_name[s.service_name] = []
                services_by_name[s.service_name].append(s)
            
            return {
                "total_services": len(self._services),
                "total_endpoints": len(self._services),
                "services_by_name": {
                    name: {
                        "count": len(endpoints),
                        "healthy": sum(1 for e in endpoints if e.status == "healthy")
                    }
                    for name, endpoints in services_by_name.items()
                },
                "strategy": self.strategy,
                "metadata_count": len(self._metadata)
            }
    
    def start_health_checker(self) -> None:
        """Start the background health checker."""
        if self._running:
            return
        
        self._running = True
        self._health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._health_thread.start()
        logger.info("Health checker started")
    
    def stop_health_checker(self) -> None:
        """Stop the background health checker."""
        self._running = False
        if self._health_thread:
            self._health_thread.join(timeout=5)
        logger.info("Health checker stopped")
    
    def _health_check_loop(self) -> None:
        """Background health check loop."""
        import time
        
        while self._running:
            try:
                self._check_health()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            
            time.sleep(self.health_check_interval)
    
    def _check_health(self) -> None:
        """Check health of all registered services."""
        now = datetime.utcnow()
        expired_threshold = timedelta(seconds=self.ttl)
        
        with self._lock:
            for service_id, service in list(self._services.items()):
                # Check TTL
                if now - service.last_heartbeat > expired_threshold:
                    service.status = "unhealthy"
                    service.unhealthy_count += 1
                    logger.warning(
                        f"Service {service.service_name} ({service_id}) "
                        f"heartbeat expired"
                    )
                
                # Clean old requests
                if service_id in self._last_requests:
                    cutoff = now - timedelta(seconds=60)
                    self._last_requests[service_id] = [
                        t for t in self._last_requests[service_id]
                        if t > cutoff
                    ]


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create registry
    registry = ApiRegistryAction(strategy="weighted")
    
    # Register services
    registry.register(
        "user-service",
        "localhost",
        8001,
        weight=2,
        tags=["v1"],
        metadata={"version": "1.0.0"}
    )
    registry.register(
        "user-service",
        "localhost",
        8002,
        weight=1,
        tags=["v1"],
        metadata={"version": "1.0.0"}
    )
    registry.register(
        "order-service",
        "localhost",
        8003,
        tags=["v1"]
    )
    
    # Set metadata
    registry.set_metadata(
        "user-service",
        version="1.0.0",
        description="User management service",
        owner="team-users"
    )
    
    # Discover services
    endpoint = registry.discover("user-service")
    print(f"Discovered user service: {endpoint.url if endpoint else 'None'}")
    
    all_endpoints = registry.get_endpoints("user-service")
    print(f"User service endpoints: {len(all_endpoints)}")
    
    print(f"\nServices: {registry.list_services()}")
    print(f"Stats: {registry.get_stats()}")
