"""
API Registry Discovery Action.

Provides service registry with built-in discovery.
Supports:
- Service registration
- Health monitoring
- Load balancing
- Service discovery
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import logging
import hashlib
import time
import random

logger = logging.getLogger(__name__)


@dataclass
class ServiceInstance:
    """Service instance."""
    instance_id: str
    service_name: str
    host: str
    port: int
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "healthy"
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


class ServiceRegistry:
    """Service registry for managing instances."""
    
    def __init__(self):
        self._services: Dict[str, List[ServiceInstance]] = {}
        self._lock = threading.RLock()
    
    def register(self, instance: ServiceInstance) -> None:
        """Register a service instance."""
        with self._lock:
            if instance.service_name not in self._services:
                self._services[instance.service_name] = []
            
            # Check if instance already exists
            for i, existing in enumerate(self._services[instance.service_name]):
                if existing.instance_id == instance.instance_id:
                    self._services[instance.service_name][i] = instance
                    return
            
            self._services[instance.service_name].append(instance)
    
    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        with self._lock:
            if service_name not in self._services:
                return False
            
            self._services[service_name] = [
                i for i in self._services[service_name]
                if i.instance_id != instance_id
            ]
            return True
    
    def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True
    ) -> List[ServiceInstance]:
        """Get service instances."""
        with self._lock:
            if service_name not in self._services:
                return []
            
            instances = self._services[service_name]
            
            if healthy_only:
                timeout = timedelta(seconds=30)
                instances = [
                    i for i in instances
                    if datetime.utcnow() - i.last_heartbeat < timeout
                ]
            
            return instances


class ApiRegistryDiscoveryAction:
    """
    API Registry Discovery Action.
    
    Provides service registry and discovery with support for:
    - Service registration
    - Health monitoring
    - Multiple load balancing strategies
    - Service discovery
    """
    
    STRATEGIES = ["random", "round_robin", "weighted", "least_connections"]
    
    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: int = 30
    ):
        """
        Initialize the API Registry Discovery Action.
        
        Args:
            strategy: Load balancing strategy
            health_check_interval: Health check interval in seconds
        """
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        self.registry = ServiceRegistry()
        self._round_robin_counters: Dict[str, int] = {}
        self._connections: Dict[str, int] = {}
        self._running = False
        self._health_thread: Optional[threading.Thread] = None
    
    def register(
        self,
        service_name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
        weight: int = 1,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ServiceInstance:
        """
        Register a service instance.
        
        Args:
            service_name: Name of the service
            host: Service host
            port: Service port
            instance_id: Unique instance ID
            weight: Instance weight
            metadata: Additional metadata
        
        Returns:
            Registered ServiceInstance
        """
        if instance_id is None:
            content = f"{service_name}:{host}:{port}:{time.time()}"
            instance_id = hashlib.md5(content.encode()).hexdigest()[:16]
        
        instance = ServiceInstance(
            instance_id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            weight=weight,
            metadata=metadata or {}
        )
        
        self.registry.register(instance)
        logger.info(f"Registered service: {service_name} ({instance_id}) at {instance.url}")
        
        return instance
    
    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service."""
        return self.registry.deregister(service_name, instance_id)
    
    def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Send heartbeat for a service."""
        instances = self.registry.get_instances(service_name, healthy_only=False)
        
        for instance in instances:
            if instance.instance_id == instance_id:
                instance.last_heartbeat = datetime.utcnow()
                instance.status = "healthy"
                return True
        
        return False
    
    def discover(self, service_name: str) -> Optional[ServiceInstance]:
        """
        Discover a service instance.
        
        Args:
            service_name: Name of the service
        
        Returns:
            A ServiceInstance or None
        """
        instances = self.registry.get_instances(service_name)
        
        if not instances:
            logger.warning(f"No instances found for service: {service_name}")
            return None
        
        return self._select_instance(instances)
    
    def discover_all(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances for a service."""
        return self.registry.get_instances(service_name)
    
    def _select_instance(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Select an instance based on load balancing strategy."""
        if self.strategy == "random":
            return random.choice(instances)
        
        elif self.strategy == "round_robin":
            service_name = instances[0].service_name
            if service_name not in self._round_robin_counters:
                self._round_robin_counters[service_name] = 0
            
            index = self._round_robin_counters[service_name] % len(instances)
            self._round_robin_counters[service_name] += 1
            return instances[index]
        
        elif self.strategy == "weighted":
            total_weight = sum(i.weight for i in instances)
            r = random.uniform(0, total_weight)
            cumulative = 0
            
            for instance in instances:
                cumulative += instance.weight
                if r <= cumulative:
                    return instance
            
            return instances[-1]
        
        elif self.strategy == "least_connections":
            return min(
                instances,
                key=lambda i: self._connections.get(i.instance_id, 0)
            )
        
        return instances[0]
    
    def record_request(self, instance_id: str) -> None:
        """Record a request to an instance."""
        self._connections[instance_id] = self._connections.get(instance_id, 0) + 1
    
    def record_response(self, instance_id: str) -> None:
        """Record a response from an instance."""
        if self._connections.get(instance_id, 0) > 0:
            self._connections[instance_id] -= 1
    
    def list_services(self) -> List[str]:
        """List all registered services."""
        return list(self.registry._services.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        stats = {
            "strategy": self.strategy,
            "services": {},
            "total_instances": 0
        }
        
        for service_name, instances in self.registry._services.items():
            healthy = [
                i for i in instances
                if datetime.utcnow() - i.last_heartbeat < timedelta(seconds=30)
            ]
            
            stats["services"][service_name] = {
                "total": len(instances),
                "healthy": len(healthy),
                "unhealthy": len(instances) - len(healthy)
            }
            stats["total_instances"] += len(instances)
        
        return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    discovery = ApiRegistryDiscoveryAction(strategy="weighted")
    
    # Register services
    discovery.register("user-service", "localhost", 8001, weight=2)
    discovery.register("user-service", "localhost", 8002, weight=1)
    discovery.register("order-service", "localhost", 8003)
    
    # Discover
    instance = discovery.discover("user-service")
    print(f"Discovered: {instance.url if instance else 'None'}")
    
    # All instances
    instances = discovery.discover_all("user-service")
    print(f"Instances: {len(instances)}")
    
    # Stats
    print(f"Services: {discovery.list_services()}")
    print(f"Stats: {discovery.get_stats()}")
