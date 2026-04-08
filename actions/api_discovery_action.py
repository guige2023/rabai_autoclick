"""
API Service Discovery Action.

Provides service discovery capabilities for microservices architecture.
Supports multiple discovery strategies: static, dynamic, and hybrid.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import logging
import hashlib
import json

logger = logging.getLogger(__name__)


@dataclass
class ServiceInstance:
    """Represents a single service instance."""
    instance_id: str
    service_name: str
    host: str
    port: int
    health_url: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "healthy"
    weight: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)

    @property
    def address(self) -> str:
        """Return the service address."""
        return f"{self.host}:{self.port}"

    @property
    def is_healthy(self) -> bool:
        """Check if the instance is healthy based on heartbeat."""
        timeout = timedelta(seconds=30)
        return datetime.utcnow() - self.last_heartbeat < timeout


@dataclass
class ServiceRegistry:
    """Registry for managing service instances."""
    services: Dict[str, List[ServiceInstance]] = field(default_factory=dict)
    lock: threading.RLock = field(default_factory=threading.RLock)

    def register(self, instance: ServiceInstance) -> bool:
        """Register a new service instance."""
        with self.lock:
            if instance.service_name not in self.services:
                self.services[instance.service_name] = []
            
            # Check if instance already exists
            for existing in self.services[instance.service_name]:
                if existing.instance_id == instance.instance_id:
                    existing.last_heartbeat = datetime.utcnow()
                    return False
            
            self.services[instance.service_name].append(instance)
            logger.info(f"Registered service: {instance.service_name}/{instance.instance_id}")
            return True

    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        with self.lock:
            if service_name not in self.services:
                return False
            
            self.services[service_name] = [
                i for i in self.services[service_name]
                if i.instance_id != instance_id
            ]
            logger.info(f"Deregistered service: {service_name}/{instance_id}")
            return True

    def get_instances(self, service_name: str, tags: Optional[List[str]] = None) -> List[ServiceInstance]:
        """Get healthy instances for a service, optionally filtered by tags."""
        with self.lock:
            if service_name not in self.services:
                return []
            
            instances = [
                i for i in self.services[service_name]
                if i.is_healthy
            ]
            
            if tags:
                instances = [
                    i for i in instances
                    if any(tag in i.tags for tag in tags)
                ]
            
            return instances

    def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update heartbeat for a service instance."""
        with self.lock:
            if service_name not in self.services:
                return False
            
            for instance in self.services[service_name]:
                if instance.instance_id == instance_id:
                    instance.last_heartbeat = datetime.utcnow()
                    return True
            return False


class ApiDiscoveryAction:
    """
    API Service Discovery Action.
    
    Provides dynamic service discovery with support for:
    - Multiple load balancing strategies
    - Health checking
    - Service tagging and filtering
    - Caching with TTL
    """
    
    STRATEGIES = ["random", "round_robin", "weighted", "least_connections"]
    
    def __init__(
        self,
        strategy: str = "random",
        cache_ttl: int = 60,
        health_check_interval: int = 10
    ):
        """
        Initialize the API Discovery Action.
        
        Args:
            strategy: Load balancing strategy
            cache_ttl: Cache TTL in seconds
            health_check_interval: Health check interval in seconds
        """
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        self.strategy = strategy
        self.cache_ttl = cache_ttl
        self.health_check_interval = health_check_interval
        self.registry = ServiceRegistry()
        self.cache: Dict[str, tuple] = {}
        self.cache_lock = threading.RLock()
        self.round_robin_counters: Dict[str, int] = {}
        self._running = False
        self._health_thread: Optional[threading.Thread] = None
    
    def register(
        self,
        service_name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
        health_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        weight: int = 1
    ) -> ServiceInstance:
        """
        Register a new service instance.
        
        Args:
            service_name: Name of the service
            host: Service host
            port: Service port
            instance_id: Unique instance ID (auto-generated if not provided)
            health_url: Health check URL
            metadata: Additional metadata
            tags: Service tags for filtering
            weight: Instance weight for load balancing
        
        Returns:
            The registered ServiceInstance
        """
        if instance_id is None:
            content = f"{service_name}:{host}:{port}:{datetime.utcnow().isoformat()}"
            instance_id = hashlib.md5(content.encode()).hexdigest()[:12]
        
        instance = ServiceInstance(
            instance_id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            health_url=health_url or f"http://{host}:{port}/health",
            metadata=metadata or {},
            weight=weight,
            tags=tags or []
        )
        
        self.registry.register(instance)
        self._invalidate_cache(service_name)
        return instance
    
    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        result = self.registry.deregister(service_name, instance_id)
        if result:
            self._invalidate_cache(service_name)
        return result
    
    def discover(self, service_name: str, tags: Optional[List[str]] = None) -> Optional[ServiceInstance]:
        """
        Discover a service instance using the configured strategy.
        
        Args:
            service_name: Name of the service to discover
            tags: Optional tags to filter instances
        
        Returns:
            A service instance or None if not found
        """
        instances = self._get_cached_instances(service_name, tags)
        
        if not instances:
            return None
        
        return self._select_instance(instances)
    
    def discover_all(self, service_name: str, tags: Optional[List[str]] = None) -> List[ServiceInstance]:
        """Get all healthy instances for a service."""
        return self._get_cached_instances(service_name, tags)
    
    def _get_cached_instances(
        self,
        service_name: str,
        tags: Optional[List[str]] = None
    ) -> List[ServiceInstance]:
        """Get instances with caching."""
        cache_key = f"{service_name}:{','.join(tags or [])}"
        
        with self.cache_lock:
            if cache_key in self.cache:
                cached_time, cached_instances = self.cache[cache_key]
                if datetime.utcnow() - cached_time < timedelta(seconds=self.cache_ttl):
                    return cached_instances
        
        instances = self.registry.get_instances(service_name, tags)
        
        with self.cache_lock:
            self.cache[cache_key] = (datetime.utcnow(), instances)
        
        return instances
    
    def _invalidate_cache(self, service_name: str) -> None:
        """Invalidate cache for a service."""
        with self.cache_lock:
            self.cache = {
                k: v for k, v in self.cache.items()
                if not k.startswith(f"{service_name}:")
            }
    
    def _select_instance(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Select an instance based on the configured strategy."""
        if not instances:
            raise ValueError("No instances available")
        
        if self.strategy == "random":
            import random
            return random.choice(instances)
        
        elif self.strategy == "round_robin":
            service_name = instances[0].service_name
            if service_name not in self.round_robin_counters:
                self.round_robin_counters[service_name] = 0
            
            index = self.round_robin_counters[service_name] % len(instances)
            self.round_robin_counters[service_name] += 1
            return instances[index]
        
        elif self.strategy == "weighted":
            total_weight = sum(i.weight for i in instances)
            import random
            r = random.uniform(0, total_weight)
            cumulative = 0
            for instance in instances:
                cumulative += instance.weight
                if r <= cumulative:
                    return instance
            return instances[-1]
        
        elif self.strategy == "least_connections":
            # Placeholder: would track active connections per instance
            return instances[0]
        
        raise ValueError(f"Unknown strategy: {self.strategy}")
    
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
                self._check_all_health()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            
            time.sleep(self.health_check_interval)
    
    def _check_all_health(self) -> None:
        """Check health of all registered instances."""
        # Placeholder for actual health check implementation
        # Would use httpx or similar to check health endpoints
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get discovery statistics."""
        stats = {
            "strategy": self.strategy,
            "cache_ttl": self.cache_ttl,
            "services": {},
            "total_instances": 0
        }
        
        for service_name, instances in self.registry.services.items():
            healthy = [i for i in instances if i.is_healthy]
            stats["services"][service_name] = {
                "total": len(instances),
                "healthy": len(healthy),
                "unhealthy": len(instances) - len(healthy)
            }
            stats["total_instances"] += len(instances)
        
        return stats


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    discovery = ApiDiscoveryAction(strategy="round_robin")
    
    # Register some services
    discovery.register("user-service", "localhost", 8001, tags=["v1"], weight=1)
    discovery.register("user-service", "localhost", 8002, tags=["v1"], weight=2)
    discovery.register("order-service", "localhost", 8003, tags=["v1"])
    
    # Discover services
    user_instance = discovery.discover("user-service")
    print(f"Discovered user service: {user_instance.address if user_instance else 'None'}")
    
    order_instances = discovery.discover_all("order-service")
    print(f"Order service instances: {len(order_instances)}")
    
    print(f"Stats: {json.dumps(discovery.get_stats(), indent=2, default=str)}")
