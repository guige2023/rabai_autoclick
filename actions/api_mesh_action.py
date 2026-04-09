"""API Mesh Action Module.

Provides API mesh utilities: service discovery, load balancing,
request routing, circuit breaking, and observability for microservices.

Example:
    result = execute(context, {"action": "register_service", "name": "auth"})
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import random


@dataclass
class ServiceInstance:
    """A single instance of a service."""
    
    id: str
    service_name: str
    host: str
    port: int
    weight: int = 1
    healthy: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)
    registered_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    
    @property
    def address(self) -> str:
        """Get service address."""
        return f"{self.host}:{self.port}"


@dataclass
class ServiceEndpoint:
    """Service endpoint definition."""
    
    name: str
    version: str = "v1"
    instances: list[ServiceInstance] = field(default_factory=list)
    health_check_path: str = "/health"
    timeout_seconds: float = 30.0


class ServiceRegistry:
    """Service registry for API mesh."""
    
    def __init__(self) -> None:
        """Initialize service registry."""
        self._services: dict[str, ServiceEndpoint] = {}
        self._instance_health: dict[str, datetime] = {}
    
    def register(self, instance: ServiceInstance) -> None:
        """Register a service instance.
        
        Args:
            instance: Service instance to register
        """
        if instance.service_name not in self._services:
            self._services[instance.service_name] = ServiceEndpoint(
                name=instance.service_name,
            )
        
        endpoint = self._services[instance.service_name]
        
        existing = [i for i in endpoint.instances if i.id == instance.id]
        if existing:
            existing[0].healthy = instance.healthy
            existing[0].last_heartbeat = datetime.now()
        else:
            endpoint.instances.append(instance)
        
        self._instance_health[instance.id] = datetime.now()
    
    def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance.
        
        Args:
            service_name: Service name
            instance_id: Instance ID
            
        Returns:
            True if deregistered
        """
        if service_name not in self._services:
            return False
        
        endpoint = self._services[service_name]
        endpoint.instances = [i for i in endpoint.instances if i.id != instance_id]
        
        if instance_id in self._instance_health:
            del self._instance_health[instance_id]
        
        return True
    
    def get_healthy_instances(self, service_name: str) -> list[ServiceInstance]:
        """Get healthy instances for a service.
        
        Args:
            service_name: Service name
            
        Returns:
            List of healthy instances
        """
        if service_name not in self._services:
            return []
        
        endpoint = self._services[service_name]
        cutoff = datetime.now() - timedelta(seconds=30)
        
        healthy = [
            i for i in endpoint.instances
            if i.healthy and i.last_heartbeat > cutoff
        ]
        
        return healthy
    
    def get_service(self, service_name: str) -> Optional[ServiceEndpoint]:
        """Get service endpoint.
        
        Args:
            service_name: Service name
            
        Returns:
            Service endpoint or None
        """
        return self._services.get(service_name)
    
    def list_services(self) -> list[str]:
        """List all registered services."""
        return list(self._services.keys())


class LoadBalancer:
    """Load balancer for service instances."""
    
    STRATEGY_ROUND_ROBIN = "round_robin"
    STRATEGY_WEIGHTED = "weighted"
    STRATEGY_LEAST_CONNECTIONS = "least_connections"
    STRATEGY_RANDOM = "random"
    STRATEGY_IP_HASH = "ip_hash"
    
    def __init__(self, strategy: str = STRATEGY_ROUND_ROBIN) -> None:
        """Initialize load balancer.
        
        Args:
            strategy: Balancing strategy
        """
        self.strategy = strategy
        self._counters: dict[str, int] = defaultdict(int)
        self._connection_counts: dict[str, int] = defaultdict(int)
    
    def select_instance(
        self,
        instances: list[ServiceInstance],
        client_ip: Optional[str] = None,
    ) -> Optional[ServiceInstance]:
        """Select an instance using configured strategy.
        
        Args:
            instances: Available instances
            client_ip: Client IP for hash-based strategies
            
        Returns:
            Selected instance or None
        """
        if not instances:
            return None
        
        if self.strategy == self.STRATEGY_ROUND_ROBIN:
            return self._round_robin(instances)
        elif self.strategy == self.STRATEGY_WEIGHTED:
            return self._weighted(instances)
        elif self.strategy == self.STRATEGY_LEAST_CONNECTIONS:
            return self._least_connections(instances)
        elif self.strategy == self.STRATEGY_RANDOM:
            return random.choice(instances)
        elif self.strategy == self.STRATEGY_IP_HASH:
            return self._ip_hash(instances, client_ip)
        
        return instances[0]
    
    def _round_robin(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Round-robin selection."""
        key = id(instances)
        index = self._counters[key] % len(instances)
        self._counters[key] += 1
        return instances[index]
    
    def _weighted(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Weighted selection based on instance weight."""
        total_weight = sum(i.weight for i in instances)
        if total_weight == 0:
            return instances[0]
        
        rand = random.randint(1, total_weight)
        cumulative = 0
        
        for instance in instances:
            cumulative += instance.weight
            if cumulative >= rand:
                return instance
        
        return instances[-1]
    
    def _least_connections(self, instances: list[ServiceInstance]) -> ServiceInstance:
        """Select instance with least active connections."""
        return min(instances, key=lambda i: self._connection_counts.get(i.id, 0))
    
    def _ip_hash(self, instances: list[ServiceInstance], client_ip: Optional[str]) -> ServiceInstance:
        """Hash-based selection using client IP."""
        if not client_ip:
            return instances[0]
        
        hash_value = hash(client_ip) % len(instances)
        return instances[hash_value]
    
    def increment_connections(self, instance_id: str) -> None:
        """Increment connection count for instance."""
        self._connection_counts[instance_id] += 1
    
    def decrement_connections(self, instance_id: str) -> None:
        """Decrement connection count for instance."""
        if self._connection_counts.get(instance_id, 0) > 0:
            self._connection_counts[instance_id] -= 1


class CircuitBreaker:
    """Circuit breaker for service mesh fault tolerance."""
    
    STATE_CLOSED = "closed"
    STATE_OPEN = "open"
    STATE_HALF_OPEN = "half_open"
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> None:
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Failures before opening
            timeout_seconds: Time before attempting reset
            half_open_max_calls: Max calls in half-open state
        """
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_max_calls = half_open_max_calls
        
        self.state = self.STATE_CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
    
    def record_success(self) -> None:
        """Record successful call."""
        if self.state == self.STATE_HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= self.half_open_max_calls:
                self.state = self.STATE_CLOSED
                self.failure_count = 0
        elif self.state == self.STATE_CLOSED:
            self.failure_count = 0
    
    def record_failure(self) -> None:
        """Record failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == self.STATE_HALF_OPEN:
            self.state = self.STATE_OPEN
            self.half_open_calls = 0
        elif self.failure_count >= self.failure_threshold:
            self.state = self.STATE_OPEN
    
    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == self.STATE_CLOSED:
            return True
        
        if self.state == self.STATE_OPEN:
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.timeout_seconds:
                    self.state = self.STATE_HALF_OPEN
                    self.half_open_calls = 0
                    return True
            return False
        
        if self.state == self.STATE_HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        
        return False
    
    def get_state(self) -> str:
        """Get current state."""
        return self.state


class MeshRouter:
    """Routes requests through the service mesh."""
    
    def __init__(self) -> None:
        """Initialize mesh router."""
        self.registry = ServiceRegistry()
        self.load_balancer = LoadBalancer()
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
    
    def register_service(
        self,
        name: str,
        host: str,
        port: int,
        weight: int = 1,
    ) -> None:
        """Register a service instance.
        
        Args:
            name: Service name
            host: Service host
            port: Service port
            weight: Instance weight
        """
        instance = ServiceInstance(
            id=f"{name}-{host}-{port}",
            service_name=name,
            host=host,
            port=port,
            weight=weight,
        )
        self.registry.register(instance)
    
    def route_request(
        self,
        service_name: str,
        client_ip: Optional[str] = None,
    ) -> Optional[ServiceInstance]:
        """Route request to service instance.
        
        Args:
            service_name: Target service name
            client_ip: Client IP for load balancing
            
        Returns:
            Selected service instance or None
        """
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker()
        
        cb = self.circuit_breakers[service_name]
        
        if not cb.can_execute():
            return None
        
        instances = self.registry.get_healthy_instances(service_name)
        
        if not instances:
            cb.record_failure()
            return None
        
        instance = self.load_balancer.select_instance(instances, client_ip)
        
        if instance:
            self.load_balancer.increment_connections(instance.id)
        
        return instance
    
    def record_success(self, service_name: str) -> None:
        """Record successful request.
        
        Args:
            service_name: Service name
        """
        if service_name in self.circuit_breakers:
            self.circuit_breakers[service_name].record_success()
    
    def record_failure(self, service_name: str) -> None:
        """Record failed request.
        
        Args:
            service_name: Service name
        """
        if service_name in self.circuit_breakers:
            self.circuit_breakers[service_name].record_failure()


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute API mesh action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "register_service":
        instance = ServiceInstance(
            id=params.get("instance_id", ""),
            service_name=params.get("name", ""),
            host=params.get("host", "localhost"),
            port=params.get("port", 8080),
            weight=params.get("weight", 1),
        )
        registry = ServiceRegistry()
        registry.register(instance)
        result["data"] = {"instance_id": instance.id}
    
    elif action == "deregister_service":
        registry = ServiceRegistry()
        registry.deregister(params.get("name", ""), params.get("instance_id", ""))
        result["data"] = {"deregistered": True}
    
    elif action == "list_services":
        registry = ServiceRegistry()
        services = registry.list_services()
        result["data"] = {"services": services}
    
    elif action == "get_instances":
        registry = ServiceRegistry()
        instances = registry.get_healthy_instances(params.get("name", ""))
        result["data"] = {
            "instances": [{"id": i.id, "address": i.address} for i in instances]
        }
    
    elif action == "route":
        router = MeshRouter()
        instance = router.route_request(
            params.get("service_name", ""),
            params.get("client_ip"),
        )
        result["data"] = {
            "instance": {"address": instance.address} if instance else None,
        }
    
    elif action == "circuit_status":
        cb = CircuitBreaker()
        result["data"] = {"state": cb.get_state()}
    
    elif action == "load_balance":
        lb = LoadBalancer(strategy=params.get("strategy", "round_robin"))
        result["data"] = {"strategy": lb.strategy}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
