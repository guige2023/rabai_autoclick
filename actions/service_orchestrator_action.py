"""Service orchestrator action module for RabAI AutoClick.

Provides service orchestration:
- ServiceOrchestrator: Orchestrate multiple services
- ServiceRegistry: Register and discover services
- ServiceChain: Chain service calls
- ServicePool: Pool of service instances
- HealthChecker: Health check services
- LoadBalancer: Distribute load across services
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import random

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ServiceStatus(Enum):
    """Service status."""
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
    LATENCY = "latency"


@dataclass
class ServiceEndpoint:
    """Service endpoint."""
    id: str
    url: str
    name: str
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_healthy: bool = True
    last_check: float = 0.0
    latency: float = 0.0
    connections: int = 0


@dataclass
class ServiceDefinition:
    """Service definition."""
    name: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    health_check_interval: float = 30.0
    health_check_url: Optional[str] = None
    timeout: float = 30.0
    retry_count: int = 3
    fallback: Optional[str] = None


class ServiceRegistry:
    """Service registry for discovery."""

    def __init__(self):
        self._services: Dict[str, ServiceDefinition] = {}
        self._lock = threading.RLock()

    def register(self, definition: ServiceDefinition) -> bool:
        """Register a service."""
        with self._lock:
            self._services[definition.name] = definition
            return True

    def unregister(self, name: str) -> bool:
        """Unregister a service."""
        with self._lock:
            if name in self._services:
                del self._services[name]
                return True
            return False

    def get(self, name: str) -> Optional[ServiceDefinition]:
        """Get service definition."""
        with self._lock:
            return self._services.get(name)

    def list_services(self) -> List[str]:
        """List all service names."""
        with self._lock:
            return list(self._services.keys())

    def add_endpoint(self, service_name: str, endpoint: ServiceEndpoint) -> bool:
        """Add endpoint to service."""
        with self._lock:
            service = self._services.get(service_name)
            if not service:
                return False
            service.endpoints.append(endpoint)
            return True

    def remove_endpoint(self, service_name: str, endpoint_id: str) -> bool:
        """Remove endpoint from service."""
        with self._lock:
            service = self._services.get(service_name)
            if not service:
                return False
            for i, ep in enumerate(service.endpoints):
                if ep.id == endpoint_id:
                    service.endpoints.pop(i)
                    return True
            return False


class HealthChecker:
    """Health checker for services."""

    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._health_callbacks: Dict[str, Callable] = {}

    def start(self):
        """Start health checker."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._check_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop health checker."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def register_callback(self, service_name: str, callback: Callable[[str, bool], None]):
        """Register health check callback."""
        self._health_callbacks[service_name] = callback

    def _check_loop(self):
        """Health check loop."""
        while self._running:
            for name in self.registry.list_services():
                service = self.registry.get(name)
                if not service:
                    continue

                for endpoint in service.endpoints:
                    is_healthy = self._check_endpoint(endpoint, service.health_check_url)
                    endpoint.is_healthy = is_healthy
                    endpoint.last_check = time.time()

                    if name in self._health_callbacks:
                        try:
                            self._health_callbacks[name](endpoint.id, is_healthy)
                        except Exception:
                            pass

            time.sleep(10)

    def _check_endpoint(self, endpoint: ServiceEndpoint, health_url: Optional[str]) -> bool:
        """Check endpoint health."""
        try:
            start = time.time()
            import urllib.request
            url = health_url or endpoint.url
            if not url.startswith("http"):
                url = f"http://{url}"
            urllib.request.urlopen(url, timeout=5)
            endpoint.latency = time.time() - start
            return True
        except Exception:
            return False


class LoadBalancer:
    """Load balancer for service endpoints."""

    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._round_robin_index: Dict[str, int] = {}
        self._lock = threading.Lock()

    def select(self, endpoints: List[ServiceEndpoint]) -> Optional[ServiceEndpoint]:
        """Select an endpoint based on strategy."""
        healthy = [ep for ep in endpoints if ep.is_healthy]
        if not healthy:
            return None

        with self._lock:
            if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
                return self._round_robin(healthy)
            elif self.strategy == LoadBalancingStrategy.RANDOM:
                return random.choice(healthy)
            elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
                return min(healthy, key=lambda x: x.connections)
            elif self.strategy == LoadBalancingStrategy.WEIGHTED:
                return self._weighted(healthy)
            elif self.strategy == LoadBalancingStrategy.LATENCY:
                return min(healthy, key=lambda x: x.latency if x.latency > 0 else float("inf"))
            return healthy[0]

    def _round_robin(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Round robin selection."""
        key = id(endpoints)
        if key not in self._round_robin_index:
            self._round_robin_index[key] = 0
        index = self._round_robin_index[key]
        selected = endpoints[index % len(endpoints)]
        self._round_robin_index[key] = index + 1
        return selected

    def _weighted(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Weighted selection."""
        total_weight = sum(ep.weight for ep in endpoints)
        r = random.uniform(0, total_weight)
        cumsum = 0
        for ep in endpoints:
            cumsum += ep.weight
            if r <= cumsum:
                return ep
        return endpoints[-1]


class ServiceOrchestrator:
    """Orchestrate service calls."""

    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self.health_checker = HealthChecker(registry)
        self._load_balancers: Dict[str, LoadBalancer] = {}
        self._lock = threading.RLock()

    def get_load_balancer(self, service_name: str, strategy: LoadBalancingStrategy) -> LoadBalancer:
        """Get or create load balancer."""
        with self._lock:
            key = f"{service_name}_{strategy.value}"
            if key not in self._load_balancers:
                self._load_balancers[key] = LoadBalancer(strategy)
            return self._load_balancers[key]

    def call_service(
        self,
        service_name: str,
        method: str = "GET",
        path: str = "",
        data: Any = None,
        headers: Dict = None,
    ) -> Dict[str, Any]:
        """Call a service."""
        service = self.registry.get(service_name)
        if not service:
            return {"success": False, "error": f"Service '{service_name}' not found"}

        if not service.endpoints:
            return {"success": False, "error": "No endpoints available"}

        lb = self.get_load_balancer(service_name, LoadBalancingStrategy.ROUND_ROBIN)
        endpoint = lb.select(service.endpoints)

        if not endpoint:
            if service.fallback:
                return self.call_service(service.fallback, method, path, data, headers)
            return {"success": False, "error": "No healthy endpoints"}

        endpoint.connections += 1

        try:
            result = self._make_request(endpoint, method, path, data, headers)
            endpoint.connections = max(0, endpoint.connections - 1)
            return result
        except Exception as e:
            endpoint.connections = max(0, endpoint.connections - 1)
            return {"success": False, "error": str(e)}

    def _make_request(
        self,
        endpoint: ServiceEndpoint,
        method: str,
        path: str,
        data: Any,
        headers: Dict,
    ) -> Dict[str, Any]:
        """Make HTTP request to endpoint."""
        import urllib.request
        import json

        url = f"{endpoint.url}{path}" if path else endpoint.url
        if not url.startswith("http"):
            url = f"http://{url}"

        req = urllib.request.Request(url, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)

        if data:
            if isinstance(data, dict):
                data = json.dumps(data).encode()
            req.data = data
            if "Content-Type" not in req.headers:
                req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read()
                try:
                    result = json.loads(body)
                except Exception:
                    result = body.decode()
                return {
                    "success": True,
                    "status_code": response.status,
                    "data": result,
                    "endpoint": endpoint.id,
                }
        except urllib.error.HTTPError as e:
            return {
                "success": False,
                "error": f"HTTP {e.code}: {e.reason}",
                "status_code": e.code,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


class ServicePool:
    """Pool of service instances."""

    def __init__(self, factory: Callable[[], Any], min_size: int = 1, max_size: int = 10):
        self.factory = factory
        self.min_size = min_size
        self.max_size = max_size
        self._pool: List[Any] = []
        self._in_use: Set[Any] = set()
        self._lock = threading.Semaphore(max_size)
        self._init_lock = threading.Lock()

    def acquire(self) -> Any:
        """Acquire a service instance."""
        self._lock.acquire()
        with self._init_lock:
            if self._pool:
                instance = self._pool.pop()
            else:
                instance = self.factory()
            self._in_use.add(instance)
            return instance

    def release(self, instance: Any):
        """Release a service instance."""
        with self._init_lock:
            if instance in self._in_use:
                self._in_use.remove(instance)
                if len(self._pool) < self.max_size:
                    self._pool.append(instance)
                self._lock.release()

    def close(self):
        """Close all instances."""
        with self._init_lock:
            for instance in self._pool:
                if hasattr(instance, "close"):
                    try:
                        instance.close()
                    except Exception:
                        pass
            self._pool.clear()
            self._in_use.clear()


class ServiceOrchestratorAction(BaseAction):
    """Service orchestrator action."""
    action_type = "service_orchestrator"
    display_name = "服务编排"
    description = "服务编排和负载均衡"

    def __init__(self):
        super().__init__()
        self._registry = ServiceRegistry()
        self._orchestrator = ServiceOrchestrator(self._registry)
        self._orchestrator.health_checker.start()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")

            if operation == "register":
                return self._register_service(params)
            elif operation == "unregister":
                return self._unregister_service(params)
            elif operation == "call":
                return self._call_service(params)
            elif operation == "list":
                return self._list_services()
            elif operation == "add_endpoint":
                return self._add_endpoint(params)
            elif operation == "remove_endpoint":
                return self._remove_endpoint(params)
            elif operation == "health":
                return self._get_health(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Orchestrator error: {str(e)}")

    def _register_service(self, params: Dict) -> ActionResult:
        """Register a service."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        definition = ServiceDefinition(
            name=name,
            health_check_interval=params.get("health_check_interval", 30.0),
            health_check_url=params.get("health_check_url"),
            timeout=params.get("timeout", 30.0),
            retry_count=params.get("retry_count", 3),
            fallback=params.get("fallback"),
        )

        self._registry.register(definition)
        return ActionResult(success=True, message=f"Service '{name}' registered")

    def _unregister_service(self, params: Dict) -> ActionResult:
        """Unregister a service."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        success = self._registry.unregister(name)
        return ActionResult(success=success, message="Service unregistered" if success else "Service not found")

    def _call_service(self, params: Dict) -> ActionResult:
        """Call a service."""
        service_name = params.get("service")
        if not service_name:
            return ActionResult(success=False, message="service is required")

        result = self._orchestrator.call_service(
            service_name=service_name,
            method=params.get("method", "GET"),
            path=params.get("path", ""),
            data=params.get("data"),
            headers=params.get("headers"),
        )

        return ActionResult(
            success=result.get("success", False),
            message=result.get("error", "Service called"),
            data=result,
        )

    def _list_services(self) -> ActionResult:
        """List all services."""
        services = self._registry.list_services()
        result = []
        for name in services:
            svc = self._registry.get(name)
            if svc:
                endpoints = [
                    {
                        "id": ep.id,
                        "url": ep.url,
                        "healthy": ep.is_healthy,
                        "latency": ep.latency,
                    }
                    for ep in svc.endpoints
                ]
                result.append({"name": name, "endpoints": endpoints})

        return ActionResult(success=True, message=f"{len(services)} services", data={"services": result})

    def _add_endpoint(self, params: Dict) -> ActionResult:
        """Add endpoint to service."""
        service_name = params.get("service")
        endpoint_id = params.get("endpoint_id")
        url = params.get("url")

        if not service_name or not endpoint_id or not url:
            return ActionResult(success=False, message="service, endpoint_id, and url are required")

        endpoint = ServiceEndpoint(
            id=endpoint_id,
            url=url,
            name=params.get("name", endpoint_id),
            weight=params.get("weight", 1.0),
        )

        success = self._registry.add_endpoint(service_name, endpoint)
        return ActionResult(success=success, message="Endpoint added" if success else "Failed to add endpoint")

    def _remove_endpoint(self, params: Dict) -> ActionResult:
        """Remove endpoint from service."""
        service_name = params.get("service")
        endpoint_id = params.get("endpoint_id")

        if not service_name or not endpoint_id:
            return ActionResult(success=False, message="service and endpoint_id are required")

        success = self._registry.remove_endpoint(service_name, endpoint_id)
        return ActionResult(success=success, message="Endpoint removed" if success else "Endpoint not found")

    def _get_health(self, params: Dict) -> ActionResult:
        """Get service health status."""
        service_name = params.get("service")

        if service_name:
            service = self._registry.get(service_name)
            if not service:
                return ActionResult(success=False, message="Service not found")

            endpoints = [
                {
                    "id": ep.id,
                    "healthy": ep.is_healthy,
                    "latency": ep.latency,
                    "last_check": ep.last_check,
                }
                for ep in service.endpoints
            ]
            return ActionResult(success=True, message="Health status", data={"endpoints": endpoints})
        else:
            services = self._registry.list_services()
            result = {}
            for name in services:
                service = self._registry.get(name)
                if service:
                    healthy_count = sum(1 for ep in service.endpoints if ep.is_healthy)
                    result[name] = {
                        "total": len(service.endpoints),
                        "healthy": healthy_count,
                        "status": "healthy" if healthy_count == len(service.endpoints) else "degraded",
                    }
            return ActionResult(success=True, message="Health status", data=result)
