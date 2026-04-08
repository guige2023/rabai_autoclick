"""Service discovery action module for RabAI AutoClick.

Provides service discovery operations:
- ServiceRegisterAction: Register service
- ServiceDeregisterAction: Deregister service
- ServiceDiscoverAction: Discover services
- ServiceListAction: List registered services
- ServiceHealthAction: Check service health
- ServiceEndpointAction: Get service endpoints
- ServiceWatchAction: Watch for changes
- ServiceResolveAction: Resolve service name
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ServiceRegistry:
    """In-memory service registry."""
    
    _services: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def register(cls, name: str, host: str, port: int, metadata: Dict[str, Any] = None) -> None:
        if name not in cls._services:
            cls._services[name] = {"name": name, "instances": [], "metadata": {}}
        cls._services[name]["instances"].append({
            "host": host,
            "port": port,
            "registered_at": time.time(),
            "healthy": True
        })
        if metadata:
            cls._services[name]["metadata"].update(metadata)
    
    @classmethod
    def deregister(cls, name: str, host: str = None, port: int = None) -> bool:
        if name not in cls._services:
            return False
        if host and port:
            cls._services[name]["instances"] = [
                i for i in cls._services[name]["instances"]
                if not (i["host"] == host and i["port"] == port)
            ]
        else:
            cls._services[name]["instances"] = cls._services[name]["instances"][:-1]
        return True
    
    @classmethod
    def discover(cls, name: str) -> Optional[Dict[str, Any]]:
        return cls._services.get(name)
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        return list(cls._services.values())
    
    @classmethod
    def update_health(cls, name: str, healthy: bool) -> None:
        if name in cls._services:
            for instance in cls._services[name]["instances"]:
                instance["healthy"] = healthy


class ServiceRegisterAction(BaseAction):
    """Register a service."""
    action_type = "service_register"
    display_name = "注册服务"
    description = "注册服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            host = params.get("host", "localhost")
            port = params.get("port", 8080)
            metadata = params.get("metadata", {})
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            ServiceRegistry.register(name, host, port, metadata)
            
            return ActionResult(
                success=True,
                message=f"Registered service: {name} at {host}:{port}",
                data={"name": name, "host": host, "port": port}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service register failed: {str(e)}")


class ServiceDeregisterAction(BaseAction):
    """Deregister a service."""
    action_type = "service_deregister"
    display_name = "注销服务"
    description = "注销服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            host = params.get("host")
            port = params.get("port")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            deregistered = ServiceRegistry.deregister(name, host, port)
            
            return ActionResult(
                success=deregistered,
                message=f"Deregistered service: {name}" if deregistered else f"Service not found: {name}",
                data={"name": name, "deregistered": deregistered}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service deregister failed: {str(e)}")


class ServiceDiscoverAction(BaseAction):
    """Discover a service."""
    action_type = "service_discover"
    display_name = "发现服务"
    description = "发现服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            service = ServiceRegistry.discover(name)
            
            if not service:
                return ActionResult(success=False, message=f"Service not found: {name}")
            
            healthy_instances = [i for i in service["instances"] if i.get("healthy", True)]
            
            return ActionResult(
                success=True,
                message=f"Discovered service: {name} with {len(healthy_instances)} healthy instances",
                data={
                    "name": name,
                    "service": service,
                    "healthy_count": len(healthy_instances),
                    "total_count": len(service["instances"])
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service discover failed: {str(e)}")


class ServiceListAction(BaseAction):
    """List all registered services."""
    action_type = "service_list"
    display_name = "服务列表"
    description = "列出所有服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            services = ServiceRegistry.list_all()
            
            return ActionResult(
                success=True,
                message=f"Found {len(services)} registered services",
                data={"services": services, "count": len(services)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service list failed: {str(e)}")


class ServiceHealthAction(BaseAction):
    """Check service health."""
    action_type = "service_health"
    display_name = "服务健康检查"
    description = "检查服务健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            service = ServiceRegistry.discover(name)
            if not service:
                return ActionResult(success=False, message=f"Service not found: {name}")
            
            healthy = sum(1 for i in service["instances"] if i.get("healthy", True))
            total = len(service["instances"])
            
            health_status = "healthy" if healthy == total else "degraded" if healthy > 0 else "unhealthy"
            
            return ActionResult(
                success=True,
                message=f"Service {name} health: {health_status}",
                data={
                    "name": name,
                    "healthy_count": healthy,
                    "total_count": total,
                    "health_status": health_status
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service health check failed: {str(e)}")


class ServiceEndpointAction(BaseAction):
    """Get service endpoints."""
    action_type = "service_endpoint"
    display_name = "获取服务端点"
    description = "获取服务端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            service = ServiceRegistry.discover(name)
            if not service:
                return ActionResult(success=False, message=f"Service not found: {name}")
            
            endpoints = [f"http://{i['host']}:{i['port']}" for i in service["instances"]]
            
            return ActionResult(
                success=True,
                message=f"Endpoints for {name}: {len(endpoints)}",
                data={"name": name, "endpoints": endpoints}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service endpoint failed: {str(e)}")


class ServiceWatchAction(BaseAction):
    """Watch for service changes."""
    action_type = "service_watch"
    display_name = "监控服务"
    description = "监控服务变化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            names = params.get("names", [])
            duration = params.get("duration", 60)
            
            if not names:
                names = list(ServiceRegistry._services.keys())
            
            start_time = time.time()
            changes = []
            
            while time.time() - start_time < duration:
                for name in names:
                    service = ServiceRegistry.discover(name)
                    if service:
                        pass
                time.sleep(1)
            
            return ActionResult(
                success=True,
                message=f"Watched {len(names)} services for {duration}s",
                data={"names": names, "duration": duration, "changes": changes}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service watch failed: {str(e)}")


class ServiceResolveAction(BaseAction):
    """Resolve service to endpoint."""
    action_type = "service_resolve"
    display_name = "解析服务"
    description = "解析服务名称到端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            strategy = params.get("strategy", "random")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            service = ServiceRegistry.discover(name)
            if not service:
                return ActionResult(success=False, message=f"Service not found: {name}")
            
            instances = [i for i in service["instances"] if i.get("healthy", True)]
            
            if not instances:
                return ActionResult(success=False, message=f"No healthy instances for {name}")
            
            if strategy == "random":
                import random
                instance = random.choice(instances)
            elif strategy == "round_robin":
                instance = instances[0]
            elif strategy == "least_connections":
                instance = instances[0]
            else:
                instance = instances[0]
            
            endpoint = f"http://{instance['host']}:{instance['port']}"
            
            return ActionResult(
                success=True,
                message=f"Resolved {name} to {endpoint}",
                data={"name": name, "endpoint": endpoint, "strategy": strategy}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service resolve failed: {str(e)}")
