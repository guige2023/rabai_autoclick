"""Service discovery action module for RabAI AutoClick.

Provides service discovery with registry management,
health monitoring, and dynamic endpoint resolution.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from threading import Lock
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ServiceStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class ServiceInstance:
    """A service instance."""
    id: str
    service_name: str
    host: str
    port: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: ServiceStatus = ServiceStatus.HEALTHY
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    health_check_url: Optional[str] = None


@dataclass
class ServiceDefinition:
    """A service definition."""
    name: str
    version: str = "1.0"
    instances: List[ServiceInstance] = field(default_factory=list)
    health_check_interval: float = 30.0
    unhealthy_threshold: int = 3


class ServiceDiscoveryAction(BaseAction):
    """Manage service registry and discovery.
    
    Supports service registration, deregistration, health monitoring,
    and instance lookup with filtering.
    """
    action_type = "service_discovery"
    display_name = "服务发现"
    description = "服务注册表和动态发现"
    
    def __init__(self):
        super().__init__()
        self._services: Dict[str, ServiceDefinition] = {}
        self._instances: Dict[str, ServiceInstance] = {}  # instance_id -> instance
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute service discovery operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'deregister', 'discover', 'heartbeat', 'status'
                - service_name: Service name
                - instance: Instance config (for register)
                - instance_id: Instance ID (for other operations)
                - filter: Filter criteria (for discover)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'discover').lower()
        
        if operation == 'register':
            return self._register(params)
        elif operation == 'deregister':
            return self._deregister(params)
        elif operation == 'discover':
            return self._discover(params)
        elif operation == 'heartbeat':
            return self._heartbeat(params)
        elif operation == 'status':
            return self._status(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a service instance."""
        instance_id = params.get('instance_id')
        service_name = params.get('service_name')
        host = params.get('host')
        port = params.get('port')
        metadata = params.get('metadata', {})
        health_check_url = params.get('health_check_url')
        
        if not all([instance_id, service_name, host, port]):
            return ActionResult(
                success=False,
                message="instance_id, service_name, host, and port are required"
            )
        
        instance = ServiceInstance(
            id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            metadata=metadata,
            health_check_url=health_check_url
        )
        
        with self._lock:
            # Create service definition if not exists
            if service_name not in self._services:
                self._services[service_name] = ServiceDefinition(
                    name=service_name
                )
            
            # Add instance
            self._instances[instance_id] = instance
            self._services[service_name].instances.append(instance)
        
        return ActionResult(
            success=True,
            message=f"Registered instance '{instance_id}' for service '{service_name}'",
            data={
                'instance_id': instance_id,
                'service_name': service_name,
                'endpoint': f"{host}:{port}"
            }
        )
    
    def _deregister(self, params: Dict[str, Any]) -> ActionResult:
        """Deregister a service instance."""
        instance_id = params.get('instance_id')
        
        with self._lock:
            if instance_id not in self._instances:
                return ActionResult(
                    success=False,
                    message=f"Instance '{instance_id}' not found"
                )
            
            instance = self._instances[instance_id]
            service_name = instance.service_name
            
            # Remove from service
            if service_name in self._services:
                self._services[service_name].instances = [
                    i for i in self._services[service_name].instances
                    if i.id != instance_id
                ]
            
            # Remove instance
            del self._instances[instance_id]
        
        return ActionResult(
            success=True,
            message=f"Deregistered instance '{instance_id}'"
        )
    
    def _discover(self, params: Dict[str, Any]) -> ActionResult:
        """Discover service instances."""
        service_name = params.get('service_name')
        filter_criteria = params.get('filter', {})
        healthy_only = params.get('healthy_only', True)
        
        if not service_name:
            return ActionResult(success=False, message="service_name is required")
        
        with self._lock:
            service = self._services.get(service_name)
            
            if not service:
                return ActionResult(
                    success=True,
                    message=f"Service '{service_name}' not found",
                    data={'instances': [], 'count': 0}
                )
            
            instances = service.instances
        
        # Apply filters
        if healthy_only:
            instances = [i for i in instances if i.status == ServiceStatus.HEALTHY]
        
        # Apply custom filters
        for key, value in filter_criteria.items():
            instances = [
                i for i in instances
                if i.metadata.get(key) == value
            ]
        
        # Build result
        result = [
            {
                'id': i.id,
                'service_name': i.service_name,
                'host': i.host,
                'port': i.port,
                'endpoint': f"http://{i.host}:{i.port}",
                'status': i.status.value,
                'metadata': i.metadata
            }
            for i in instances
        ]
        
        return ActionResult(
            success=True,
            message=f"Discovered {len(result)} instances",
            data={'instances': result, 'count': len(result)}
        )
    
    def _heartbeat(self, params: Dict[str, Any]) -> ActionResult:
        """Process instance heartbeat."""
        instance_id = params.get('instance_id')
        
        with self._lock:
            if instance_id not in self._instances:
                return ActionResult(
                    success=False,
                    message=f"Instance '{instance_id}' not found"
                )
            
            instance = self._instances[instance_id]
            instance.last_heartbeat = time.time()
            instance.status = ServiceStatus.HEALTHY
        
        return ActionResult(
            success=True,
            message=f"Heartbeat received for '{instance_id}'"
        )
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get service registry status."""
        service_name = params.get('service_name')
        
        with self._lock:
            if service_name:
                service = self._services.get(service_name)
                if not service:
                    return ActionResult(
                        success=True,
                        message=f"Service '{service_name}' not found",
                        data={}
                    )
                
                instances = service.instances
                healthy = sum(1 for i in instances if i.status == ServiceStatus.HEALTHY)
                
                return ActionResult(
                    success=True,
                    message=f"Status for '{service_name}'",
                    data={
                        'service_name': service_name,
                        'total_instances': len(instances),
                        'healthy_instances': healthy,
                        'instances': [
                            {
                                'id': i.id,
                                'status': i.status.value,
                                'last_heartbeat': i.last_heartbeat
                            }
                            for i in instances
                        ]
                    }
                )
            else:
                # Return all services
                services = []
                for name, service in self._services.items():
                    healthy = sum(
                        1 for i in service.instances
                        if i.status == ServiceStatus.HEALTHY
                    )
                    services.append({
                        'name': name,
                        'total_instances': len(service.instances),
                        'healthy_instances': healthy
                    })
                
                return ActionResult(
                    success=True,
                    message=f"{len(services)} services registered",
                    data={'services': services}
                )
    
    def _cleanup_stale(self, timeout: float = 120.0) -> int:
        """Remove instances that haven't sent heartbeat."""
        now = time.time()
        removed = 0
        
        with self._lock:
            stale_ids = [
                iid for iid, inst in self._instances.items()
                if (now - inst.last_heartbeat) > timeout
            ]
            
            for iid in stale_ids:
                inst = self._instances[iid]
                service_name = inst.service_name
                
                # Remove from service
                if service_name in self._services:
                    self._services[service_name].instances = [
                        i for i in self._services[service_name].instances
                        if i.id != iid
                    ]
                
                del self._instances[iid]
                removed += 1
        
        return removed


class ServiceRouterAction(BaseAction):
    """Route requests to service instances."""
    action_type = "service_router"
    display_name = "服务路由"
    description = "请求路由到服务实例"
    
    def __init__(self):
        super().__init__()
        self._discovery = ServiceDiscoveryAction()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route request to service."""
        service_name = params.get('service_name')
        path = params.get('path', '/')
        method = params.get('method', 'GET')
        
        if not service_name:
            return ActionResult(success=False, message="service_name is required")
        
        # Discover instances
        result = self._discovery._discover({
            'service_name': service_name,
            'healthy_only': True
        })
        
        instances = result.data.get('instances', [])
        
        if not instances:
            return ActionResult(
                success=False,
                message=f"No healthy instances for '{service_name}'"
            )
        
        # Simple round-robin: pick first
        instance = instances[0]
        
        return ActionResult(
            success=True,
            message=f"Routed to {instance['id']}",
            data={
                'endpoint': instance['endpoint'],
                'instance_id': instance['id'],
                'full_url': f"{instance['endpoint']}{path}"
            }
        )
