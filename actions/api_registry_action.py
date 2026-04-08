"""API Registry Action Module.

Provides API registry and discovery capabilities including
service registration, lookup, and health monitoring.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ServiceRegistryAction(BaseAction):
    """Register and discover API services.
    
    Supports service registration, deregistration, and lookup.
    """
    action_type = "service_registry"
    display_name = "服务注册"
    description = "注册和发现API服务"

    def __init__(self):
        super().__init__()
        self._services: Dict[str, Dict] = {}
        self._service_instances: Dict[str, List[Dict]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage service registry.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'deregister', 'discover', 'list_services'.
                - service_name: Service name.
                - service_url: Service URL endpoint.
                - metadata: Service metadata.
                - version: Service version.
                - tags: Service tags.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with registry result or error.
        """
        operation = params.get('operation', 'register')
        service_name = params.get('service_name', '')
        service_url = params.get('service_url', '')
        metadata = params.get('metadata', {})
        version = params.get('version', '1.0')
        tags = params.get('tags', [])
        output_var = params.get('output_var', 'registry_result')

        try:
            if operation == 'register':
                return self._register_service(
                    service_name, service_url, metadata, version, tags, output_var
                )
            elif operation == 'deregister':
                return self._deregister_service(service_name, service_url, output_var)
            elif operation == 'discover':
                return self._discover_service(service_name, output_var)
            elif operation == 'list_services':
                return self._list_services(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Service registry failed: {str(e)}"
            )

    def _register_service(
        self,
        service_name: str,
        service_url: str,
        metadata: Dict,
        version: str,
        tags: List[str],
        output_var: str
    ) -> ActionResult:
        """Register a service."""
        if not service_name or not service_url:
            return ActionResult(
                success=False,
                message="service_name and service_url are required"
            )

        instance_id = f"{service_name}-{int(time.time() * 1000)}"

        instance = {
            'instance_id': instance_id,
            'service_name': service_name,
            'service_url': service_url,
            'version': version,
            'metadata': metadata,
            'tags': tags,
            'registered_at': datetime.now().isoformat(),
            'last_heartbeat': time.time(),
            'healthy': True
        }

        self._services[instance_id] = instance
        self._service_instances[service_name].append(instance)

        result = {
            'instance_id': instance_id,
            'service_name': service_name,
            'registered': True
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Service '{service_name}' registered: {service_url}"
        )

    def _deregister_service(
        self, service_name: str, service_url: str, output_var: str
    ) -> ActionResult:
        """Deregister a service instance."""
        instances = self._service_instances.get(service_name, [])

        for i, instance in enumerate(instances):
            if instance['service_url'] == service_url:
                instance_id = instance['instance_id']
                instances.pop(i)
                del self._services[instance_id]

                context.variables[output_var] = {'deregistered': True, 'instance_id': instance_id}
                return ActionResult(
                    success=True,
                    data={'deregistered': True, 'instance_id': instance_id},
                    message=f"Service '{service_name}' deregistered: {service_url}"
                )

        return ActionResult(
            success=False,
            message=f"Service instance not found: {service_name} at {service_url}"
        )

    def _discover_service(self, service_name: str, output_var: str) -> ActionResult:
        """Discover service instances."""
        instances = self._service_instances.get(service_name, [])

        # Filter healthy instances
        healthy = [inst for inst in instances if inst.get('healthy', True)]

        result = {
            'service_name': service_name,
            'instances': healthy,
            'count': len(healthy)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Discovered {len(healthy)} instances of '{service_name}'"
        )

    def _list_services(self, output_var: str) -> ActionResult:
        """List all registered services."""
        services = []
        for service_name, instances in self._service_instances.items():
            services.append({
                'service_name': service_name,
                'instance_count': len(instances),
                'healthy_count': len([i for i in instances if i.get('healthy', True)])
            })

        context.variables[output_var] = {
            'services': services,
            'count': len(services)
        }
        return ActionResult(
            success=True,
            data={'services': services, 'count': len(services)},
            message=f"Listed {len(services)} services"
        )


class ServiceDiscoveryAction(BaseAction):
    """Discover services using various strategies.
    
    Supports random, round-robin, and weighted discovery.
    """
    action_type = "service_discovery"
    display_name = "服务发现"
    description = "使用多种策略发现服务"

    def __init__(self):
        super().__init__()
        self._discovery_counters: Dict[str, int] = defaultdict(int)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Discover a service.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'discover', 'discover_with_strategy'.
                - service_name: Service to discover.
                - strategy: 'random', 'round_robin', 'weighted', 'first'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with discovery result or error.
        """
        operation = params.get('operation', 'discover')
        service_name = params.get('service_name', '')
        strategy = params.get('strategy', 'round_robin')
        output_var = params.get('output_var', 'discovery_result')

        try:
            if operation == 'discover':
                return self._discover(service_name, strategy, output_var)
            elif operation == 'discover_with_strategy':
                return self._discover(service_name, strategy, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Service discovery failed: {str(e)}"
            )

    def _discover(self, service_name: str, strategy: str, output_var: str) -> ActionResult:
        """Discover a service using specified strategy."""
        import random

        # Get service instances from registry
        registry = getattr(self, '_registry', None)
        if not registry:
            # Use default empty discovery
            return ActionResult(
                success=False,
                message="Service registry not available"
            )

        instances = registry._service_instances.get(service_name, [])
        healthy = [inst for inst in instances if inst.get('healthy', True)]

        if not healthy:
            return ActionResult(
                success=False,
                message=f"No healthy instances of '{service_name}'"
            )

        selected = None

        if strategy == 'random':
            selected = random.choice(healthy)
        elif strategy == 'round_robin':
            index = self._discovery_counters[service_name] % len(healthy)
            selected = healthy[index]
            self._discovery_counters[service_name] += 1
        elif strategy == 'first':
            selected = healthy[0]
        elif strategy == 'weighted':
            total_weight = sum(inst.get('metadata', {}).get('weight', 1) for inst in healthy)
            r = random.uniform(0, total_weight)
            cumsum = 0
            for inst in healthy:
                cumsum += inst.get('metadata', {}).get('weight', 1)
                if r <= cumsum:
                    selected = inst
                    break
            if not selected:
                selected = healthy[-1]
        else:
            selected = healthy[0]

        result = {
            'service_name': service_name,
            'instance': selected,
            'strategy': strategy
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Discovered instance of '{service_name}': {selected.get('service_url')}"
        )


class ServiceHealthMonitorAction(BaseAction):
    """Monitor service health with heartbeat tracking.
    
    Supports heartbeat-based health checks and automatic deregistration.
    """
    action_type = "service_health_monitor"
    display_name = "服务健康监控"
    description = "通过心跳跟踪监控服务健康"

    def __init__(self):
        super().__init__()
        self._heartbeat_timestamps: Dict[str, float] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor service health.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'heartbeat', 'check_health', 'cleanup_unhealthy'.
                - instance_id: Service instance ID.
                - timeout: Heartbeat timeout in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with health monitoring result or error.
        """
        operation = params.get('operation', 'heartbeat')
        instance_id = params.get('instance_id', '')
        timeout = params.get('timeout', 60)
        output_var = params.get('output_var', 'health_monitor_result')

        try:
            if operation == 'heartbeat':
                return self._record_heartbeat(instance_id, output_var)
            elif operation == 'check_health':
                return self._check_health(instance_id, timeout, output_var)
            elif operation == 'cleanup_unhealthy':
                return self._cleanup_unhealthy(timeout, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Service health monitor failed: {str(e)}"
            )

    def _record_heartbeat(self, instance_id: str, output_var: str) -> ActionResult:
        """Record a heartbeat from a service."""
        self._heartbeat_timestamps[instance_id] = time.time()

        result = {
            'instance_id': instance_id,
            'heartbeat_recorded': True,
            'timestamp': self._heartbeat_timestamps[instance_id]
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Heartbeat recorded for '{instance_id}'"
        )

    def _check_health(
        self, instance_id: str, timeout: float, output_var: str
    ) -> ActionResult:
        """Check if a service instance is healthy based on heartbeat."""
        last_heartbeat = self._heartbeat_timestamps.get(instance_id)

        if not last_heartbeat:
            result = {
                'instance_id': instance_id,
                'healthy': False,
                'reason': 'no_heartbeat'
            }
        else:
            elapsed = time.time() - last_heartbeat
            healthy = elapsed <= timeout

            result = {
                'instance_id': instance_id,
                'healthy': healthy,
                'last_heartbeat': last_heartbeat,
                'elapsed': elapsed,
                'timeout': timeout
            }

        context.variables[output_var] = result
        return ActionResult(
            success=result['healthy'],
            data=result,
            message=f"Health check for '{instance_id}': {'healthy' if result['healthy'] else 'unhealthy'}"
        )

    def _cleanup_unhealthy(self, timeout: float, output_var: str) -> ActionResult:
        """Cleanup unhealthy service instances."""
        current_time = time.time()
        removed = []

        # Access registry through context if available
        registry = getattr(self, '_registry', None)

        if registry:
            for instance_id, last_heartbeat in list(self._heartbeat_timestamps.items()):
                if current_time - last_heartbeat > timeout:
                    # Find and remove from registry
                    for service_name, instances in list(registry._service_instances.items()):
                        for i, inst in enumerate(instances):
                            if inst['instance_id'] == instance_id:
                                instances.pop(i)
                                removed.append(instance_id)
                                del self._heartbeat_timestamps[instance_id]
                                break

        result = {
            'removed_count': len(removed),
            'removed_instances': removed
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Cleaned up {len(removed)} unhealthy instances"
        )


class APIRegistryAction(BaseAction):
    """Manage API endpoint registry.
    
    Supports endpoint registration, versioning, and documentation.
    """
    action_type = "api_registry"
    display_name = "API注册表"
    description = "管理API端点注册表"

    def __init__(self):
        super().__init__()
        self._endpoints: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage API registry.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'get', 'list', 'search'.
                - path: API endpoint path.
                - method: HTTP method.
                - version: API version.
                - handler: Handler function info.
                - spec: OpenAPI spec snippet.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with registry result or error.
        """
        operation = params.get('operation', 'register')
        path = params.get('path', '')
        method = params.get('method', 'GET')
        version = params.get('version', 'v1')
        handler = params.get('handler', {})
        spec = params.get('spec', {})
        output_var = params.get('output_var', 'api_registry_result')

        try:
            if operation == 'register':
                return self._register_endpoint(
                    path, method, version, handler, spec, output_var
                )
            elif operation == 'get':
                return self._get_endpoint(path, method, version, output_var)
            elif operation == 'list':
                return self._list_endpoints(version, output_var)
            elif operation == 'search':
                return self._search_endpoints(params.get('query', ''), output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API registry failed: {str(e)}"
            )

    def _register_endpoint(
        self,
        path: str,
        method: str,
        version: str,
        handler: Dict,
        spec: Dict,
        output_var: str
    ) -> ActionResult:
        """Register an API endpoint."""
        if not path:
            return ActionResult(
                success=False,
                message="Endpoint path is required"
            )

        key = f"{version}:{method}:{path}"

        self._endpoints[key] = {
            'path': path,
            'method': method,
            'version': version,
            'handler': handler,
            'spec': spec,
            'registered_at': datetime.now().isoformat()
        }

        result = {
            'key': key,
            'path': path,
            'method': method,
            'version': version,
            'registered': True
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Endpoint registered: {method} {path} ({version})"
        )

    def _get_endpoint(
        self, path: str, method: str, version: str, output_var: str
    ) -> ActionResult:
        """Get a registered endpoint."""
        key = f"{version}:{method}:{path}"

        if key not in self._endpoints:
            return ActionResult(
                success=False,
                message=f"Endpoint not found: {method} {path} ({version})"
            )

        context.variables[output_var] = self._endpoints[key]
        return ActionResult(
            success=True,
            data=self._endpoints[key],
            message=f"Endpoint found: {method} {path}"
        )

    def _list_endpoints(self, version: str, output_var: str) -> ActionResult:
        """List all registered endpoints."""
        endpoints = [
            ep for key, ep in self._endpoints.items()
            if version == 'all' or ep['version'] == version
        ]

        context.variables[output_var] = {
            'endpoints': endpoints,
            'count': len(endpoints),
            'version': version
        }
        return ActionResult(
            success=True,
            data={'endpoints': endpoints, 'count': len(endpoints)},
            message=f"Listed {len(endpoints)} endpoints"
        )

    def _search_endpoints(self, query: str, output_var: str) -> ActionResult:
        """Search endpoints by path or spec."""
        results = []
        query_lower = query.lower()

        for key, endpoint in self._endpoints.items():
            if query_lower in endpoint['path'].lower():
                results.append(endpoint)
            elif endpoint.get('spec', {}).get('summary', '').lower().find(query_lower) >= 0:
                results.append(endpoint)

        context.variables[output_var] = {
            'results': results,
            'count': len(results),
            'query': query
        }
        return ActionResult(
            success=True,
            data={'results': results, 'count': len(results), 'query': query},
            message=f"Found {len(results)} endpoints matching '{query}'"
        )
