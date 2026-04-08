"""API Federation Action Module.

Provides API federation capabilities including cross-service
orchestration, distributed request handling, and federation discovery.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FederationOrchestratorAction(BaseAction):
    """Orchestrate requests across federated API services.
    
    Supports fan-out, fan-in, and chained federation patterns.
    """
    action_type = "federation_orchestrator"
    display_name = "联邦编排"
    description = "跨联邦API服务编排请求"

    def __init__(self):
        super().__init__()
        self._federated_services: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Orchestrate federated requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'orchestrate', 'get_services'.
                - service_name: Service to register/orchestrate.
                - service_config: Service configuration.
                - pattern: 'fan_out', 'fan_in', 'chain'.
                - requests: List of requests for orchestration.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with orchestration result or error.
        """
        operation = params.get('operation', 'orchestrate')
        service_name = params.get('service_name', '')
        service_config = params.get('service_config', {})
        pattern = params.get('pattern', 'fan_out')
        requests = params.get('requests', [])
        output_var = params.get('output_var', 'federation_result')

        try:
            if operation == 'register':
                return self._register_service(service_name, service_config, output_var)
            elif operation == 'orchestrate':
                return self._orchestrate_requests(pattern, requests, output_var)
            elif operation == 'get_services':
                return self._get_services(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Federation orchestrator failed: {str(e)}"
            )

    def _register_service(
        self, service_name: str, service_config: Dict, output_var: str
    ) -> ActionResult:
        """Register a federated service."""
        self._federated_services[service_name] = {
            'name': service_name,
            'config': service_config,
            'registered_at': time.time(),
            'healthy': True
        }

        context.variables[output_var] = {
            'service_name': service_name,
            'registered': True
        }
        return ActionResult(
            success=True,
            data={'service_name': service_name, 'registered': True},
            message=f"Federated service '{service_name}' registered"
        )

    def _orchestrate_requests(
        self, pattern: str, requests: List[Dict], output_var: str
    ) -> ActionResult:
        """Orchestrate requests using specified pattern."""
        if pattern == 'fan_out':
            return self._fan_out_requests(requests, output_var)
        elif pattern == 'fan_in':
            return self._fan_in_requests(requests, output_var)
        elif pattern == 'chain':
            return self._chain_requests(requests, output_var)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown pattern: {pattern}"
            )

    def _fan_out_requests(self, requests: List[Dict], output_var: str) -> ActionResult:
        """Fan out requests to multiple services."""
        results = []
        start_time = time.time()

        # Execute requests in parallel
        with ThreadPoolExecutor(max_workers=len(requests)) as executor:
            futures = {executor.submit(self._execute_request, req): req for req in requests}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        duration = time.time() - start_time

        result = {
            'pattern': 'fan_out',
            'total_requests': len(requests),
            'successful': sum(1 for r in results if r.get('success', False)),
            'duration': duration,
            'results': results
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Fan-out completed: {len(requests)} requests in {duration:.3f}s"
        )

    def _fan_in_requests(self, requests: List[Dict], output_var: str) -> ActionResult:
        """Fan in requests from multiple sources."""
        results = []
        for req in requests:
            result = self._execute_request(req)
            results.append(result)

        # Aggregate results
        aggregated = {
            'total_results': len(results),
            'successful': sum(1 for r in results if r.get('success', False))
        }

        result = {
            'pattern': 'fan_in',
            'results': results,
            'aggregated': aggregated
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Fan-in completed: aggregated {len(results)} results"
        )

    def _chain_requests(self, requests: List[Dict], output_var: str) -> ActionResult:
        """Chain requests sequentially."""
        results = []
        previous_result = None

        for i, req in enumerate(requests):
            # Pass previous result to next request if configured
            if previous_result and req.get('use_previous', False):
                req['data'] = previous_result

            result = self._execute_request(req)
            results.append(result)

            if not result.get('success', False):
                return ActionResult(
                    success=False,
                    data={'results': results, 'failed_at': i},
                    message=f"Chain failed at request {i}"
                )

            previous_result = result.get('data')

        result = {
            'pattern': 'chain',
            'total_requests': len(requests),
            'results': results,
            'final_result': previous_result
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Chain completed: {len(requests)} requests"
        )

    def _execute_request(self, request: Dict) -> Dict:
        """Execute a single federated request."""
        # Simulate request execution
        return {
            'success': True,
            'data': request.get('data', {}),
            'timestamp': time.time()
        }

    def _get_services(self, output_var: str) -> ActionResult:
        """Get registered federated services."""
        services = [
            {
                'name': name,
                'healthy': info.get('healthy', False),
                'registered_at': info.get('registered_at')
            }
            for name, info in self._federated_services.items()
        ]

        context.variables[output_var] = {
            'services': services,
            'count': len(services)
        }
        return ActionResult(
            success=True,
            data={'services': services, 'count': len(services)},
            message=f"Retrieved {len(services)} federated services"
        )


class CrossServiceQueryAction(BaseAction):
    """Execute queries across multiple API services.
    
    Supports distributed queries with result aggregation.
    """
    action_type = "cross_service_query"
    display_name: "跨服务查询"
    description = "跨多个API服务执行查询"

    def __init__(self):
        super().__init__()
        self._query_handlers: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cross-service query.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register_handler', 'query'.
                - service: Service name.
                - query: Query specification.
                - aggregation: Result aggregation method.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with query result or error.
        """
        operation = params.get('operation', 'query')
        service = params.get('service', '')
        query = params.get('query', {})
        aggregation = params.get('aggregation', 'merge')
        output_var = params.get('output_var', 'query_result')

        try:
            if operation == 'register_handler':
                return self._register_handler(service, query.get('handler_var', ''), context, output_var)
            elif operation == 'query':
                return self._execute_query(service, query, aggregation, context, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cross-service query failed: {str(e)}"
            )

    def _register_handler(
        self, service: str, handler_var: str, context: Any, output_var: str
    ) -> ActionResult:
        """Register a query handler for a service."""
        handler = context.variables.get(handler_var) if handler_var else None
        self._query_handlers[service] = handler

        context.variables[output_var] = {
            'service': service,
            'handler_registered': handler is not None
        }
        return ActionResult(
            success=True,
            data={'service': service, 'handler_registered': handler is not None},
            message=f"Query handler registered for '{service}'"
        )

    def _execute_query(
        self,
        service: str,
        query: Dict,
        aggregation: str,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Execute query against a service."""
        handler = self._query_handlers.get(service)

        if not handler:
            return ActionResult(
                success=False,
                message=f"No handler registered for service '{service}'"
            )

        try:
            result = handler(query)
            return ActionResult(
                success=True,
                data={'service': service, 'result': result},
                message=f"Query executed against '{service}'"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query failed: {str(e)}"
            )


class ServiceMeshAction(BaseAction):
    """Manage service mesh configuration and routing.
    
    Supports traffic management, circuit breaking, and retries.
    """
    action_type = "service_mesh"
    display_name = "服务网格"
    description = "管理服务网格配置和路由"

    def __init__(self):
        super().__init__()
        self._routing_rules: Dict[str, Dict] = {}
        self._traffic_policies: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage service mesh.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add_route', 'configure_traffic', 'get_mesh_status'.
                - route: Routing rule configuration.
                - traffic_policy: Traffic management policy.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with mesh management result or error.
        """
        operation = params.get('operation', 'add_route')
        route = params.get('route', {})
        traffic_policy = params.get('traffic_policy', {})
        output_var = params.get('output_var', 'mesh_result')

        try:
            if operation == 'add_route':
                return self._add_route_rule(route, output_var)
            elif operation == 'configure_traffic':
                return self._configure_traffic_policy(traffic_policy, output_var)
            elif operation == 'get_mesh_status':
                return self._get_mesh_status(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Service mesh operation failed: {str(e)}"
            )

    def _add_route_rule(self, route: Dict, output_var: str) -> ActionResult:
        """Add a routing rule."""
        route_id = route.get('id', f"route_{int(time.time())}")

        self._routing_rules[route_id] = {
            'id': route_id,
            'source': route.get('source', '*'),
            'destination': route.get('destination'),
            'match': route.get('match', {}),
            'weight': route.get('weight', 100),
            'created_at': time.time()
        }

        context.variables[output_var] = {
            'route_id': route_id,
            'added': True
        }
        return ActionResult(
            success=True,
            data={'route_id': route_id, 'added': True},
            message=f"Route rule '{route_id}' added"
        )

    def _configure_traffic_policy(
        self, traffic_policy: Dict, output_var: str
    ) -> ActionResult:
        """Configure traffic management policy."""
        policy_id = traffic_policy.get('id', f"policy_{int(time.time())}")

        self._traffic_policies[policy_id] = {
            'id': policy_id,
            'retries': traffic_policy.get('retries', {}),
            'timeout': traffic_policy.get('timeout', {}),
            'circuit_breaker': traffic_policy.get('circuit_breaker', {}),
            'created_at': time.time()
        }

        context.variables[output_var] = {
            'policy_id': policy_id,
            'configured': True
        }
        return ActionResult(
            success=True,
            data={'policy_id': policy_id, 'configured': True},
            message=f"Traffic policy '{policy_id}' configured"
        )

    def _get_mesh_status(self, output_var: str) -> ActionResult:
        """Get service mesh status."""
        status = {
            'routes': len(self._routing_rules),
            'policies': len(self._traffic_policies),
            'routing_rules': list(self._routing_rules.values()),
            'traffic_policies': list(self._traffic_policies.values())
        }

        context.variables[output_var] = status
        return ActionResult(
            success=True,
            data=status,
            message=f"Mesh status: {len(self._routing_rules)} routes, {len(self._traffic_policies)} policies"
        )
