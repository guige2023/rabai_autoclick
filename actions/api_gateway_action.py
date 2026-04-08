"""API Gateway action module for RabAI AutoClick.

Provides API gateway functionality: routing, load balancing,
rate limiting, and request/response transformation.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiGatewayRouterAction(BaseAction):
    """Route API requests to backends based on rules.

    Pattern-based routing with header manipulation
    and backend selection.
    """
    action_type = "api_gateway_router"
    display_name = "API网关路由"
    description = "基于规则将API请求路由到后端"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route API request.

        Args:
            context: Execution context.
            params: Dict with keys: request, routes,
                   default_backend, sticky_session.

        Returns:
            ActionResult with routed response.
        """
        start_time = time.time()
        try:
            request_url = params.get('request_url', '')
            request_method = params.get('request_method', 'GET').upper()
            request_headers = params.get('request_headers', {})
            request_body = params.get('request_body')
            routes = params.get('routes', [])
            default_backend = params.get('default_backend')
            sticky_session = params.get('sticky_session', False)

            if not request_url:
                return ActionResult(
                    success=False,
                    message="request_url is required",
                    duration=time.time() - start_time,
                )

            # Find matching route
            matched_backend = default_backend
            route_params = {}
            for route in routes:
                path_pattern = route.get('path_pattern', '')
                header_conditions = route.get('headers', {})

                if self._match_path(request_url, path_pattern):
                    if self._match_headers(request_headers, header_conditions):
                        matched_backend = route.get('backend', default_backend)
                        route_params = self._extract_params(request_url, path_pattern)
                        break

            if not matched_backend:
                return ActionResult(
                    success=False,
                    message="No matching route found",
                    duration=time.time() - start_time,
                )

            # Build backend URL
            backend_url = matched_backend
            if not backend_url.endswith('/'):
                backend_url += '/'
            path = request_url.split('?', 1)[0]
            backend_url += path.lstrip('/')

            # Add route params as query params
            if route_params:
                qs = '&'.join(f"{k}={v}" for k, v in route_params.items())
                backend_url = backend_url + ('?' if '?' not in backend_url else '&') + qs

            # Forward request
            body_bytes = None
            if request_body:
                if isinstance(request_body, str):
                    body_bytes = request_body.encode('utf-8')
                else:
                    body_bytes = json.dumps(request_body).encode('utf-8')

            headers = {**request_headers}
            if request_body:
                headers.setdefault('Content-Type', 'application/json')

            req = Request(backend_url, data=body_bytes, headers=headers, method=request_method)
            try:
                with urlopen(req, timeout=30) as resp:
                    response_body = resp.read()
                    response_data = None
                    try:
                        response_data = json.loads(response_body)
                    except Exception:
                        response_data = response_body.decode('utf-8', errors='ignore')

                    duration = time.time() - start_time
                    return ActionResult(
                        success=True,
                        message=f"Routed to {matched_backend}",
                        data={
                            'backend': matched_backend,
                            'backend_url': backend_url,
                            'status': resp.status,
                            'response': response_data,
                        },
                        duration=duration,
                    )
            except HTTPError as e:
                duration = time.time() - start_time
                return ActionResult(
                    success=False,
                    message=f"Backend error: {e.code}",
                    data={
                        'backend': matched_backend,
                        'status': e.code,
                        'error': e.read().decode('utf-8', errors='ignore') if e.fp else str(e),
                    },
                    duration=duration,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Gateway router error: {str(e)}",
                duration=duration,
            )

    def _match_path(self, url: str, pattern: str) -> bool:
        """Match URL against path pattern."""
        import re
        regex = pattern.replace('{', '(?P<').replace('}', '>[^/]+)')
        regex = '^' + regex.replace('*', '[^/]*') + '$'
        return bool(re.match(regex, url))

    def _match_headers(self, headers: Dict, conditions: Dict) -> bool:
        """Check if headers match conditions."""
        for key, value in conditions.items():
            if headers.get(key) != value:
                return False
        return True

    def _extract_params(self, url: str, pattern: str) -> Dict:
        """Extract path parameters."""
        import re
        params = {}
        regex = pattern.replace('{', '(?P<').replace('}', '>[^/]+)')
        regex = '^' + regex + '$'
        match = re.match(regex, url.split('?')[0])
        if match:
            params = match.groupdict()
        return params


class ApiGatewayLoadBalancerAction(BaseAction):
    """Load balance requests across multiple backends.

    Supports round-robin, least-connections, and
    weighted distribution.
    """
    action_type = "api_gateway_load_balancer"
    display_name = "API网关负载均衡"
    description = "跨多个后端负载均衡请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Balance request across backends.

        Args:
            context: Execution context.
            params: Dict with keys: backends, strategy (round_robin/least_conn/weighted),
                   request_url, request_method, request_headers.

        Returns:
            ActionResult with backend-selected response.
        """
        start_time = time.time()
        try:
            backends = params.get('backends', [])
            strategy = params.get('strategy', 'round_robin')
            request_url = params.get('request_url', '')
            request_method = params.get('request_method', 'GET').upper()
            request_headers = params.get('request_headers', {})
            request_body = params.get('request_body')

            if not backends:
                return ActionResult(
                    success=False,
                    message="At least one backend is required",
                    duration=time.time() - start_time,
                )

            # Initialize state
            if not hasattr(context, '_gateway_state'):
                context._gateway_state = {
                    'round_robin_index': 0,
                    'connections': {},
                }
            state = context._gateway_state

            # Select backend
            if strategy == 'round_robin':
                idx = state['round_robin_index'] % len(backends)
                state['round_robin_index'] += 1
                selected = backends[idx]
            elif strategy == 'least_conn':
                connections = state['connections']
                min_conn = float('inf')
                selected = backends[0]
                for backend in backends:
                    conn_count = connections.get(backend.get('url', backend), 0)
                    if conn_count < min_conn:
                        min_conn = conn_count
                        selected = backend
                backend_url = selected.get('url', selected) if isinstance(selected, dict) else selected
                state['connections'][backend_url] = state['connections'].get(backend_url, 0) + 1
            else:
                selected = backends[0]

            if isinstance(selected, dict):
                backend_url = selected.get('url', '')
            else:
                backend_url = selected

            # Forward request to selected backend
            full_url = backend_url
            if not full_url.endswith('/') and request_url:
                full_url += '/'
            full_url += request_url.lstrip('/')

            body_bytes = None
            if request_body:
                if isinstance(request_body, str):
                    body_bytes = request_body.encode('utf-8')
                else:
                    body_bytes = json.dumps(request_body).encode('utf-8')

            headers = {**request_headers}
            if request_body:
                headers.setdefault('Content-Type', 'application/json')

            req = Request(full_url, data=body_bytes, headers=headers, method=request_method)
            try:
                with urlopen(req, timeout=30) as resp:
                    response_data = json.loads(resp.read())
                    duration = time.time() - start_time
                    return ActionResult(
                        success=True,
                        message=f"Balanced to {backend_url}",
                        data={
                            'backend': backend_url,
                            'strategy': strategy,
                            'status': resp.status,
                            'response': response_data,
                        },
                        duration=duration,
                    )
            except HTTPError as e:
                duration = time.time() - start_time
                return ActionResult(
                    success=False,
                    message=f"Backend error: {e.code}",
                    data={'backend': backend_url, 'status': e.code},
                    duration=duration,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Load balancer error: {str(e)}",
                duration=duration,
            )
