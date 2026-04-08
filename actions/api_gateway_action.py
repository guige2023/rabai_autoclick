"""API Gateway action module for RabAI AutoClick.

Provides API gateway operations:
- GatewayRouteAction: Define API routes
- GatewayAuthAction: API authentication middleware
- GatewayRateLimitAction: Rate limiting
- GatewayTransformAction: Request/response transformation
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GatewayRouteAction(BaseAction):
    """Define API routes."""
    action_type = "gateway_route"
    display_name = "API路由"
    description = "定义API路由"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute route definition."""
        routes = params.get('routes', [])
        path = params.get('path', '')
        method = params.get('method', 'get').upper()
        output_var = params.get('output_var', 'route_result')

        if not path or not routes:
            return ActionResult(success=False, message="path and routes are required")

        try:
            resolved_routes = context.resolve_value(routes) if context else routes

            matched = None
            for route in resolved_routes:
                if route.get('path') == path and route.get('method', 'GET').upper() == method:
                    matched = route
                    break

            if matched:
                result = {
                    'matched': True,
                    'handler': matched.get('handler', ''),
                    'middleware': matched.get('middleware', []),
                    'path': path,
                    'method': method,
                }
            else:
                result = {
                    'matched': False,
                    'path': path,
                    'method': method,
                    'available_routes': [{'path': r.get('path'), 'method': r.get('method')} for r in resolved_routes],
                }

            return ActionResult(
                success=matched is not None,
                data={output_var: result},
                message=f"Route {'matched' if matched else 'not found'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Route error: {e}")


class GatewayAuthAction(BaseAction):
    """API authentication middleware."""
    action_type = "gateway_auth"
    display_name = "API认证"
    description = "API认证中间件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute authentication."""
        auth_type = params.get('auth_type', 'bearer')
        token = params.get('token', '')
        api_key = params.get('api_key', '')
        secret = params.get('secret', '')
        output_var = params.get('output_var', 'auth_result')

        try:
            resolved_token = context.resolve_value(token) if context else token
            resolved_api_key = context.resolve_value(api_key) if context else api_key
            resolved_secret = context.resolve_value(secret) if context else secret

            result = {'auth_type': auth_type, 'authenticated': False}

            if auth_type == 'bearer' and resolved_token:
                result['authenticated'] = len(resolved_token) > 10
            elif auth_type == 'api_key' and resolved_api_key:
                result['authenticated'] = len(resolved_api_key) > 5
            elif auth_type == 'hmac':
                result['authenticated'] = bool(resolved_secret)
            elif auth_type == 'basic':
                result['authenticated'] = bool(resolved_api_key)

            return ActionResult(
                success=result['authenticated'],
                data={output_var: result},
                message="Authenticated" if result['authenticated'] else "Authentication failed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {e}")


class GatewayRateLimitAction(BaseAction):
    """Rate limiting middleware."""
    action_type = "gateway_rate_limit"
    display_name = "API限流"
    description = "API限流中间件"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._buckets = defaultdict(lambda: {'tokens': 100, 'last_refill': time.time()})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit check."""
        key = params.get('key', 'default')
        rate = params.get('rate', 100)
        period = params.get('period', 60)
        cost = params.get('cost', 1)
        output_var = params.get('output_var', 'rate_limit_result')

        try:
            bucket = self._buckets[key]
            now = time.time()
            elapsed = now - bucket['last_refill']
            tokens_to_add = (elapsed / period) * rate
            bucket['tokens'] = min(rate, bucket['tokens'] + tokens_to_add)
            bucket['last_refill'] = now

            allowed = bucket['tokens'] >= cost
            if allowed:
                bucket['tokens'] -= cost

            result = {
                'allowed': allowed,
                'key': key,
                'remaining': int(bucket['tokens']),
                'limit': rate,
                'reset_in': int(period * (1 - bucket['tokens'] / rate)) if bucket['tokens'] < rate else 0,
            }

            return ActionResult(
                success=allowed,
                data={output_var: result},
                message=f"{'Allowed' if allowed else 'Rate limited'}, remaining: {result['remaining']}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit error: {e}")


class GatewayTransformAction(BaseAction):
    """Request/response transformation."""
    action_type = "gateway_transform"
    display_name = "API转换"
    description = "请求/响应转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute transformation."""
        data = params.get('data', {})
        transforms = params.get('transforms', [])
        output_var = params.get('output_var', 'transformed_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_transforms = context.resolve_value(transforms) if context else transforms

            transformed = resolved_data.copy()

            for transform in resolved_transforms:
                transform_type = transform.get('type', '')
                field = transform.get('field', '')
                config = transform.get('config', {})

                if transform_type == 'rename' and field in transformed:
                    new_name = config.get('to', field)
                    transformed[new_name] = transformed.pop(field)
                elif transform_type == 'remove' and field in transformed:
                    del transformed[field]
                elif transform_type == 'map' and field in transformed:
                    mapping = config.get('mapping', {})
                    transformed[field] = mapping.get(transformed[field], transformed[field])
                elif transform_type == 'default' and field not in transformed:
                    transformed[field] = config.get('value', None)
                elif transform_type == 'uppercase' and field in transformed:
                    transformed[field] = str(transformed[field]).upper()
                elif transform_type == 'lowercase' and field in transformed:
                    transformed[field] = str(transformed[field]).lower()
                elif transform_type == 'cast' and field in transformed:
                    cast_type = config.get('to', 'string')
                    if cast_type == 'int':
                        transformed[field] = int(transformed[field])
                    elif cast_type == 'float':
                        transformed[field] = float(transformed[field])
                    elif cast_type == 'bool':
                        transformed[field] = bool(transformed[field])

            result = {
                'transformed': transformed,
                'applied': len(resolved_transforms),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Applied {len(resolved_transforms)} transforms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")
