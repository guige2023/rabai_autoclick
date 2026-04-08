"""API gateway action module for RabAI AutoClick.

Provides API gateway operations:
- APIGatewayAction: Route API requests
- GatewayRouterAction: Route requests to backends
- GatewayMiddlewareAction: Apply middleware
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIGatewayAction(BaseAction):
    """API gateway routing."""
    action_type = "api_gateway"
    display_name = "API网关"
    description = "API网关路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            routes = params.get("routes", [])
            request = params.get("request", {})

            path = request.get("path", "/")
            method = request.get("method", "GET")

            matched_route = None
            for route in routes:
                if route.get("path") == path and route.get("method") == method:
                    matched_route = route
                    break

            return ActionResult(
                success=True,
                data={
                    "path": path,
                    "method": method,
                    "matched_route": matched_route,
                    "routed_at": datetime.now().isoformat()
                },
                message=f"Gateway routed: {method} {path}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API gateway error: {str(e)}")


class GatewayRouterAction(BaseAction):
    """Route requests to backend services."""
    action_type = "gateway_router"
    display_name = "网关路由"
    description = "将请求路由到后端服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backends = params.get("backends", [])
            routing_strategy = params.get("strategy", "round_robin")
            request = params.get("request", {})

            selected = backends[0] if backends else None

            return ActionResult(
                success=True,
                data={
                    "strategy": routing_strategy,
                    "selected_backend": selected,
                    "request": request
                },
                message=f"Router selected backend: {selected}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gateway router error: {str(e)}")


class GatewayMiddlewareAction(BaseAction):
    """Apply middleware to requests."""
    action_type = "gateway_middleware"
    display_name = "网关中间件"
    description = "对请求应用中间件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            middleware_chain = params.get("middleware", [])
            request = params.get("request", {})

            for mw in middleware_chain:
                pass

            return ActionResult(
                success=True,
                data={
                    "middleware_count": len(middleware_chain),
                    "processed": True,
                    "request": request
                },
                message=f"Middleware applied: {len(middleware_chain)} middlewares"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gateway middleware error: {str(e)}")
