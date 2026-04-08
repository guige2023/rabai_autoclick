"""API routing action module for RabAI AutoClick.

Provides API routing operations:
- RouteRegisterAction: Register a route
- RouteMatchAction: Match request to route
- RouteDispatchAction: Dispatch to route handler
- RouteListAction: List all routes
- RouteUnregisterAction: Unregister a route
"""

import re
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RouteRegisterAction(BaseAction):
    """Register an API route."""
    action_type = "route_register"
    display_name = "注册路由"
    description = "注册API路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            path = params.get("path", "")
            methods = params.get("methods", ["GET"])
            handler = params.get("handler", "")
            middleware = params.get("middleware", [])

            if not path or not handler:
                return ActionResult(success=False, message="path and handler are required")

            route_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "api_routes"):
                context.api_routes = {}
            context.api_routes[route_id] = {
                "route_id": route_id,
                "path": path,
                "methods": methods,
                "handler": handler,
                "middleware": middleware,
                "registered_at": time.time(),
                "call_count": 0,
            }

            return ActionResult(
                success=True,
                data={"route_id": route_id, "path": path, "methods": methods},
                message=f"Route {route_id} registered: {path}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Route register failed: {e}")


class RouteMatchAction(BaseAction):
    """Match request to route."""
    action_type = "route_match"
    display_name = "路由匹配"
    description = "将请求匹配到路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            path = params.get("path", "")
            method = params.get("method", "GET")

            if not path:
                return ActionResult(success=False, message="path is required")

            routes = getattr(context, "api_routes", {})
            matched = None

            for route in routes.values():
                pattern = route["path"].replace("{", "(?P<").replace("}", ">[^/]+)")
                pattern = f"^{pattern}$"
                if re.match(pattern, path) and method in route["methods"]:
                    matched = route
                    break

            if matched:
                return ActionResult(
                    success=True,
                    data={"route_id": matched["route_id"], "path": matched["path"], "handler": matched["handler"], "matched": True},
                    message=f"Matched route: {matched['path']}",
                )
            else:
                return ActionResult(success=True, data={"matched": False}, message="No route matched")
        except Exception as e:
            return ActionResult(success=False, message=f"Route match failed: {e}")


class RouteDispatchAction(BaseAction):
    """Dispatch request to route handler."""
    action_type = "route_dispatch"
    display_name = "路由分发"
    description = "将请求分发到路由处理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            route_id = params.get("route_id", "")
            request_data = params.get("request_data", {})

            if not route_id:
                return ActionResult(success=False, message="route_id is required")

            routes = getattr(context, "api_routes", {})
            if route_id not in routes:
                return ActionResult(success=False, message=f"Route {route_id} not found")

            route = routes[route_id]
            route["call_count"] += 1

            return ActionResult(
                success=True,
                data={"route_id": route_id, "handler": route["handler"], "call_count": route["call_count"]},
                message=f"Dispatched to {route['handler']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Route dispatch failed: {e}")


class RouteListAction(BaseAction):
    """List all routes."""
    action_type = "route_list"
    display_name = "路由列表"
    description = "列出所有路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            routes = getattr(context, "api_routes", {})
            route_list = [{"route_id": r["route_id"], "path": r["path"], "methods": r["methods"], "call_count": r["call_count"]} for r in routes.values()]

            return ActionResult(
                success=True,
                data={"routes": route_list, "count": len(route_list)},
                message=f"Found {len(route_list)} routes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Route list failed: {e}")


class RouteUnregisterAction(BaseAction):
    """Unregister a route."""
    action_type = "route_unregister"
    display_name = "注销路由"
    description = "注销API路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            route_id = params.get("route_id", "")
            if not route_id:
                return ActionResult(success=False, message="route_id is required")

            routes = getattr(context, "api_routes", {})
            if route_id not in routes:
                return ActionResult(success=False, message=f"Route {route_id} not found")

            route_path = routes[route_id]["path"]
            del routes[route_id]

            return ActionResult(success=True, data={"route_id": route_id, "path": route_path}, message=f"Route {route_path} unregistered")
        except Exception as e:
            return ActionResult(success=False, message=f"Route unregister failed: {e}")
