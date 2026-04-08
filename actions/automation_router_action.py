"""Automation router action module for RabAI AutoClick.

Provides workflow routing operations:
- ContentRouterAction: Route based on content analysis
- RuleRouterAction: Route based on configurable rules
- PriorityRouterAction: Route based on priority levels
- LoadBalancerRouterAction: Distribute load across handlers
- DynamicRouterAction: Dynamic routing based on runtime data
"""

from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime
import hashlib

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContentRouterAction(BaseAction):
    """Route based on content analysis."""
    action_type = "automation_content_router"
    display_name = "内容路由"
    description = "基于内容分析路由工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            content = params.get("content", "")
            routes = params.get("routes", [])
            default_route = params.get("default_route", "default")
            case_sensitive = params.get("case_sensitive", False)

            if not content:
                return ActionResult(success=False, message="No content provided for routing")

            if not routes:
                return ActionResult(success=False, message="No routes defined")

            matched_route = default_route

            for route in routes:
                route_name = route.get("name", "unnamed")
                patterns = route.get("patterns", [])
                keywords = route.get("keywords", [])
                route_type = route.get("type", "keyword")

                if route_type == "keyword":
                    check_content = content if case_sensitive else content.lower()
                    for keyword in keywords:
                        kw = keyword if case_sensitive else keyword.lower()
                        if kw in check_content:
                            matched_route = route_name
                            break

                elif route_type == "pattern":
                    import re
                    for pattern in patterns:
                        if re.search(pattern, content):
                            matched_route = route_name
                            break

                if matched_route != default_route:
                    break

            return ActionResult(
                success=True,
                data={
                    "matched_route": matched_route,
                    "content_length": len(content),
                    "routes_checked": len(routes)
                },
                message=f"Content routed to '{matched_route}'"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Content router error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["content"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"routes": [], "default_route": "default", "case_sensitive": False}


class RuleRouterAction(BaseAction):
    """Route based on configurable rules."""
    action_type = "automation_rule_router"
    display_name = "规则路由"
    description = "基于可配置规则路由工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", [])
            default_route = params.get("default_route", "default")
            match_all = params.get("match_all", False)

            if not rules:
                return ActionResult(success=False, message="No rules defined")

            matched_rules = []

            for rule in rules:
                rule_name = rule.get("name", "unnamed")
                conditions = rule.get("conditions", [])
                route_to = rule.get("route_to", default_route)
                priority = rule.get("priority", 0)

                all_match = True
                for condition in conditions:
                    field = condition.get("field")
                    operator = condition.get("operator", "eq")
                    value = condition.get("value")

                    data_value = data.get(field) if field else None

                    if operator == "eq":
                        match = data_value == value
                    elif operator == "ne":
                        match = data_value != value
                    elif operator == "gt":
                        match = data_value is not None and data_value > value
                    elif operator == "lt":
                        match = data_value is not None and data_value < value
                    elif operator == "contains":
                        match = value in str(data_value) if data_value else False
                    elif operator == "exists":
                        match = field in data
                    elif operator == "in":
                        match = data_value in value if isinstance(value, list) else False
                    else:
                        match = False

                    if not match:
                        all_match = False
                        break

                if all_match:
                    matched_rules.append({"rule": rule_name, "route": route_to, "priority": priority})
                    if not match_all:
                        break

            if matched_rules:
                matched_rules.sort(key=lambda x: x["priority"], reverse=True)
                best_match = matched_rules[0]["route"]
            else:
                best_match = default_route

            return ActionResult(
                success=True,
                data={
                    "matched_route": best_match,
                    "matched_rules": [r["rule"] for r in matched_rules],
                    "rules_checked": len(rules)
                },
                message=f"Rule-based routing: '{best_match}'"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rule router error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"rules": [], "default_route": "default", "match_all": False}


class PriorityRouterAction(BaseAction):
    """Route based on priority levels."""
    action_type = "automation_priority_router"
    display_name = "优先级路由"
    description = "基于优先级级别路由工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            priority_field = params.get("priority_field", "priority")
            route_mapping = params.get("route_mapping", {})
            default_route = params.get("default_route", "normal")

            if not items:
                return ActionResult(success=False, message="No items to route")

            route_counts = {v: 0 for v in list(route_mapping.values()) + [default_route]}
            routed_items = []

            for item in items:
                priority = item.get(priority_field, 0) if isinstance(item, dict) else 0
                target_route = default_route

                for level, route in sorted(route_mapping.items(), key=lambda x: int(x[0]), reverse=True):
                    if priority >= int(level):
                        target_route = route
                        break

                route_counts[target_route] += 1
                routed_items.append({"item": item, "route": target_route, "priority": priority})

            return ActionResult(
                success=True,
                data={
                    "routed_items": routed_items,
                    "route_counts": route_counts,
                    "total_items": len(items)
                },
                message=f"Routed {len(items)} items by priority"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Priority router error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["items"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"priority_field": "priority", "route_mapping": {}, "default_route": "normal"}


class LoadBalancerRouterAction(BaseAction):
    """Distribute load across handlers."""
    action_type = "automation_load_balancer_router"
    display_name = "负载均衡路由"
    description = "跨处理器分配负载"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            handlers = params.get("handlers", [])
            strategy = params.get("strategy", "round_robin")
            weights = params.get("weights", {})

            if not handlers:
                return ActionResult(success=False, message="No handlers defined")

            if not items:
                return ActionResult(success=False, message="No items to distribute")

            handler_loads = {h: 0 for h in handlers}
            routed = []

            if strategy == "round_robin":
                for i, item in enumerate(items):
                    target = handlers[i % len(handlers)]
                    routed.append({"item": item, "handler": target})
                    handler_loads[target] += 1

            elif strategy == "random":
                import random
                for item in items:
                    target = random.choice(handlers)
                    routed.append({"item": item, "handler": target})
                    handler_loads[target] += 1

            elif strategy == "weighted":
                weighted_handlers = []
                for h in handlers:
                    weight = weights.get(h, 1)
                    weighted_handlers.extend([h] * weight)
                if not weighted_handlers:
                    weighted_handlers = handlers
                for item in items:
                    import random
                    target = random.choice(weighted_handlers)
                    routed.append({"item": item, "handler": target})
                    handler_loads[target] += 1

            elif strategy == "least_load":
                for item in items:
                    target = min(handler_loads, key=handler_loads.get)
                    routed.append({"item": item, "handler": target})
                    handler_loads[target] += 1

            return ActionResult(
                success=True,
                data={
                    "routed_items": routed,
                    "handler_loads": handler_loads,
                    "strategy": strategy,
                    "total_items": len(items)
                },
                message=f"Distributed {len(items)} items using {strategy} strategy"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Load balancer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["items", "handlers"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"strategy": "round_robin", "weights": {}}


class DynamicRouterAction(BaseAction):
    """Dynamic routing based on runtime data."""
    action_type = "automation_dynamic_router"
    display_name = "动态路由"
    description = "基于运行时数据的动态路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            routing_config = params.get("routing_config", {})
            resolver_function = params.get("resolver_function")
            default_route = params.get("default_route", "default")

            if not routing_config and not resolver_function:
                return ActionResult(success=False, message="No routing configuration provided")

            resolved_route = default_route

            if routing_config:
                conditions = routing_config.get("conditions", [])
                for condition in conditions:
                    field = condition.get("field")
                    resolver = condition.get("resolver", "static")
                    route = condition.get("route")

                    data_value = data.get(field) if field else None

                    if resolver == "static":
                        if data_value == condition.get("value"):
                            resolved_route = route
                            break
                    elif resolver == "range":
                        min_val = condition.get("min", float("-inf"))
                        max_val = condition.get("max", float("inf"))
                        if min_val <= (data_value or 0) <= max_val:
                            resolved_route = route
                            break
                    elif resolver == "hash":
                        hash_key = str(data_value or "")
                        route_count = len(conditions)
                        hash_val = int(hashlib.md5(hash_key.encode()).hexdigest(), 16)
                        selected_idx = hash_val % route_count
                        resolved_route = conditions[selected_idx].get("route", default_route)
                        break

            return ActionResult(
                success=True,
                data={
                    "resolved_route": resolved_route,
                    "data_snapshot": {k: data.get(k) for k in list(data.keys())[:5]},
                    "routing_method": "dynamic"
                },
                message=f"Dynamic routing resolved to '{resolved_route}'"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dynamic router error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"routing_config": {}, "resolver_function": None, "default_route": "default"}
