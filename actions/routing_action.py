"""Routing action module for RabAI AutoClick.

Provides data routing actions based on conditions,
content type, and dynamic routing rules.
"""

import time
import threading
import sys
import os
import re
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RouteRule:
    """A single routing rule.
    
    Attributes:
        name: Rule name.
        condition: Condition to match.
        target: Target route.
        priority: Rule priority (higher = first).
    """
    name: str
    condition: str
    target: Any
    priority: int = 0


class DataRouter:
    """Route data based on configurable rules."""
    
    def __init__(self):
        self._rules: List[RouteRule] = []
        self._lock = threading.RLock()
    
    def add_rule(self, rule: RouteRule) -> None:
        """Add a routing rule.
        
        Args:
            rule: RouteRule to add.
        """
        with self._lock:
            self._rules.append(rule)
            self._rules.sort(key=lambda r: -r.priority)
    
    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name.
        
        Args:
            name: Rule name to remove.
        
        Returns:
            True if removed.
        """
        with self._lock:
            for i, rule in enumerate(self._rules):
                if rule.name == name:
                    self._rules.pop(i)
                    return True
            return False
    
    def clear_rules(self) -> None:
        """Clear all rules."""
        with self._lock:
            self._rules.clear()
    
    def route(self, data: Any, context: Dict[str, Any] = None) -> Optional[Any]:
        """Route data through rules.
        
        Args:
            data: Data to route.
            context: Evaluation context.
        
        Returns:
            Target from first matching rule or None.
        """
        if context is None:
            context = {}
        
        context['_data'] = data
        
        with self._lock:
            for rule in self._rules:
                if self._evaluate_condition(rule.condition, context):
                    return rule.target
        
        return None
    
    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a condition expression.
        
        Args:
            condition: Condition string.
            context: Evaluation context.
        
        Returns:
            True if condition matches.
        """
        try:
            for key, value in context.items():
                globals_dict = {key: value for key, value in context.items()}
            
            result = eval(condition, {"__builtins__": {}}, globals_dict)
            return bool(result)
        except Exception:
            return False
    
    def route_all(self, data: Any, context: Dict[str, Any] = None) -> List[Any]:
        """Route data through all matching rules.
        
        Args:
            data: Data to route.
            context: Evaluation context.
        
        Returns:
            List of all matching targets.
        """
        if context is None:
            context = {}
        
        context['_data'] = data
        matches = []
        
        with self._lock:
            for rule in self._rules:
                if self._evaluate_condition(rule.condition, context):
                    matches.append(rule.target)
        
        return matches


# Global router
_router = DataRouter()


class AddRouteRuleAction(BaseAction):
    """Add a routing rule."""
    action_type = "add_route_rule"
    display_name = "添加路由规则"
    description = "添加数据路由规则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add routing rule.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, condition, target, priority.
        
        Returns:
            ActionResult with rule addition status.
        """
        name = params.get('name', '')
        condition = params.get('condition', '')
        target = params.get('target', None)
        priority = params.get('priority', 0)
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        if not condition:
            return ActionResult(success=False, message="condition is required")
        
        rule = RouteRule(name=name, condition=condition, target=target, priority=priority)
        _router.add_rule(rule)
        
        return ActionResult(success=True, message=f"Added route rule: {name}", data={"name": name, "priority": priority})


class RouteDataAction(BaseAction):
    """Route data through rules."""
    action_type = "route_data"
    display_name = "路由数据"
    description = "按规则路由数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, context, match_all.
        
        Returns:
            ActionResult with routing result.
        """
        data = params.get('data', None)
        route_context = params.get('context', {})
        match_all = params.get('match_all', False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        try:
            if match_all:
                targets = _router.route_all(data, route_context)
                return ActionResult(success=True, message=f"Matched {len(targets)} rules", data={"targets": targets, "count": len(targets)})
            else:
                target = _router.route(data, route_context)
                return ActionResult(success=True, message=f"Route result: {target}", data={"target": target})
        except Exception as e:
            return ActionResult(success=False, message=f"Routing error: {str(e)}")


class RemoveRouteRuleAction(BaseAction):
    """Remove a routing rule."""
    action_type = "remove_route_rule"
    display_name = "移除路由规则"
    description = "移除数据路由规则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Remove rule.
        
        Args:
            context: Execution context.
            params: Dict with keys: name.
        
        Returns:
            ActionResult with removal status.
        """
        name = params.get('name', '')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        removed = _router.remove_rule(name)
        
        if removed:
            return ActionResult(success=True, message=f"Removed route rule: {name}")
        else:
            return ActionResult(success=False, message=f"Rule not found: {name}")


class ClearRouteRulesAction(BaseAction):
    """Clear all routing rules."""
    action_type = "clear_route_rules"
    display_name = "清空路由规则"
    description = "清空所有路由规则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear rules.
        
        Args:
            context: Execution context.
            params: Dict (unused).
        
        Returns:
            ActionResult with cleared status.
        """
        _router.clear_rules()
        
        return ActionResult(success=True, message="All route rules cleared")


class ConditionalRouteAction(BaseAction):
    """Route based on value conditions."""
    action_type = "conditional_route"
    display_name = "条件路由"
    description = "基于条件路由数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Conditionally route.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, conditions (list of {condition, target}).
        
        Returns:
            ActionResult with routing result.
        """
        value = params.get('value', None)
        conditions = params.get('conditions', [])
        default = params.get('default', None)
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        if not conditions:
            return ActionResult(success=True, message="No conditions, returning default", data={"target": default})
        
        try:
            eval_context = {'value': value, '_data': value}
            
            for cond in conditions:
                condition = cond.get('condition', '')
                target = cond.get('target', None)
                
                try:
                    if eval(condition, {"__builtins__": {}}, eval_context):
                        return ActionResult(success=True, message=f"Matched condition: {condition}", data={"target": target, "matched_condition": condition})
                except Exception:
                    continue
            
            return ActionResult(success=True, message="No conditions matched, returning default", data={"target": default, "matched": False})
        except Exception as e:
            return ActionResult(success=False, message=f"Conditional route error: {str(e)}")


class TypeBasedRouteAction(BaseAction):
    """Route based on data type."""
    action_type = "type_route"
    display_name = "类型路由"
    description = "基于数据类型路由"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route by type.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, type_routes (dict of type->target).
        
        Returns:
            ActionResult with routing result.
        """
        data = params.get('data', None)
        type_routes = params.get('type_routes', {})
        default = params.get('default', None)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        data_type = type(data).__name__
        target = type_routes.get(data_type, default)
        
        return ActionResult(success=True, message=f"Type {data_type} routed to {target}", data={"target": target, "type": data_type})
