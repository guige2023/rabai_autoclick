"""Automation dynamic branch action module for RabAI AutoClick.

Provides dynamic branching for automation:
- AutomationDynamicBranchAction: Runtime branch selection
- AutomationDynamicRouterAction: Route to dynamic handlers
- AutomationDynamicStepAction: Add/replace steps at runtime
- AutomationDynamicConditionAction: Dynamic condition evaluation
"""

import time
import random
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationDynamicBranchAction(BaseAction):
    """Dynamic branch selection at runtime."""
    action_type = "automation_dynamic_branch"
    display_name = "自动化动态分支"
    description = "运行时动态选择分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            context_data = params.get("context_data", {})
            selection_fn = params.get("selection_fn")
            default_index = params.get("default_index", 0)
            callback = params.get("callback")

            if not branches:
                return ActionResult(success=False, message="branches list is required")

            selected_index = default_index
            selection_reason = "default"

            if callable(selection_fn):
                try:
                    selected_index = selection_fn(branches, context_data)
                    selected_reason = "function"
                except Exception as e:
                    selection_reason = f"function error: {e}"

            else:
                conditions = context_data.get("conditions", {})
                priority_scores = {}

                for i, branch in enumerate(branches):
                    score = 0
                    branch_conditions = branch.get("conditions", {})

                    for key, expected in branch_conditions.items():
                        actual = conditions.get(key)
                        if actual == expected:
                            score += 1
                        elif isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
                            if abs(actual - expected) < 0.1:
                                score += 1

                    priority_scores[i] = score

                if priority_scores:
                    max_score = max(priority_scores.values())
                    if max_score > 0:
                        candidates = [i for i, s in priority_scores.items() if s == max_score]
                        selected_index = random.choice(candidates) if len(candidates) > 1 else candidates[0]
                        selection_reason = f"score={max_score}"

            selected_branch = branches[selected_index] if selected_index < len(branches) else branches[0]

            action_result = selected_branch.get("action")
            if callable(action_result):
                result = action_result(context=context_data)
            else:
                result = {"success": True, "message": "Branch executed"}

            return ActionResult(
                success=result.get("success", False),
                message=f"Selected branch {selected_index}: {selection_reason}",
                data={"selected_index": selected_index, "reason": selection_reason, "result": result, "total_branches": len(branches)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dynamic branch error: {e}")


class AutomationDynamicRouterAction(BaseAction):
    """Route to dynamic handlers based on content."""
    action_type = "automation_dynamic_router"
    display_name = "自动化动态路由"
    description = "基于内容动态路由到处理器"

    def __init__(self):
        super().__init__()
        self._routes: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "route")
            route_key = params.get("route_key")
            data = params.get("data")
            handler = params.get("handler")

            if operation == "register":
                if not route_key or not callable(handler):
                    return ActionResult(success=False, message="route_key and callable handler required")
                self._routes[route_key] = handler
                return ActionResult(success=True, message=f"Registered route '{route_key}'", data={"route_key": route_key, "route_count": len(self._routes)})

            elif operation == "route":
                if not route_key:
                    return ActionResult(success=False, message="route_key required")
                if route_key not in self._routes:
                    return ActionResult(success=False, message=f"Route '{route_key}' not found")
                handler = self._routes[route_key]
                result = handler(data)
                return ActionResult(success=True, message=f"Routed to '{route_key}'", data={"result": result})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._routes)} routes registered", data={"routes": list(self._routes.keys())})

            elif operation == "unregister":
                if route_key and route_key in self._routes:
                    del self._routes[route_key]
                    return ActionResult(success=True, message=f"Unregistered '{route_key}'")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Dynamic router error: {e}")


class AutomationDynamicStepAction(BaseAction):
    """Add or replace steps at runtime."""
    action_type = "automation_dynamic_step"
    display_name = "自动化动态步骤"
    description = "运行时动态添加或替换步骤"

    def __init__(self):
        super().__init__()
        self._step_registry: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            step_name = params.get("step_name")
            step_fn = params.get("step_fn")
            steps = params.get("steps", [])
            insert_at = params.get("insert_at", -1)
            replace = params.get("replace", False)

            if operation == "register":
                if not step_name or not callable(step_fn):
                    return ActionResult(success=False, message="step_name and callable step_fn required")
                self._step_registry[step_name] = step_fn
                return ActionResult(success=True, message=f"Registered step '{step_name}'")

            elif operation == "add":
                if not steps or not step_name:
                    return ActionResult(success=False, message="steps and step_name required")

                if step_name not in self._step_registry:
                    return ActionResult(success=False, message=f"Step '{step_name}' not registered")

                new_steps = list(steps)
                step_def = {"name": step_name, "fn": self._step_registry[step_name]}

                if insert_at >= 0 and insert_at <= len(new_steps):
                    new_steps.insert(insert_at, step_def)
                else:
                    new_steps.append(step_def)

                return ActionResult(success=True, message=f"Added step '{step_name}' at position {insert_at}", data={"steps": new_steps, "count": len(new_steps)})

            elif operation == "replace":
                if not steps or step_name is None:
                    return ActionResult(success=False, message="steps and step_name required")

                if step_name not in self._step_registry and step_name != "*":
                    return ActionResult(success=False, message=f"Step '{step_name}' not registered")

                new_steps = []
                for step in steps:
                    if step.get("name") == step_name or step_name == "*":
                        new_steps.append({"name": step_name, "fn": self._step_registry.get(step_name, step.get("fn"))})
                    else:
                        new_steps.append(step)

                return ActionResult(success=True, message=f"Replaced step '{step_name}'", data={"steps": new_steps})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._step_registry)} registered steps", data={"steps": list(self._step_registry.keys())})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Dynamic step error: {e}")


class AutomationDynamicConditionAction(BaseAction):
    """Dynamic condition evaluation."""
    action_type = "automation_dynamic_condition"
    display_name = "自动化动态条件"
    description = "动态条件评估"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition")
            context_data = params.get("context_data", {})
            expression = params.get("expression")

            if not condition and not expression:
                return ActionResult(success=False, message="condition or expression required")

            result = False
            reason = ""

            if callable(condition):
                result = condition(context_data)
                reason = "function"
            elif isinstance(condition, dict):
                result = self._evaluate_dict_condition(condition, context_data)
                reason = "dict"
            elif isinstance(condition, str) and expression:
                result = self._evaluate_expression(condition, expression, context_data)
                reason = "expression"
            else:
                result = bool(condition)

            return ActionResult(
                success=True,
                message=f"Condition evaluated: {result} ({reason})",
                data={"result": result, "reason": reason, "condition": condition}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dynamic condition error: {e}")

    def _evaluate_dict_condition(self, condition: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Evaluate dict-style conditions."""
        operator = condition.get("operator", "eq")
        key = condition.get("key")
        value = condition.get("value")

        actual = context.get(key) if key else context

        if operator == "eq":
            return actual == value
        elif operator == "ne":
            return actual != value
        elif operator == "gt":
            return actual > value
        elif operator == "ge":
            return actual >= value
        elif operator == "lt":
            return actual < value
        elif operator == "le":
            return actual <= value
        elif operator == "in":
            return actual in value
        elif operator == "not_in":
            return actual not in value
        elif operator == "contains":
            return value in actual
        elif operator == "exists":
            return key in context
        return False

    def _evaluate_expression(self, condition: str, expression: str, context: Dict[str, Any]) -> bool:
        """Evaluate string expression."""
        try:
            safe_dict = {
                "context": context,
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "list": list,
                "dict": dict,
            }
            return bool(eval(expression, {"__builtins__": {}}, safe_dict))
        except Exception:
            return False
