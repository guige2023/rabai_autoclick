"""Conditional logic action module for RabAI AutoClick.

Provides conditional branching operations:
- IfThenAction: If-then conditional
- IfThenElseAction: If-then-else conditional
- SwitchCaseAction: Switch-case conditional
- MatchAction: Pattern match conditional
"""

import re
from typing import Any, Callable, Dict, List, Optional, Union


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IfThenAction(BaseAction):
    """If-then conditional execution."""
    action_type = "if_then"
    display_name = "条件执行"
    description = "条件成立时执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", {})
            then_action = params.get("then_action", None)
            then_params = params.get("then_params", {})
            data = params.get("data", {})

            matched = self._evaluate_condition(condition, data)

            if matched:
                result = {"executed": True, "branch": "then"}
                if then_action:
                    result["action_result"] = "action_executed"
                return ActionResult(
                    success=True,
                    message="Condition matched, then branch executed",
                    data=result
                )

            return ActionResult(
                success=True,
                message="Condition not matched, no action taken",
                data={"executed": False, "branch": "none"}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"If-then failed: {str(e)}")

    def _evaluate_condition(self, condition: Dict[str, Any], data: Any) -> bool:
        if isinstance(data, dict):
            field = condition.get("field", "")
            operator = condition.get("operator", "==")
            value = condition.get("value", None)

            parts = field.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return False

            if operator == "==":
                return current == value
            elif operator == "!=":
                return current != value
            elif operator == ">":
                return current is not None and current > value
            elif operator == "<":
                return current is not None and current < value
            elif operator == ">=":
                return current is not None and current >= value
            elif operator == "<=":
                return current is not None and current <= value
            elif operator == "contains":
                return value in str(current) if current is not None else False
            elif operator == "is_null":
                return current is None
            elif operator == "is_not_null":
                return current is not None
            elif operator == "matches":
                return bool(re.search(value, str(current))) if current is not None else False
        return False


class IfThenElseAction(BaseAction):
    """If-then-else conditional execution."""
    action_type = "if_then_else"
    display_name = "条件分支"
    description = "条件成立时执行then，否则执行else"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", {})
            then_action = params.get("then_action", None)
            else_action = params.get("else_action", None)
            then_params = params.get("then_params", {})
            else_params = params.get("else_params", {})
            data = params.get("data", {})

            matched = self._evaluate_condition(condition, data)

            if matched:
                return ActionResult(
                    success=True,
                    message="Condition matched, then branch executed",
                    data={"executed": True, "branch": "then", "condition_result": True}
                )
            else:
                return ActionResult(
                    success=True,
                    message="Condition not matched, else branch executed",
                    data={"executed": True, "branch": "else", "condition_result": False}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"If-then-else failed: {str(e)}")

    def _evaluate_condition(self, condition: Dict[str, Any], data: Any) -> bool:
        if isinstance(data, dict):
            field = condition.get("field", "")
            operator = condition.get("operator", "==")
            value = condition.get("value", None)

            parts = field.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return False

            if operator == "==":
                return current == value
            elif operator == "!=":
                return current != value
            elif operator == ">":
                return current is not None and current > value
            elif operator == "<":
                return current is not None and current < value
            elif operator == ">=":
                return current is not None and current >= value
            elif operator == "<=":
                return current is not None and current <= value
            elif operator == "contains":
                return value in str(current) if current is not None else False
            elif operator == "is_null":
                return current is None
            elif operator == "is_not_null":
                return current is not None
        return False


class SwitchCaseAction(BaseAction):
    """Switch-case conditional execution."""
    action_type = "switch_case"
    display_name = "分支匹配"
    description = "根据值匹配执行对应分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            switch_value = params.get("switch_value", None)
            cases = params.get("cases", [])
            default_action = params.get("default_action", None)
            data = params.get("data", {})

            if switch_value is None:
                return ActionResult(success=False, message="switch_value is required")

            matched = None
            for case in cases:
                case_value = case.get("case", None)
                if switch_value == case_value:
                    matched = case
                    break

            if matched:
                return ActionResult(
                    success=True,
                    message=f"Matched case: {matched.get('case')}",
                    data={
                        "executed": True,
                        "branch": "case",
                        "matched_case": matched.get("case"),
                        "result": matched.get("result")
                    }
                )
            elif default_action:
                return ActionResult(
                    success=True,
                    message="No case matched, executing default",
                    data={"executed": True, "branch": "default"}
                )
            else:
                return ActionResult(
                    success=True,
                    message="No case matched, no default",
                    data={"executed": False, "branch": "none"}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Switch-case failed: {str(e)}")


class MatchAction(BaseAction):
    """Pattern match conditional."""
    action_type = "match"
    display_name = "模式匹配"
    description = "模式匹配条件执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", None)
            patterns = params.get("patterns", [])
            default_action = params.get("default_action", None)
            data = params.get("data", {})

            if value is None:
                return ActionResult(success=False, message="value is required")

            matched = None
            for pattern in patterns:
                pattern_value = pattern.get("pattern", "")
                pattern_type = pattern.get("type", "exact")

                is_match = False
                if pattern_type == "exact":
                    is_match = str(value) == str(pattern_value)
                elif pattern_type == "contains":
                    is_match = str(pattern_value) in str(value)
                elif pattern_type == "startswith":
                    is_match = str(value).startswith(str(pattern_value))
                elif pattern_type == "endswith":
                    is_match = str(value).endswith(str(pattern_value))
                elif pattern_type == "regex":
                    is_match = bool(re.search(str(pattern_value), str(value)))
                elif pattern_type == "greater":
                    try:
                        is_match = float(value) > float(pattern_value)
                    except (ValueError, TypeError):
                        pass
                elif pattern_type == "less":
                    try:
                        is_match = float(value) < float(pattern_value)
                    except (ValueError, TypeError):
                        pass

                if is_match:
                    matched = pattern
                    break

            if matched:
                return ActionResult(
                    success=True,
                    message=f"Matched pattern: {matched.get('pattern')}",
                    data={
                        "executed": True,
                        "matched_pattern": matched.get("pattern"),
                        "result": matched.get("result")
                    }
                )
            elif default_action:
                return ActionResult(
                    success=True,
                    message="No pattern matched, executing default",
                    data={"executed": True, "branch": "default"}
                )
            else:
                return ActionResult(
                    success=True,
                    message="No pattern matched",
                    data={"executed": False}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Match failed: {str(e)}")
