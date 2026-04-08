"""Automation condition action module for RabAI AutoClick.

Provides conditional automation logic:
- ConditionEvaluatorAction: Evaluate conditions for automation
- BranchConditionAction: Execute branches based on conditions
- SwitchConditionAction: Multi-way branching
- GuardConditionAction: Guard conditions for action execution
- ConditionalLoopAction: Loop based on condition
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConditionEvaluatorAction(BaseAction):
    """Evaluate conditions for automation."""
    action_type = "automation_condition_evaluator"
    display_name = "条件评估器"
    description = "评估自动化条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            logical_op = params.get("logical_op", "and")
            case_sensitive = params.get("case_sensitive", False)

            if not conditions:
                return ActionResult(success=False, message="No conditions to evaluate")

            results = []
            for condition in conditions:
                result = self._evaluate_single(condition, data, case_sensitive)
                results.append(result)

            if logical_op == "and":
                overall = all(results)
            elif logical_op == "or":
                overall = any(results)
            elif logical_op == "nor":
                overall = not any(results)
            elif logical_op == "nand":
                overall = not all(results)
            else:
                overall = all(results)

            return ActionResult(
                success=True,
                data={
                    "condition_met": overall,
                    "individual_results": results,
                    "logical_op": logical_op,
                    "conditions_checked": len(conditions)
                },
                message=f"Conditions evaluated: {'met' if overall else 'not met'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Condition evaluator error: {str(e)}")

    def _evaluate_single(self, condition: Dict, data: Dict, case_sensitive: bool) -> bool:
        field = condition.get("field", "")
        operator = condition.get("operator", "eq")
        value = condition.get("value")
        value2 = condition.get("value2")

        data_value = data.get(field) if field else None

        if operator == "eq":
            if not case_sensitive and isinstance(data_value, str) and isinstance(value, str):
                return data_value.lower() == value.lower()
            return data_value == value
        elif operator == "ne":
            if not case_sensitive and isinstance(data_value, str) and isinstance(value, str):
                return data_value.lower() != value.lower()
            return data_value != value
        elif operator == "gt":
            return data_value is not None and data_value > value
        elif operator == "gte":
            return data_value is not None and data_value >= value
        elif operator == "lt":
            return data_value is not None and data_value < value
        elif operator == "lte":
            return data_value is not None and data_value <= value
        elif operator == "between":
            return data_value is not None and value <= data_value <= (value2 or value)
        elif operator == "contains":
            return value in str(data_value) if data_value else False
        elif operator == "startswith":
            return str(data_value).startswith(str(value)) if data_value else False
        elif operator == "endswith":
            return str(data_value).endswith(str(value)) if data_value else False
        elif operator == "matches":
            import re
            return bool(re.search(value, str(data_value))) if data_value else False
        elif operator == "in":
            return data_value in (value if isinstance(value, list) else [value])
        elif operator == "exists":
            return field in data
        elif operator == "truthy":
            return bool(data_value)
        elif operator == "falsy":
            return not data_value
        return False

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "logical_op": "and", "case_sensitive": False}


class BranchConditionAction(BaseAction):
    """Execute branches based on conditions."""
    action_type = "automation_branch_condition"
    display_name = "条件分支"
    description = "根据条件执行分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            data = params.get("data", {})
            default_action = params.get("default_action")
            match_type = params.get("match_type", "first")

            if not branches:
                return ActionResult(success=False, message="No branches defined")

            executed = None
            result_data = None

            for branch in branches:
                name = branch.get("name", "unnamed")
                condition = branch.get("condition", {})
                action = branch.get("action", {})
                priority = branch.get("priority", 0)

                eval_action = ConditionEvaluatorAction()
                eval_result = eval_action.execute(context, {
                    "conditions": [condition] if condition else [],
                    "data": data
                })

                if eval_result.data.get("condition_met", False):
                    success = action.get("success", True)
                    executed = name
                    result_data = {
                        "branch": name,
                        "priority": priority,
                        "action_result": success,
                        "condition": condition
                    }

                    if match_type == "first":
                        break

            if executed is None and default_action:
                executed = "default"
                result_data = {
                    "branch": "default",
                    "action_result": default_action.get("success", True)
                }

            return ActionResult(
                success=executed is not None,
                data={
                    "executed_branch": executed,
                    "result": result_data,
                    "branches_evaluated": len(branches),
                    "match_type": match_type
                },
                message=f"Branch executed: '{executed}'" if executed else "No branch matched"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Branch condition error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["branches"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "default_action": None, "match_type": "first"}


class SwitchConditionAction(BaseAction):
    """Multi-way branching."""
    action_type = "automation_switch_condition"
    display_name = "多路分支"
    description = "多路分支执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            switch_value = params.get("switch_value")
            cases = params.get("cases", [])
            default_case = params.get("default_case")
            case_field = params.get("case_field")

            data = params.get("data", {})
            if case_field:
                switch_value = data.get(case_field, switch_value)

            matched_case = None
            result_data = None

            for case in cases:
                case_value = case.get("value")
                action = case.get("action", {})
                name = case.get("name", str(case_value))

                if switch_value == case_value:
                    matched_case = name
                    result_data = {
                        "case": name,
                        "value": case_value,
                        "action_result": action.get("success", True)
                    }
                    break

            if matched_case is None and default_case:
                matched_case = "default"
                result_data = {
                    "case": "default",
                    "value": None,
                    "action_result": default_case.get("success", True)
                }

            return ActionResult(
                success=matched_case is not None,
                data={
                    "matched_case": matched_case,
                    "result": result_data,
                    "cases_checked": len(cases),
                    "switch_value": switch_value
                },
                message=f"Switch matched: '{matched_case}'" if matched_case else "No case matched"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Switch condition error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["cases"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"switch_value": None, "case_field": None, "default_case": None, "data": {}}


class GuardConditionAction(BaseAction):
    """Guard conditions for action execution."""
    action_type = "automation_guard_condition"
    display_name = "保护条件"
    description = "动作执行前的保护条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            action = params.get("action", {})
            guard_mode = params.get("guard_mode", "require_all")
            skip_on_fail = params.get("skip_on_fail", True)

            eval_action = ConditionEvaluatorAction()
            eval_result = eval_action.execute(context, {
                "conditions": conditions,
                "data": data,
                "logical_op": "and" if guard_mode == "require_all" else "or"
            })

            guard_passed = eval_result.data.get("condition_met", False)

            if guard_passed:
                return ActionResult(
                    success=True,
                    data={
                        "guard_passed": True,
                        "action_executed": True,
                        "action_result": action.get("success", True),
                        "conditions_checked": len(conditions)
                    },
                    message="Guard passed, action executed"
                )
            else:
                if skip_on_fail:
                    return ActionResult(
                        success=True,
                        data={
                            "guard_passed": False,
                            "action_executed": False,
                            "skipped": True,
                            "conditions_checked": len(conditions)
                        },
                        message="Guard failed, action skipped"
                    )
                else:
                    return ActionResult(
                        success=False,
                        data={
                            "guard_passed": False,
                            "action_executed": False,
                            "blocked": True,
                            "conditions_checked": len(conditions)
                        },
                        message="Guard failed, action blocked"
                    )
        except Exception as e:
            return ActionResult(success=False, message=f"Guard condition error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "action": {}, "guard_mode": "require_all", "skip_on_fail": True}


class ConditionalLoopAction(BaseAction):
    """Loop based on condition."""
    action_type = "automation_conditional_loop"
    display_name = "条件循环"
    description = "基于条件的循环执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            max_iterations = params.get("max_iterations", 100)
            loop_action = params.get("loop_action", {})
            update_data = params.get("update_data", False)

            iteration = 0
            iterations = []
            current_data = dict(data)

            while iteration < max_iterations:
                eval_action = ConditionEvaluatorAction()
                eval_result = eval_action.execute(context, {
                    "conditions": conditions,
                    "data": current_data
                })

                if not eval_result.data.get("condition_met", False):
                    break

                iterations.append({
                    "iteration": iteration,
                    "data_snapshot": dict(current_data),
                    "action_result": loop_action.get("success", True)
                })

                if update_data:
                    iteration += 1

            return ActionResult(
                success=True,
                data={
                    "iterations": iterations,
                    "total_iterations": len(iterations),
                    "max_iterations": max_iterations,
                    "terminated_by": "condition" if len(iterations) < max_iterations else "max_iterations"
                },
                message=f"Loop completed: {len(iterations)} iterations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Conditional loop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "max_iterations": 100, "loop_action": {}, "update_data": False}
