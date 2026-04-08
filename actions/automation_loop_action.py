"""Automation loop action module for RabAI AutoClick.

Provides loop-based automation:
- ForEachLoopAction: Iterate over collections
- WhileLoopAction: Loop while condition is true
- UntilLoopAction: Loop until condition is true
- DoWhileLoopAction: Execute then check condition
- LoopControlAction: Break and continue controls
"""

import time
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ForEachLoopAction(BaseAction):
    """Iterate over collections."""
    action_type = "automation_foreach_loop"
    display_name = "ForEach循环"
    description = "遍历集合进行迭代"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            collection = params.get("collection", [])
            item_var = params.get("item_var", "item")
            index_var = params.get("index_var", "index")
            action = params.get("action", {})
            max_iterations = params.get("max_iterations", 1000)
            start_index = params.get("start_index", 0)
            step = params.get("step", 1)

            if not isinstance(collection, (list, dict, str, tuple)):
                collection = [collection]

            if isinstance(collection, dict):
                items = list(collection.items())
            else:
                items = list(collection)

            if start_index > 0:
                items = items[start_index:]

            iterations = []
            break_early = False
            break_value = params.get("break_value")

            for i, item in enumerate(items[:max_iterations]):
                if step > 1 and i % step != 0:
                    continue

                item_data = item[1] if isinstance(item, tuple) else item
                key_data = item[0] if isinstance(item, tuple) else None

                iteration_vars = {item_var: item_data}
                if index_var:
                    iteration_vars[index_var] = i
                if key_data is not None:
                    iteration_vars["key"] = key_data

                success = action.get("success", True)

                if break_value is not None and item_data == break_value:
                    iterations.append({
                        "index": i,
                        "item": item_data,
                        "action_result": success,
                        "break_triggered": True
                    })
                    break_early = True
                    break

                iterations.append({
                    "index": i,
                    "item": item_data,
                    "action_result": success
                })

            return ActionResult(
                success=True,
                data={
                    "iterations": iterations,
                    "total_iterations": len(iterations),
                    "collection_size": len(items),
                    "break_early": break_early,
                    "item_var": item_var,
                    "index_var": index_var
                },
                message=f"ForEach loop: {len(iterations)} iterations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ForEach loop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["collection"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"item_var": "item", "index_var": "index", "action": {}, "max_iterations": 1000, "start_index": 0, "step": 1, "break_value": None}


class WhileLoopAction(BaseAction):
    """Loop while condition is true."""
    action_type = "automation_while_loop"
    display_name = "While循环"
    description = "条件为真时循环"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            max_iterations = params.get("max_iterations", 1000)
            loop_action = params.get("loop_action", {})
            update_func = params.get("update_func")

            iteration = 0
            iterations = []
            terminated_by = None

            while iteration < max_iterations:
                from core.base_action import BaseAction
                eval_action = BaseAction()

                cond_met = True
                if conditions:
                    for c in conditions:
                        if c.get("type") == "counter":
                            if iteration >= c.get("limit", max_iterations):
                                cond_met = False
                                terminated_by = "condition"
                                break
                        elif c.get("type") == "equals":
                            field_val = data.get(c.get("field", ""), 0)
                            if field_val != c.get("value"):
                                cond_met = False
                                terminated_by = "condition"
                                break

                if not cond_met:
                    break

                iterations.append({
                    "iteration": iteration,
                    "data_snapshot": dict(data),
                    "action_result": loop_action.get("success", True)
                })

                if update_func:
                    iteration += 1
                else:
                    iteration += 1

            if terminated_by is None:
                terminated_by = "max_iterations" if iteration >= max_iterations else "condition"

            return ActionResult(
                success=True,
                data={
                    "iterations": iterations,
                    "total_iterations": len(iterations),
                    "max_iterations": max_iterations,
                    "terminated_by": terminated_by
                },
                message=f"While loop: {len(iterations)} iterations, terminated by {terminated_by}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"While loop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "max_iterations": 1000, "loop_action": {}, "update_func": None}


class UntilLoopAction(BaseAction):
    """Loop until condition is true."""
    action_type = "automation_until_loop"
    display_name = "Until循环"
    description = "条件为真时停止循环"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            max_iterations = params.get("max_iterations", 1000)
            loop_action = params.get("loop_action", {})

            iteration = 0
            iterations = []
            condition_met = False

            while iteration < max_iterations:
                from core.base_action import BaseAction

                condition_met = False
                if conditions:
                    for c in conditions:
                        if c.get("type") == "counter":
                            if iteration >= c.get("limit", max_iterations):
                                condition_met = True
                                break
                        elif c.get("type") == "equals":
                            field_val = data.get(c.get("field", ""), 0)
                            if field_val == c.get("value"):
                                condition_met = True
                                break

                if condition_met:
                    break

                iterations.append({
                    "iteration": iteration,
                    "data_snapshot": dict(data),
                    "action_result": loop_action.get("success", True)
                })

                iteration += 1

            return ActionResult(
                success=True,
                data={
                    "iterations": iterations,
                    "total_iterations": len(iterations),
                    "max_iterations": max_iterations,
                    "condition_met": condition_met,
                    "terminated_by": "condition" if condition_met else "max_iterations"
                },
                message=f"Until loop: {len(iterations)} iterations, condition met: {condition_met}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Until loop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "max_iterations": 1000, "loop_action": {}}


class DoWhileLoopAction(BaseAction):
    """Execute then check condition."""
    action_type = "automation_dowhile_loop"
    display_name = "DoWhile循环"
    description = "先执行后检查条件的循环"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conditions = params.get("conditions", [])
            data = params.get("data", {})
            max_iterations = params.get("max_iterations", 1000)
            loop_action = params.get("loop_action", {})

            iteration = 0
            iterations = []

            while iteration < max_iterations:
                iterations.append({
                    "iteration": iteration,
                    "data_snapshot": dict(data),
                    "action_result": loop_action.get("success", True)
                })

                condition_met = False
                if conditions:
                    for c in conditions:
                        if c.get("type") == "counter":
                            if iteration >= c.get("limit", max_iterations):
                                condition_met = True
                                break

                iteration += 1

                if condition_met:
                    break

            return ActionResult(
                success=True,
                data={
                    "iterations": iterations,
                    "total_iterations": len(iterations),
                    "max_iterations": max_iterations,
                    "always_runs_at_least_once": True
                },
                message=f"DoWhile loop: {len(iterations)} iterations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DoWhile loop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"data": {}, "max_iterations": 1000, "loop_action": {}}


class LoopControlAction(BaseAction):
    """Break and continue controls."""
    action_type = "automation_loop_control"
    display_name = "循环控制"
    description = "Break和Continue控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "break")
            label = params.get("label")
            condition = params.get("condition")
            data = params.get("data", {})

            if condition:
                from core.base_action import BaseAction
                eval_action = BaseAction()
                should_execute = True
                if isinstance(condition, dict):
                    if condition.get("type") == "equals":
                        field_val = data.get(condition.get("field", ""), 0)
                        should_execute = (field_val == condition.get("value"))
                else:
                    should_execute = bool(condition)

                if not should_execute:
                    return ActionResult(
                        success=True,
                        data={"executed": False, "action": action, "reason": "condition_not_met"},
                        message=f"Loop control: {action} skipped (condition not met)"
                    )

            return ActionResult(
                success=True,
                data={
                    "executed": True,
                    "action": action,
                    "label": label,
                    "control_signal": action.upper()
                },
                message=f"Loop control: {action}" + (f" (label: {label})" if label else "")
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Loop control error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"label": None, "condition": None, "data": {}}
