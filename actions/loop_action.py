"""Loop control action module for RabAI AutoClick.

Provides loop control operations:
- LoopRepeatAction: Repeat execution N times
- LoopWhileAction: While loop
- LoopForAction: For each loop
- LoopBreakAction: Break/continue loop control
"""

from typing import Any, Callable, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LoopRepeatAction(BaseAction):
    """Repeat execution N times."""
    action_type = "loop_repeat"
    display_name = "重复循环"
    description = "重复执行N次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            count = params.get("count", 1)
            body_ref = params.get("body_ref", None)
            collect_results = params.get("collect_results", False)
            break_on_error = params.get("break_on_error", False)

            if count <= 0:
                return ActionResult(success=True, message="Count <= 0, no iterations", data={"iterations": 0})

            results = []
            errors = []
            for i in range(count):
                try:
                    if body_ref:
                        result = body_ref(i, context)
                        if collect_results:
                            results.append(result)
                except Exception as e:
                    errors.append({"iteration": i, "error": str(e)})
                    if break_on_error:
                        break

            return ActionResult(
                success=len(errors) == 0,
                message=f"Completed {len(results)} iterations, {len(errors)} errors",
                data={
                    "iterations": len(results),
                    "errors": len(errors),
                    "results": results if collect_results else None
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Loop repeat failed: {str(e)}")


class LoopWhileAction(BaseAction):
    """While loop."""
    action_type = "loop_while"
    display_name = "条件循环"
    description = "条件为真时循环"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition_ref = params.get("condition_ref", None)
            condition_expr = params.get("condition_expr", {})
            max_iterations = params.get("max_iterations", 1000)
            body_ref = params.get("body_ref", None)
            collect_results = params.get("collect_results", False)

            if not condition_ref and not condition_expr:
                return ActionResult(success=False, message="condition_ref or condition_expr is required")

            results = []
            iteration = 0
            while iteration < max_iterations:
                try:
                    if condition_ref:
                        cond_result = condition_ref(context, iteration)
                    else:
                        cond_result = self._evaluate_condition(condition_expr, context)

                    if not cond_result:
                        break
                except Exception:
                    break

                try:
                    if body_ref:
                        result = body_ref(context, iteration)
                        if collect_results:
                            results.append(result)
                except Exception:
                    pass

                iteration += 1

            return ActionResult(
                success=True,
                message=f"While loop completed: {iteration} iterations",
                data={"iterations": iteration, "results": results if collect_results else None}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Loop while failed: {str(e)}")

    def _evaluate_condition(self, condition: Dict[str, Any], context: Any) -> bool:
        field = condition.get("field", "")
        operator = condition.get("operator", "==")
        value = condition.get("value", None)

        if isinstance(context, dict):
            current = context.get(field)
        else:
            current = getattr(context, field, None)

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
        return False


class LoopForAction(BaseAction):
    """For each loop."""
    action_type = "loop_for"
    display_name = "遍历循环"
    description = "遍历集合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            body_ref = params.get("body_ref", None)
            index_var = params.get("index_var", "index")
            item_var = params.get("item_var", "item")
            collect_results = params.get("collect_results", False)
            break_on_error = params.get("break_on_error", False)

            if not items:
                return ActionResult(success=True, message="No items to iterate", data={"iterations": 0})

            results = []
            errors = []
            for i, item in enumerate(items):
                try:
                    if body_ref:
                        result = body_ref(item, i, context)
                        if collect_results:
                            results.append(result)
                except Exception as e:
                    errors.append({"iteration": i, "item": str(item)[:50], "error": str(e)})
                    if break_on_error:
                        break

            return ActionResult(
                success=len(errors) == 0,
                message=f"For loop completed: {len(results)} iterations, {len(errors)} errors",
                data={
                    "iterations": len(results),
                    "errors": len(errors),
                    "results": results if collect_results else None
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Loop for failed: {str(e)}")


class LoopBreakAction(BaseAction):
    """Loop break/continue control."""
    action_type = "loop_break"
    display_name = "循环控制"
    description = "循环中断控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "break")
            condition = params.get("condition", None)
            value = params.get("value", None)

            if action == "break":
                return ActionResult(
                    success=True,
                    message="Break signal",
                    data={"action": "break", "should_break": True}
                )
            elif action == "continue":
                return ActionResult(
                    success=True,
                    message="Continue signal",
                    data={"action": "continue", "should_continue": True}
                )
            elif action == "return":
                return ActionResult(
                    success=True,
                    message="Return signal",
                    data={"action": "return", "return_value": value}
                )
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Loop break failed: {str(e)}")
