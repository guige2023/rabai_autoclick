"""Data calculator action module for RabAI AutoClick.

Provides data calculation operations:
- CalculateFieldAction: Calculate new field
- CalculateExpressionAction: Evaluate expression
- CalculateAggregateAction: Aggregate calculations
- CalculateRollingAction: Rolling calculations
- CalculateCumulativeAction: Cumulative calculations
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CalculateFieldAction(BaseAction):
    """Calculate a new field."""
    action_type = "calculate_field"
    display_name = "计算字段"
    description = "计算新字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            output_field = params.get("output_field", "result")
            operation = params.get("operation", "add")
            fields = params.get("fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            results = []
            for item in data:
                new_item = item.copy()
                if operation == "add" and len(fields) == 2:
                    new_item[output_field] = item.get(fields[0], 0) + item.get(fields[1], 0)
                elif operation == "subtract" and len(fields) == 2:
                    new_item[output_field] = item.get(fields[0], 0) - item.get(fields[1], 0)
                elif operation == "multiply" and len(fields) == 2:
                    new_item[output_field] = item.get(fields[0], 0) * item.get(fields[1], 0)
                elif operation == "divide" and len(fields) == 2:
                    divisor = item.get(fields[1], 0)
                    new_item[output_field] = item.get(fields[0], 0) / divisor if divisor != 0 else None
                elif operation == "square":
                    new_item[output_field] = item.get(fields[0], 0) ** 2
                elif operation == "sqrt":
                    import math
                    val = item.get(fields[0], 0)
                    new_item[output_field] = math.sqrt(val) if val >= 0 else None
                elif operation == "abs":
                    new_item[output_field] = abs(item.get(fields[0], 0))
                else:
                    new_item[output_field] = None
                results.append(new_item)

            return ActionResult(
                success=True,
                data={"results": results, "output_field": output_field, "operation": operation, "count": len(results)},
                message=f"Calculated {output_field} using {operation} on {len(results)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculate field failed: {e}")


class CalculateExpressionAction(BaseAction):
    """Evaluate expression."""
    action_type = "calculate_expression"
    display_name = "计算表达式"
    description = "计算表达式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            expression = params.get("expression", "")
            output_field = params.get("output_field", "expression_result")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not expression:
                return ActionResult(success=False, message="expression is required")

            results = []
            for item in data:
                try:
                    local_vars = {k: v for k, v in item.items() if isinstance(v, (int, float))}
                    result = eval(expression, {"__builtins__": {}}, local_vars)
                    new_item = item.copy()
                    new_item[output_field] = result
                    results.append(new_item)
                except Exception:
                    new_item = item.copy()
                    new_item[output_field] = None
                    results.append(new_item)

            return ActionResult(
                success=True,
                data={"results": results, "output_field": output_field, "expression": expression, "count": len(results)},
                message=f"Evaluated expression on {len(results)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculate expression failed: {e}")


class CalculateAggregateAction(BaseAction):
    """Aggregate calculations."""
    action_type = "calculate_aggregate"
    display_name = "聚合计算"
    description = "聚合计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            functions = params.get("functions", ["sum", "avg", "min", "max", "count"])

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            result = {}
            if "sum" in functions:
                result["sum"] = sum(values)
            if "avg" in functions:
                result["avg"] = sum(values) / len(values) if values else 0
            if "min" in functions:
                result["min"] = min(values) if values else None
            if "max" in functions:
                result["max"] = max(values) if values else None
            if "count" in functions:
                result["count"] = len(values)

            return ActionResult(
                success=True,
                data={"aggregates": result, "field": field, "functions": functions},
                message=f"Aggregate {field}: " + ", ".join(f"{k}={v}" for k, v in result.items()),
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculate aggregate failed: {e}")


class CalculateRollingAction(BaseAction):
    """Rolling calculations."""
    action_type = "calculate_rolling"
    display_name = "滚动计算"
    description = "滚动窗口计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            window = params.get("window", 3)
            function = params.get("function", "avg")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            results = []
            for i in range(len(values)):
                window_vals = values[max(0, i - window + 1) : i + 1]
                if function == "avg":
                    val = sum(window_vals) / len(window_vals)
                elif function == "sum":
                    val = sum(window_vals)
                elif function == "min":
                    val = min(window_vals)
                elif function == "max":
                    val = max(window_vals)
                else:
                    val = window_vals[-1]
                new_item = data[i].copy()
                new_item[f"{field}_rolling_{function}"] = val
                results.append(new_item)

            return ActionResult(
                success=True,
                data={"results": results, "window": window, "function": function, "count": len(results)},
                message=f"Rolling {function} with window={window} on {len(results)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculate rolling failed: {e}")


class CalculateCumulativeAction(BaseAction):
    """Cumulative calculations."""
    action_type = "calculate_cumulative"
    display_name = "累计计算"
    description = "累计计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            function = params.get("function", "sum")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            cumulative = []
            running = 0
            for v in values:
                if function == "sum":
                    running += v
                elif function == "min":
                    running = min(running, v) if cumulative else v
                elif function == "max":
                    running = max(running, v) if cumulative else v
                cumulative.append(running)

            results = []
            for i, d in enumerate(data):
                new_item = d.copy()
                new_item[f"{field}_cumulative_{function}"] = cumulative[i]
                results.append(new_item)

            return ActionResult(
                success=True,
                data={"results": results, "function": function, "count": len(results)},
                message=f"Cumulative {function} on {len(results)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculate cumulative failed: {e}")
