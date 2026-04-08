"""Data calculator action module for RabAI AutoClick.

Provides data calculation operations:
- DataCalculatorAction: Perform data calculations
- AggregateCalculatorAction: Calculate aggregations
- StatisticalCalculatorAction: Statistical calculations
- FormulaEvaluatorAction: Evaluate formulas
- MetricCalculatorAction: Calculate metrics
"""

import math
import statistics
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataCalculatorAction(BaseAction):
    """Perform data calculations."""
    action_type = "data_calculator"
    display_name = "数据计算"
    description = "执行数据计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            values = params.get("values", [])
            operand = params.get("operand", None)

            if not values and operand is None:
                return ActionResult(success=False, message="values or operand is required")

            supported_ops = ["add", "subtract", "multiply", "divide", "power", "mod", "avg", "min", "max", "sum"]

            if operation not in supported_ops:
                return ActionResult(success=False, message=f"Unsupported operation: {operation}")

            if operation == "sum" or operation == "add":
                result = sum(values) if values else 0
            elif operation == "subtract":
                result = values[0] - sum(values[1:]) if values else 0
            elif operation == "multiply":
                result = math.prod(values) if values else 0
            elif operation == "divide":
                if operand == 0:
                    return ActionResult(success=False, message="Division by zero")
                result = values[0] / operand if values else 0
            elif operation == "power":
                result = math.pow(values[0], values[1]) if len(values) >= 2 else 0
            elif operation == "mod":
                result = values[0] % operand if values else 0
            elif operation == "avg":
                result = sum(values) / len(values) if values else 0
            elif operation == "min":
                result = min(values) if values else None
            elif operation == "max":
                result = max(values) if values else None
            else:
                result = 0

            return ActionResult(
                success=True,
                data={
                    "operation": operation,
                    "result": result,
                    "input_count": len(values),
                    "operand": operand
                },
                message=f"Calculation result: {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data calculator error: {str(e)}")


class AggregateCalculatorAction(BaseAction):
    """Calculate aggregations on data."""
    action_type = "aggregate_calculator"
    display_name = "聚合计算"
    description = "计算数据聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            aggregation_type = params.get("aggregation_type", "sum")
            group_by = params.get("group_by", None)
            having = params.get("having", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            numeric_data = [x for x in data if isinstance(x, (int, float))]

            if aggregation_type == "sum":
                result = sum(numeric_data) if numeric_data else 0
            elif aggregation_type == "count":
                result = len(data)
            elif aggregation_type == "avg":
                result = statistics.mean(numeric_data) if numeric_data else 0
            elif aggregation_type == "min":
                result = min(numeric_data) if numeric_data else None
            elif aggregation_type == "max":
                result = max(numeric_data) if numeric_data else None
            elif aggregation_type == "median":
                result = statistics.median(numeric_data) if numeric_data else None
            elif aggregation_type == "stddev":
                result = statistics.stdev(numeric_data) if len(numeric_data) > 1 else 0
            else:
                return ActionResult(success=False, message=f"Unknown aggregation: {aggregation_type}")

            return ActionResult(
                success=True,
                data={
                    "aggregation_type": aggregation_type,
                    "result": result,
                    "record_count": len(data),
                    "numeric_count": len(numeric_data),
                    "group_by": group_by,
                    "having": having
                },
                message=f"Aggregation '{aggregation_type}' = {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate calculator error: {str(e)}")


class StatisticalCalculatorAction(BaseAction):
    """Statistical calculations."""
    action_type = "statistical_calculator"
    display_name = "统计计算"
    description = "执行统计计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            stats = params.get("stats", ["mean", "median", "stdev", "variance"])

            if not data:
                return ActionResult(success=False, message="data is required")

            numeric_data = [x for x in data if isinstance(x, (int, float))]
            if not numeric_data:
                return ActionResult(success=False, message="No numeric data found")

            result = {}

            if "mean" in stats:
                result["mean"] = statistics.mean(numeric_data)
            if "median" in stats:
                result["median"] = statistics.median(numeric_data)
            if "stdev" in stats:
                result["stdev"] = statistics.stdev(numeric_data) if len(numeric_data) > 1 else 0
            if "variance" in stats:
                result["variance"] = statistics.variance(numeric_data) if len(numeric_data) > 1 else 0
            if "min" in stats:
                result["min"] = min(numeric_data)
            if "max" in stats:
                result["max"] = max(numeric_data)
            if "range" in stats:
                result["range"] = max(numeric_data) - min(numeric_data)
            if "sum" in stats:
                result["sum"] = sum(numeric_data)
            if "count" in stats:
                result["count"] = len(numeric_data)
            if "q1" in stats:
                sorted_data = sorted(numeric_data)
                q1_idx = len(sorted_data) // 4
                result["q1"] = sorted_data[q1_idx]
            if "q3" in stats:
                sorted_data = sorted(numeric_data)
                q3_idx = 3 * len(sorted_data) // 4
                result["q3"] = sorted_data[q3_idx]
            if "iqr" in stats:
                sorted_data = sorted(numeric_data)
                q1_idx = len(sorted_data) // 4
                q3_idx = 3 * len(sorted_data) // 4
                result["iqr"] = sorted_data[q3_idx] - sorted_data[q1_idx]

            return ActionResult(
                success=True,
                data={
                    "stats": result,
                    "data_points": len(numeric_data),
                    "stats_calculated": list(result.keys())
                },
                message=f"Statistics calculated: {', '.join(result.keys())}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Statistical calculator error: {str(e)}")


class FormulaEvaluatorAction(BaseAction):
    """Evaluate mathematical formulas."""
    action_type = "formula_evaluator"
    display_name = "公式计算"
    description = "计算数学公式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            formula = params.get("formula", "")
            variables = params.get("variables", {})

            if not formula:
                return ActionResult(success=False, message="formula is required")

            try:
                local_vars = {**variables}
                result = eval(formula, {"__builtins__": {}, "math": math}, local_vars)
            except Exception as e:
                return ActionResult(success=False, message=f"Formula evaluation error: {str(e)}")

            return ActionResult(
                success=True,
                data={
                    "formula": formula,
                    "result": result,
                    "variables": variables,
                    "evaluated_at": datetime.now().isoformat()
                },
                message=f"Formula evaluated: {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Formula evaluator error: {str(e)}")


class MetricCalculatorAction(BaseAction):
    """Calculate metrics."""
    action_type = "metric_calculator"
    display_name = "指标计算"
    description = "计算各种指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric_type = params.get("metric_type", "ratio")
            value = params.get("value", 0)
            baseline = params.get("baseline", 1)
            unit = params.get("unit", "")

            if metric_type == "ratio":
                result = value / baseline if baseline != 0 else 0
                display = f"{result:.2f}"
            elif metric_type == "percentage":
                result = (value / baseline * 100) if baseline != 0 else 0
                display = f"{result:.1f}%"
            elif metric_type == "change":
                result = value - baseline
                display = f"+{result:.2f}" if result > 0 else f"{result:.2f}"
            elif metric_type == "percent_change":
                result = ((value - baseline) / baseline * 100) if baseline != 0 else 0
                display = f"{result:+.1f}%"
            elif metric_type == "rate":
                result = value / baseline if baseline != 0 else 0
                display = f"{result:.2f}/{unit}"
            else:
                result = value
                display = str(value)

            return ActionResult(
                success=True,
                data={
                    "metric_type": metric_type,
                    "value": value,
                    "baseline": baseline,
                    "result": result,
                    "display": display,
                    "unit": unit
                },
                message=f"Metric '{metric_type}': {display}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metric calculator error: {str(e)}")


class PercentageCalculatorAction(BaseAction):
    """Calculate percentages."""
    action_type = "percentage_calculator"
    display_name = "百分比计算"
    description = "计算百分比"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "calculate")
            part = params.get("part", 0)
            whole = params.get("whole", 1)
            percentage = params.get("percentage", 0)

            if operation == "calculate":
                if whole == 0:
                    return ActionResult(success=False, message="whole cannot be zero")
                result = (part / whole) * 100
                return ActionResult(
                    success=True,
                    data={"part": part, "whole": whole, "percentage": result},
                    message=f"{part}/{whole} = {result:.2f}%"
                )
            elif operation == "of":
                result = (percentage / 100) * whole
                return ActionResult(
                    success=True,
                    data={"percentage": percentage, "whole": whole, "part": result},
                    message=f"{percentage}% of {whole} = {result}"
                )
            elif operation == "from":
                result = (part / (percentage / 100)) if percentage != 0 else 0
                return ActionResult(
                    success=True,
                    data={"part": part, "percentage": percentage, "whole": result},
                    message=f"{part} is {percentage}% of {result:.2f}"
                )
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Percentage calculator error: {str(e)}")
