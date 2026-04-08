"""Data evaluator action module for RabAI AutoClick.

Provides data evaluation operations:
- DataEvaluatorAction: Evaluate data expressions
- ConditionEvaluatorAction: Evaluate conditions
- ExpressionEvaluatorAction: Evaluate expressions
- ScoreEvaluatorAction: Calculate scores
- ThresholdEvaluatorAction: Evaluate against thresholds
"""

import re
import ast
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataEvaluatorAction(BaseAction):
    """Evaluate data expressions."""
    action_type = "data_evaluator"
    display_name = "数据评估"
    description = "评估数据表达式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            expression = params.get("expression", "")
            data = params.get("data", {})
            variables = params.get("variables", {})

            if not expression:
                return ActionResult(success=False, message="expression is required")

            try:
                result = self._evaluate_expression(expression, data, variables)
            except Exception as e:
                result = f"Error: {str(e)}"

            return ActionResult(
                success=not str(result).startswith("Error"),
                data={
                    "expression": expression,
                    "result": result,
                    "evaluated_at": datetime.now().isoformat()
                },
                message=f"Expression evaluated: {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data evaluator error: {str(e)}")

    def _evaluate_expression(self, expr: str, data: Dict, variables: Dict) -> Any:
        combined = {**data, **variables}

        if expr.startswith("="):
            expr = expr[1:]

        try:
            for key, value in combined.items():
                if isinstance(value, str):
                    expr = expr.replace(key, f'"{value}"')
                else:
                    expr = expr.replace(key, str(value))

            result = eval(expr, {"__builtins__": {}}, {})
            return result
        except:
            return f"Error evaluating expression"


class ConditionEvaluatorAction(BaseAction):
    """Evaluate conditions."""
    action_type = "condition_evaluator"
    display_name = "条件评估"
    description = "评估条件表达式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", {})
            data = params.get("data", {})
            operator = params.get("operator", "and")

            if not condition:
                return ActionResult(success=False, message="condition is required")

            if "field" in condition and "value" in condition:
                field = condition["field"]
                expected = condition["value"]
                actual = data.get(field)
                op = condition.get("op", "==")

                result = self._compare_values(actual, op, expected)
            elif "conditions" in condition:
                results = [self._evaluate_single(c, data) for c in condition["conditions"]]
                if operator == "and":
                    result = all(results)
                else:
                    result = any(results)
            else:
                result = True

            return ActionResult(
                success=True,
                data={
                    "condition": condition,
                    "result": result,
                    "data": data,
                    "evaluated_at": datetime.now().isoformat()
                },
                message=f"Condition evaluated: {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Condition evaluator error: {str(e)}")

    def _compare_values(self, actual: Any, op: str, expected: Any) -> bool:
        if op == "==":
            return actual == expected
        elif op == "!=":
            return actual != expected
        elif op == ">":
            return actual > expected
        elif op == "<":
            return actual < expected
        elif op == ">=":
            return actual >= expected
        elif op == "<=":
            return actual <= expected
        elif op == "in":
            return actual in expected
        elif op == "not in":
            return actual not in expected
        return False

    def _evaluate_single(self, condition: Dict, data: Dict) -> bool:
        if "field" in condition:
            return self._compare_values(data.get(condition["field"]), condition.get("op", "=="), condition["value"])
        return True


class ExpressionEvaluatorAction(BaseAction):
    """Evaluate expressions."""
    action_type = "expression_evaluator"
    display_name = "表达式评估"
    description = "评估表达式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            expr_type = params.get("expression_type", "arithmetic")
            expression = params.get("expression", "")
            operands = params.get("operands", [])

            if not expression and not operands:
                return ActionResult(success=False, message="expression or operands is required")

            if expr_type == "arithmetic":
                result = self._evaluate_arithmetic(expression, operands)
            elif expr_type == "logical":
                result = self._evaluate_logical(expression, operands)
            elif expr_type == "string":
                result = self._evaluate_string(expression, operands)
            elif expr_type == "ternary":
                result = self._evaluate_ternary(params.get("condition"), params.get("true_value"), params.get("false_value"))
            else:
                result = expression

            return ActionResult(
                success=True,
                data={
                    "expression_type": expr_type,
                    "expression": expression,
                    "result": result
                },
                message=f"Expression evaluated: {result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Expression evaluator error: {str(e)}")

    def _evaluate_arithmetic(self, expr: str, operands: List) -> float:
        if operands:
            return sum(operands) / len(operands) if operands else 0
        return 0

    def _evaluate_logical(self, expr: str, operands: List) -> bool:
        return all(operands) if operands else True

    def _evaluate_string(self, expr: str, operands: List) -> str:
        return expr.format(*operands) if operands else expr

    def _evaluate_ternary(self, condition: Any, true_val: Any, false_val: Any) -> Any:
        return true_val if condition else false_val


class ScoreEvaluatorAction(BaseAction):
    """Calculate scores."""
    action_type = "score_evaluator"
    display_name = "评分计算"
    description = "计算评分"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            scoring_rules = params.get("scoring_rules", [])
            score_type = params.get("score_type", "sum")

            if not scoring_rules:
                return ActionResult(success=False, message="scoring_rules is required")

            total_score = 0
            rule_results = []

            for rule in scoring_rules:
                rule_name = rule.get("name", "unnamed")
                field = rule.get("field", "")
                weight = rule.get("weight", 1.0)
                condition = rule.get("condition", {})

                score = 0
                if condition:
                    actual = data.get(field)
                    expected = condition.get("value")
                    op = condition.get("op", "==")
                    if self._compare(actual, op, expected):
                        score = rule.get("points", 1) * weight

                total_score += score
                rule_results.append({"rule": rule_name, "score": score})

            if score_type == "average":
                total_score = total_score / len(scoring_rules) if scoring_rules else 0
            elif score_type == "max":
                total_score = max(r["score"] for r in rule_results) if rule_results else 0

            return ActionResult(
                success=True,
                data={
                    "score_type": score_type,
                    "total_score": total_score,
                    "rule_results": rule_results,
                    "rules_count": len(scoring_rules)
                },
                message=f"Score calculated: {total_score:.2f} ({score_type})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Score evaluator error: {str(e)}")

    def _compare(self, actual: Any, op: str, expected: Any) -> bool:
        if op == "==":
            return actual == expected
        elif op == "!=":
            return actual != expected
        elif op == ">":
            return actual > expected
        elif op == "<":
            return actual < expected
        return False


class ThresholdEvaluatorAction(BaseAction):
    """Evaluate against thresholds."""
    action_type = "threshold_evaluator"
    display_name = "阈值评估"
    description = "评估阈值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", 0)
            thresholds = params.get("thresholds", [])
            evaluation_mode = params.get("mode", "first_match")

            if not thresholds:
                return ActionResult(success=False, message="thresholds is required")

            matched = None
            for threshold in thresholds:
                threshold_value = threshold.get("value", 0)
                operator = threshold.get("operator", "<=")
                label = threshold.get("label", "unknown")

                if self._check_threshold(value, operator, threshold_value):
                    matched = {
                        "label": label,
                        "value": threshold_value,
                        "operator": operator,
                        "matched": True
                    }
                    if evaluation_mode == "first_match":
                        break

            return ActionResult(
                success=True,
                data={
                    "value": value,
                    "matched": matched,
                    "thresholds_count": len(thresholds),
                    "evaluation_mode": evaluation_mode
                },
                message=f"Threshold evaluation: {matched['label'] if matched else 'NO_MATCH'} (value={value})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Threshold evaluator error: {str(e)}")

    def _check_threshold(self, value: float, op: str, threshold: float) -> bool:
        if op == "<":
            return value < threshold
        elif op == "<=":
            return value <= threshold
        elif op == ">":
            return value > threshold
        elif op == ">=":
            return value >= threshold
        elif op == "==":
            return value == threshold
        elif op == "!=":
            return value != threshold
        return False
