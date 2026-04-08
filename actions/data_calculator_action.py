"""Data calculator action module for RabAI AutoClick.

Provides row-level calculations, computed fields, arithmetic
operations, and formula evaluation for data records.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ComputeFieldAction(BaseAction):
    """Add computed field to records based on formula.
    
    Evaluates arithmetic expressions and functions to
    create new calculated fields.
    """
    action_type = "compute_field"
    display_name = "计算字段"
    description = "基于公式添加计算字段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute new field.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field_name, formula,
                   type (int|float|str|bool).
        
        Returns:
            ActionResult with computed data.
        """
        data = params.get('data', [])
        field_name = params.get('field_name', 'computed')
        formula = params.get('formula', '')
        result_type = params.get('type', 'float')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not formula:
            return ActionResult(
                success=False,
                message="formula is required"
            )

        results = []
        for row in data:
            new_row = dict(row)
            try:
                computed = self._evaluate_formula(formula, row)
                computed = self._cast(computed, result_type)
                new_row[field_name] = computed
            except Exception as e:
                new_row[field_name] = None
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Computed field '{field_name}' for {len(results)} rows",
            data={
                'data': results,
                'field_name': field_name,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _evaluate_formula(self, formula: str, row: Dict) -> Any:
        """Evaluate formula against row data."""
        expr = formula
        for key, value in sorted(row.items(), key=lambda x: -len(x[0])):
            placeholder = f'__{key}__'
            if isinstance(value, str):
                expr = expr.replace(placeholder, repr(value))
            else:
                expr = expr.replace(placeholder, str(value))

        math_funcs = {
            'abs': abs, 'min': min, 'max': max, 'sum': sum,
            'round': round, 'pow': pow, 'sqrt': lambda x: x ** 0.5,
            'log': __import__('math').log, 'exp': __import__('math').exp,
        }

        safe_names = {'__builtins__': {}, **math_funcs}
        return eval(expr, safe_names)

    def _cast(self, value: Any, result_type: str) -> Any:
        """Cast result to specified type."""
        if result_type == 'int':
            return int(value) if value is not None else None
        elif result_type == 'float':
            return float(value) if value is not None else None
        elif result_type == 'str':
            return str(value) if value is not None else None
        elif result_type == 'bool':
            return bool(value) if value is not None else None
        return value


class ArithmeticOpAction(BaseAction):
    """Perform arithmetic operations on numeric fields.
    
    Supports add, subtract, multiply, divide, modulo,
    and exponentiation between two field values.
    """
    action_type = "arithmetic_op"
    display_name = "算术运算"
    description = "对数值字段执行算术运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform arithmetic operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, operation (add|sub|mul|div|mod|pow),
                   left_field, right_field, result_field, default_value.
        
        Returns:
            ActionResult with computed data.
        """
        data = params.get('data', [])
        operation = params.get('operation', 'add')
        left_field = params.get('left_field', '')
        right_field = params.get('right_field', '')
        result_field = params.get('result_field', 'result')
        default_value = params.get('default_value', 0)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        ops = {
            'add': lambda a, b: a + b,
            'sub': lambda a, b: a - b,
            'mul': lambda a, b: a * b,
            'div': lambda a, b: a / b if b != 0 else default_value,
            'mod': lambda a, b: a % b if b != 0 else default_value,
            'pow': lambda a, b: a ** b,
            'floor_div': lambda a, b: a // b if b != 0 else default_value,
        }

        op_func = ops.get(operation, ops['add'])
        results = []

        for row in data:
            new_row = dict(row)
            try:
                left_val = self._get_field(row, left_field)
                right_val = self._get_field(row, right_field)
                left_num = float(left_val) if left_val is not None else 0
                right_num = float(right_val) if right_val is not None else 0
                new_row[result_field] = op_func(left_num, right_num)
            except (TypeError, ValueError, ZeroDivisionError):
                new_row[result_field] = default_value
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Arithmetic {operation} on {len(results)} rows",
            data={
                'data': results,
                'result_field': result_field,
                'operation': operation,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return None
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class PercentageCalcAction(BaseAction):
    """Calculate percentages and proportions.
    
    Computes percentage of total, percentage change,
    and percentage difference.
    """
    action_type = "percentage_calc"
    display_name = "百分比计算"
    description = "计算百分比和比例"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate percentages.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, calculation (of_total|change|diff),
                   numerator_field, denominator_field, result_field.
        
        Returns:
            ActionResult with calculated percentages.
        """
        data = params.get('data', [])
        calculation = params.get('calculation', 'of_total')
        numerator_field = params.get('numerator_field', '')
        denominator_field = params.get('denominator_field', '')
        result_field = params.get('result_field', 'percentage')
        default_value = params.get('default_value', 0)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        total = None

        if calculation == 'of_total':
            total = sum(
                float(self._get_field(r, numerator_field) or 0)
                for r in data
            )

        prev_value = None
        for row in data:
            new_row = dict(row)
            try:
                if calculation == 'of_total':
                    num = float(self._get_field(row, numerator_field) or 0)
                    new_row[result_field] = (num / total * 100) if total else default_value
                elif calculation == 'change':
                    num = float(self._get_field(row, numerator_field) or 0)
                    if prev_value and prev_value != 0:
                        new_row[result_field] = ((num - prev_value) / prev_value) * 100
                    else:
                        new_row[result_field] = default_value
                    prev_value = num
                elif calculation == 'diff':
                    num = float(self._get_field(row, numerator_field) or 0)
                    den = float(self._get_field(row, denominator_field) or 1)
                    new_row[result_field] = ((num - den) / den) * 100 if den else default_value
                else:
                    new_row[result_field] = default_value
            except (TypeError, ValueError, ZeroDivisionError):
                new_row[result_field] = default_value
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Percentage calculation ({calculation}) on {len(results)} rows",
            data={
                'data': results,
                'calculation': calculation,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return None
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class RunningTotalAction(BaseAction):
    """Calculate running/cumulative totals.
    
    Computes cumulative sum, product, min, or max
    across rows in sorted order.
    """
    action_type = "running_total"
    display_name = "累计计算"
    description = "计算累计总量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate running totals.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, result_field,
                   func (sum|product|min|max), reset_on_change.
        
        Returns:
            ActionResult with cumulative values.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        result_field = params.get('result_field', 'running_total')
        func = params.get('func', 'sum')
        reset_on_change = params.get('reset_on_change', '')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        funcs = {
            'sum': lambda acc, v: acc + v,
            'product': lambda acc, v: acc * v,
            'min': lambda acc, v: min(acc, v),
            'max': lambda acc, v: max(acc, v),
        }
        op = funcs.get(func, funcs['sum'])
        results = []
        running = 0
        prev_key = None

        for row in data:
            new_row = dict(row)
            val = self._get_field(row, field)
            try:
                num = float(val) if val is not None else 0
            except (TypeError, ValueError):
                num = 0

            if reset_on_change:
                key = self._get_field(row, reset_on_change)
                if key != prev_key:
                    running = num
                    prev_key = key
                else:
                    running = op(running, num)
            else:
                running = op(running, num)

            new_row[result_field] = running
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Running {func} for {len(results)} rows",
            data={
                'data': results,
                'func': func,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return None
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class RatioCalcAction(BaseAction):
    """Calculate ratios between two values.
    
    Computes ratio as a decimal, fraction, or
    percentage form.
    """
    action_type = "ratio_calc"
    display_name = "比率计算"
    description = "计算两个值的比率"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate ratios.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, numerator_field, denominator_field,
                   result_field, format (decimal|fraction|percentage),
                   precision, default_value.
        
        Returns:
            ActionResult with ratio values.
        """
        data = params.get('data', [])
        numerator_field = params.get('numerator_field', '')
        denominator_field = params.get('denominator_field', '')
        result_field = params.get('result_field', 'ratio')
        fmt = params.get('format', 'decimal')
        precision = params.get('precision', 2)
        default_value = params.get('default_value', 0)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        for row in data:
            new_row = dict(row)
            try:
                num = float(self._get_field(row, numerator_field) or 0)
                den = float(self._get_field(row, denominator_field) or 0)
                if den == 0:
                    ratio = default_value
                else:
                    ratio = num / den
                    if fmt == 'percentage':
                        ratio = ratio * 100
                    elif fmt == 'fraction':
                        import fractions
                        ratio = fractions.Fraction(num, den)
                ratio = round(ratio, precision) if fmt != 'fraction' else ratio
                new_row[result_field] = ratio
            except (TypeError, ValueError):
                new_row[result_field] = default_value
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Ratio calculation ({fmt}) for {len(results)} rows",
            data={
                'data': results,
                'format': fmt,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return None
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class ConditionalCalcAction(BaseAction):
    """Conditional calculation with if/then/else logic.
    
    Evaluates conditions and computes different
    values based on the result.
    """
    action_type = "conditional_calc"
    display_name = "条件计算"
    description = "条件计算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate with conditional logic.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules (list of {condition, value}),
                   default_value, result_field.
        
        Returns:
            ActionResult with conditional values.
        """
        data = params.get('data', [])
        rules = params.get('rules', [])
        default_value = params.get('default_value', None)
        result_field = params.get('result_field', 'conditional_result')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not rules:
            return ActionResult(
                success=False,
                message="At least one rule is required"
            )

        results = []
        for row in data:
            new_row = dict(row)
            value_assigned = default_value

            for rule in rules:
                condition = rule.get('condition', {})
                value = rule.get('value')
                if self._evaluate_condition(condition, row):
                    value_assigned = value
                    break

            new_row[result_field] = value_assigned
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Conditional calculation for {len(results)} rows",
            data={
                'data': results,
                'result_field': result_field,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _evaluate_condition(self, condition: Dict, row: Dict) -> bool:
        """Evaluate if condition matches row."""
        operator = condition.get('operator', 'eq')
        left_val = self._get_field(row, condition.get('field', ''))
        right_val = condition.get('value')
        ops = {
            'eq': left_val == right_val,
            'ne': left_val != right_val,
            'gt': left_val > right_val if left_val is not None else False,
            'gte': left_val >= right_val if left_val is not None else False,
            'lt': left_val < right_val if left_val is not None else False,
            'lte': left_val <= right_val if left_val is not None else False,
            'is_null': left_val is None,
            'is_not_null': left_val is not None,
        }
        return ops.get(operator, False)

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return None
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value
