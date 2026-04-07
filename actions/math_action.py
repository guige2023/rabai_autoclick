"""Mathematical operations action module for RabAI AutoClick.

Provides math operations:
- MathAddAction: Addition
- MathSubtractAction: Subtraction
- MathMultiplyAction: Multiplication
- MathDivideAction: Division
- MathPowerAction: Power
- MathSqrtAction: Square root
- MathAbsAction: Absolute value
- MathRoundAction: Round number
- MathMinMaxAction: Min/max of numbers
- MathSumProductAction: Sum and product
- MathPercentAction: Percentage calculation
"""

from __future__ import annotations

import math
import sys
from typing import Any, Dict, List, Optional, Union

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathAddAction(BaseAction):
    """Addition."""
    action_type = "math_add"
    display_name = "数学加法"
    description = "加法运算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute add."""
        a = params.get('a', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_a = context.resolve_value(a) if context else a
            resolved_b = context.resolve_value(b) if context else b
            result = float(resolved_a) + float(resolved_b)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{resolved_a} + {resolved_b} = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Add error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'a': 0, 'b': 0, 'output_var': 'math_result'}


class MathSubtractAction(BaseAction):
    """Subtraction."""
    action_type = "math_subtract"
    display_name = "数学减法"
    description = "减法运算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute subtract."""
        a = params.get('a', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_a = context.resolve_value(a) if context else a
            resolved_b = context.resolve_value(b) if context else b
            result = float(resolved_a) - float(resolved_b)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{resolved_a} - {resolved_b} = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Subtract error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'a': 0, 'b': 0, 'output_var': 'math_result'}


class MathMultiplyAction(BaseAction):
    """Multiplication."""
    action_type = "math_multiply"
    display_name = "数学乘法"
    description = "乘法运算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute multiply."""
        a = params.get('a', 1)
        b = params.get('b', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_a = context.resolve_value(a) if context else a
            resolved_b = context.resolve_value(b) if context else b
            result = float(resolved_a) * float(resolved_b)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{resolved_a} * {resolved_b} = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Multiply error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'a': 1, 'b': 1, 'output_var': 'math_result'}


class MathDivideAction(BaseAction):
    """Division."""
    action_type = "math_divide"
    display_name = "数学除法"
    description = "除法运算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute divide."""
        a = params.get('a', 1)
        b = params.get('b', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_a = context.resolve_value(a) if context else a
            resolved_b = context.resolve_value(b) if context else b
            if float(resolved_b) == 0:
                return ActionResult(success=False, message="Division by zero")
            result = float(resolved_a) / float(resolved_b)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{resolved_a} / {resolved_b} = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Divide error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'a': 1, 'b': 1, 'output_var': 'math_result'}


class MathPowerAction(BaseAction):
    """Power."""
    action_type = "math_power"
    display_name = "数学幂运算"
    description = "幂运算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute power."""
        base = params.get('base', 2)
        exponent = params.get('exponent', 3)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_base = context.resolve_value(base) if context else base
            resolved_exp = context.resolve_value(exponent) if context else exponent
            result = float(resolved_base) ** float(resolved_exp)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{resolved_base}^{resolved_exp} = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Power error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'base': 2, 'exponent': 3, 'output_var': 'math_result'}


class MathSqrtAction(BaseAction):
    """Square root."""
    action_type = "math_sqrt"
    display_name = "数学平方根"
    description = "平方根"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sqrt."""
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved = context.resolve_value(value) if context else value
            val = float(resolved)
            if val < 0:
                return ActionResult(success=False, message="Cannot take square root of negative number")
            result = math.sqrt(val)
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"sqrt({val}) = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Sqrt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 0, 'output_var': 'math_result'}


class MathAbsAction(BaseAction):
    """Absolute value."""
    action_type = "math_abs"
    display_name = "数学绝对值"
    description = "绝对值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute abs."""
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved = context.resolve_value(value) if context else value
            result = abs(float(resolved))
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"abs({resolved}) = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Abs error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 0, 'output_var': 'math_result'}


class MathRoundAction(BaseAction):
    """Round number."""
    action_type = "math_round"
    display_name = "数学四舍五入"
    description = "四舍五入"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute round."""
        value = params.get('value', 0)
        decimals = params.get('decimals', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_d = context.resolve_value(decimals) if context else decimals
            result = round(float(resolved), int(resolved_d))
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"round({resolved}, {resolved_d}) = {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Round error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 0, 'decimals': 0, 'output_var': 'math_result'}


class MathMinMaxAction(BaseAction):
    """Min and max of numbers."""
    action_type = "math_minmax"
    display_name = "数学最值"
    description = "最小值最大值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute min/max."""
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'math_result')

        if not numbers:
            return ActionResult(success=False, message="numbers is required")

        try:
            resolved = context.resolve_value(numbers) if context else numbers
            nums = [float(n) for n in resolved]
            result = {'min': min(nums), 'max': max(nums), 'count': len(nums)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"min={result['min']}, max={result['max']}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"MinMax error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathSumProductAction(BaseAction):
    """Sum and product of numbers."""
    action_type = "math_sump"
    display_name = "数学总和与乘积"
    description = "总和与乘积"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sum/product."""
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'math_result')

        if not numbers:
            return ActionResult(success=False, message="numbers is required")

        try:
            resolved = context.resolve_value(numbers) if context else numbers
            nums = [float(n) for n in resolved]
            result = {'sum': sum(nums), 'product': math.prod(nums), 'count': len(nums), 'avg': sum(nums) / len(nums)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"sum={result['sum']}, product={result['product']}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"SumProduct error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathPercentAction(BaseAction):
    """Percentage calculation."""
    action_type = "math_percent"
    display_name = "数学百分比"
    description = "百分比计算"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute percentage."""
        value = params.get('value', 0)
        total = params.get('total', 100)
        operation = params.get('operation', 'of')  # of, what_percent, percent_of
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_val = context.resolve_value(value) if context else value
            resolved_total = context.resolve_value(total) if context else total

            val = float(resolved_val)
            total_f = float(resolved_total)

            if operation == 'of':
                result = (val / 100) * total_f
                message = f"{val}% of {total_f} = {result}"
            elif operation == 'what_percent':
                result = (val / total_f) * 100 if total_f != 0 else 0
                message = f"{val} / {total_f} = {result}%"
            else:  # percent_of
                result = (val / 100) * total_f
                message = f"{val}% of {total_f} = {result}"

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=message, data={'result': result, 'operation': operation})
        except Exception as e:
            return ActionResult(success=False, message=f"Percent error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'total': 100, 'operation': 'of', 'output_var': 'math_result'}
