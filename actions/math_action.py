"""Math action module for RabAI AutoClick.

Provides mathematical operations:
- MathAddAction: Add numbers
- MathSubtractAction: Subtract numbers
- MathMultiplyAction: Multiply numbers
- MathDivideAction: Divide numbers
- MathPowerAction: Power operation
- MathSqrtAction: Square root
- MathAbsAction: Absolute value
- MathRoundAction: Round number
- MathFloorAction: Floor operation
- MathCeilAction: Ceiling operation
- MathModAction: Modulo operation
- MathMinAction: Minimum of values
- MathMaxAction: Maximum of values
- MathAvgAction: Average of values
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MathAddAction(BaseAction):
    """Add numbers."""
    action_type = "math_add"
    display_name = "数学加法"
    description = "加法运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute addition.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with sum.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(values) < 2:
            return ActionResult(
                success=False,
                message="至少需要2个值进行加法运算"
            )

        try:
            resolved_values = [context.resolve_value(v) for v in values]
            result = sum(float(v) for v in resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"加法结果: {result}",
                data={
                    'result': result,
                    'operation': 'add',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"加法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathSubtractAction(BaseAction):
    """Subtract numbers."""
    action_type = "math_subtract"
    display_name = "数学减法"
    description = "减法运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtraction.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with difference.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = float(resolved_v1) - float(resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"减法结果: {result}",
                data={
                    'result': result,
                    'operation': 'subtract',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"减法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathMultiplyAction(BaseAction):
    """Multiply numbers."""
    action_type = "math_multiply"
    display_name = "数学乘法"
    description = "乘法运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute multiplication.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with product.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(values) < 2:
            return ActionResult(
                success=False,
                message="至少需要2个值进行乘法运算"
            )

        try:
            resolved_values = [context.resolve_value(v) for v in values]
            result = 1
            for v in resolved_values:
                result *= float(v)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"乘法结果: {result}",
                data={
                    'result': result,
                    'operation': 'multiply',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"乘法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathDivideAction(BaseAction):
    """Divide numbers."""
    action_type = "math_divide"
    display_name = "数学除法"
    description = "除法运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute division.

        Args:
            context: Execution context.
            params: Dict with dividend, divisor, output_var.

        Returns:
            ActionResult with quotient.
        """
        dividend = params.get('dividend', 1)
        divisor = params.get('divisor', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_dividend = context.resolve_value(dividend)
            resolved_divisor = context.resolve_value(divisor)

            if float(resolved_divisor) == 0:
                return ActionResult(
                    success=False,
                    message="除数不能为零"
                )

            result = float(resolved_dividend) / float(resolved_divisor)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"除法结果: {result}",
                data={
                    'result': result,
                    'operation': 'divide',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"除法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dividend', 'divisor']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathPowerAction(BaseAction):
    """Power operation."""
    action_type = "math_power"
    display_name = "数学幂运算"
    description = "幂运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute power operation.

        Args:
            context: Execution context.
            params: Dict with base, exponent, output_var.

        Returns:
            ActionResult with power result.
        """
        base = params.get('base', 2)
        exponent = params.get('exponent', 2)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_base = context.resolve_value(base)
            resolved_exponent = context.resolve_value(exponent)
            result = float(resolved_base) ** float(resolved_exponent)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"幂运算结果: {result}",
                data={
                    'result': result,
                    'operation': 'power',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"幂运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['base', 'exponent']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathSqrtAction(BaseAction):
    """Square root operation."""
    action_type = "math_sqrt"
    display_name = "数学平方根"
    description = "平方根运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute square root.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with square root.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            val = float(resolved_value)

            if val < 0:
                return ActionResult(
                    success=False,
                    message="负数没有实数平方根"
                )

            result = math.sqrt(val)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平方根结果: {result}",
                data={
                    'result': result,
                    'operation': 'sqrt',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"平方根失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathAbsAction(BaseAction):
    """Absolute value."""
    action_type = "math_abs"
    display_name = "数学绝对值"
    description = "绝对值运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute absolute value.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with absolute value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            result = abs(float(resolved_value))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值结果: {result}",
                data={
                    'result': result,
                    'operation': 'abs',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"绝对值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathRoundAction(BaseAction):
    """Round number."""
    action_type = "math_round"
    display_name = "数学四舍五入"
    description = "四舍五入运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute round.

        Args:
            context: Execution context.
            params: Dict with value, decimals, output_var.

        Returns:
            ActionResult with rounded value.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_decimals = context.resolve_value(decimals)
            result = round(float(resolved_value), int(resolved_decimals))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"四舍五入结果: {result}",
                data={
                    'result': result,
                    'operation': 'round',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"四舍五入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 0, 'output_var': 'math_result'}


class MathFloorAction(BaseAction):
    """Floor operation."""
    action_type = "math_floor"
    display_name = "数学向下取整"
    description = "向下取整运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute floor.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with floored value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            result = math.floor(float(resolved_value))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向下取整结果: {result}",
                data={
                    'result': result,
                    'operation': 'floor',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"向下取整失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathCeilAction(BaseAction):
    """Ceiling operation."""
    action_type = "math_ceil"
    display_name = "数学向上取整"
    description = "向上取整运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ceiling.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with ceiled value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            result = math.ceil(float(resolved_value))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向上取整结果: {result}",
                data={
                    'result': result,
                    'operation': 'ceil',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"向上取整失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathModAction(BaseAction):
    """Modulo operation."""
    action_type = "math_mod"
    display_name = "数学取模"
    description = "取模运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute modulo.

        Args:
            context: Execution context.
            params: Dict with dividend, divisor, output_var.

        Returns:
            ActionResult with modulo result.
        """
        dividend = params.get('dividend', 0)
        divisor = params.get('divisor', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_dividend = context.resolve_value(dividend)
            resolved_divisor = context.resolve_value(divisor)

            if float(resolved_divisor) == 0:
                return ActionResult(
                    success=False,
                    message="除数不能为零"
                )

            result = float(resolved_dividend) % float(resolved_divisor)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"取模结果: {result}",
                data={
                    'result': result,
                    'operation': 'mod',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"取模失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dividend', 'divisor']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathMinAction(BaseAction):
    """Minimum of values."""
    action_type = "math_min"
    display_name = "数学最小值"
    description = "获取最小值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with minimum value.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(values) == 0:
            return ActionResult(
                success=False,
                message="值列表为空"
            )

        try:
            resolved_values = [context.resolve_value(v) for v in values]
            result = min(float(v) for v in resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小值: {result}",
                data={
                    'result': result,
                    'operation': 'min',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathMaxAction(BaseAction):
    """Maximum of values."""
    action_type = "math_max"
    display_name = "数学最大值"
    description = "获取最大值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with maximum value.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(values) == 0:
            return ActionResult(
                success=False,
                message="值列表为空"
            )

        try:
            resolved_values = [context.resolve_value(v) for v in values]
            result = max(float(v) for v in resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大值: {result}",
                data={
                    'result': result,
                    'operation': 'max',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathAvgAction(BaseAction):
    """Average of values."""
    action_type = "math_avg"
    display_name = "数学平均值"
    description = "获取平均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with average value.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(values) == 0:
            return ActionResult(
                success=False,
                message="值列表为空"
            )

        try:
            resolved_values = [context.resolve_value(v) for v in values]
            result = sum(float(v) for v in resolved_values) / len(resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平均值: {result}",
                data={
                    'result': result,
                    'operation': 'avg',
                    'count': len(resolved_values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}