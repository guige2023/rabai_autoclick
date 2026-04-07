"""Math action module for RabAI AutoClick.

Provides math operations:
- MathAddAction: Addition
- MathSubtractAction: Subtraction
- MathMultiplyAction: Multiplication
- MathDivideAction: Division
- MathModAction: Modulo
- MathPowerAction: Power
- MathSqrtAction: Square root
- MathAbsAction: Absolute value
- MathRoundAction: Round number
- MathMinMaxAction: Min/Max
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathAddAction(BaseAction):
    """Addition."""
    action_type = "math_add"
    display_name = "加法"
    description = "加法运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with sum.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = context.resolve_value(values)
            total = sum(resolved_values)

            context.set(output_var, total)

            return ActionResult(
                success=True,
                message=f"加法结果: {total}",
                data={'result': total, 'values': resolved_values, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"加法失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathSubtractAction(BaseAction):
    """Subtraction."""
    action_type = "math_subtract"
    display_name = "减法"
    description = "减法运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract.

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

            result = resolved_v1 - resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"减法结果: {result}",
                data={'result': result, 'value1': resolved_v1, 'value2': resolved_v2, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"减法失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathMultiplyAction(BaseAction):
    """Multiplication."""
    action_type = "math_multiply"
    display_name = "乘法"
    description = "乘法运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute multiply.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with product.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = context.resolve_value(values)
            result = 1
            for v in resolved_values:
                result *= v

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"乘法结果: {result}",
                data={'result': result, 'values': resolved_values, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"乘法失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathDivideAction(BaseAction):
    """Division."""
    action_type = "math_divide"
    display_name = "除法"
    description = "除法运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute divide.

        Args:
            context: Execution context.
            params: Dict with dividend, divisor, output_var.

        Returns:
            ActionResult with quotient.
        """
        dividend = params.get('dividend', 0)
        divisor = params.get('divisor', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_dividend = context.resolve_value(dividend)
            resolved_divisor = context.resolve_value(divisor)

            if resolved_divisor == 0:
                return ActionResult(success=False, message="除数不能为零")

            result = resolved_dividend / resolved_divisor
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"除法结果: {result}",
                data={'result': result, 'dividend': resolved_dividend, 'divisor': resolved_divisor, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"除法失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dividend', 'divisor']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathModAction(BaseAction):
    """Modulo."""
    action_type = "math_mod"
    display_name = "取模"
    description = "取模运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mod.

        Args:
            context: Execution context.
            params: Dict with value, divisor, output_var.

        Returns:
            ActionResult with remainder.
        """
        value = params.get('value', 0)
        divisor = params.get('divisor', 1)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_divisor = context.resolve_value(divisor)

            if resolved_divisor == 0:
                return ActionResult(success=False, message="除数不能为零")

            result = resolved_value % resolved_divisor
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"取模结果: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"取模失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'divisor']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathPowerAction(BaseAction):
    """Power."""
    action_type = "math_power"
    display_name = "幂运算"
    description = "幂运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute power.

        Args:
            context: Execution context.
            params: Dict with base, exponent, output_var.

        Returns:
            ActionResult with power.
        """
        base = params.get('base', 2)
        exponent = params.get('exponent', 3)
        output_var = params.get('output_var', 'math_result')

        try:
            resolved_base = context.resolve_value(base)
            resolved_exp = context.resolve_value(exponent)

            result = math.pow(resolved_base, resolved_exp)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"幂运算结果: {result}",
                data={'result': result, 'base': resolved_base, 'exponent': resolved_exp, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"幂运算失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['base', 'exponent']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathSqrtAction(BaseAction):
    """Square root."""
    action_type = "math_sqrt"
    display_name = "平方根"
    description = "平方根运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sqrt.

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

            if resolved_value < 0:
                return ActionResult(success=False, message="负数无法计算平方根")

            result = math.sqrt(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平方根结果: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"平方根失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathAbsAction(BaseAction):
    """Absolute value."""
    action_type = "math_abs"
    display_name = "绝对值"
    description = "绝对值运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute abs.

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

            result = abs(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值结果: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"绝对值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathRoundAction(BaseAction):
    """Round number."""
    action_type = "math_round"
    display_name = "四舍五入"
    description = "四舍五入"
    version = "1.0"

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

            result = round(resolved_value, resolved_decimals)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"四舍五入结果: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"四舍五入失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'decimals']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}


class MathMinMaxAction(BaseAction):
    """Min/Max."""
    action_type = "math_minmax"
    display_name = "最小/最大值"
    description = "最小/最大值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minmax.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with min and max.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'math_result')

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = context.resolve_value(values)

            if not resolved_values:
                return ActionResult(success=False, message="列表不能为空")

            min_val = min(resolved_values)
            max_val = max(resolved_values)
            result = {'min': min_val, 'max': max_val}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小值: {min_val}, 最大值: {max_val}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"最小/最大值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'math_result'}
