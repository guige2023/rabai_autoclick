"""Math6 action module for RabAI AutoClick.

Provides additional math operations:
- MathPowAction: Power operation
- MathSqrtAction: Square root
- MathLogAction: Logarithm
- MathExpAction: Exponential
- MathRadiansAction: Convert degrees to radians
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathPowAction(BaseAction):
    """Power operation."""
    action_type = "math6_pow"
    display_name = "幂运算"
    description = "计算幂次方"

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
            ActionResult with power result.
        """
        base = params.get('base', 0)
        exponent = params.get('exponent', 1)
        output_var = params.get('output_var', 'power_result')

        try:
            resolved_base = float(context.resolve_value(base))
            resolved_exp = float(context.resolve_value(exponent))

            result = math.pow(resolved_base, resolved_exp)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"幂运算: {resolved_base}^{resolved_exp} = {result}",
                data={
                    'base': resolved_base,
                    'exponent': resolved_exp,
                    'result': result,
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
        return {'output_var': 'power_result'}


class MathSqrtAction(BaseAction):
    """Square root."""
    action_type = "math6_sqrt"
    display_name = "平方根"
    description = "计算平方根"

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
        output_var = params.get('output_var', 'sqrt_result')

        try:
            resolved = float(context.resolve_value(value))

            if resolved < 0:
                return ActionResult(
                    success=False,
                    message="不能计算负数的平方根"
                )

            result = math.sqrt(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平方根: sqrt({resolved}) = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"平方根计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sqrt_result'}


class MathLogAction(BaseAction):
    """Logarithm."""
    action_type = "math6_log"
    display_name = "对数"
    description = "计算对数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute log.

        Args:
            context: Execution context.
            params: Dict with value, base, output_var.

        Returns:
            ActionResult with logarithm.
        """
        value = params.get('value', 1)
        base = params.get('base', math.e)
        output_var = params.get('output_var', 'log_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_base = float(context.resolve_value(base)) if base else math.e

            if resolved_value <= 0:
                return ActionResult(
                    success=False,
                    message="对数参数必须为正数"
                )

            result = math.log(resolved_value, resolved_base)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"对数: log_{resolved_base}({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'base': resolved_base,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"对数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'base': math.e, 'output_var': 'log_result'}


class MathExpAction(BaseAction):
    """Exponential."""
    action_type = "math6_exp"
    display_name = "指数"
    description = "计算e的幂次方"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exp.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with exponential.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'exp_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.exp(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"指数: exp({resolved}) = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"指数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exp_result'}


class MathRadiansAction(BaseAction):
    """Convert degrees to radians."""
    action_type = "math6_radians"
    display_name = "角度转弧度"
    description = "将角度转换为弧度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute radians.

        Args:
            context: Execution context.
            params: Dict with degrees, output_var.

        Returns:
            ActionResult with radians.
        """
        degrees = params.get('degrees', 0)
        output_var = params.get('output_var', 'radians_result')

        try:
            resolved = float(context.resolve_value(degrees))
            result = math.radians(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"弧度: {resolved}° = {result} rad",
                data={
                    'degrees': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"角度转弧度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['degrees']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'radians_result'}
