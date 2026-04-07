"""Math4 action module for RabAI AutoClick.

Provides additional math operations:
- MathPowAction: Power
- MathSqrtAction: Square root
- MathCbrtAction: Cube root
- MathHypotAction: Hypotenuse
- MathRadiansAction: Degrees to radians
- MathDegreesAction: Radians to degrees
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathPowAction(BaseAction):
    """Power."""
    action_type = "math_pow"
    display_name = "幂运算"
    description = "计算幂"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pow.

        Args:
            context: Execution context.
            params: Dict with base, exponent, output_var.

        Returns:
            ActionResult with power.
        """
        base = params.get('base', 2)
        exponent = params.get('exponent', 3)
        output_var = params.get('output_var', 'pow_result')

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
        return {'output_var': 'pow_result'}


class MathSqrtAction(BaseAction):
    """Square root."""
    action_type = "math_sqrt"
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
            resolved_value = float(context.resolve_value(value))

            if resolved_value < 0:
                return ActionResult(
                    success=False,
                    message="负数不能计算平方根"
                )

            result = math.sqrt(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平方根: sqrt({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算平方根失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sqrt_result'}


class MathCbrtAction(BaseAction):
    """Cube root."""
    action_type = "math_cbrt"
    display_name = "立方根"
    description = "计算立方根"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cbrt.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with cube root.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'cbrt_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.pow(abs(resolved_value), 1/3)
            if resolved_value < 0:
                result = -result

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"立方根: cbrt({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算立方根失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cbrt_result'}


class MathHypotAction(BaseAction):
    """Hypotenuse."""
    action_type = "math_hypot"
    display_name = "斜边"
    description = "计算直角三角形的斜边"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hypot.

        Args:
            context: Execution context.
            params: Dict with x, y, output_var.

        Returns:
            ActionResult with hypotenuse.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        output_var = params.get('output_var', 'hypot_result')

        try:
            resolved_x = float(context.resolve_value(x))
            resolved_y = float(context.resolve_value(y))

            result = math.hypot(resolved_x, resolved_y)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"斜边: hypot({resolved_x}, {resolved_y}) = {result}",
                data={
                    'x': resolved_x,
                    'y': resolved_y,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算斜边失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hypot_result'}


class MathRadiansAction(BaseAction):
    """Degrees to radians."""
    action_type = "math_radians"
    display_name = "转弧度"
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
            resolved_degrees = float(context.resolve_value(degrees))

            result = math.radians(resolved_degrees)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"弧度: {resolved_degrees}° = {result} rad",
                data={
                    'degrees': resolved_degrees,
                    'radians': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换弧度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['degrees']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'radians_result'}


class MathDegreesAction(BaseAction):
    """Radians to degrees."""
    action_type = "math_degrees"
    display_name = "转角度"
    description = "将弧度转换为角度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute degrees.

        Args:
            context: Execution context.
            params: Dict with radians, output_var.

        Returns:
            ActionResult with degrees.
        """
        radians = params.get('radians', 0)
        output_var = params.get('output_var', 'degrees_result')

        try:
            resolved_radians = float(context.resolve_value(radians))

            result = math.degrees(resolved_radians)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"角度: {resolved_radians} rad = {result}°",
                data={
                    'radians': resolved_radians,
                    'degrees': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换角度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['radians']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'degrees_result'}
