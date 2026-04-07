"""Math2 action module for RabAI AutoClick.

Provides advanced math operations:
- MathLogAction: Logarithm
- MathExpAction: Exponential
- MathCeilAction: Ceiling
- MathFloorAction: Floor
- MathRoundAction: Round
- MathAbsAction: Absolute value
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathLogAction(BaseAction):
    """Logarithm."""
    action_type = "math_log"
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
        base = params.get('base', 'e')
        output_var = params.get('output_var', 'log_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_base = context.resolve_value(base)

            if resolved_value <= 0:
                return ActionResult(
                    success=False,
                    message="对数参数必须大于0"
                )

            if resolved_base == 'e':
                result = math.log(resolved_value)
            elif resolved_base == '10':
                result = math.log10(resolved_value)
            elif resolved_base == '2':
                result = math.log2(resolved_value)
            else:
                base_float = float(resolved_base)
                if base_float <= 0 or base_float == 1:
                    return ActionResult(
                        success=False,
                        message="底数必须大于0且不等于1"
                    )
                result = math.log(resolved_value, base_float)

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
                message=f"计算对数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'base': 'e', 'output_var': 'log_result'}


class MathExpAction(BaseAction):
    """Exponential."""
    action_type = "math_exp"
    display_name = "指数"
    description = "计算指数函数"

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
        value = params.get('value', 1)
        output_var = params.get('output_var', 'exp_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.exp(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"指数: e^{resolved_value} = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算指数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exp_result'}


class MathCeilAction(BaseAction):
    """Ceiling."""
    action_type = "math_ceil"
    display_name = "向上取整"
    description = "向上取整"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ceil.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with ceiling.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'ceil_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.ceil(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向上取整: ceil({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
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
        return {'output_var': 'ceil_result'}


class MathFloorAction(BaseAction):
    """Floor."""
    action_type = "math_floor"
    display_name = "向下取整"
    description = "向下取整"

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
            ActionResult with floor.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'floor_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.floor(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向下取整: floor({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
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
        return {'output_var': 'floor_result'}


class MathRoundAction(BaseAction):
    """Round."""
    action_type = "math_round"
    display_name = "四舍五入"
    description = "四舍五入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute round.

        Args:
            context: Execution context.
            params: Dict with value, digits, output_var.

        Returns:
            ActionResult with rounded value.
        """
        value = params.get('value', 0)
        digits = params.get('digits', 0)
        output_var = params.get('output_var', 'round_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_digits = int(context.resolve_value(digits))

            result = round(resolved_value, resolved_digits)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"四舍五入: round({resolved_value}, {resolved_digits}) = {result}",
                data={
                    'value': resolved_value,
                    'digits': resolved_digits,
                    'result': result,
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
        return {'digits': 0, 'output_var': 'round_result'}


class MathAbsAction(BaseAction):
    """Absolute value."""
    action_type = "math_abs"
    display_name = "绝对值"
    description = "计算绝对值"

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
        output_var = params.get('output_var', 'abs_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = abs(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值: abs({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算绝对值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'abs_result'}
