"""Math13 action module for RabAI AutoClick.

Provides additional math operations:
- MathRoundAction: Round number
- MathFloorAction: Floor number
- MathCeilAction: Ceiling number
- MathAbsoluteAction: Absolute value
- MathClampAction: Clamp value
- MathLogAction: Logarithm
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathRoundAction(BaseAction):
    """Round number."""
    action_type = "math13_round"
    display_name = "四舍五入"
    description = "四舍五入"
    version = "13.0"

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
        output_var = params.get('output_var', 'rounded_value')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_decimals = int(context.resolve_value(decimals)) if decimals else 0

            result = round(resolved_value, resolved_decimals)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"四舍五入: {result}",
                data={
                    'value': resolved_value,
                    'decimals': resolved_decimals,
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
        return {'decimals': 0, 'output_var': 'rounded_value'}


class MathFloorAction(BaseAction):
    """Floor number."""
    action_type = "math13_floor"
    display_name = "向下取整"
    description = "向下取整"
    version = "13.0"

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
        output_var = params.get('output_var', 'floored_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 0

            result = math.floor(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向下取整: {result}",
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
        return {'output_var': 'floored_value'}


class MathCeilAction(BaseAction):
    """Ceiling number."""
    action_type = "math13_ceil"
    display_name = "向上取整"
    description = "向上取整"
    version = "13.0"

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
            ActionResult with ceiled value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'ceiled_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 0

            result = math.ceil(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向上取整: {result}",
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
        return {'output_var': 'ceiled_value'}


class MathAbsoluteAction(BaseAction):
    """Absolute value."""
    action_type = "math13_abs"
    display_name = "绝对值"
    description = "绝对值"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute absolute.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with absolute value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'absolute_value')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0

            result = abs(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值: {result}",
                data={
                    'value': resolved_value,
                    'result': result,
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
        return {'output_var': 'absolute_value'}


class MathClampAction(BaseAction):
    """Clamp value."""
    action_type = "math13_clamp"
    display_name = "限制范围"
    description: "限制值在范围内"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clamp.

        Args:
            context: Execution context.
            params: Dict with value, min, max, output_var.

        Returns:
            ActionResult with clamped value.
        """
        value = params.get('value', 0)
        min_val = params.get('min', 0)
        max_val = params.get('max', 1)
        output_var = params.get('output_var', 'clamped_value')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_min = float(context.resolve_value(min_val)) if min_val else 0
            resolved_max = float(context.resolve_value(max_val)) if max_val else 1

            result = max(resolved_min, min(resolved_max, resolved_value))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"限制范围: {result}",
                data={
                    'value': resolved_value,
                    'min': resolved_min,
                    'max': resolved_max,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"限制范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'min', 'max']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clamped_value'}


class MathLogAction(BaseAction):
    """Logarithm."""
    action_type = "math13_log"
    display_name = "对数"
    description = "对数计算"
    version = "13.0"

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
            ActionResult with log value.
        """
        value = params.get('value', 1)
        base = params.get('base', 10)
        output_var = params.get('output_var', 'log_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 1
            resolved_base = float(context.resolve_value(base)) if base else 10

            if resolved_value <= 0:
                return ActionResult(
                    success=False,
                    message=f"对数必须为正数: {resolved_value}"
                )

            result = math.log(resolved_value, resolved_base)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"对数: {result}",
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
        return ['value', 'base']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_value'}