"""Math5 action module for RabAI AutoClick.

Provides additional math operations:
- MathAbsAction: Absolute value
- MathCeilAction: Ceiling
- MathFloorAction: Floor
- MathRoundAction: Round
- MathFactorialAction: Factorial
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathAbsAction(BaseAction):
    """Absolute value."""
    action_type = "math5_abs"
    display_name = "绝对值"
    description = "计算绝对值"

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
        output_var = params.get('output_var', 'abs_result')

        try:
            resolved = float(context.resolve_value(value))
            result = abs(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值: {result}",
                data={
                    'value': resolved,
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
        return {'output_var': 'abs_result'}


class MathCeilAction(BaseAction):
    """Ceiling."""
    action_type = "math5_ceil"
    display_name = "向上取整"
    description = "向上取整"

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
            ActionResult with ceiling value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'ceil_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.ceil(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向上取整: {result}",
                data={
                    'value': resolved,
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
    action_type = "math5_floor"
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
            ActionResult with floor value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'floor_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.floor(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"向下取整: {result}",
                data={
                    'value': resolved,
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
    action_type = "math5_round"
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
            params: Dict with value, ndigits, output_var.

        Returns:
            ActionResult with rounded value.
        """
        value = params.get('value', 0)
        ndigits = params.get('ndigits', 0)
        output_var = params.get('output_var', 'round_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_ndigits = int(context.resolve_value(ndigits)) if ndigits else 0

            result = round(resolved, resolved_ndigits)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"四舍五入: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'ndigits': resolved_ndigits,
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
        return {'ndigits': 0, 'output_var': 'round_result'}


class MathFactorialAction(BaseAction):
    """Factorial."""
    action_type = "math5_factorial"
    display_name = "阶乘"
    description = "计算阶乘"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute factorial.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with factorial value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'factorial_result')

        try:
            resolved = int(context.resolve_value(value))

            if resolved < 0:
                return ActionResult(
                    success=False,
                    message=f"阶乘不能为负数: {resolved}"
                )

            result = math.factorial(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"阶乘: {resolved}! = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"阶乘失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'factorial_result'}