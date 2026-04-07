"""Math10 action module for RabAI AutoClick.

Provides additional math operations:
- MathLogAction: Logarithm
- MathLog10Action: Log base 10
- MathExpAction: Exponential
- MathRadiansAction: Degrees to radians
- MathDegreesAction: Radians to degrees
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathLogAction(BaseAction):
    """Logarithm."""
    action_type = "math10_log"
    display_name = "对数"
    description = "计算对数"
    version = "10.0"

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
            ActionResult with log result.
        """
        value = params.get('value', 1)
        base = params.get('base', None)
        output_var = params.get('output_var', 'log_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_base = float(context.resolve_value(base)) if base else None

            if resolved_value <= 0:
                return ActionResult(
                    success=False,
                    message=f"对数失败: 值必须为正数"
                )

            if resolved_base:
                result = math.log(resolved_value, resolved_base)
            else:
                result = math.log(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"对数: log{'_' + str(resolved_base) if resolved_base else ''}({resolved_value}) = {result}",
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
                message=f"对数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'base': None, 'output_var': 'log_result'}


class MathLog10Action(BaseAction):
    """Log base 10."""
    action_type = "math10_log10"
    display_name = "常用对数"
    description = "计算常用对数"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute log10.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with log10 result.
        """
        value = params.get('value', 1)
        output_var = params.get('output_var', 'log10_result')

        try:
            resolved_value = float(context.resolve_value(value))

            if resolved_value <= 0:
                return ActionResult(
                    success=False,
                    message=f"常用对数失败: 值必须为正数"
                )

            result = math.log10(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"常用对数: log10({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"常用对数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log10_result'}


class MathExpAction(BaseAction):
    """Exponential."""
    action_type = "math10_exp"
    display_name = "指数"
    description = "计算e的指数"
    version = "10.0"

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
            ActionResult with exp result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'exp_result')

        try:
            resolved_value = float(context.resolve_value(value))
            result = math.exp(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"指数: exp({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
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
    """Degrees to radians."""
    action_type = "math10_radians"
    display_name = "角度转弧度"
    description = "将角度转换为弧度"
    version = "10.0"

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
                message=f"角度转弧度: {resolved_degrees}° = {result} rad",
                data={
                    'degrees': resolved_degrees,
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


class MathDegreesAction(BaseAction):
    """Radians to degrees."""
    action_type = "math10_degrees"
    display_name = "弧度转角度"
    description = "将弧度转换为角度"
    version = "10.0"

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
                message=f"弧度转角度: {resolved_radians} rad = {result}°",
                data={
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"弧度转角度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['radians']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'degrees_result'}