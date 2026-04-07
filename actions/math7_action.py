"""Math7 action module for RabAI AutoClick.

Provides additional math operations:
- MathDegreesAction: Convert radians to degrees
- MathSinAction: Sine
- MathCosAction: Cosine
- MathTanAction: Tangent
- MathAsinAction: Arcsine
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathDegreesAction(BaseAction):
    """Convert radians to degrees."""
    action_type = "math7_degrees"
    display_name = "弧度转角度"
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
            resolved = float(context.resolve_value(radians))
            result = math.degrees(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"角度: {resolved} rad = {result}°",
                data={
                    'radians': resolved,
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


class MathSinAction(BaseAction):
    """Sine."""
    action_type = "math7_sin"
    display_name = "正弦"
    description = "计算正弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sin.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with sine.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'sin_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            if resolved_degrees:
                resolved = math.radians(resolved)

            result = math.sin(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正弦: sin({resolved}) = {result}",
                data={
                    'value': resolved,
                    'is_degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'sin_result'}


class MathCosAction(BaseAction):
    """Cosine."""
    action_type = "math7_cos"
    display_name = "余弦"
    description = "计算余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cos.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with cosine.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'cos_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            if resolved_degrees:
                resolved = math.radians(resolved)

            result = math.cos(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"余弦: cos({resolved}) = {result}",
                data={
                    'value': resolved,
                    'is_degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"余弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'cos_result'}


class MathTanAction(BaseAction):
    """Tangent."""
    action_type = "math7_tan"
    display_name = "正切"
    description = "计算正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tan.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with tangent.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'tan_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            if resolved_degrees:
                resolved = math.radians(resolved)

            result = math.tan(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正切: tan({resolved}) = {result}",
                data={
                    'value': resolved,
                    'is_degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正切计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'tan_result'}


class MathAsinAction(BaseAction):
    """Arcsine."""
    action_type = "math7_asin"
    display_name = "反正弦"
    description = "计算反正弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute asin.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with arcsine.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'asin_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            if abs(resolved) > 1:
                return ActionResult(
                    success=False,
                    message="反正弦参数必须在[-1, 1]范围内"
                )

            result = math.asin(resolved)
            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正弦: asin({resolved}) = {result}",
                data={
                    'value': resolved,
                    'is_degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反正弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'asin_result'}
