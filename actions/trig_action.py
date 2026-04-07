"""Trig action module for RabAI AutoClick.

Provides trigonometric operations:
- TrigSinAction: Sine
- TrigCosAction: Cosine
- TrigTanAction: Tangent
- TrigAsinAction: Arc sine
- TrigAcosAction: Arc cosine
- TrigAtanAction: Arc tangent
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TrigSinAction(BaseAction):
    """Sine."""
    action_type = "trig_sin"
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
            params: Dict with angle, radians, output_var.

        Returns:
            ActionResult with sine.
        """
        angle = params.get('angle', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'sin_result')

        try:
            resolved_angle = float(context.resolve_value(angle))
            resolved_radians = bool(context.resolve_value(radians))

            if not resolved_radians:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.sin(angle_rad)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正弦: sin({resolved_angle}) = {result}",
                data={
                    'angle': resolved_angle,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算正弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'sin_result'}


class TrigCosAction(BaseAction):
    """Cosine."""
    action_type = "trig_cos"
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
            params: Dict with angle, radians, output_var.

        Returns:
            ActionResult with cosine.
        """
        angle = params.get('angle', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'cos_result')

        try:
            resolved_angle = float(context.resolve_value(angle))
            resolved_radians = bool(context.resolve_value(radians))

            if not resolved_radians:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.cos(angle_rad)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"余弦: cos({resolved_angle}) = {result}",
                data={
                    'angle': resolved_angle,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算余弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'cos_result'}


class TrigTanAction(BaseAction):
    """Tangent."""
    action_type = "trig_tan"
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
            params: Dict with angle, radians, output_var.

        Returns:
            ActionResult with tangent.
        """
        angle = params.get('angle', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'tan_result')

        try:
            resolved_angle = float(context.resolve_value(angle))
            resolved_radians = bool(context.resolve_value(radians))

            if not resolved_radians:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.tan(angle_rad)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正切: tan({resolved_angle}) = {result}",
                data={
                    'angle': resolved_angle,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算正切失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'tan_result'}


class TrigAsinAction(BaseAction):
    """Arc sine."""
    action_type = "trig_asin"
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
            params: Dict with value, radians, output_var.

        Returns:
            ActionResult with arc sine.
        """
        value = params.get('value', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'asin_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_radians = bool(context.resolve_value(radians))

            if abs(resolved_value) > 1:
                return ActionResult(
                    success=False,
                    message="反正弦参数必须在[-1, 1]范围内"
                )

            result = math.asin(resolved_value)

            if not resolved_radians:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正弦: asin({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算反正弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'asin_result'}


class TrigAcosAction(BaseAction):
    """Arc cosine."""
    action_type = "trig_acos"
    display_name = "反余弦"
    description = "计算反余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute acos.

        Args:
            context: Execution context.
            params: Dict with value, radians, output_var.

        Returns:
            ActionResult with arc cosine.
        """
        value = params.get('value', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'acos_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_radians = bool(context.resolve_value(radians))

            if abs(resolved_value) > 1:
                return ActionResult(
                    success=False,
                    message="反余弦参数必须在[-1, 1]范围内"
                )

            result = math.acos(resolved_value)

            if not resolved_radians:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反余弦: acos({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算反余弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'acos_result'}


class TrigAtanAction(BaseAction):
    """Arc tangent."""
    action_type = "trig_atan"
    display_name = "反正切"
    description = "计算反正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute atan.

        Args:
            context: Execution context.
            params: Dict with value, radians, output_var.

        Returns:
            ActionResult with arc tangent.
        """
        value = params.get('value', 0)
        radians = params.get('radians', True)
        output_var = params.get('output_var', 'atan_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_radians = bool(context.resolve_value(radians))

            result = math.atan(resolved_value)

            if not resolved_radians:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正切: atan({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'radians': resolved_radians,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算反正切失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radians': True, 'output_var': 'atan_result'}
