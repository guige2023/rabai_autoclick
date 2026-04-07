"""Trigonometry13 action module for RabAI AutoClick.

Provides additional trigonometry operations:
- TrigonometrySinAction: Sine
- TrigonometryCosAction: Cosine
- TrigonometryTanAction: Tangent
- TrigonometryAsinAction: Arc sine
- TrigonometryAcosAction: Arc cosine
- TrigonometryAtanAction: Arc tangent
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TrigonometrySinAction(BaseAction):
    """Sine."""
    action_type = "trigonometry13_sin"
    display_name = "正弦"
    description = "正弦函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sine.

        Args:
            context: Execution context.
            params: Dict with angle, degrees, output_var.

        Returns:
            ActionResult with sine value.
        """
        angle = params.get('angle', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'sin_value')

        try:
            import math

            resolved_angle = float(context.resolve_value(angle)) if angle else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            if resolved_degrees:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.sin(angle_rad)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正弦: {result}",
                data={
                    'angle': resolved_angle,
                    'degrees': resolved_degrees,
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
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'degrees': True, 'output_var': 'sin_value'}


class TrigonometryCosAction(BaseAction):
    """Cosine."""
    action_type = "trigonometry13_cos"
    display_name = "余弦"
    description = "余弦函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cosine.

        Args:
            context: Execution context.
            params: Dict with angle, degrees, output_var.

        Returns:
            ActionResult with cosine value.
        """
        angle = params.get('angle', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'cos_value')

        try:
            import math

            resolved_angle = float(context.resolve_value(angle)) if angle else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            if resolved_degrees:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.cos(angle_rad)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"余弦: {result}",
                data={
                    'angle': resolved_angle,
                    'degrees': resolved_degrees,
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
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'degrees': True, 'output_var': 'cos_value'}


class TrigonometryTanAction(BaseAction):
    """Tangent."""
    action_type = "trigonometry13_tan"
    display_name = "正切"
    description = "正切函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tangent.

        Args:
            context: Execution context.
            params: Dict with angle, degrees, output_var.

        Returns:
            ActionResult with tangent value.
        """
        angle = params.get('angle', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'tan_value')

        try:
            import math

            resolved_angle = float(context.resolve_value(angle)) if angle else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            if resolved_degrees:
                angle_rad = math.radians(resolved_angle)
            else:
                angle_rad = resolved_angle

            result = math.tan(angle_rad)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正切: {result}",
                data={
                    'angle': resolved_angle,
                    'degrees': resolved_degrees,
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
        return ['angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'degrees': True, 'output_var': 'tan_value'}


class TrigonometryAsinAction(BaseAction):
    """Arc sine."""
    action_type = "trigonometry13_asin"
    display_name = "反正弦"
    description = "反正弦函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arc sine.

        Args:
            context: Execution context.
            params: Dict with value, degrees, output_var.

        Returns:
            ActionResult with arc sine value.
        """
        value = params.get('value', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'asin_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            if abs(resolved_value) > 1:
                return ActionResult(
                    success=False,
                    message=f"值必须在[-1, 1]范围内: {resolved_value}"
                )

            result = math.asin(resolved_value)

            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正弦: {result}",
                data={
                    'value': resolved_value,
                    'degrees': resolved_degrees,
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
        return {'degrees': True, 'output_var': 'asin_value'}


class TrigonometryAcosAction(BaseAction):
    """Arc cosine."""
    action_type = "trigonometry13_acos"
    display_name = "反余弦"
    description = "反余弦函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arc cosine.

        Args:
            context: Execution context.
            params: Dict with value, degrees, output_var.

        Returns:
            ActionResult with arc cosine value.
        """
        value = params.get('value', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'acos_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            if abs(resolved_value) > 1:
                return ActionResult(
                    success=False,
                    message=f"值必须在[-1, 1]范围内: {resolved_value}"
                )

            result = math.acos(resolved_value)

            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反余弦: {result}",
                data={
                    'value': resolved_value,
                    'degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反余弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'degrees': True, 'output_var': 'acos_value'}


class TrigonometryAtanAction(BaseAction):
    """Arc tangent."""
    action_type = "trigonometry13_atan"
    display_name = "反正切"
    description = "反正切函数"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arc tangent.

        Args:
            context: Execution context.
            params: Dict with value, degrees, output_var.

        Returns:
            ActionResult with arc tangent value.
        """
        value = params.get('value', 0)
        degrees = params.get('degrees', True)
        output_var = params.get('output_var', 'atan_value')

        try:
            import math

            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_degrees = context.resolve_value(degrees) if degrees else True

            result = math.atan(resolved_value)

            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正切: {result}",
                data={
                    'value': resolved_value,
                    'degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反正切计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'degrees': True, 'output_var': 'atan_value'}