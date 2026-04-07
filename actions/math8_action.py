"""Math8 action module for RabAI AutoClick.

Provides additional math operations:
- MathAcosAction: Arccosine
- MathAtanAction: Arctangent
- MathAtan2Action: Arctangent2
- MathCoshAction: Hyperbolic cosine
- MathSinhAction: Hyperbolic sine
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathAcosAction(BaseAction):
    """Arccosine."""
    action_type = "math8_acos"
    display_name = "反余弦"
    description = "计算反余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arccosine.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with arccosine.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'acos_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            if abs(resolved) > 1:
                return ActionResult(
                    success=False,
                    message="反余弦参数必须在[-1, 1]范围内"
                )

            result = math.acos(resolved)
            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反余弦: acos({resolved}) = {result}",
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
                message=f"反余弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'acos_result'}


class MathAtanAction(BaseAction):
    """Arctangent."""
    action_type = "math8_atan"
    display_name = "反正切"
    description = "计算反正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arctangent.

        Args:
            context: Execution context.
            params: Dict with value, is_degrees, output_var.

        Returns:
            ActionResult with arctangent.
        """
        value = params.get('value', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'atan_result')

        try:
            resolved = float(context.resolve_value(value))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            result = math.atan(resolved)
            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反正切: atan({resolved}) = {result}",
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
                message=f"反正切计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'atan_result'}


class MathAtan2Action(BaseAction):
    """Arctangent2."""
    action_type = "math8_atan2"
    display_name = "二维反正切"
    description = "计算二维反正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute arctangent2.

        Args:
            context: Execution context.
            params: Dict with y, x, is_degrees, output_var.

        Returns:
            ActionResult with arctangent2.
        """
        y = params.get('y', 0)
        x = params.get('x', 0)
        is_degrees = params.get('is_degrees', False)
        output_var = params.get('output_var', 'atan2_result')

        try:
            resolved_y = float(context.resolve_value(y))
            resolved_x = float(context.resolve_value(x))
            resolved_degrees = bool(context.resolve_value(is_degrees)) if is_degrees else False

            result = math.atan2(resolved_y, resolved_x)
            if resolved_degrees:
                result = math.degrees(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"二维反正切: atan2({resolved_y}, {resolved_x}) = {result}",
                data={
                    'y': resolved_y,
                    'x': resolved_x,
                    'is_degrees': resolved_degrees,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"二维反正切计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['y', 'x']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'is_degrees': False, 'output_var': 'atan2_result'}


class MathCoshAction(BaseAction):
    """Hyperbolic cosine."""
    action_type = "math8_cosh"
    display_name = "双曲余弦"
    description = "计算双曲余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hyperbolic cosine.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic cosine.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'cosh_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.cosh(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲余弦: cosh({resolved}) = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双曲余弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cosh_result'}


class MathSinhAction(BaseAction):
    """Hyperbolic sine."""
    action_type = "math8_sinh"
    display_name = "双曲正弦"
    description = "计算双曲正弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hyperbolic sine.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic sine.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'sinh_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.sinh(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲正弦: sinh({resolved}) = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双曲正弦计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sinh_result'}
