"""Math9 action module for RabAI AutoClick.

Provides additional math operations:
- MathTanhAction: Hyperbolic tangent
- MathCopiSignAction: Copy sign
- MathIsFiniteAction: Check if finite
- MathIsInfAction: Check if infinite
- MathIsNanAction: Check if NaN
"""

import math
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathTanhAction(BaseAction):
    """Hyperbolic tangent."""
    action_type = "math9_tanh"
    display_name = "双曲正切"
    description = "计算双曲正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hyperbolic tangent.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic tangent.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'tanh_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.tanh(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲正切: tanh({resolved}) = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双曲正切计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tanh_result'}


class MathCopiSignAction(BaseAction):
    """Copy sign."""
    action_type = "math9_copysign"
    display_name = "复制符号"
    description = "复制数值符号"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy sign.

        Args:
            context: Execution context.
            params: Dict with x, y, output_var.

        Returns:
            ActionResult with copied sign.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        output_var = params.get('output_var', 'copysign_result')

        try:
            resolved_x = float(context.resolve_value(x))
            resolved_y = float(context.resolve_value(y))
            result = math.copysign(resolved_x, resolved_y)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"复制符号: copysign({resolved_x}, {resolved_y}) = {result}",
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
                message=f"复制符号失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copysign_result'}


class MathIsFiniteAction(BaseAction):
    """Check if finite."""
    action_type = "math9_isfinite"
    display_name = "判断有限"
    description = "判断数值是否有限"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is finite.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with finite check.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'isfinite_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.isfinite(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"有限判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断有限失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isfinite_result'}


class MathIsInfAction(BaseAction):
    """Check if infinite."""
    action_type = "math9_isinf"
    display_name = "判断无穷"
    description = "判断数值是否无穷"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is infinite.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with infinite check.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'isinf_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.isinf(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"无穷判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断无穷失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isinf_result'}


class MathIsNanAction(BaseAction):
    """Check if NaN."""
    action_type = "math9_isnan"
    display_name = "判断NaN"
    description = "判断数值是否为NaN"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is NaN.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with NaN check.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'isnan_result')

        try:
            resolved = float(context.resolve_value(value))
            result = math.isnan(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"NaN判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断NaN失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isnan_result'}
