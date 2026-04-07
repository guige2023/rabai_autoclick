"""Hyperbolic action module for RabAI AutoClick.

Provides hyperbolic trigonometric operations:
- HyperbolicSinhAction: Hyperbolic sine
- HyperbolicCoshAction: Hyperbolic cosine
- HyperbolicTanhAction: Hyperbolic tangent
- HyperbolicAsinhAction: Hyperbolic arc sine
- HyperbolicAcoshAction: Hyperbolic arc cosine
- HyperbolicAtanhAction: Hyperbolic arc tangent
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HyperbolicSinhAction(BaseAction):
    """Hyperbolic sine."""
    action_type = "hyperbolic_sinh"
    display_name = "双曲正弦"
    description = "计算双曲正弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sinh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic sine.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'sinh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.sinh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲正弦: sinh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲正弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sinh_result'}


class HyperbolicCoshAction(BaseAction):
    """Hyperbolic cosine."""
    action_type = "hyperbolic_cosh"
    display_name = "双曲余弦"
    description = "计算双曲余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cosh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic cosine.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'cosh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.cosh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲余弦: cosh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲余弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cosh_result'}


class HyperbolicTanhAction(BaseAction):
    """Hyperbolic tangent."""
    action_type = "hyperbolic_tanh"
    display_name = "双曲正切"
    description = "计算双曲正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tanh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic tangent.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'tanh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.tanh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲正切: tanh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲正切失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tanh_result'}


class HyperbolicAsinhAction(BaseAction):
    """Hyperbolic arc sine."""
    action_type = "hyperbolic_asinh"
    display_name = "双曲反正弦"
    description = "计算双曲反正弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute asinh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic arc sine.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'asinh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            result = math.asinh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲反正弦: asinh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲反正弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'asinh_result'}


class HyperbolicAcoshAction(BaseAction):
    """Hyperbolic arc cosine."""
    action_type = "hyperbolic_acosh"
    display_name = "双曲反余弦"
    description = "计算双曲反余弦值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute acosh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic arc cosine.
        """
        value = params.get('value', 1)
        output_var = params.get('output_var', 'acosh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            if resolved_value < 1:
                return ActionResult(
                    success=False,
                    message="双曲反余弦参数必须>=1"
                )

            result = math.acosh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲反余弦: acosh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲反余弦失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'acosh_result'}


class HyperbolicAtanhAction(BaseAction):
    """Hyperbolic arc tangent."""
    action_type = "hyperbolic_atanh"
    display_name = "双曲反正切"
    description = "计算双曲反正切值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute atanh.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hyperbolic arc tangent.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'atanh_result')

        try:
            resolved_value = float(context.resolve_value(value))

            if abs(resolved_value) >= 1:
                return ActionResult(
                    success=False,
                    message="双曲反正切参数必须在(-1, 1)范围内"
                )

            result = math.atanh(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双曲反正切: atanh({resolved_value}) = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算双曲反正切失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'atanh_result'}
