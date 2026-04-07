"""Compare action module for RabAI AutoClick.

Provides comparison operations:
- CompareEqualAction: Check if equal
- CompareNotEqualAction: Check if not equal
- CompareGreaterAction: Check if greater
- CompareLessAction: Check if less
- CompareInRangeAction: Check if in range
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompareEqualAction(BaseAction):
    """Check if equal."""
    action_type = "compare_equal"
    display_name = "判断相等"
    description = "判断两个值是否相等"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute equal check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with equal result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'equal_result')

        try:
            resolved1 = context.resolve_value(value1)
            resolved2 = context.resolve_value(value2)
            result = resolved1 == resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断相等: {'是' if result else '否'}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断相等失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'equal_result'}


class CompareNotEqualAction(BaseAction):
    """Check if not equal."""
    action_type = "compare_not_equal"
    display_name = "判断不相等"
    description = "判断两个值是否不相等"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute not equal check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with not equal result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'not_equal_result')

        try:
            resolved1 = context.resolve_value(value1)
            resolved2 = context.resolve_value(value2)
            result = resolved1 != resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断不相等: {'是' if result else '否'}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断不相等失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'not_equal_result'}


class CompareGreaterAction(BaseAction):
    """Check if greater."""
    action_type = "compare_greater"
    display_name = "判断大于"
    description = "判断第一个值是否大于第二个值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute greater check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with greater result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'greater_result')

        try:
            resolved1 = float(context.resolve_value(value1))
            resolved2 = float(context.resolve_value(value2))
            result = resolved1 > resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断大于: {'是' if result else '否'} ({resolved1} > {resolved2})",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断大于失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'greater_result'}


class CompareLessAction(BaseAction):
    """Check if less."""
    action_type = "compare_less"
    display_name = "判断小于"
    description = "判断第一个值是否小于第二个值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute less check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with less result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'less_result')

        try:
            resolved1 = float(context.resolve_value(value1))
            resolved2 = float(context.resolve_value(value2))
            result = resolved1 < resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断小于: {'是' if result else '否'} ({resolved1} < {resolved2})",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断小于失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'less_result'}


class CompareInRangeAction(BaseAction):
    """Check if in range."""
    action_type = "compare_in_range"
    display_name = "判断在范围内"
    description = "判断值是否在指定范围内"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute in range check.

        Args:
            context: Execution context.
            params: Dict with value, min, max, output_var.

        Returns:
            ActionResult with in range result.
        """
        value = params.get('value', 0)
        min_val = params.get('min', 0)
        max_val = params.get('max', 100)
        output_var = params.get('output_var', 'in_range_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_min = float(context.resolve_value(min_val))
            resolved_max = float(context.resolve_value(max_val))
            result = resolved_min <= resolved_value <= resolved_max
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断在范围内: {'是' if result else '否'}",
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
                message=f"判断在范围内失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'min', 'max']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'in_range_result'}