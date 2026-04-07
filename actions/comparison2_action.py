"""Comparison2 action module for RabAI AutoClick.

Provides additional comparison operations:
- ComparisonEqualAction: Check equality
- ComparisonNotEqualAction: Check inequality
- ComparisonGreaterAction: Check greater than
- ComparisonLessAction: Check less than
- ComparisonGreaterEqualAction: Check greater or equal
- ComparisonLessEqualAction: Check less or equal
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ComparisonEqualAction(BaseAction):
    """Check equality."""
    action_type = "comparison_equal"
    display_name = "等于"
    description = "检查两个值是否相等"

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
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'equal_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)

            result = resolved_v1 == resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"等于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'equal_result'}


class ComparisonNotEqualAction(BaseAction):
    """Check inequality."""
    action_type = "comparison_not_equal"
    display_name = "不等于"
    description = "检查两个值是否不相等"

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
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'not_equal_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)

            result = resolved_v1 != resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"不等于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"不等于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'not_equal_result'}


class ComparisonGreaterAction(BaseAction):
    """Check greater than."""
    action_type = "comparison_greater"
    display_name = "大于"
    description = "检查第一个值是否大于第二个值"

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
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'greater_result')

        try:
            resolved_v1 = float(context.resolve_value(value1))
            resolved_v2 = float(context.resolve_value(value2))

            result = resolved_v1 > resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message="比较操作需要数值类型"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'greater_result'}


class ComparisonLessAction(BaseAction):
    """Check less than."""
    action_type = "comparison_less"
    display_name = "小于"
    description = "检查第一个值是否小于第二个值"

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
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'less_result')

        try:
            resolved_v1 = float(context.resolve_value(value1))
            resolved_v2 = float(context.resolve_value(value2))

            result = resolved_v1 < resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"小于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message="比较操作需要数值类型"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"小于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'less_result'}


class ComparisonGreaterEqualAction(BaseAction):
    """Check greater or equal."""
    action_type = "comparison_greater_equal"
    display_name = "大于等于"
    description = "检查第一个值是否大于或等于第二个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute greater or equal check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'greater_equal_result')

        try:
            resolved_v1 = float(context.resolve_value(value1))
            resolved_v2 = float(context.resolve_value(value2))

            result = resolved_v1 >= resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大于等于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message="比较操作需要数值类型"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大于等于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'greater_equal_result'}


class ComparisonLessEqualAction(BaseAction):
    """Check less or equal."""
    action_type = "comparison_less_equal"
    display_name = "小于等于"
    description = "检查第一个值是否小于或等于第二个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute less or equal check.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with check result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'less_equal_result')

        try:
            resolved_v1 = float(context.resolve_value(value1))
            resolved_v2 = float(context.resolve_value(value2))

            result = resolved_v1 <= resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"小于等于: {'是' if result else '否'}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message="比较操作需要数值类型"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"小于等于检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'less_equal_result'}