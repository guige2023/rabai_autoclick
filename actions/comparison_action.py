"""Comparison action module for RabAI AutoClick.

Provides comparison operations:
- CompareEqualAction: Check if values are equal
- CompareNotEqualAction: Check if values are not equal
- CompareGreaterAction: Check if greater than
- CompareLessAction: Check if less than
- CompareGreaterEqualAction: Check if greater than or equal
- CompareLessEqualAction: Check if less than or equal
- CompareIsEmptyAction: Check if value is empty
- CompareIsNoneAction: Check if value is None
- CompareIsInAction: Check if value is in collection
- CompareTypeAction: Check type of value
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CompareEqualAction(BaseAction):
    """Check if values are equal."""
    action_type = "compare_equal"
    display_name = "比较等于"
    description = "检查两个值是否相等"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute equality comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = resolved_v1 == resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'等于' if result else '不等于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareNotEqualAction(BaseAction):
    """Check if values are not equal."""
    action_type = "compare_not_equal"
    display_name = "比较不等于"
    description = "检查两个值是否不相等"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inequality comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = resolved_v1 != resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'不等于' if result else '等于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareGreaterAction(BaseAction):
    """Check if value is greater than another."""
    action_type = "compare_greater"
    display_name = "比较大于"
    description = "检查值是否大于另一个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute greater than comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = float(resolved_v1) > float(resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'大于' if result else '不大于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"比较失败: 无法比较这些值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareLessAction(BaseAction):
    """Check if value is less than another."""
    action_type = "compare_less"
    display_name = "比较小于"
    description = "检查值是否小于另一个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute less than comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = float(resolved_v1) < float(resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'小于' if result else '不小于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"比较失败: 无法比较这些值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareGreaterEqualAction(BaseAction):
    """Check if value is greater than or equal to another."""
    action_type = "compare_greater_equal"
    display_name = "比较大于等于"
    description = "检查值是否大于或等于另一个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute greater than or equal comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = float(resolved_v1) >= float(resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'大于等于' if result else '小于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"比较失败: 无法比较这些值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareLessEqualAction(BaseAction):
    """Check if value is less than or equal to another."""
    action_type = "compare_less_equal"
    display_name = "比较小于等于"
    description = "检查值是否小于或等于另一个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute less than or equal comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)
            result = float(resolved_v1) <= float(resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {'小于等于' if result else '大于'}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"比较失败: 无法比较这些值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareIsEmptyAction(BaseAction):
    """Check if value is empty."""
    action_type = "compare_is_empty"
    display_name = "比较为空"
    description = "检查值是否为空"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is empty check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved = context.resolve_value(value)

            if resolved is None:
                result = True
            elif isinstance(resolved, (str, list, tuple, dict, set)):
                result = len(resolved) == 0
            else:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"为空检查: {'是' if result else '否'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareIsNoneAction(BaseAction):
    """Check if value is None."""
    action_type = "compare_is_none"
    display_name = "比较为None"
    description = "检查值是否为None"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is None check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved = context.resolve_value(value)
            result = resolved is None
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"None检查: {'是' if result else '否'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareIsInAction(BaseAction):
    """Check if value is in collection."""
    action_type = "compare_is_in"
    display_name = "比较包含"
    description = "检查值是否在集合中"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is in check.

        Args:
            context: Execution context.
            params: Dict with value, collection, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_collection = context.resolve_value(collection)

            if isinstance(resolved_collection, (list, tuple, set, str)):
                result = resolved_value in resolved_collection
            elif isinstance(resolved_collection, dict):
                result = resolved_value in resolved_collection
            else:
                return ActionResult(
                    success=False,
                    message="collection参数必须是列表、元组、集合、字典或字符串"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含检查: {'是' if result else '否'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareTypeAction(BaseAction):
    """Check type of value."""
    action_type = "compare_type"
    display_name = "比较类型"
    description = "检查值的类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute type check.

        Args:
            context: Execution context.
            params: Dict with value, expected_type, output_var.

        Returns:
            ActionResult with type check result.
        """
        value = params.get('value', None)
        expected_type = params.get('expected_type', 'string')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(expected_type, str, 'expected_type')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_type = context.resolve_value(expected_type)

            type_mapping = {
                'string': str,
                'int': int,
                'integer': int,
                'float': float,
                'number': (int, float),
                'bool': bool,
                'boolean': bool,
                'list': list,
                'tuple': tuple,
                'dict': dict,
                'dictionary': dict,
                'set': set,
                'none': type(None),
            }

            expected = type_mapping.get(resolved_type.lower(), str)
            result = isinstance(resolved_value, expected)
            actual_type = type(resolved_value).__name__

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型检查: {'匹配' if result else f'不匹配 ({actual_type})'}",
                data={
                    'result': result,
                    'expected': resolved_type,
                    'actual': actual_type,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'expected_type']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}