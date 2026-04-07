"""Compare9 action module for RabAI AutoClick.

Provides additional comparison operations:
- CompareEqualAction: Check equality
- CompareGreaterAction: Check greater than
- CompareLessAction: Check less than
- CompareBetweenAction: Check if value between range
- CompareInAction: Check if value in collection
- CompareTypeAction: Check type
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompareEqualAction(BaseAction):
    """Check equality."""
    action_type = "compare9_equal"
    display_name = "等于比较"
    description = "检查是否相等"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute equal comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', '')
        value2 = params.get('value2', '')
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved1 = context.resolve_value(value1)
            resolved2 = context.resolve_value(value2)

            result = resolved1 == resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"等于比较: {'是' if result else '否'}",
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
                message=f"等于比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareGreaterAction(BaseAction):
    """Check greater than."""
    action_type = "compare9_greater"
    display_name = "大于比较"
    description = "检查是否大于"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute greater comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, strict, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        strict = params.get('strict', True)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved1 = context.resolve_value(value1)
            resolved2 = context.resolve_value(value2)
            resolved_strict = context.resolve_value(strict) if strict else True

            if resolved_strict:
                result = resolved1 > resolved2
            else:
                result = resolved1 >= resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大于比较: {'是' if result else '否'}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'strict': resolved_strict,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大于比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'strict': True, 'output_var': 'compare_result'}


class CompareLessAction(BaseAction):
    """Check less than."""
    action_type = "compare9_less"
    display_name = "小于比较"
    description = "检查是否小于"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute less comparison.

        Args:
            context: Execution context.
            params: Dict with value1, value2, strict, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        strict = params.get('strict', True)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved1 = context.resolve_value(value1)
            resolved2 = context.resolve_value(value2)
            resolved_strict = context.resolve_value(strict) if strict else True

            if resolved_strict:
                result = resolved1 < resolved2
            else:
                result = resolved1 <= resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"小于比较: {'是' if result else '否'}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'strict': resolved_strict,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"小于比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'strict': True, 'output_var': 'compare_result'}


class CompareBetweenAction(BaseAction):
    """Check if value between range."""
    action_type = "compare9_between"
    display_name = "范围比较"
    description = "检查值是否在范围内"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute between comparison.

        Args:
            context: Execution context.
            params: Dict with value, min, max, inclusive, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', 0)
        min_val = params.get('min', 0)
        max_val = params.get('max', 0)
        inclusive = params.get('inclusive', True)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_min = context.resolve_value(min_val)
            resolved_max = context.resolve_value(max_val)
            resolved_inclusive = context.resolve_value(inclusive) if inclusive else True

            if resolved_inclusive:
                result = resolved_min <= resolved_value <= resolved_max
            else:
                result = resolved_min < resolved_value < resolved_max

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'min': resolved_min,
                    'max': resolved_max,
                    'inclusive': resolved_inclusive,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'min', 'max']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'inclusive': True, 'output_var': 'compare_result'}


class CompareInAction(BaseAction):
    """Check if value in collection."""
    action_type = "compare9_in"
    display_name = "包含比较"
    description = "检查值是否在集合中"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute in comparison.

        Args:
            context: Execution context.
            params: Dict with value, collection, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', '')
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_collection = context.resolve_value(collection)

            if not isinstance(resolved_collection, (list, tuple, set, dict)):
                resolved_collection = [resolved_collection]

            if isinstance(resolved_collection, dict):
                result = resolved_value in resolved_collection.keys()
            else:
                result = resolved_value in resolved_collection

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'collection': resolved_collection,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"包含比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareTypeAction(BaseAction):
    """Check type."""
    action_type = "compare9_type"
    display_name = "类型比较"
    description = "检查值的类型"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute type comparison.

        Args:
            context: Execution context.
            params: Dict with value, type_name, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', '')
        type_name = params.get('type_name', 'str')
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_type = context.resolve_value(type_name)

            type_mapping = {
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'list': list,
                'tuple': tuple,
                'dict': dict,
                'set': set,
                'none': type(None)
            }

            expected_type = type_mapping.get(resolved_type, str)
            result = isinstance(resolved_value, expected_type)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'expected_type': resolved_type,
                    'actual_type': type(resolved_value).__name__,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'type_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}