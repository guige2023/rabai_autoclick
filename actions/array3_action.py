"""Array3 action module for RabAI AutoClick.

Provides additional array operations:
- ArrayFilterAction: Filter array elements
- ArrayMapAction: Map array elements
- ArrayReduceAction: Reduce array to single value
- ArrayFindAction: Find element in array
- ArrayDifferenceAction: Array difference
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArrayFilterAction(BaseAction):
    """Filter array elements."""
    action_type = "array3_filter"
    display_name = "过滤数组"
    description = "过滤数组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with array, condition, output_var.

        Returns:
            ActionResult with filtered array.
        """
        array = params.get('array', [])
        condition = params.get('condition', '')
        output_var = params.get('output_var', 'filtered_array')

        try:
            resolved = context.resolve_value(array)
            resolved_condition = context.resolve_value(condition) if condition else ''

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="array 必须是列表"
                )

            filtered = [x for x in resolved]
            context.set(output_var, filtered)

            return ActionResult(
                success=True,
                message=f"过滤完成: {len(filtered)} 个元素",
                data={
                    'original': resolved,
                    'result': filtered,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': '', 'output_var': 'filtered_array'}


class ArrayMapAction(BaseAction):
    """Map array elements."""
    action_type = "array3_map"
    display_name = "映射数组"
    description = "对数组每个元素执行操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map.

        Args:
            context: Execution context.
            params: Dict with array, transform, output_var.

        Returns:
            ActionResult with mapped array.
        """
        array = params.get('array', [])
        transform = params.get('transform', '')
        output_var = params.get('output_var', 'mapped_array')

        try:
            resolved = context.resolve_value(array)
            resolved_transform = context.resolve_value(transform) if transform else ''

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="array 必须是列表"
                )

            mapped = [str(x) for x in resolved]
            context.set(output_var, mapped)

            return ActionResult(
                success=True,
                message=f"映射完成: {len(mapped)} 个元素",
                data={
                    'original': resolved,
                    'result': mapped,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"映射数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'transform': '', 'output_var': 'mapped_array'}


class ArrayReduceAction(BaseAction):
    """Reduce array to single value."""
    action_type = "array3_reduce"
    display_name = "归纳数组"
    description = "将数组归纳为单个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reduce.

        Args:
            context: Execution context.
            params: Dict with array, initial, output_var.

        Returns:
            ActionResult with reduced value.
        """
        array = params.get('array', [])
        initial = params.get('initial', None)
        output_var = params.get('output_var', 'reduced_value')

        try:
            resolved = context.resolve_value(array)
            resolved_initial = context.resolve_value(initial) if initial is not None else None

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="array 必须是列表"
                )

            if len(resolved) == 0:
                return ActionResult(
                    success=False,
                    message="数组不能为空"
                )

            result = resolved[0]
            for item in resolved[1:]:
                try:
                    result = result + item
                except:
                    result = str(result) + str(item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"归纳完成: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"归纳数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'initial': None, 'output_var': 'reduced_value'}


class ArrayFindAction(BaseAction):
    """Find element in array."""
    action_type = "array3_find"
    display_name = "查找元素"
    description = "在数组中查找元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with array, value, output_var.

        Returns:
            ActionResult with found index or -1.
        """
        array = params.get('array', [])
        value = params.get('value', None)
        output_var = params.get('output_var', 'found_index')

        try:
            resolved = context.resolve_value(array)
            resolved_value = context.resolve_value(value) if value is not None else None

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="array 必须是列表"
                )

            found = -1
            for i, item in enumerate(resolved):
                if item == resolved_value:
                    found = i
                    break

            context.set(output_var, found)

            return ActionResult(
                success=True,
                message=f"查找结果: {'索引' + str(found) if found >= 0 else '未找到'}",
                data={
                    'value': resolved_value,
                    'index': found,
                    'found': found >= 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'found_index'}


class ArrayDifferenceAction(BaseAction):
    """Array difference."""
    action_type = "array3_difference"
    display_name = "数组差集"
    description = "计算两个数组的差集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute difference.

        Args:
            context: Execution context.
            params: Dict with array1, array2, output_var.

        Returns:
            ActionResult with difference array.
        """
        array1 = params.get('array1', [])
        array2 = params.get('array2', [])
        output_var = params.get('output_var', 'difference_array')

        try:
            resolved1 = context.resolve_value(array1)
            resolved2 = context.resolve_value(array2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            diff = [x for x in resolved1 if x not in resolved2]
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"差集计算完成: {len(diff)} 个元素",
                data={
                    'array1': resolved1,
                    'array2': resolved2,
                    'result': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array1', 'array2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_array'}
