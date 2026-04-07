"""Sort2 action module for RabAI AutoClick.

Provides additional sorting operations:
- SortBubbleAction: Bubble sort
- SortQuickAction: Quick sort
- SortMergeAction: Merge sort
- SortInsertionAction: Insertion sort
- SortSelectionAction: Selection sort
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortBubbleAction(BaseAction):
    """Bubble sort."""
    action_type = "sort2_bubble"
    display_name = "冒泡排序"
    description = "使用冒泡排序算法"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bubble sort.

        Args:
            context: Execution context.
            params: Dict with list, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        input_list = params.get('list', [])
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            n = len(result)
            reversed_order = bool(context.resolve_value(reverse)) if reverse else False

            for i in range(n):
                for j in range(0, n - i - 1):
                    if reversed_order:
                        if result[j] < result[j + 1]:
                            result[j], result[j + 1] = result[j + 1], result[j]
                    else:
                        if result[j] > result[j + 1]:
                            result[j], result[j + 1] = result[j + 1], result[j]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"冒泡排序完成: {len(result)}个元素",
                data={
                    'original': resolved,
                    'sorted': result,
                    'algorithm': 'bubble',
                    'reverse': reversed_order,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"冒泡排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}


class SortQuickAction(BaseAction):
    """Quick sort."""
    action_type = "sort2_quick"
    display_name = "快速排序"
    description = "使用快速排序算法"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quick sort.

        Args:
            context: Execution context.
            params: Dict with list, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        input_list = params.get('list', [])
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            reversed_order = bool(context.resolve_value(reverse)) if reverse else False

            def quicksort(arr):
                if len(arr) <= 1:
                    return arr
                pivot = arr[len(arr) // 2]
                left = [x for x in arr if x < pivot]
                middle = [x for x in arr if x == pivot]
                right = [x for x in arr if x > pivot]
                return quicksort(left) + middle + quicksort(right)

            result = quicksort(result)

            if reversed_order:
                result = result[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"快速排序完成: {len(result)}个元素",
                data={
                    'original': resolved,
                    'sorted': result,
                    'algorithm': 'quick',
                    'reverse': reversed_order,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"快速排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}


class SortMergeAction(BaseAction):
    """Merge sort."""
    action_type = "sort2_merge"
    display_name = "归并排序"
    description = "使用归并排序算法"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge sort.

        Args:
            context: Execution context.
            params: Dict with list, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        input_list = params.get('list', [])
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            reversed_order = bool(context.resolve_value(reverse)) if reverse else False

            def merge_sort(arr):
                if len(arr) <= 1:
                    return arr
                mid = len(arr) // 2
                left = merge_sort(arr[:mid])
                right = merge_sort(arr[mid:])
                return merge(left, right)

            def merge(left, right):
                result = []
                i = j = 0
                while i < len(left) and j < len(right):
                    if left[i] <= right[j]:
                        result.append(left[i])
                        i += 1
                    else:
                        result.append(right[j])
                        j += 1
                result.extend(left[i:])
                result.extend(right[j:])
                return result

            result = merge_sort(result)

            if reversed_order:
                result = result[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"归并排序完成: {len(result)}个元素",
                data={
                    'original': resolved,
                    'sorted': result,
                    'algorithm': 'merge',
                    'reverse': reversed_order,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"归并排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}


class SortInsertionAction(BaseAction):
    """Insertion sort."""
    action_type = "sort2_insertion"
    display_name = "插入排序"
    description = "使用插入排序算法"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute insertion sort.

        Args:
            context: Execution context.
            params: Dict with list, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        input_list = params.get('list', [])
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            reversed_order = bool(context.resolve_value(reverse)) if reverse else False

            for i in range(1, len(result)):
                key = result[i]
                j = i - 1
                while j >= 0 and result[j] > key:
                    result[j + 1] = result[j]
                    j -= 1
                result[j + 1] = key

            if reversed_order:
                result = result[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"插入排序完成: {len(result)}个元素",
                data={
                    'original': resolved,
                    'sorted': result,
                    'algorithm': 'insertion',
                    'reverse': reversed_order,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"插入排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}


class SortSelectionAction(BaseAction):
    """Selection sort."""
    action_type = "sort2_selection"
    display_name = "选择排序"
    description = "使用选择排序算法"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute selection sort.

        Args:
            context: Execution context.
            params: Dict with list, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        input_list = params.get('list', [])
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            n = len(result)
            reversed_order = bool(context.resolve_value(reverse)) if reverse else False

            for i in range(n):
                extremum_idx = i
                for j in range(i + 1, n):
                    if reversed_order:
                        if result[j] > result[extremum_idx]:
                            extremum_idx = j
                    else:
                        if result[j] < result[extremum_idx]:
                            extremum_idx = j
                result[i], result[extremum_idx] = result[extremum_idx], result[i]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"选择排序完成: {len(result)}个元素",
                data={
                    'original': resolved,
                    'sorted': result,
                    'algorithm': 'selection',
                    'reverse': reversed_order,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"选择排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}