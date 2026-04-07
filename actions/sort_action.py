"""Sort action module for RabAI AutoClick.

Provides sorting operations:
- SortListAction: Sort list
- SortReverseAction: Reverse list
- SortUniqueAction: Get unique sorted list
- SortByKeyAction: Sort by key function
- SortBubbleAction: Bubble sort
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortListAction(BaseAction):
    """Sort list."""
    action_type = "sort_list"
    display_name = "排序列表"
    description = "对列表排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with list_var, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        list_var = params.get('list_var', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_reverse = bool(context.resolve_value(reverse))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = sorted(list(items), reverse=resolved_reverse)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表排序: {len(result)} 项",
                data={
                    'count': len(result),
                    'reverse': resolved_reverse,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"排序列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_list'}


class SortReverseAction(BaseAction):
    """Reverse list."""
    action_type = "sort_reverse"
    display_name = "反转列表"
    description = "反转列表顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with reversed list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'reversed_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = list(items)[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表反转: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_list'}


class SortUniqueAction(BaseAction):
    """Get unique sorted list."""
    action_type = "sort_unique"
    display_name = "去重排序"
    description = "获取去重后的排序列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unique.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with unique sorted list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'unique_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = sorted(set(items))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去重排序: {len(result)} 项 (原 {len(items)} 项)",
                data={
                    'original_count': len(items),
                    'unique_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去重排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unique_list'}


class SortByKeyAction(BaseAction):
    """Sort by key function."""
    action_type = "sort_by_key"
    display_name = "按键排序"
    description = "根据键函数排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort by key.

        Args:
            context: Execution context.
            params: Dict with list_var, key_func, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        list_var = params.get('list_var', '')
        key_func = params.get('key_func', 'lambda x: x')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_by_key')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_key = context.resolve_value(key_func)
            resolved_reverse = bool(context.resolve_value(reverse))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            key_fn = context.safe_exec(f"return_value = {resolved_key}")
            result = sorted(list(items), key=key_fn, reverse=resolved_reverse)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按键排序: {len(result)} 项",
                data={
                    'count': len(result),
                    'key_func': resolved_key,
                    'reverse': resolved_reverse,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按键排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_func': 'lambda x: x', 'reverse': False, 'output_var': 'sorted_by_key'}


class SortBubbleAction(BaseAction):
    """Bubble sort."""
    action_type = "sort_bubble"
    display_name = "冒泡排序"
    description = "使用冒泡排序算法排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bubble sort.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with sorted list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'bubble_sorted')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = list(context.get(resolved_var))
            if not isinstance(items, list):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            n = len(items)
            for i in range(n):
                for j in range(0, n - i - 1):
                    if items[j] > items[j + 1]:
                        items[j], items[j + 1] = items[j + 1], items[j]

            context.set(output_var, items)

            return ActionResult(
                success=True,
                message=f"冒泡排序完成: {len(items)} 项",
                data={
                    'count': len(items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"冒泡排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bubble_sorted'}
