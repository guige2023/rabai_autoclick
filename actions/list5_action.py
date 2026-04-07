"""List5 action module for RabAI AutoClick.

Provides additional list operations:
- ListExtendAction: Extend list
- ListCopyAction: Copy list
- ListConcatAction: Concatenate lists
- ListSliceAction: Slice list
- ListSliceFromEndAction: Slice from end
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListExtendAction(BaseAction):
    """Extend list."""
    action_type = "list5_extend"
    display_name = "扩展列表"
    description = "向列表添加多个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extend.

        Args:
            context: Execution context.
            params: Dict with list_var, items, output_var.

        Returns:
            ActionResult with extended list.
        """
        list_var = params.get('list_var', '')
        items = params.get('items', [])
        output_var = params.get('output_var', 'extended_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_items = context.resolve_value(items)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            if isinstance(resolved_items, (list, tuple)):
                lst.extend(resolved_items)
            else:
                lst.append(resolved_items)

            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"扩展列表: +{len(resolved_items) if isinstance(resolved_items, (list, tuple)) else 1} 个元素",
                data={
                    'result': lst,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扩展列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'extended_list'}


class ListCopyAction(BaseAction):
    """Copy list."""
    action_type = "list5_copy"
    display_name = "复制列表"
    description = "复制列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with copied list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'copied_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            result = lst.copy()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"复制列表: {len(result)} 个元素",
                data={
                    'original': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copied_list'}


class ListConcatAction(BaseAction):
    """Concatenate lists."""
    action_type = "list5_concat"
    display_name = "连接列表"
    description = "连接两个列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute concat.

        Args:
            context: Execution context.
            params: Dict with list1, list2, output_var.

        Returns:
            ActionResult with concatenated list.
        """
        list1 = params.get('list1', [])
        list2 = params.get('list2', [])
        output_var = params.get('output_var', 'concatenated_list')

        try:
            resolved1 = context.resolve_value(list1)
            resolved2 = context.resolve_value(list2)

            if isinstance(resolved1, str):
                resolved1 = [resolved1]
            if isinstance(resolved2, str):
                resolved2 = [resolved2]

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            result = list(resolved1) + list(resolved2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接列表: {len(result)} 个元素",
                data={
                    'list1': resolved1,
                    'list2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list1', 'list2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'concatenated_list'}


class ListSliceAction(BaseAction):
    """Slice list."""
    action_type = "list5_slice"
    display_name = "切片列表"
    description = "对列表进行切片"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with list_var, start, end, step, output_var.

        Returns:
            ActionResult with sliced list.
        """
        list_var = params.get('list_var', '')
        start = params.get('start', None)
        end = params.get('end', None)
        step = params.get('step', None)
        output_var = params.get('output_var', 'sliced_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            resolved_start = int(context.resolve_value(start)) if start is not None else None
            resolved_end = int(context.resolve_value(end)) if end is not None else None
            resolved_step = int(context.resolve_value(step)) if step is not None else None

            result = lst[resolved_start:resolved_end:resolved_step]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"切片列表: {len(result)} 个元素",
                data={
                    'original': lst,
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': None, 'end': None, 'step': None, 'output_var': 'sliced_list'}


class ListSliceFromEndAction(BaseAction):
    """Slice from end."""
    action_type = "list5_slice_from_end"
    display_name = "从末尾切片"
    description = "从列表末尾取n个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice from end.

        Args:
            context: Execution context.
            params: Dict with list_var, count, output_var.

        Returns:
            ActionResult with sliced list.
        """
        list_var = params.get('list_var', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'sliced_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            resolved_count = int(context.resolve_value(count))
            result = lst[-resolved_count:] if resolved_count > 0 else []
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"从末尾切片: {len(result)} 个元素",
                data={
                    'original': lst,
                    'count': resolved_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从末尾切片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sliced_list'}
