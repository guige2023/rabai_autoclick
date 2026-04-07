"""List3 action module for RabAI AutoClick.

Provides additional list operations:
- ListAppendAction: Append element
- ListExtendAction: Extend list
- ListInsertAction: Insert element
- ListPopAction: Pop element
- ListSortAction: Sort list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListAppendAction(BaseAction):
    """Append element."""
    action_type = "list3_append"
    display_name = "追加元素"
    description = "向列表追加元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list append.

        Args:
            context: Execution context.
            params: Dict with list_var, item, output_var.

        Returns:
            ActionResult with modified list.
        """
        list_var = params.get('list_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'list_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            lst.append(resolved_item)
            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"追加元素: {resolved_item}",
                data={
                    'item': resolved_item,
                    'result': lst,
                    'count': len(lst),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追加元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_result'}


class ListExtendAction(BaseAction):
    """Extend list."""
    action_type = "list3_extend"
    display_name = "扩展列表"
    description = "扩展列表添加多个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list extend.

        Args:
            context: Execution context.
            params: Dict with list_var, items, output_var.

        Returns:
            ActionResult with modified list.
        """
        list_var = params.get('list_var', '')
        items = params.get('items', [])
        output_var = params.get('output_var', 'list_result')

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

            if not isinstance(resolved_items, list):
                resolved_items = [resolved_items]

            lst.extend(resolved_items)
            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"扩展列表: +{len(resolved_items)} 个元素",
                data={
                    'items': resolved_items,
                    'result': lst,
                    'count': len(lst),
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
        return {'output_var': 'list_result'}


class ListInsertAction(BaseAction):
    """Insert element."""
    action_type = "list3_insert"
    display_name = "插入元素"
    description = "在指定位置插入元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list insert.

        Args:
            context: Execution context.
            params: Dict with list_var, index, item, output_var.

        Returns:
            ActionResult with modified list.
        """
        list_var = params.get('list_var', '')
        index = params.get('index', 0)
        item = params.get('item', None)
        output_var = params.get('output_var', 'list_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_index = int(context.resolve_value(index))
            resolved_item = context.resolve_value(item) if item is not None else None

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            lst.insert(resolved_index, resolved_item)
            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"插入元素: [{resolved_index}] = {resolved_item}",
                data={
                    'index': resolved_index,
                    'item': resolved_item,
                    'result': lst,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"插入元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'index', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_result'}


class ListPopAction(BaseAction):
    """Pop element."""
    action_type = "list3_pop"
    display_name = "弹出元素"
    description = "弹出并返回指定位置的元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list pop.

        Args:
            context: Execution context.
            params: Dict with list_var, index, output_var.

        Returns:
            ActionResult with popped element.
        """
        list_var = params.get('list_var', '')
        index = params.get('index', -1)
        output_var = params.get('output_var', 'popped_element')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_index = int(context.resolve_value(index))

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            if not lst:
                return ActionResult(
                    success=False,
                    message="列表为空"
                )

            result = lst.pop(resolved_index)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"弹出元素: {result}",
                data={
                    'item': result,
                    'index': resolved_index,
                    'result': lst,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"弹出元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'index': -1, 'output_var': 'popped_element'}


class ListSortAction(BaseAction):
    """Sort list."""
    action_type = "list3_sort"
    display_name = "排序列表"
    description = "对列表排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list sort.

        Args:
            context: Execution context.
            params: Dict with list_var, reverse, key, output_var.

        Returns:
            ActionResult with sorted list.
        """
        list_var = params.get('list_var', '')
        reverse = params.get('reverse', False)
        key = params.get('key', None)
        output_var = params.get('output_var', 'sorted_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_reverse = context.resolve_value(reverse) if reverse else False
            resolved_key = context.resolve_value(key) if key else None

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            sorted_list = sorted(lst, key=resolved_key, reverse=resolved_reverse)
            context.set(output_var, sorted_list)

            return ActionResult(
                success=True,
                message=f"排序列表: {len(sorted_list)} 个元素",
                data={
                    'original': lst,
                    'result': sorted_list,
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
        return {'reverse': False, 'key': None, 'output_var': 'sorted_list'}