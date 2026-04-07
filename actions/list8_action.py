"""List8 action module for RabAI AutoClick.

Provides additional list operations:
- ListAppendAction: Append to list
- ListExtendAction: Extend list
- ListInsertAction: Insert into list
- ListRemoveAction: Remove from list
- ListPopAction: Pop from list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListAppendAction(BaseAction):
    """Append to list."""
    action_type = "list8_append"
    display_name = "列表追加"
    description = "向列表追加元素"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append.

        Args:
            context: Execution context.
            params: Dict with list, item, output_var.

        Returns:
            ActionResult with appended list.
        """
        list_param = params.get('list', [])
        item = params.get('item', None)
        output_var = params.get('output_var', 'appended_list')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_list, list):
                resolved_list = []

            result = list(resolved_list)
            result.append(resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表追加完成: {len(result)} 项",
                data={
                    'original': resolved_list,
                    'item': resolved_item,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'appended_list'}


class ListExtendAction(BaseAction):
    """Extend list."""
    action_type = "list8_extend"
    display_name = "列表扩展"
    description = "扩展列表"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extend.

        Args:
            context: Execution context.
            params: Dict with list, items, output_var.

        Returns:
            ActionResult with extended list.
        """
        list_param = params.get('list', [])
        items = params.get('items', [])
        output_var = params.get('output_var', 'extended_list')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_items = context.resolve_value(items)

            if not isinstance(resolved_list, list):
                resolved_list = []
            if not isinstance(resolved_items, (list, tuple)):
                resolved_items = [resolved_items]

            result = list(resolved_list) + list(resolved_items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表扩展完成: {len(result)} 项",
                data={
                    'original': resolved_list,
                    'items': resolved_items,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表扩展失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'extended_list'}


class ListInsertAction(BaseAction):
    """Insert into list."""
    action_type = "list8_insert"
    display_name = "列表插入"
    description = "在列表指定位置插入"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute insert.

        Args:
            context: Execution context.
            params: Dict with list, index, item, output_var.

        Returns:
            ActionResult with inserted list.
        """
        list_param = params.get('list', [])
        index = params.get('index', 0)
        item = params.get('item', None)
        output_var = params.get('output_var', 'inserted_list')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_index = int(context.resolve_value(index))
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_list, list):
                resolved_list = []

            result = list(resolved_list)
            result.insert(resolved_index, resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表插入完成: 位置 {resolved_index}",
                data={
                    'original': resolved_list,
                    'index': resolved_index,
                    'item': resolved_item,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表插入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'index', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inserted_list'}


class ListRemoveAction(BaseAction):
    """Remove from list."""
    action_type = "list8_remove"
    display_name = "列表删除"
    description = "从列表删除元素"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove.

        Args:
            context: Execution context.
            params: Dict with list, item, output_var.

        Returns:
            ActionResult with removed list.
        """
        list_param = params.get('list', [])
        item = params.get('item', None)
        output_var = params.get('output_var', 'removed_list')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_list, list):
                return ActionResult(
                    success=False,
                    message=f"列表删除失败: 输入不是列表"
                )

            result = list(resolved_list)
            result.remove(resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表删除完成: {len(result)} 项",
                data={
                    'original': resolved_list,
                    'item': resolved_item,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"列表删除失败: 元素不存在"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'removed_list'}


class ListPopAction(BaseAction):
    """Pop from list."""
    action_type = "list8_pop"
    display_name = "列表弹出"
    description = "弹出列表元素"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with list, index, output_var.

        Returns:
            ActionResult with popped item and list.
        """
        list_param = params.get('list', [])
        index = params.get('index', -1)
        output_var = params.get('output_var', 'pop_result')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_index = int(context.resolve_value(index))

            if not isinstance(resolved_list, list):
                return ActionResult(
                    success=False,
                    message=f"列表弹出失败: 输入不是列表"
                )

            result = list(resolved_list)
            popped = result.pop(resolved_index)
            context.set(output_var, popped)

            return ActionResult(
                success=True,
                message=f"列表弹出: {popped}",
                data={
                    'original': resolved_list,
                    'popped': popped,
                    'remaining': result,
                    'output_var': output_var
                }
            )
        except IndexError:
            return ActionResult(
                success=False,
                message=f"列表弹出失败: 索引超出范围"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表弹出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'index': -1, 'output_var': 'pop_result'}