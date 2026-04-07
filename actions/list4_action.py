"""List4 action module for RabAI AutoClick.

Provides additional list operations:
- ListIndexAction: Find element index
- ListCountAction: Count element occurrences
- ListRemoveAction: Remove element by value
- ListReverseAction: Reverse list
- ListContainsAction: Check if list contains element
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListIndexAction(BaseAction):
    """Find element index."""
    action_type = "list4_index"
    display_name = "查找元素索引"
    description = "查找元素在列表中的索引位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list index.

        Args:
            context: Execution context.
            params: Dict with list_var, item, output_var.

        Returns:
            ActionResult with index.
        """
        list_var = params.get('list_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'index_result')

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

            try:
                index = lst.index(resolved_item)
                found = True
            except ValueError:
                index = -1
                found = False

            context.set(output_var, index)

            return ActionResult(
                success=True,
                message=f"元素索引: {index}",
                data={
                    'item': resolved_item,
                    'index': index,
                    'found': found,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找元素索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class ListCountAction(BaseAction):
    """Count element occurrences."""
    action_type = "list4_count"
    display_name = "计数元素"
    description = "统计元素在列表中出现的次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list count.

        Args:
            context: Execution context.
            params: Dict with list_var, item, output_var.

        Returns:
            ActionResult with count.
        """
        list_var = params.get('list_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'count_result')

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

            count = lst.count(resolved_item)
            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"元素计数: {count}",
                data={
                    'item': resolved_item,
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class ListRemoveAction(BaseAction):
    """Remove element by value."""
    action_type = "list4_remove"
    display_name = "删除元素"
    description = "删除列表中第一个匹配的元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list remove.

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

            try:
                lst.remove(resolved_item)
                found = True
            except ValueError:
                found = False

            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"删除元素: {'成功' if found else '元素不存在'}",
                data={
                    'item': resolved_item,
                    'found': found,
                    'result': lst,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_result'}


class ListReverseAction(BaseAction):
    """Reverse list."""
    action_type = "list4_reverse"
    display_name = "反转列表"
    description = "反转列表顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list reverse.

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
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            reversed_list = lst.copy()
            reversed_list.reverse()
            context.set(output_var, reversed_list)

            return ActionResult(
                success=True,
                message=f"反转列表: {len(reversed_list)} 个元素",
                data={
                    'original': lst,
                    'result': reversed_list,
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


class ListContainsAction(BaseAction):
    """Check if list contains element."""
    action_type = "list4_contains"
    display_name = "检查列表包含"
    description = "检查列表是否包含指定元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list contains.

        Args:
            context: Execution context.
            params: Dict with list_var, item, output_var.

        Returns:
            ActionResult with contains result.
        """
        list_var = params.get('list_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'contains_result')

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

            contains = resolved_item in lst
            context.set(output_var, contains)

            return ActionResult(
                success=True,
                message=f"列表包含: {'是' if contains else '否'}",
                data={
                    'item': resolved_item,
                    'contains': contains,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查列表包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}
