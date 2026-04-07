"""Lookup action module for RabAI AutoClick.

Provides lookup/search operations:
- LookupFindAction: Find item in list
- LookupIndexOfAction: Get index of item
- LookupContainsAction: Check if list contains item
- LookupCountAction: Count occurrences
- LookupMaxAction: Find maximum
- LookupMinAction: Find minimum
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LookupFindAction(BaseAction):
    """Find item in list."""
    action_type = "lookup_find"
    display_name = "查找元素"
    description = "在列表中查找元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with list_var, value, output_var.

        Returns:
            ActionResult with find result.
        """
        list_var = params.get('list_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'find_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_value = context.resolve_value(value)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = None
            for i, item in enumerate(items):
                if item == resolved_value:
                    result = {'found': True, 'index': i, 'value': item}
                    break

            if result is None:
                result = {'found': False, 'index': -1, 'value': None}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查找{'成功' if result['found'] else '失败'}",
                data={
                    'found': result['found'],
                    'index': result['index'],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'find_result'}


class LookupIndexOfAction(BaseAction):
    """Get index of item."""
    action_type = "lookup_index_of"
    display_name = "获取索引"
    description = "获取元素在列表中的索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index of.

        Args:
            context: Execution context.
            params: Dict with list_var, value, output_var.

        Returns:
            ActionResult with index.
        """
        list_var = params.get('list_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'index_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_value = context.resolve_value(value)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            try:
                index = items.index(resolved_value)
            except ValueError:
                index = -1

            context.set(output_var, index)

            return ActionResult(
                success=True,
                message=f"索引: {index}",
                data={
                    'index': index,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class LookupContainsAction(BaseAction):
    """Check if list contains item."""
    action_type = "lookup_contains"
    display_name = "检查包含"
    description = "检查列表是否包含元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with list_var, value, output_var.

        Returns:
            ActionResult with contains result.
        """
        list_var = params.get('list_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'contains_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_value = context.resolve_value(value)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = resolved_value in items
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含{'是' if result else '否'}",
                data={
                    'contains': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class LookupCountAction(BaseAction):
    """Count occurrences."""
    action_type = "lookup_count"
    display_name = "统计出现次数"
    description = "统计元素在列表中出现的次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with list_var, value, output_var.

        Returns:
            ActionResult with count.
        """
        list_var = params.get('list_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'count_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_value = context.resolve_value(value)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            count = items.count(resolved_value)
            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"出现 {count} 次",
                data={
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计出现次数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class LookupMaxAction(BaseAction):
    """Find maximum."""
    action_type = "lookup_max"
    display_name = "查找最大值"
    description = "查找列表中的最大值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with max value.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'max_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法查找最大值"
                )

            result = max(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大值: {result}",
                data={
                    'max': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'max_result'}


class LookupMinAction(BaseAction):
    """Find minimum."""
    action_type = "lookup_min"
    display_name = "查找最小值"
    description = "查找列表中的最小值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with min value.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'min_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            if len(items) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法查找最小值"
                )

            result = min(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小值: {result}",
                data={
                    'min': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'min_result'}
