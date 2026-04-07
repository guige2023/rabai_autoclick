"""Filter action module for RabAI AutoClick.

Provides filtering operations:
- FilterListAction: Filter list by condition
- FilterUniqueAction: Get unique elements
- FilterDuplicatesAction: Remove duplicates
- FilterEmptyAction: Remove empty elements
- FilterNoneAction: Remove None elements
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterListAction(BaseAction):
    """Filter list by condition."""
    action_type = "filter_list"
    display_name = "过滤列表"
    description = "根据条件过滤列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with list_var, condition, output_var.

        Returns:
            ActionResult with filtered list.
        """
        list_var = params.get('list_var', '')
        condition = params.get('condition', 'lambda x: True')
        output_var = params.get('output_var', 'filtered_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_cond = context.resolve_value(condition)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            filter_fn = context.safe_exec(f"return_value = {resolved_cond}")
            result = [item for item in items if filter_fn(item)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤完成: {len(result)}/{len(items)} 项",
                data={
                    'original_count': len(items),
                    'filtered_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': 'lambda x: True', 'output_var': 'filtered_list'}


class FilterUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "filter_unique"
    display_name = "获取唯一元素"
    description = "获取列表中的唯一元素"

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
            ActionResult with unique list.
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

            seen = set()
            result = []
            for item in items:
                if item not in seen:
                    seen.add(item)
                    result.append(item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"唯一元素: {len(result)}/{len(items)} 项",
                data={
                    'original_count': len(items),
                    'unique_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取唯一元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unique_list'}


class FilterDuplicatesAction(BaseAction):
    """Remove duplicates."""
    action_type = "filter_duplicates"
    display_name = "移除重复"
    description = "移除列表中的重复元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove duplicates.

        Args:
            context: Execution context.
            params: Dict with list_var, keep, output_var.

        Returns:
            ActionResult with deduplicated list.
        """
        list_var = params.get('list_var', '')
        keep = params.get('keep', 'first')
        output_var = params.get('output_var', 'deduplicated_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_keep = context.resolve_value(keep)

            items = list(context.get(resolved_var))
            if not isinstance(items, list):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            if resolved_keep == 'last':
                seen = set()
                result = []
                for item in reversed(items):
                    if item not in seen:
                        seen.add(item)
                        result.append(item)
                result = result[::-1]
            else:
                seen = set()
                result = []
                for item in items:
                    if item not in seen:
                        seen.add(item)
                        result.append(item)

            duplicates_removed = len(items) - len(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"移除 {duplicates_removed} 个重复项",
                data={
                    'original_count': len(items),
                    'result_count': len(result),
                    'duplicates_removed': duplicates_removed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除重复失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keep': 'first', 'output_var': 'deduplicated_list'}


class FilterEmptyAction(BaseAction):
    """Remove empty elements."""
    action_type = "filter_empty"
    display_name = "移除空元素"
    description = "移除列表中的空元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove empty.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with filtered list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'filtered_list')

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

            result = [item for item in items if item != '' and item != [] and item != {}]
            empty_removed = len(items) - len(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"移除 {empty_removed} 个空元素",
                data={
                    'original_count': len(items),
                    'result_count': len(result),
                    'empty_removed': empty_removed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除空元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class FilterNoneAction(BaseAction):
    """Remove None elements."""
    action_type = "filter_none"
    display_name = "移除None"
    description = "移除列表中的None元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove None.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with filtered list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'filtered_list')

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

            result = [item for item in items if item is not None]
            none_removed = len(items) - len(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"移除 {none_removed} 个None元素",
                data={
                    'original_count': len(items),
                    'result_count': len(result),
                    'none_removed': none_removed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除None失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}
