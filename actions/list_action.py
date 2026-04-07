"""List action module for RabAI AutoClick.

Provides list operations:
- ListAppendAction: Append to list
- ListExtendAction: Extend list
- ListIndexAction: Get index of item
- ListFilterAction: Filter list by condition
- ListMapAction: Transform list elements
- ListReduceAction: Reduce list to single value
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ListAppendAction(BaseAction):
    """Append an item to a list."""
    action_type = "list_append"
    display_name = "列表追加"
    description = "向列表追加元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute appending to list.

        Args:
            context: Execution context.
            params: Dict with list_var, item.

        Returns:
            ActionResult indicating success.
        """
        list_var = params.get('list_var', 'items')
        item = params.get('item', None)

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                current_list = []

            current_list.append(item)
            context.set(list_var, current_list)

            return ActionResult(
                success=True,
                message=f"已追加元素到 {list_var}: {len(current_list)} 项",
                data={
                    'list': current_list,
                    'count': len(current_list)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'item': None}


class ListExtendAction(BaseAction):
    """Extend a list with multiple items."""
    action_type = "list_extend"
    display_name = "列表扩展"
    description = "扩展列表添加多个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extending list.

        Args:
            context: Execution context.
            params: Dict with list_var, items.

        Returns:
            ActionResult indicating success.
        """
        list_var = params.get('list_var', 'items')
        items = params.get('items', [])

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                current_list = []

            current_list.extend(items)
            context.set(list_var, current_list)

            return ActionResult(
                success=True,
                message=f"已扩展列表 {list_var}: {len(current_list)} 项",
                data={
                    'list': current_list,
                    'count': len(current_list)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表扩展失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ListIndexAction(BaseAction):
    """Get index of item in list."""
    action_type = "list_index"
    display_name = "列表索引"
    description = "获取元素在列表中的索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting index.

        Args:
            context: Execution context.
            params: Dict with list_var, item, output_var.

        Returns:
            ActionResult with index.
        """
        list_var = params.get('list_var', 'items')
        item = params.get('item', None)
        output_var = params.get('output_var', 'item_index')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                return ActionResult(
                    success=False,
                    message=f"变量 {list_var} 不是列表"
                )

            index = current_list.index(item)

            context.set(output_var, index)

            return ActionResult(
                success=True,
                message=f"找到索引: {index}",
                data={
                    'index': index,
                    'item': item,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=True,
                message=f"元素不在列表中: {item}",
                data={
                    'index': -1,
                    'item': item,
                    'found': False,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'item_index'}


class ListFilterAction(BaseAction):
    """Filter list by a condition."""
    action_type = "list_filter"
    display_name = "列表过滤"
    description = "根据条件过滤列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filtering list.

        Args:
            context: Execution context.
            params: Dict with list_var, condition, output_var.

        Returns:
            ActionResult with filtered list.
        """
        list_var = params.get('list_var', 'items')
        condition = params.get('condition', '')  # e.g., "x > 5"
        output_var = params.get('output_var', 'filtered_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                return ActionResult(
                    success=False,
                    message=f"变量 {list_var} 不是列表"
                )

            if not condition:
                return ActionResult(
                    success=False,
                    message="未指定过滤条件"
                )

            # Filter items
            filtered = []
            for item in current_list:
                context.set('_filter_item', item)
                try:
                    result = context.safe_exec(f"return_value = {condition}")
                    if result:
                        filtered.append(item)
                except Exception:
                    pass

            context.set(output_var, filtered)

            return ActionResult(
                success=True,
                message=f"过滤完成: {len(filtered)}/{len(current_list)} 项",
                data={
                    'filtered': filtered,
                    'original_count': len(current_list),
                    'filtered_count': len(filtered),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表过滤失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class ListMapAction(BaseAction):
    """Transform list elements."""
    action_type = "list_map"
    display_name = "列表映射"
    description = "对列表每个元素执行转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mapping list.

        Args:
            context: Execution context.
            params: Dict with list_var, transform, output_var.

        Returns:
            ActionResult with mapped list.
        """
        list_var = params.get('list_var', 'items')
        transform = params.get('transform', '')  # e.g., "x * 2"
        output_var = params.get('output_var', 'mapped_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(transform, str, 'transform')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                return ActionResult(
                    success=False,
                    message=f"变量 {list_var} 不是列表"
                )

            if not transform:
                return ActionResult(
                    success=False,
                    message="未指定转换表达式"
                )

            # Map items
            mapped = []
            for item in current_list:
                context.set('_map_item', item)
                try:
                    result = context.safe_exec(f"return_value = {transform}")
                    mapped.append(result)
                except Exception:
                    mapped.append(item)  # Keep original on error

            context.set(output_var, mapped)

            return ActionResult(
                success=True,
                message=f"映射完成: {len(mapped)} 项",
                data={
                    'mapped': mapped,
                    'count': len(mapped),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表映射失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'transform']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mapped_list'}


class ListReduceAction(BaseAction):
    """Reduce list to single value."""
    action_type = "list_reduce"
    display_name = "列表聚合"
    description = "将列表聚合为单个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reducing list.

        Args:
            context: Execution context.
            params: Dict with list_var, reduce_func, initial, output_var.

        Returns:
            ActionResult with reduced value.
        """
        list_var = params.get('list_var', 'items')
        reduce_func = params.get('reduce_func', 'sum')  # sum, min, max, avg
        initial = params.get('initial', 0)
        output_var = params.get('output_var', 'reduced_value')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid_funcs = ['sum', 'min', 'max', 'avg', 'count']
        valid, msg = self.validate_in(reduce_func, valid_funcs, 'reduce_func')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_list = context.get(list_var, [])

            if not isinstance(current_list, list):
                return ActionResult(
                    success=False,
                    message=f"变量 {list_var} 不是列表"
                )

            if len(current_list) == 0:
                context.set(output_var, initial)
                return ActionResult(
                    success=True,
                    message=f"列表为空，返回初始值: {initial}",
                    data={
                        'result': initial,
                        'output_var': output_var
                    }
                )

            if reduce_func == 'sum':
                result = sum(current_list)
            elif reduce_func == 'min':
                result = min(current_list)
            elif reduce_func == 'max':
                result = max(current_list)
            elif reduce_func == 'avg':
                result = sum(current_list) / len(current_list)
            elif reduce_func == 'count':
                result = len(current_list)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"聚合完成: {reduce_func} = {result}",
                data={
                    'result': result,
                    'function': reduce_func,
                    'count': len(current_list),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表聚合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'reduce_func': 'sum',
            'initial': 0,
            'output_var': 'reduced_value'
        }