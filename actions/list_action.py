"""List action module for RabAI AutoClick.

Provides list operations:
- ListLengthAction: Get list length
- ListAppendAction: Append to list
- ListExtendAction: Extend list
- ListFilterAction: Filter list
- ListMapAction: Map function over list
- ListReduceAction: Reduce list
- ListSortAction: Sort list
- ListReverseAction: Reverse list
- ListUniqueAction: Get unique elements
- ListChunkAction: Chunk list
"""

from typing import Any, Callable, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListLengthAction(BaseAction):
    """Get list length."""
    action_type = "list_length"
    display_name = "列表长度"
    description = "获取列表长度"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute length.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with length.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'list_length')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            length = len(resolved_items)

            context.set(output_var, length)

            return ActionResult(
                success=True,
                message=f"列表长度: {length}",
                data={'length': length, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取列表长度失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_length'}


class ListAppendAction(BaseAction):
    """Append to list."""
    action_type = "list_append"
    display_name = "列表追加"
    description = "追加元素到列表"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append.

        Args:
            context: Execution context.
            params: Dict with items, value, output_var.

        Returns:
            ActionResult with updated list.
        """
        items = params.get('items', [])
        value = params.get('value', None)
        output_var = params.get('output_var', 'list_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_value = context.resolve_value(value)

            resolved_items.append(resolved_value)
            context.set(output_var, resolved_items)

            return ActionResult(
                success=True,
                message=f"已追加: {len(resolved_items)} 个元素",
                data={'items': resolved_items, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表追加失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_items'}


class ListExtendAction(BaseAction):
    """Extend list."""
    action_type = "list_extend"
    display_name = "列表扩展"
    description = "扩展列表"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extend.

        Args:
            context: Execution context.
            params: Dict with items, values, output_var.

        Returns:
            ActionResult with updated list.
        """
        items = params.get('items', [])
        values = params.get('values', [])
        output_var = params.get('output_var', 'list_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_values = context.resolve_value(values)

            resolved_items.extend(resolved_values)
            context.set(output_var, resolved_items)

            return ActionResult(
                success=True,
                message=f"已扩展: {len(resolved_items)} 个元素",
                data={'items': resolved_items, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表扩展失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_items'}


class ListFilterAction(BaseAction):
    """Filter list."""
    action_type = "list_filter"
    display_name = "列表筛选"
    description = "筛选列表元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with items, condition, output_var.

        Returns:
            ActionResult with filtered list.
        """
        items = params.get('items', [])
        condition = params.get('condition', 'value')  # value, truthy, not_empty
        compare_value = params.get('compare_value', None)
        compare_op = params.get('compare_op', 'equals')
        output_var = params.get('output_var', 'filtered_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_condition = context.resolve_value(condition)
            resolved_compare = context.resolve_value(compare_value) if compare_value is not None else None
            resolved_op = context.resolve_value(compare_op)

            filtered = []

            if resolved_condition == 'truthy':
                filtered = [x for x in resolved_items if x]
            elif resolved_condition == 'not_empty':
                filtered = [x for x in resolved_items if x != '' and x is not None]
            elif resolved_condition == 'not_none':
                filtered = [x for x in resolved_items if x is not None]
            elif resolved_condition == 'equals' and resolved_compare is not None:
                filtered = [x for x in resolved_items if x == resolved_compare]
            elif resolved_condition == 'not_equals' and resolved_compare is not None:
                filtered = [x for x in resolved_items if x != resolved_compare]
            elif resolved_condition == 'contains' and resolved_compare is not None:
                filtered = [x for x in resolved_items if resolved_compare in str(x)]
            else:
                filtered = resolved_items

            context.set(output_var, filtered)

            return ActionResult(
                success=True,
                message=f"筛选结果: {len(filtered)} 个元素",
                data={'items': filtered, 'count': len(filtered), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表筛选失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compare_value': None, 'compare_op': 'equals', 'output_var': 'filtered_items'}


class ListMapAction(BaseAction):
    """Map function over list."""
    action_type = "list_map"
    display_name = "列表映射"
    description = "对列表每个元素操作"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map.

        Args:
            context: Execution context.
            params: Dict with items, transform, output_var.

        Returns:
            ActionResult with mapped list.
        """
        items = params.get('items', [])
        transform = params.get('transform', 'uppercase')  # uppercase, lowercase, title, str, int, float
        output_var = params.get('output_var', 'mapped_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_transform = context.resolve_value(transform)

            mapped = []
            for item in resolved_items:
                if resolved_transform == 'uppercase':
                    mapped.append(str(item).upper())
                elif resolved_transform == 'lowercase':
                    mapped.append(str(item).lower())
                elif resolved_transform == 'title':
                    mapped.append(str(item).title())
                elif resolved_transform == 'str':
                    mapped.append(str(item))
                elif resolved_transform == 'int':
                    try:
                        mapped.append(int(item))
                    except:
                        mapped.append(item)
                elif resolved_transform == 'float':
                    try:
                        mapped.append(float(item))
                    except:
                        mapped.append(item)
                else:
                    mapped.append(item)

            context.set(output_var, mapped)

            return ActionResult(
                success=True,
                message=f"映射完成: {len(mapped)} 个元素",
                data={'items': mapped, 'count': len(mapped), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表映射失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'transform']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mapped_items'}


class ListSortAction(BaseAction):
    """Sort list."""
    action_type = "list_sort"
    display_name = "列表排序"
    description = "对列表排序"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with items, order, output_var.

        Returns:
            ActionResult with sorted list.
        """
        items = params.get('items', [])
        order = params.get('order', 'asc')
        output_var = params.get('output_var', 'sorted_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_order = context.resolve_value(order)

            reverse = resolved_order == 'desc'
            sorted_items = sorted(resolved_items, key=lambda x: str(x).lower() if isinstance(x, str) else x, reverse=reverse)

            context.set(output_var, sorted_items)

            return ActionResult(
                success=True,
                message=f"排序完成: {len(sorted_items)} 个元素",
                data={'items': sorted_items, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表排序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'order']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_items'}


class ListReverseAction(BaseAction):
    """Reverse list."""
    action_type = "list_reverse"
    display_name = "列表反转"
    description = "反转列表"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with reversed list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'reversed_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            reversed_items = list(reversed(resolved_items))

            context.set(output_var, reversed_items)

            return ActionResult(
                success=True,
                message=f"反转完成: {len(reversed_items)} 个元素",
                data={'items': reversed_items, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表反转失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_items'}


class ListUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "list_unique"
    display_name = "列表去重"
    description = "列表去重"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unique.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with unique list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'unique_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            seen = set()
            unique = []
            for item in resolved_items:
                if item not in seen:
                    seen.add(item)
                    unique.append(item)

            context.set(output_var, unique)

            return ActionResult(
                success=True,
                message=f"去重完成: {len(unique)} 个元素 (移除 {len(resolved_items) - len(unique)} 个重复)",
                data={'items': unique, 'count': len(unique), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表去重失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unique_items'}


class ListChunkAction(BaseAction):
    """Chunk list."""
    action_type = "list_chunk"
    display_name = "列表分块"
    description = "将列表分块"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute chunk.

        Args:
            context: Execution context.
            params: Dict with items, chunk_size, output_var.

        Returns:
            ActionResult with chunks.
        """
        items = params.get('items', [])
        chunk_size = params.get('chunk_size', 10)
        output_var = params.get('output_var', 'chunks')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_size = context.resolve_value(chunk_size)

            chunks = []
            for i in range(0, len(resolved_items), resolved_size):
                chunks.append(resolved_items[i:i + resolved_size])

            context.set(output_var, chunks)

            return ActionResult(
                success=True,
                message=f"分块完成: {len(chunks)} 块",
                data={'chunks': chunks, 'count': len(chunks), 'chunk_size': resolved_size, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列表分块失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items', 'chunk_size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chunks'}
