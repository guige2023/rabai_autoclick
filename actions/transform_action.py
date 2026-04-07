"""Transform action module for RabAI AutoClick.

Provides data transformation operations:
- TransformMapAction: Transform each element
- TransformFilterAction: Filter elements
- TransformFlattenAction: Flatten nested structure
- TransformGroupByAction: Group elements by key
- TransformSortAction: Sort elements
- TransformReverseAction: Reverse order
- TransformUniqueAction: Get unique elements
- TransformChunkAction: Chunk elements into groups
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Transform each element."""
    action_type = "transform_map"
    display_name = "转换映射"
    description = "对每个元素执行转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map transformation.

        Args:
            context: Execution context.
            params: Dict with items, expression, output_var.

        Returns:
            ActionResult with transformed list.
        """
        items = params.get('items', [])
        expression = params.get('expression', 'x')
        output_var = params.get('output_var', 'transformed')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_expr = context.resolve_value(expression)

            result = []
            for i, item in enumerate(resolved_items):
                context.set('_transform_item', item)
                context.set('_transform_index', i)
                try:
                    transformed = context.safe_exec(f"return_value = {resolved_expr}")
                    result.append(transformed)
                except Exception:
                    result.append(item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换映射失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transformed'}


class TransformFilterAction(BaseAction):
    """Filter elements."""
    action_type = "transform_filter"
    display_name = "转换过滤"
    description = "根据条件过滤元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter transformation.

        Args:
            context: Execution context.
            params: Dict with items, condition, output_var.

        Returns:
            ActionResult with filtered list.
        """
        items = params.get('items', [])
        condition = params.get('condition', 'True')
        output_var = params.get('output_var', 'filtered')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_cond = context.resolve_value(condition)

            result = []
            for i, item in enumerate(resolved_items):
                context.set('_filter_item', item)
                context.set('_filter_index', i)
                try:
                    if context.safe_exec(f"return_value = {resolved_cond}"):
                        result.append(item)
                except Exception:
                    pass

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤完成: {len(result)}/{len(resolved_items)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'original_count': len(resolved_items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换过滤失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered'}


class TransformFlattenAction(BaseAction):
    """Flatten nested structure."""
    action_type = "transform_flatten"
    display_name = "转换扁平化"
    description = "扁平化嵌套结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten transformation.

        Args:
            context: Execution context.
            params: Dict with items, depth, output_var.

        Returns:
            ActionResult with flattened list.
        """
        items = params.get('items', [])
        depth = params.get('depth', -1)
        output_var = params.get('output_var', 'flattened')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_depth = context.resolve_value(depth)

            def flatten(lst, current_depth=0, max_depth=-1):
                result = []
                for item in lst:
                    if isinstance(item, (list, tuple)) and (max_depth == -1 or current_depth < max_depth):
                        result.extend(flatten(item, current_depth + 1, max_depth))
                    else:
                        result.append(item)
                return result

            result = flatten(resolved_items, max_depth=int(resolved_depth))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扁平化完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换扁平化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'depth': -1, 'output_var': 'flattened'}


class TransformGroupByAction(BaseAction):
    """Group elements by key."""
    action_type = "transform_group_by"
    display_name = "转换分组"
    description = "按键分组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute group by transformation.

        Args:
            context: Execution context.
            params: Dict with items, key, output_var.

        Returns:
            ActionResult with grouped dict.
        """
        items = params.get('items', [])
        key = params.get('key', 'x')
        output_var = params.get('output_var', 'grouped')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_key = context.resolve_value(key)

            groups = {}
            for i, item in enumerate(resolved_items):
                context.set('_group_item', item)
                context.set('_group_index', i)
                try:
                    group_key = context.safe_exec(f"return_value = {resolved_key}")
                    if group_key not in groups:
                        groups[group_key] = []
                    groups[group_key].append(item)
                except Exception:
                    pass

            context.set(output_var, groups)

            return ActionResult(
                success=True,
                message=f"分组完成: {len(groups)} 组",
                data={
                    'result': groups,
                    'group_count': len(groups),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换分组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'grouped'}


class TransformSortAction(BaseAction):
    """Sort elements."""
    action_type = "transform_sort"
    display_name = "转换排序"
    description = "对元素排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort transformation.

        Args:
            context: Execution context.
            params: Dict with items, key, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        items = params.get('items', [])
        key = params.get('key', None)
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_reverse = context.resolve_value(reverse)

            if key is not None:
                resolved_key = context.resolve_value(key)
                sorted_items = sorted(
                    resolved_items,
                    key=lambda x: context.safe_exec(f"return_value = {resolved_key}") if False else x,
                    reverse=resolved_reverse
                )
            else:
                sorted_items = sorted(resolved_items, reverse=resolved_reverse)

            context.set(output_var, sorted_items)

            return ActionResult(
                success=True,
                message=f"排序完成: {len(sorted_items)} 项",
                data={
                    'result': sorted_items,
                    'count': len(sorted_items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': None, 'reverse': False, 'output_var': 'sorted'}


class TransformReverseAction(BaseAction):
    """Reverse order."""
    action_type = "transform_reverse"
    display_name = "转换反转"
    description = "反转元素顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse transformation.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with reversed list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'reversed')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            result = list(resolved_items)[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换反转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed'}


class TransformUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "transform_unique"
    display_name = "转换去重"
    description = "获取唯一元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unique transformation.

        Args:
            context: Execution context.
            params: Dict with items, key, output_var.

        Returns:
            ActionResult with unique list.
        """
        items = params.get('items', [])
        key = params.get('key', None)
        output_var = params.get('output_var', 'unique')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)

            if key is not None:
                resolved_key = context.resolve_value(key)
                seen = set()
                result = []
                for item in resolved_items:
                    context.set('_unique_item', item)
                    try:
                        k = context.safe_exec(f"return_value = {resolved_key}")
                        if k not in seen:
                            seen.add(k)
                            result.append(item)
                    except Exception:
                        if item not in seen:
                            seen.add(str(item))
                            result.append(item)
            else:
                # Preserve order while removing duplicates
                seen = set()
                result = []
                for item in resolved_items:
                    if item not in seen:
                        seen.add(item)
                        result.append(item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去重完成: {len(result)}/{len(resolved_items)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'original_count': len(resolved_items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换去重失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': None, 'output_var': 'unique'}


class TransformChunkAction(BaseAction):
    """Chunk elements into groups."""
    action_type = "transform_chunk"
    display_name = "转换分块"
    description = "将元素分块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute chunk transformation.

        Args:
            context: Execution context.
            params: Dict with items, size, output_var.

        Returns:
            ActionResult with chunked list.
        """
        items = params.get('items', [])
        size = params.get('size', 2)
        output_var = params.get('output_var', 'chunked')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(size, int, 'size')
        if not valid:
            return ActionResult(success=False, message=msg)

        if size < 1:
            return ActionResult(
                success=False,
                message=f"块大小必须 >= 1, 收到 {size}"
            )

        try:
            resolved_items = context.resolve_value(items)
            resolved_size = context.resolve_value(size)

            result = [
                resolved_items[i:i + int(resolved_size)]
                for i in range(0, len(resolved_items), int(resolved_size))
            ]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分块完成: {len(result)} 块",
                data={
                    'result': result,
                    'chunk_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换分块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chunked'}