"""Transform2 action module for RabAI AutoClick.

Provides additional transform operations:
- TransformMapAction: Map function over list
- TransformFilterAction: Filter list by condition
- TransformReduceAction: Reduce list to single value
- TransformGroupByAction: Group list by key
- TransformSortByAction: Sort list by key
"""

from typing import Any, Dict, List, Callable

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Map function over list."""
    action_type = "transform_map"
    display_name = "映射转换"
    description = "对列表中每个元素应用转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map transform.

        Args:
            context: Execution context.
            params: Dict with items, transform_type, output_var.

        Returns:
            ActionResult with transformed list.
        """
        items = params.get('items', [])
        transform_type = params.get('transform_type', 'double')
        output_var = params.get('output_var', 'mapped_items')

        try:
            resolved_items = context.resolve_value(items)
            resolved_type = context.resolve_value(transform_type)

            if not isinstance(resolved_items, list):
                return ActionResult(
                    success=False,
                    message="映射需要列表类型"
                )

            result = []
            for item in resolved_items:
                try:
                    if resolved_type == 'double':
                        result.append(item * 2)
                    elif resolved_type == 'square':
                        result.append(item ** 2)
                    elif resolved_type == 'abs':
                        result.append(abs(item))
                    elif resolved_type == 'str':
                        result.append(str(item))
                    elif resolved_type == 'int':
                        result.append(int(item))
                    elif resolved_type == 'float':
                        result.append(float(item))
                    elif resolved_type == 'upper':
                        result.append(str(item).upper())
                    elif resolved_type == 'lower':
                        result.append(str(item).lower())
                    else:
                        result.append(item)
                except:
                    result.append(item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"映射转换: {len(result)} 项",
                data={
                    'original': resolved_items,
                    'transform_type': resolved_type,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"映射转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'transform_type': 'double', 'output_var': 'mapped_items'}


class TransformFilterAction(BaseAction):
    """Filter list by condition."""
    action_type = "transform_filter"
    display_name = "过滤转换"
    description = "根据条件过滤列表元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter transform.

        Args:
            context: Execution context.
            params: Dict with items, condition, threshold, output_var.

        Returns:
            ActionResult with filtered list.
        """
        items = params.get('items', [])
        condition = params.get('condition', 'gt')
        threshold = params.get('threshold', 0)
        output_var = params.get('output_var', 'filtered_items')

        try:
            resolved_items = context.resolve_value(items)
            resolved_condition = context.resolve_value(condition)
            resolved_threshold = context.resolve_value(threshold)

            if not isinstance(resolved_items, list):
                return ActionResult(
                    success=False,
                    message="过滤需要列表类型"
                )

            result = []
            for item in resolved_items:
                try:
                    if resolved_condition == 'gt':
                        keep = item > resolved_threshold
                    elif resolved_condition == 'gte':
                        keep = item >= resolved_threshold
                    elif resolved_condition == 'lt':
                        keep = item < resolved_threshold
                    elif resolved_condition == 'lte':
                        keep = item <= resolved_threshold
                    elif resolved_condition == 'eq':
                        keep = item == resolved_threshold
                    elif resolved_condition == 'neq':
                        keep = item != resolved_threshold
                    elif resolved_condition == 'empty':
                        keep = not item
                    elif resolved_condition == 'not_empty':
                        keep = bool(item)
                    else:
                        keep = True

                    if keep:
                        result.append(item)
                except:
                    pass

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤转换: {len(result)}/{len(resolved_items)} 项",
                data={
                    'original': resolved_items,
                    'condition': resolved_condition,
                    'threshold': resolved_threshold,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': 'gt', 'threshold': 0, 'output_var': 'filtered_items'}


class TransformReduceAction(BaseAction):
    """Reduce list to single value."""
    action_type = "transform_reduce"
    display_name = "聚合转换"
    description = "将列表聚合为单个值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reduce transform.

        Args:
            context: Execution context.
            params: Dict with items, reduce_type, output_var.

        Returns:
            ActionResult with reduced value.
        """
        items = params.get('items', [])
        reduce_type = params.get('reduce_type', 'sum')
        output_var = params.get('output_var', 'reduced_value')

        try:
            resolved_items = context.resolve_value(items)
            resolved_type = context.resolve_value(reduce_type)

            if not isinstance(resolved_items, list):
                return ActionResult(
                    success=False,
                    message="聚合需要列表类型"
                )

            if not resolved_items:
                return ActionResult(
                    success=False,
                    message="聚合列表不能为空"
                )

            if resolved_type == 'sum':
                result = sum(resolved_items)
            elif resolved_type == 'product':
                result = 1
                for item in resolved_items:
                    result *= item
            elif resolved_type == 'min':
                result = min(resolved_items)
            elif resolved_type == 'max':
                result = max(resolved_items)
            elif resolved_type == 'avg' or resolved_type == 'mean':
                result = sum(resolved_items) / len(resolved_items)
            elif resolved_type == 'count':
                result = len(resolved_items)
            elif resolved_type == 'first':
                result = resolved_items[0]
            elif resolved_type == 'last':
                result = resolved_items[-1]
            elif resolved_type == 'join':
                result = ''.join(str(item) for item in resolved_items)
            else:
                result = sum(resolved_items)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"聚合转换: {result}",
                data={
                    'original': resolved_items,
                    'reduce_type': resolved_type,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reduce_type': 'sum', 'output_var': 'reduced_value'}


class TransformGroupByAction(BaseAction):
    """Group list by key."""
    action_type = "transform_group_by"
    display_name = "分组转换"
    description = "按键对列表分组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute group by transform.

        Args:
            context: Execution context.
            params: Dict with items, key, output_var.

        Returns:
            ActionResult with grouped dict.
        """
        items = params.get('items', [])
        key = params.get('key', '')
        output_var = params.get('output_var', 'grouped_items')

        try:
            resolved_items = context.resolve_value(items)
            resolved_key = context.resolve_value(key) if key else None

            if not isinstance(resolved_items, list):
                return ActionResult(
                    success=False,
                    message="分组需要列表类型"
                )

            grouped = {}
            for item in resolved_items:
                if resolved_key:
                    if isinstance(item, dict):
                        group_key = item.get(resolved_key, 'unknown')
                    else:
                        group_key = 'unknown'
                else:
                    group_key = str(type(item).__name__)

                if group_key not in grouped:
                    grouped[group_key] = []
                grouped[group_key].append(item)

            context.set(output_var, grouped)

            return ActionResult(
                success=True,
                message=f"分组转换: {len(grouped)} 组",
                data={
                    'original': resolved_items,
                    'key': resolved_key,
                    'groups': grouped,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分组转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': '', 'output_var': 'grouped_items'}


class TransformSortByAction(BaseAction):
    """Sort list by key."""
    action_type = "transform_sort_by"
    display_name = "排序转换"
    description = "按键对列表排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort by transform.

        Args:
            context: Execution context.
            params: Dict with items, key, reverse, output_var.

        Returns:
            ActionResult with sorted list.
        """
        items = params.get('items', [])
        key = params.get('key', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_items')

        try:
            resolved_items = context.resolve_value(items)
            resolved_key = context.resolve_value(key) if key else None
            resolved_reverse = context.resolve_value(reverse) if reverse else False

            if not isinstance(resolved_items, list):
                return ActionResult(
                    success=False,
                    message="排序需要列表类型"
                )

            if resolved_key:
                result = sorted(
                    resolved_items,
                    key=lambda x: x.get(resolved_key, 0) if isinstance(x, dict) else x,
                    reverse=resolved_reverse
                )
            else:
                result = sorted(resolved_items, reverse=resolved_reverse)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"排序转换: {len(result)} 项",
                data={
                    'original': resolved_items,
                    'key': resolved_key,
                    'reverse': resolved_reverse,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"排序转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': '', 'reverse': False, 'output_var': 'sorted_items'}