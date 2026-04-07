"""Tuple action module for RabAI AutoClick.

Provides tuple operations:
- TupleCreateAction: Create tuple
- TupleGetAction: Get tuple element
- TupleSliceAction: Slice tuple
- TupleIndexAction: Find tuple index
- TupleCountAction: Count tuple elements
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleCreateAction(BaseAction):
    """Create tuple."""
    action_type = "tuple_create"
    display_name = "创建元组"
    description = "创建元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult indicating created.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'tuple_result')

        try:
            resolved_items = context.resolve_value(items)

            result = tuple(resolved_items) if isinstance(resolved_items, list) else resolved_items
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组创建: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': [], 'output_var': 'tuple_result'}


class TupleGetAction(BaseAction):
    """Get tuple element."""
    action_type = "tuple_get"
    display_name = "获取元组元素"
    description = "获取元组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with tuple_var, index, output_var.

        Returns:
            ActionResult with element.
        """
        tuple_var = params.get('tuple_var', '')
        index = params.get('index', 0)
        output_var = params.get('output_var', 'tuple_element')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(tuple_var)
            resolved_index = int(context.resolve_value(index))

            t = context.get(resolved_var)
            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是元组或列表"
                )

            if resolved_index < -len(t) or resolved_index >= len(t):
                return ActionResult(
                    success=False,
                    message=f"索引 {resolved_index} 超出范围"
                )

            result = t[resolved_index]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取元素: {result}",
                data={
                    'index': resolved_index,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取元组元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_element'}


class TupleSliceAction(BaseAction):
    """Slice tuple."""
    action_type = "tuple_slice"
    display_name = "切片元组"
    description = "切片元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with tuple_var, start, end, output_var.

        Returns:
            ActionResult with sliced tuple.
        """
        tuple_var = params.get('tuple_var', '')
        start = params.get('start', 0)
        end = params.get('end', None)
        output_var = params.get('output_var', 'tuple_slice')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(tuple_var)
            resolved_start = int(context.resolve_value(start))
            resolved_end = int(context.resolve_value(end)) if end is not None else None

            t = context.get(resolved_var)
            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是元组或列表"
                )

            result = t[resolved_start:resolved_end]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组切片: {len(result)} 项",
                data={
                    'start': resolved_start,
                    'end': resolved_end,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'output_var': 'tuple_slice'}


class TupleIndexAction(BaseAction):
    """Find tuple index."""
    action_type = "tuple_index"
    display_name = "查找元组索引"
    description = "查找元素在元组中的索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index.

        Args:
            context: Execution context.
            params: Dict with tuple_var, value, output_var.

        Returns:
            ActionResult with index.
        """
        tuple_var = params.get('tuple_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'tuple_index')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(tuple_var)
            resolved_value = context.resolve_value(value)

            t = context.get(resolved_var)
            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是元组或列表"
                )

            try:
                index = t.index(resolved_value)
                context.set(output_var, index)

                return ActionResult(
                    success=True,
                    message=f"找到索引: {index}",
                    data={
                        'value': resolved_value,
                        'index': index,
                        'output_var': output_var
                    }
                )
            except ValueError:
                context.set(output_var, -1)
                return ActionResult(
                    success=True,
                    message=f"未找到元素",
                    data={
                        'value': resolved_value,
                        'index': -1,
                        'output_var': output_var
                    }
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找元组索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_index'}


class TupleCountAction(BaseAction):
    """Count tuple elements."""
    action_type = "tuple_count"
    display_name = "统计元组元素"
    description = "统计元素在元组中出现的次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with tuple_var, value, output_var.

        Returns:
            ActionResult with count.
        """
        tuple_var = params.get('tuple_var', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'tuple_count')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(tuple_var)
            resolved_value = context.resolve_value(value)

            t = context.get(resolved_var)
            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是元组或列表"
                )

            count = t.count(resolved_value)
            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"统计: {count} 次",
                data={
                    'value': resolved_value,
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计元组元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_count'}
