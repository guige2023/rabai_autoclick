"""Tuple2 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleCreateAction: Create tuple from items
- TupleGetAction: Get element by index
- TupleSliceAction: Slice tuple
- TupleIndexAction: Find element index
- TupleCountAction: Count element occurrences
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleCreateAction(BaseAction):
    """Create tuple from items."""
    action_type = "tuple2_create"
    display_name = "创建元组"
    description = "从列表创建元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple create.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with created tuple.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'tuple_result')

        try:
            resolved = context.resolve_value(items)

            if isinstance(resolved, tuple):
                result = resolved
            elif isinstance(resolved, list):
                result = tuple(resolved)
            elif isinstance(resolved, (set, frozenset)):
                result = tuple(resolved)
            else:
                result = (resolved,)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建元组: {len(result)} 个元素",
                data={
                    'items': resolved,
                    'result': result,
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
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_result'}


class TupleGetAction(BaseAction):
    """Get element by index."""
    action_type = "tuple2_get"
    display_name: "获取元组元素"
    description = "通过索引获取元组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple get.

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
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_index = int(context.resolve_value(index))

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            if abs(resolved_index) >= len(t):
                return ActionResult(
                    success=False,
                    message=f"索引超出范围: {resolved_index}"
                )

            result = t[resolved_index]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取元素: [{resolved_index}] = {result}",
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
    action_type = "tuple2_slice"
    display_name = "切片元组"
    description = "切片元组获取子元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple slice.

        Args:
            context: Execution context.
            params: Dict with tuple_var, start, end, step, output_var.

        Returns:
            ActionResult with sliced tuple.
        """
        tuple_var = params.get('tuple_var', '')
        start = params.get('start', None)
        end = params.get('end', None)
        step = params.get('step', None)
        output_var = params.get('output_var', 'tuple_slice')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_start = int(context.resolve_value(start)) if start is not None else None
            resolved_end = int(context.resolve_value(end)) if end is not None else None
            resolved_step = int(context.resolve_value(step)) if step is not None else None

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            result = t[resolved_start:resolved_end:resolved_step]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"切片元组: {len(result)} 个元素",
                data={
                    'original': t,
                    'result': result,
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
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
        return {'start': None, 'end': None, 'step': None, 'output_var': 'tuple_slice'}


class TupleIndexAction(BaseAction):
    """Find element index."""
    action_type = "tuple2_index"
    display_name = "元组索引"
    description = "查找元素在元组中的索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple index.

        Args:
            context: Execution context.
            params: Dict with tuple_var, item, output_var.

        Returns:
            ActionResult with index.
        """
        tuple_var = params.get('tuple_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'tuple_index')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            try:
                result = t.index(resolved_item)
                found = True
            except ValueError:
                result = -1
                found = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组索引: {result}",
                data={
                    'item': resolved_item,
                    'index': result,
                    'found': found,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_index'}


class TupleCountAction(BaseAction):
    """Count element occurrences."""
    action_type = "tuple2_count"
    display_name = "元组计数"
    description = "统计元素在元组中出现的次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple count.

        Args:
            context: Execution context.
            params: Dict with tuple_var, item, output_var.

        Returns:
            ActionResult with count.
        """
        tuple_var = params.get('tuple_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'tuple_count')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            result = t.count(resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组计数: {result}",
                data={
                    'item': resolved_item,
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_count'}