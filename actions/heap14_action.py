"""Heap14 action module for RabAI AutoClick.

Provides additional heap operations:
- HeapPushAction: Push to heap
- HeapPopAction: Pop from heap
- HeapPeekAction: View top item
- HeapSizeAction: Get heap size
- HeapEmptyAction: Check if empty
- HeapClearAction: Clear heap
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HeapPushAction(BaseAction):
    """Push to heap."""
    action_type = "heap14_push"
    display_name = "堆推送"
    description = "推入堆"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with heap_name, value, output_var.

        Returns:
            ActionResult with push result.
        """
        heap_name = params.get('heap_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'push_result')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            if resolved_heap not in context._heaps:
                context._heaps[resolved_heap] = []

            heapq.heappush(context._heaps[resolved_heap], resolved_value)

            context.set(output_var, len(context._heaps[resolved_heap]))

            return ActionResult(
                success=True,
                message=f"堆推送: {resolved_value} -> {resolved_heap} ({len(context._heaps[resolved_heap])}项)",
                data={
                    'heap': resolved_heap,
                    'value': resolved_value,
                    'size': len(context._heaps[resolved_heap]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆推送失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'push_result'}


class HeapPopAction(BaseAction):
    """Pop from heap."""
    action_type = "heap14_pop"
    display_name = "堆弹出"
    description = "弹出堆"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with heap_name, output_var.

        Returns:
            ActionResult with popped value.
        """
        heap_name = params.get('heap_name', 'default')
        output_var = params.get('output_var', 'pop_result')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            if resolved_heap not in context._heaps or not context._heaps[resolved_heap]:
                return ActionResult(
                    success=False,
                    message=f"堆为空: {resolved_heap}"
                )

            value = heapq.heappop(context._heaps[resolved_heap])

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"堆弹出: {value} <- {resolved_heap} ({len(context._heaps[resolved_heap])}项)",
                data={
                    'heap': resolved_heap,
                    'value': value,
                    'size': len(context._heaps[resolved_heap]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆弹出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pop_result'}


class HeapPeekAction(BaseAction):
    """View top item."""
    action_type = "heap14_peek"
    display_name = "查看堆顶"
    description = "查看堆顶项目"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with heap_name, output_var.

        Returns:
            ActionResult with top item.
        """
        heap_name = params.get('heap_name', 'default')
        output_var = params.get('output_var', 'peek_result')

        try:
            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            if resolved_heap not in context._heaps or not context._heaps[resolved_heap]:
                return ActionResult(
                    success=False,
                    message=f"堆为空: {resolved_heap}"
                )

            value = context._heaps[resolved_heap][0]

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"查看堆顶: {value}",
                data={
                    'heap': resolved_heap,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看堆顶失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class HeapSizeAction(BaseAction):
    """Get heap size."""
    action_type = "heap14_size"
    display_name = "堆大小"
    description = "获取堆大小"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with heap_name, output_var.

        Returns:
            ActionResult with heap size.
        """
        heap_name = params.get('heap_name', 'default')
        output_var = params.get('output_var', 'size_result')

        try:
            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            size = len(context._heaps.get(resolved_heap, []))

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"堆大小: {resolved_heap} = {size}",
                data={
                    'heap': resolved_heap,
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'size_result'}


class HeapEmptyAction(BaseAction):
    """Check if empty."""
    action_type = "heap14_empty"
    display_name = "堆是否为空"
    description = "检查堆是否为空"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute empty check.

        Args:
            context: Execution context.
            params: Dict with heap_name, output_var.

        Returns:
            ActionResult with empty status.
        """
        heap_name = params.get('heap_name', 'default')
        output_var = params.get('output_var', 'empty_result')

        try:
            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            is_empty = resolved_heap not in context._heaps or not context._heaps[resolved_heap]

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"堆是否为空: {resolved_heap} = {is_empty}",
                data={
                    'heap': resolved_heap,
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆是否为空检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'empty_result'}


class HeapClearAction(BaseAction):
    """Clear heap."""
    action_type = "heap14_clear"
    display_name = "清空堆"
    description = "清空堆"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with heap_name, output_var.

        Returns:
            ActionResult with clear result.
        """
        heap_name = params.get('heap_name', 'default')
        output_var = params.get('output_var', 'clear_result')

        try:
            resolved_heap = context.resolve_value(heap_name) if heap_name else 'default'

            if not hasattr(context, '_heaps'):
                context._heaps = {}

            if resolved_heap in context._heaps:
                count = len(context._heaps[resolved_heap])
                context._heaps[resolved_heap] = []
            else:
                count = 0

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清空堆: {resolved_heap} ({count}项)",
                data={
                    'heap': resolved_heap,
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空堆失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}