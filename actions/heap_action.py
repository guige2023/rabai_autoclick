"""Heap action module for RabAI AutoClick.

Provides heap operations:
- HeapCreateAction: Create heap
- HeapPushAction: Push to heap
- HeapPopAction: Pop from heap
- HeapPeekAction: Peek heap
- HeapSizeAction: Get heap size
"""

import heapq
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HeapCreateAction(BaseAction):
    """Create heap."""
    action_type = "heap_create"
    display_name = "创建堆"
    description = "创建堆结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', 'heap')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            context.set(f'_heap_{resolved_name}', [])
            context.set(f'_heap_{resolved_name}_type', 'min')

            return ActionResult(
                success=True,
                message=f"堆 {resolved_name} 创建",
                data={'name': resolved_name, 'type': 'min'}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建堆失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'heap'}


class HeapPushAction(BaseAction):
    """Push to heap."""
    action_type = "heap_push"
    display_name = "堆入"
    description = "将元素加入堆"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult indicating pushed.
        """
        name = params.get('name', 'heap')
        value = params.get('value', 0)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = float(context.resolve_value(value))

            heap = context.get(f'_heap_{resolved_name}', [])
            heapq.heappush(heap, resolved_value)
            context.set(f'_heap_{resolved_name}', heap)

            return ActionResult(
                success=True,
                message=f"堆入: {len(heap)} 项",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'size': len(heap)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'heap'}


class HeapPopAction(BaseAction):
    """Pop from heap."""
    action_type = "heap_pop"
    display_name = "堆出"
    description = "从堆取出最小元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with popped value.
        """
        name = params.get('name', 'heap')
        output_var = params.get('output_var', 'heap_value')

        try:
            resolved_name = context.resolve_value(name)

            heap = context.get(f'_heap_{resolved_name}', [])

            if not heap:
                return ActionResult(
                    success=False,
                    message=f"堆 {resolved_name} 为空"
                )

            value = heapq.heappop(heap)
            context.set(f'_heap_{resolved_name}', heap)
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"堆出: {value}, {len(heap)} 项剩余",
                data={
                    'name': resolved_name,
                    'value': value,
                    'remaining': len(heap),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'heap', 'output_var': 'heap_value'}


class HeapPeekAction(BaseAction):
    """Peek heap."""
    action_type = "heap_peek"
    display_name = "查看堆顶"
    description = "查看堆顶元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with top value.
        """
        name = params.get('name', 'heap')
        output_var = params.get('output_var', 'heap_peek')

        try:
            resolved_name = context.resolve_value(name)

            heap = context.get(f'_heap_{resolved_name}', [])

            if not heap:
                return ActionResult(
                    success=False,
                    message=f"堆 {resolved_name} 为空"
                )

            value = heap[0]
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"堆顶: {value}",
                data={
                    'name': resolved_name,
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
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'heap', 'output_var': 'heap_peek'}


class HeapSizeAction(BaseAction):
    """Get heap size."""
    action_type = "heap_size"
    display_name = "获取堆大小"
    description = "获取堆大小"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with heap size.
        """
        name = params.get('name', 'heap')
        output_var = params.get('output_var', 'heap_size')

        try:
            resolved_name = context.resolve_value(name)

            heap = context.get(f'_heap_{resolved_name}', [])

            context.set(output_var, len(heap))

            return ActionResult(
                success=True,
                message=f"堆大小: {len(heap)}",
                data={
                    'name': resolved_name,
                    'size': len(heap),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取堆大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'heap', 'output_var': 'heap_size'}
