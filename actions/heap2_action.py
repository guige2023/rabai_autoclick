"""Heap2 action module for RabAI AutoClick.

Provides additional heap operations:
- HeapPushAction: Push element to heap
- HeapPopAction: Pop element from heap
- HeapPeekAction: Peek at heap top
- HeapReplaceAction: Replace top element
- HeapHeapifyAction: Convert list to heap
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HeapPushAction(BaseAction):
    """Push element to heap."""
    action_type = "heap2_push"
    display_name = "堆添加"
    description = "向堆中添加元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with heap, element, output_var.

        Returns:
            ActionResult with updated heap.
        """
        heap = params.get('heap', [])
        element = params.get('element', 0)
        output_var = params.get('output_var', 'updated_heap')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap)
            resolved_element = context.resolve_value(element)

            if not isinstance(resolved_heap, (list, tuple)):
                resolved_heap = [resolved_heap]

            result = list(resolved_heap)
            heapq.heappush(result, resolved_element)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"堆添加: {resolved_element}",
                data={
                    'heap': resolved_heap,
                    'element': resolved_element,
                    'updated_heap': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆添加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap', 'element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_heap'}


class HeapPopAction(BaseAction):
    """Pop element from heap."""
    action_type = "heap2_pop"
    display_name = "堆弹出"
    description: "从堆中弹出最小元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with heap, output_var.

        Returns:
            ActionResult with popped element and updated heap.
        """
        heap = params.get('heap', [])
        output_var = params.get('output_var', 'pop_result')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap)

            if not isinstance(resolved_heap, (list, tuple)):
                resolved_heap = [resolved_heap]

            if len(resolved_heap) == 0:
                return ActionResult(
                    success=False,
                    message="堆弹出失败: 堆为空"
                )

            result_heap = list(resolved_heap)
            popped = heapq.heappop(result_heap)

            context.set(output_var, {
                'element': popped,
                'heap': result_heap
            })

            return ActionResult(
                success=True,
                message=f"堆弹出: {popped}",
                data={
                    'popped_element': popped,
                    'updated_heap': result_heap,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆弹出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pop_result'}


class HeapPeekAction(BaseAction):
    """Peek at heap top."""
    action_type = "heap2_peek"
    display_name = "堆顶查看"
    description = "查看堆顶元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with heap, output_var.

        Returns:
            ActionResult with heap top element.
        """
        heap = params.get('heap', [])
        output_var = params.get('output_var', 'peek_result')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap)

            if not isinstance(resolved_heap, (list, tuple)):
                resolved_heap = [resolved_heap]

            if len(resolved_heap) == 0:
                return ActionResult(
                    success=False,
                    message="堆顶查看失败: 堆为空"
                )

            result = resolved_heap[0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"堆顶元素: {result}",
                data={
                    'top': result,
                    'heap': resolved_heap,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆顶查看失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class HeapReplaceAction(BaseAction):
    """Replace top element."""
    action_type = "heap2_replace"
    display_name = "堆顶替换"
    description = "替换堆顶元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace.

        Args:
            context: Execution context.
            params: Dict with heap, element, output_var.

        Returns:
            ActionResult with replaced element and updated heap.
        """
        heap = params.get('heap', [])
        element = params.get('element', 0)
        output_var = params.get('output_var', 'replace_result')

        try:
            import heapq

            resolved_heap = context.resolve_value(heap)
            resolved_element = context.resolve_value(element)

            if not isinstance(resolved_heap, (list, tuple)):
                resolved_heap = [resolved_heap]

            if len(resolved_heap) == 0:
                return ActionResult(
                    success=False,
                    message="堆顶替换失败: 堆为空"
                )

            result_heap = list(resolved_heap)
            replaced = heapq.heapreplace(result_heap, resolved_element)

            context.set(output_var, {
                'replaced': replaced,
                'heap': result_heap
            })

            return ActionResult(
                success=True,
                message=f"堆顶替换: {replaced} -> {resolved_element}",
                data={
                    'replaced_element': replaced,
                    'new_element': resolved_element,
                    'updated_heap': result_heap,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"堆顶替换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['heap', 'element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'replace_result'}


class HeapHeapifyAction(BaseAction):
    """Convert list to heap."""
    action_type = "heap2_heapify"
    display_name: "列表堆化"
    description = "将列表转换为堆"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute heapify.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with heapified list.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'heapified')

        try:
            import heapq

            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            heapq.heapify(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表堆化: {len(result)}个元素",
                data={
                    'original': resolved,
                    'heap': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表堆化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'heapified'}