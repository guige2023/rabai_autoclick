"""Stack2 action module for RabAI AutoClick.

Provides additional stack operations:
- StackPushAction: Push to stack
- StackPopAction: Pop from stack
- StackPeekAction: Peek at stack top
- StackSizeAction: Get stack size
- StackIsEmptyAction: Check if stack is empty
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StackPushAction(BaseAction):
    """Push to stack."""
    action_type = "stack2_push"
    display_name = "栈推入"
    description = "将元素推入栈"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with stack, item, output_var.

        Returns:
            ActionResult with updated stack.
        """
        stack = params.get('stack', [])
        item = params.get('item', None)
        output_var = params.get('output_var', 'updated_stack')

        try:
            resolved_stack = context.resolve_value(stack)
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_stack, (list, tuple)):
                resolved_stack = [resolved_stack]

            result = list(resolved_stack)
            result.append(resolved_item)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"栈推入: {resolved_item}",
                data={
                    'item': resolved_item,
                    'stack': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"栈推入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'stack': [], 'output_var': 'updated_stack'}


class StackPopAction(BaseAction):
    """Pop from stack."""
    action_type = "stack2_pop"
    display_name = "栈弹出"
    description = "从栈弹出元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with stack, output_var.

        Returns:
            ActionResult with popped element and updated stack.
        """
        stack = params.get('stack', [])
        output_var = params.get('output_var', 'pop_result')

        try:
            resolved_stack = context.resolve_value(stack)

            if not isinstance(resolved_stack, (list, tuple)):
                resolved_stack = [resolved_stack]

            if len(resolved_stack) == 0:
                return ActionResult(
                    success=False,
                    message="栈弹出失败: 栈为空"
                )

            result_stack = list(resolved_stack)
            popped = result_stack.pop()

            context.set(output_var, {
                'item': popped,
                'stack': result_stack
            })

            return ActionResult(
                success=True,
                message=f"栈弹出: {popped}",
                data={
                    'popped_item': popped,
                    'stack': result_stack,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"栈弹出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pop_result'}


class StackPeekAction(BaseAction):
    """Peek at stack top."""
    action_type = "stack2_peek"
    display_name = "查看栈顶"
    description = "查看栈顶元素"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with stack, output_var.

        Returns:
            ActionResult with stack top element.
        """
        stack = params.get('stack', [])
        output_var = params.get('output_var', 'peek_result')

        try:
            resolved_stack = context.resolve_value(stack)

            if not isinstance(resolved_stack, (list, tuple)):
                resolved_stack = [resolved_stack]

            if len(resolved_stack) == 0:
                return ActionResult(
                    success=False,
                    message="查看栈顶失败: 栈为空"
                )

            result = resolved_stack[-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"栈顶元素: {result}",
                data={
                    'top': result,
                    'stack': resolved_stack,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看栈顶失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class StackSizeAction(BaseAction):
    """Get stack size."""
    action_type = "stack2_size"
    display_name = "获取栈大小"
    description = "获取栈的大小"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with stack, output_var.

        Returns:
            ActionResult with stack size.
        """
        stack = params.get('stack', [])
        output_var = params.get('output_var', 'stack_size')

        try:
            resolved_stack = context.resolve_value(stack)

            if not isinstance(resolved_stack, (list, tuple)):
                resolved_stack = [resolved_stack]

            size = len(resolved_stack)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"栈大小: {size}",
                data={
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取栈大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stack_size'}


class StackIsEmptyAction(BaseAction):
    """Check if stack is empty."""
    action_type = "stack2_is_empty"
    display_name = "判断栈为空"
    description = "判断栈是否为空"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is empty.

        Args:
            context: Execution context.
            params: Dict with stack, output_var.

        Returns:
            ActionResult with is empty result.
        """
        stack = params.get('stack', [])
        output_var = params.get('output_var', 'is_empty')

        try:
            resolved_stack = context.resolve_value(stack)

            if not isinstance(resolved_stack, (list, tuple)):
                resolved_stack = [resolved_stack]

            is_empty = len(resolved_stack) == 0

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"栈为空: {'是' if is_empty else '否'}",
                data={
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断栈为空失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_empty'}