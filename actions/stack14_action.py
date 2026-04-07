"""Stack14 action module for RabAI AutoClick.

Provides additional stack operations:
- StackPushAction: Push onto stack
- StackPopAction: Pop from stack
- StackPeekAction: View top item
- StackSizeAction: Get stack size
- StackEmptyAction: Check if empty
- StackClearAction: Clear stack
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StackPushAction(BaseAction):
    """Push onto stack."""
    action_type = "stack14_push"
    display_name = "压栈"
    description = "压入堆栈"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with stack_name, value, output_var.

        Returns:
            ActionResult with push result.
        """
        stack_name = params.get('stack_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'push_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            if resolved_stack not in context._stacks:
                context._stacks[resolved_stack] = []

            context._stacks[resolved_stack].append(resolved_value)

            context.set(output_var, len(context._stacks[resolved_stack]))

            return ActionResult(
                success=True,
                message=f"压栈: {resolved_value} -> {resolved_stack} ({len(context._stacks[resolved_stack])}项)",
                data={
                    'stack': resolved_stack,
                    'value': resolved_value,
                    'size': len(context._stacks[resolved_stack]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'push_result'}


class StackPopAction(BaseAction):
    """Pop from stack."""
    action_type = "stack14_pop"
    display_name = "出栈"
    description = "弹出堆栈"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with stack_name, output_var.

        Returns:
            ActionResult with popped value.
        """
        stack_name = params.get('stack_name', 'default')
        output_var = params.get('output_var', 'pop_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            if resolved_stack not in context._stacks or not context._stacks[resolved_stack]:
                return ActionResult(
                    success=False,
                    message=f"堆栈为空: {resolved_stack}"
                )

            value = context._stacks[resolved_stack].pop()

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"出栈: {value} <- {resolved_stack} ({len(context._stacks[resolved_stack])}项)",
                data={
                    'stack': resolved_stack,
                    'value': value,
                    'size': len(context._stacks[resolved_stack]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"出栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pop_result'}


class StackPeekAction(BaseAction):
    """View top item."""
    action_type = "stack14_peek"
    display_name = "查看栈顶"
    description = "查看堆栈顶部项目"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with stack_name, output_var.

        Returns:
            ActionResult with top item.
        """
        stack_name = params.get('stack_name', 'default')
        output_var = params.get('output_var', 'peek_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            if resolved_stack not in context._stacks or not context._stacks[resolved_stack]:
                return ActionResult(
                    success=False,
                    message=f"堆栈为空: {resolved_stack}"
                )

            value = context._stacks[resolved_stack][-1]

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"查看栈顶: {value}",
                data={
                    'stack': resolved_stack,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看栈顶失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'peek_result'}


class StackSizeAction(BaseAction):
    """Get stack size."""
    action_type = "stack14_size"
    display_name = "栈大小"
    description = "获取堆栈大小"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with stack_name, output_var.

        Returns:
            ActionResult with stack size.
        """
        stack_name = params.get('stack_name', 'default')
        output_var = params.get('output_var', 'size_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            size = len(context._stacks.get(resolved_stack, []))

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"栈大小: {resolved_stack} = {size}",
                data={
                    'stack': resolved_stack,
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"栈大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'size_result'}


class StackEmptyAction(BaseAction):
    """Check if empty."""
    action_type = "stack14_empty"
    display_name = "栈是否为空"
    description = "检查堆栈是否为空"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute empty check.

        Args:
            context: Execution context.
            params: Dict with stack_name, output_var.

        Returns:
            ActionResult with empty status.
        """
        stack_name = params.get('stack_name', 'default')
        output_var = params.get('output_var', 'empty_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            is_empty = resolved_stack not in context._stacks or not context._stacks[resolved_stack]

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"栈是否为空: {resolved_stack} = {is_empty}",
                data={
                    'stack': resolved_stack,
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"栈是否为空检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'empty_result'}


class StackClearAction(BaseAction):
    """Clear stack."""
    action_type = "stack14_clear"
    display_name = "清空栈"
    description = "清空堆栈"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with stack_name, output_var.

        Returns:
            ActionResult with clear result.
        """
        stack_name = params.get('stack_name', 'default')
        output_var = params.get('output_var', 'clear_result')

        try:
            resolved_stack = context.resolve_value(stack_name) if stack_name else 'default'

            if not hasattr(context, '_stacks'):
                context._stacks = {}

            if resolved_stack in context._stacks:
                count = len(context._stacks[resolved_stack])
                context._stacks[resolved_stack] = []
            else:
                count = 0

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"清空栈: {resolved_stack} ({count}项)",
                data={
                    'stack': resolved_stack,
                    'cleared_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['stack_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}