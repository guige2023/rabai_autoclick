"""Stack action module for RabAI AutoClick.

Provides stack operations:
- StackCreateAction: Create stack
- StackPushAction: Push item
- StackPopAction: Pop item
- StackPeekAction: Peek stack
- StackSizeAction: Get stack size
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StackCreateAction(BaseAction):
    """Create stack."""
    action_type = "stack_create"
    display_name = "创建栈"
    description = "创建栈"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, maxsize.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', 'stack')
        maxsize = params.get('maxsize', 0)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_maxsize = int(context.resolve_value(maxsize))

            context.set(f'_stack_{resolved_name}_items', [])
            context.set(f'_stack_{resolved_name}_maxsize', resolved_maxsize)

            return ActionResult(
                success=True,
                message=f"栈 {resolved_name} 创建",
                data={
                    'name': resolved_name,
                    'maxsize': resolved_maxsize
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'stack', 'maxsize': 0}


class StackPushAction(BaseAction):
    """Push item."""
    action_type = "stack_push"
    display_name = "压栈"
    description = "将项目压入栈"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with name, item.

        Returns:
            ActionResult indicating pushed.
        """
        name = params.get('name', 'stack')
        item = params.get('item', None)

        try:
            resolved_name = context.resolve_value(name)
            resolved_item = context.resolve_value(item)

            items = context.get(f'_stack_{resolved_name}_items', [])
            maxsize = context.get(f'_stack_{resolved_name}_maxsize', 0)

            if maxsize > 0 and len(items) >= maxsize:
                return ActionResult(
                    success=False,
                    message=f"栈 {resolved_name} 已满"
                )

            items.append(resolved_item)
            context.set(f'_stack_{resolved_name}_items', items)

            return ActionResult(
                success=True,
                message=f"压栈: {len(items)} 项",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'size': len(items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'stack'}


class StackPopAction(BaseAction):
    """Pop item."""
    action_type = "stack_pop"
    display_name = "弹栈"
    description = "从栈弹出项目"

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
            ActionResult with popped item.
        """
        name = params.get('name', 'stack')
        output_var = params.get('output_var', 'popped_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_stack_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"栈 {resolved_name} 为空"
                )

            item = items.pop()
            context.set(f'_stack_{resolved_name}_items', items)
            context.set(output_var, item)

            return ActionResult(
                success=True,
                message=f"弹栈: {len(items)} 项剩余",
                data={
                    'name': resolved_name,
                    'item': item,
                    'remaining': len(items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"弹栈失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'stack', 'output_var': 'popped_item'}


class StackPeekAction(BaseAction):
    """Peek stack."""
    action_type = "stack_peek"
    display_name = "查看栈顶"
    description = "查看栈顶项目"

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
            ActionResult with top item.
        """
        name = params.get('name', 'stack')
        output_var = params.get('output_var', 'peek_item')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_stack_{resolved_name}_items', [])

            if not items:
                return ActionResult(
                    success=False,
                    message=f"栈 {resolved_name} 为空"
                )

            item = items[-1]
            context.set(output_var, item)

            return ActionResult(
                success=True,
                message=f"查看栈顶",
                data={
                    'name': resolved_name,
                    'item': item,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查看栈顶失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'stack', 'output_var': 'peek_item'}


class StackSizeAction(BaseAction):
    """Get stack size."""
    action_type = "stack_size"
    display_name = "获取栈大小"
    description = "获取栈大小"

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
            ActionResult with stack size.
        """
        name = params.get('name', 'stack')
        output_var = params.get('output_var', 'stack_size')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_stack_{resolved_name}_items', [])
            maxsize = context.get(f'_stack_{resolved_name}_maxsize', 0)

            context.set(output_var, len(items))

            return ActionResult(
                success=True,
                message=f"栈大小: {len(items)}/{maxsize if maxsize > 0 else '无限制'}",
                data={
                    'name': resolved_name,
                    'size': len(items),
                    'maxsize': maxsize,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取栈大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'stack', 'output_var': 'stack_size'}
