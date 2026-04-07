"""Counter action module for RabAI AutoClick.

Provides counter operations:
- CounterCreateAction: Create counter
- CounterNextAction: Get next counter value
- CounterSetAction: Set counter value
- CounterResetAction: Reset counter
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CounterCreateAction(BaseAction):
    """Create counter."""
    action_type = "counter_create"
    display_name = "创建计数器"
    description = "创建计数器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, start, step.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', '')
        start = params.get('start', 0)
        step = params.get('step', 1)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_start = int(context.resolve_value(start))
            resolved_step = int(context.resolve_value(step))

            context.set(f'_counter_{resolved_name}', resolved_start)
            context.set(f'_counter_{resolved_name}_step', resolved_step)

            return ActionResult(
                success=True,
                message=f"计数器 {resolved_name} 创建: {resolved_start}",
                data={
                    'name': resolved_name,
                    'start': resolved_start,
                    'step': resolved_step
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'step': 1}


class CounterNextAction(BaseAction):
    """Get next counter value."""
    action_type = "counter_next"
    display_name = "下一个计数器"
    description = "获取下一个计数器值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute next.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with counter value.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'counter_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            if not context.exists(f'_counter_{resolved_name}'):
                return ActionResult(
                    success=False,
                    message=f"计数器 {resolved_name} 不存在"
                )

            current = context.get(f'_counter_{resolved_name}')
            step = context.get(f'_counter_{resolved_name}_step', 1)

            context.set(f'_counter_{resolved_name}', current + step)
            context.set(output_var, current)

            return ActionResult(
                success=True,
                message=f"计数器 {resolved_name} 下一个: {current}",
                data={
                    'name': resolved_name,
                    'value': current,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'counter_value'}


class CounterSetAction(BaseAction):
    """Set counter value."""
    action_type = "counter_set"
    display_name = "设置计数器"
    description = "设置计数器值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult indicating set.
        """
        name = params.get('name', '')
        value = params.get('value', 0)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = int(context.resolve_value(value))

            if not context.exists(f'_counter_{resolved_name}'):
                return ActionResult(
                    success=False,
                    message=f"计数器 {resolved_name} 不存在"
                )

            old_value = context.get(f'_counter_{resolved_name}')
            context.set(f'_counter_{resolved_name}', resolved_value)

            return ActionResult(
                success=True,
                message=f"计数器 {resolved_name} 设置: {old_value} -> {resolved_value}",
                data={
                    'name': resolved_name,
                    'old_value': old_value,
                    'new_value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class CounterResetAction(BaseAction):
    """Reset counter."""
    action_type = "counter_reset"
    display_name = "重置计数器"
    description = "重置计数器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reset.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating reset.
        """
        name = params.get('name', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            if not context.exists(f'_counter_{resolved_name}'):
                return ActionResult(
                    success=False,
                    message=f"计数器 {resolved_name} 不存在"
                )

            old_value = context.get(f'_counter_{resolved_name}')
            step = context.get(f'_counter_{resolved_name}_step', 1)
            context.set(f'_counter_{resolved_name}', 0)

            return ActionResult(
                success=True,
                message=f"计数器 {resolved_name} 重置: {old_value} -> 0",
                data={
                    'name': resolved_name,
                    'old_value': old_value,
                    'new_value': 0
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重置计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
