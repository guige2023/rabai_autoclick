"""Counter2 action module for RabAI AutoClick.

Provides additional counter operations:
- CounterIncrementAction: Increment counter
- CounterDecrementAction: Decrement counter
- CounterResetAction: Reset counter
- CounterGetAction: Get counter value
- CounterSetAction: Set counter value
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CounterIncrementAction(BaseAction):
    """Increment counter."""
    action_type = "counter2_increment"
    display_name = "计数器递增"
    description = "递增计数器值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute increment.

        Args:
            context: Execution context.
            params: Dict with name, amount, output_var.

        Returns:
            ActionResult with incremented value.
        """
        name = params.get('name', 'default')
        amount = params.get('amount', 1)
        output_var = params.get('output_var', 'counter_result')

        try:
            resolved_name = context.resolve_value(name)
            resolved_amount = int(context.resolve_value(amount)) if amount else 1

            current = context.get(f'counter_{resolved_name}', 0)
            new_value = int(current) + resolved_amount

            context.set(f'counter_{resolved_name}', new_value)
            context.set(output_var, new_value)

            return ActionResult(
                success=True,
                message=f"计数器递增: {resolved_name} = {new_value}",
                data={
                    'name': resolved_name,
                    'previous': current,
                    'incremented_by': resolved_amount,
                    'new_value': new_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数器递增失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'amount': 1, 'output_var': 'counter_result'}


class CounterDecrementAction(BaseAction):
    """Decrement counter."""
    action_type = "counter2_decrement"
    display_name = "计数器递减"
    description = "递减计数器值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decrement.

        Args:
            context: Execution context.
            params: Dict with name, amount, output_var.

        Returns:
            ActionResult with decremented value.
        """
        name = params.get('name', 'default')
        amount = params.get('amount', 1)
        output_var = params.get('output_var', 'counter_result')

        try:
            resolved_name = context.resolve_value(name)
            resolved_amount = int(context.resolve_value(amount)) if amount else 1

            current = context.get(f'counter_{resolved_name}', 0)
            new_value = int(current) - resolved_amount

            context.set(f'counter_{resolved_name}', new_value)
            context.set(output_var, new_value)

            return ActionResult(
                success=True,
                message=f"计数器递减: {resolved_name} = {new_value}",
                data={
                    'name': resolved_name,
                    'previous': current,
                    'decremented_by': resolved_amount,
                    'new_value': new_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数器递减失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'amount': 1, 'output_var': 'counter_result'}


class CounterResetAction(BaseAction):
    """Reset counter."""
    action_type = "counter2_reset"
    display_name = "重置计数器"
    description = "重置计数器为零"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reset.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with reset status.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'reset_result')

        try:
            resolved_name = context.resolve_value(name)

            current = context.get(f'counter_{resolved_name}', 0)
            context.set(f'counter_{resolved_name}', 0)

            context.set(output_var, 0)

            return ActionResult(
                success=True,
                message=f"计数器重置: {resolved_name}",
                data={
                    'name': resolved_name,
                    'previous': current,
                    'new_value': 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重置计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'reset_result'}


class CounterGetAction(BaseAction):
    """Get counter value."""
    action_type = "counter2_get"
    display_name = "获取计数器值"
    description = "获取计数器当前值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with counter value.
        """
        name = params.get('name', 'default')
        output_var = params.get('output_var', 'counter_value')

        try:
            resolved_name = context.resolve_value(name)

            value = context.get(f'counter_{resolved_name}', 0)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"计数器值: {resolved_name} = {value}",
                data={
                    'name': resolved_name,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取计数器值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'counter_value'}


class CounterSetAction(BaseAction):
    """Set counter value."""
    action_type = "counter2_set"
    display_name = "设置计数器值"
    description = "设置计数器值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with set status.
        """
        name = params.get('name', 'default')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'set_result')

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = int(context.resolve_value(value)) if value else 0

            context.set(f'counter_{resolved_name}', resolved_value)
            context.set(output_var, resolved_value)

            return ActionResult(
                success=True,
                message=f"计数器设置: {resolved_name} = {resolved_value}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置计数器值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'default', 'output_var': 'set_result'}