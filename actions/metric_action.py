"""Metric action module for RabAI AutoClick.

Provides metric operations:
- MetricIncrementAction: Increment metric
- MetricDecrementAction: Decrement metric
- MetricGetAction: Get metric
- MetricResetAction: Reset metric
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricIncrementAction(BaseAction):
    """Increment metric."""
    action_type = "metric_increment"
    display_name = "递增指标"
    description = "递增指标值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute increment.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult with new value.
        """
        name = params.get('name', '')
        value = params.get('value', 1)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = int(context.resolve_value(value))

            current = context.get(f'_metric_{resolved_name}', 0)
            new_value = current + resolved_value
            context.set(f'_metric_{resolved_name}', new_value)

            return ActionResult(
                success=True,
                message=f"指标 {resolved_name} 递增: {new_value}",
                data={
                    'name': resolved_name,
                    'old_value': current,
                    'new_value': new_value,
                    'increment': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"递增指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 1}


class MetricDecrementAction(BaseAction):
    """Decrement metric."""
    action_type = "metric_decrement"
    display_name = "递减指标"
    description = "递减指标值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decrement.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult with new value.
        """
        name = params.get('name', '')
        value = params.get('value', 1)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = int(context.resolve_value(value))

            current = context.get(f'_metric_{resolved_name}', 0)
            new_value = current - resolved_value
            context.set(f'_metric_{resolved_name}', new_value)

            return ActionResult(
                success=True,
                message=f"指标 {resolved_name} 递减: {new_value}",
                data={
                    'name': resolved_name,
                    'old_value': current,
                    'new_value': new_value,
                    'decrement': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"递减指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 1}


class MetricGetAction(BaseAction):
    """Get metric."""
    action_type = "metric_get"
    display_name = "获取指标"
    description = "获取指标值"

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
            ActionResult with metric value.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'metric_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            value = context.get(f'_metric_{resolved_name}', 0)
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取指标 {resolved_name}: {value}",
                data={
                    'name': resolved_name,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'metric_value'}


class MetricResetAction(BaseAction):
    """Reset metric."""
    action_type = "metric_reset"
    display_name = "重置指标"
    description = "重置指标值"

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
            old_value = context.get(f'_metric_{resolved_name}', 0)
            context.set(f'_metric_{resolved_name}', 0)

            return ActionResult(
                success=True,
                message=f"重置指标 {resolved_name}: {old_value} -> 0",
                data={
                    'name': resolved_name,
                    'old_value': old_value,
                    'new_value': 0
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重置指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
