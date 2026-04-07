"""Metric2 action module for RabAI AutoClick.

Provides additional metric operations:
- MetricRecordAction: Record metric value
- MetricGetAction: Get metric value
- MetricSumAction: Sum metrics
- MetricAvgAction: Average metrics
- MetricClearAction: Clear metrics
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricRecordAction(BaseAction):
    """Record metric value."""
    action_type = "metric2_record"
    display_name = "记录指标"
    description = "记录指标值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute record.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with record status.
        """
        name = params.get('name', '')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'record_status')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = float(context.resolve_value(value)) if value else 0

            metric_values = context.get(f'metric_values_{resolved_name}', [])
            if not isinstance(metric_values, list):
                metric_values = [metric_values]
            metric_values.append(resolved_value)

            context.set(f'metric_values_{resolved_name}', metric_values)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"指标记录: {resolved_name} = {resolved_value}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'count': len(metric_values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'record_status'}


class MetricGetAction(BaseAction):
    """Get metric value."""
    action_type = "metric2_get"
    display_name = "获取指标"
    description = "获取指标值"
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
            ActionResult with metric values.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'metric_values')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            values = context.get(f'metric_values_{resolved_name}', [])

            context.set(output_var, values)

            return ActionResult(
                success=True,
                message=f"获取指标: {resolved_name}",
                data={
                    'name': resolved_name,
                    'values': values,
                    'count': len(values),
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
        return {'output_var': 'metric_values'}


class MetricSumAction(BaseAction):
    """Sum metrics."""
    action_type = "metric2_sum"
    display_name = "指标求和"
    description = "对指标值求和"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with sum.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'metric_sum')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            values = context.get(f'metric_values_{resolved_name}', [])
            total = sum(values) if values else 0

            context.set(output_var, total)

            return ActionResult(
                success=True,
                message=f"指标求和: {resolved_name} = {total}",
                data={
                    'name': resolved_name,
                    'sum': total,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"指标求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'metric_sum'}


class MetricAvgAction(BaseAction):
    """Average metrics."""
    action_type = "metric2_avg"
    display_name = "指标平均值"
    description = "计算指标平均值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with average.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'metric_avg')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            values = context.get(f'metric_values_{resolved_name}', [])
            avg = sum(values) / len(values) if values else 0

            context.set(output_var, avg)

            return ActionResult(
                success=True,
                message=f"指标平均值: {resolved_name} = {avg:.2f}",
                data={
                    'name': resolved_name,
                    'average': avg,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算指标平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'metric_avg'}


class MetricClearAction(BaseAction):
    """Clear metrics."""
    action_type = "metric2_clear"
    display_name = "清除指标"
    description = "清除指标数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with clear status.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'clear_status')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            context.set(f'metric_values_{resolved_name}', [])
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"指标清除: {resolved_name}",
                data={
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除指标失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_status'}