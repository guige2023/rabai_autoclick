"""Aggregate6 action module for RabAI AutoClick.

Provides additional aggregate operations:
- AggregateMinAction: Get minimum value
- AggregateMaxAction: Get maximum value
- AggregateSumAction: Get sum of values
- AggregateAvgAction: Get average of values
- AggregateMedianAction: Get median of values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateMinAction(BaseAction):
    """Get minimum value."""
    action_type = "aggregate6_min"
    display_name = "最小值"
    description = "获取最小值"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with minimum value.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'min_value')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if not resolved:
                return ActionResult(
                    success=False,
                    message="最小值失败: 列表为空"
                )

            result = min(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小值: {result}",
                data={
                    'values': resolved,
                    'min': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'min_value'}


class AggregateMaxAction(BaseAction):
    """Get maximum value."""
    action_type = "aggregate6_max"
    display_name = "最大值"
    description = "获取最大值"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with maximum value.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'max_value')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if not resolved:
                return ActionResult(
                    success=False,
                    message="最大值失败: 列表为空"
                )

            result = max(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大值: {result}",
                data={
                    'values': resolved,
                    'max': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'max_value'}


class AggregateSumAction(BaseAction):
    """Get sum of values."""
    action_type = "aggregate6_sum"
    display_name = "求和"
    description = "计算总和"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with sum.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'sum_value')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            numeric_values = [float(v) for v in resolved]
            result = sum(numeric_values)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"总和: {result}",
                data={
                    'values': resolved,
                    'sum': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算总和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_value'}


class AggregateAvgAction(BaseAction):
    """Get average of values."""
    action_type = "aggregate6_avg"
    display_name = "平均值"
    description = "计算平均值"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute avg.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with average.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'avg_value')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if not resolved:
                return ActionResult(
                    success=False,
                    message="计算平均值失败: 列表为空"
                )

            numeric_values = [float(v) for v in resolved]
            result = sum(numeric_values) / len(numeric_values)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平均值: {result}",
                data={
                    'values': resolved,
                    'avg': result,
                    'count': len(resolved),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'avg_value'}


class AggregateMedianAction(BaseAction):
    """Get median of values."""
    action_type = "aggregate6_median"
    display_name = "中位数"
    description = "计算中位数"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute median.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with median.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'median_value')

        try:
            import statistics

            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if not resolved:
                return ActionResult(
                    success=False,
                    message="计算中位数失败: 列表为空"
                )

            numeric_values = [float(v) for v in resolved]
            result = statistics.median(numeric_values)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"中位数: {result}",
                data={
                    'values': resolved,
                    'median': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算中位数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'median_value'}